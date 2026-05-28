"""Spark + Iceberg adapter, talking through the Hive Metastore catalog to MinIO.

This is the only file in the library that imports pyspark — that's the
architectural seam. See test_architecture.py for the lint that enforces it.

SparkSession is cached at module scope (see §6.11.5 of the build spec): cold
start is 30-45s, so the first Airflow task in a worker process pays the cost
and every later task reuses the session.

When the driver is the airflow-scheduler container (not the spark cluster),
it still needs the Iceberg / Hadoop-AWS jars and the catalog config — we wire
that here through spark.jars.packages + explicit .config() calls so airflow
doesn't need a parallel spark-defaults.conf.
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

from internal_etl_package.engines.base import TableEngine


_SPARK_MASTER = os.environ.get("SPARK_MASTER_URL", "spark://spark:7077")
_APP_NAME = "maria-reliability-lab"

_HMS_URI = os.environ.get("HIVE_METASTORE_URI", "thrift://catalog:9083")
_WAREHOUSE_URI = os.environ.get("WAREHOUSE_URI", "s3a://warehouse/")
_S3_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://storage:9000")
_S3_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
_S3_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "password")

_ICEBERG_VERSION = "1.5.2"
_HADOOP_AWS_VERSION = "3.3.4"
_PACKAGES = ",".join(
    [
        f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{_ICEBERG_VERSION}",
        f"org.apache.hadoop:hadoop-aws:{_HADOOP_AWS_VERSION}",
    ]
)

_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


_spark_session: SparkSession | None = None


def _get_or_create_spark() -> SparkSession:
    """Return the module-cached SparkSession, creating it on first call."""
    global _spark_session
    if _spark_session is None:
        import os as _os
        import sys as _sys

        # Keep py4j happy across the Airflow worker subprocess boundary.
        _os.environ.setdefault("PYSPARK_PIN_THREAD", "true")
        _os.environ.setdefault("PYSPARK_DRIVER_PYTHON", _sys.executable)
        _os.environ.setdefault("PYSPARK_PYTHON", _sys.executable)

        _spark_session = (
            SparkSession.builder.appName(_APP_NAME)
            .master(_SPARK_MASTER)
            .config("spark.jars.packages", _PACKAGES)
            .config("spark.sql.extensions", _EXTENSIONS)
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg.type", "hive")
            .config("spark.sql.catalog.iceberg.uri", _HMS_URI)
            .config("spark.sql.catalog.iceberg.warehouse", _WAREHOUSE_URI)
            .config("spark.sql.defaultCatalog", "iceberg")
            .config("spark.hadoop.fs.s3a.endpoint", _S3_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", _S3_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", _S3_SECRET)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", "1g")
            .getOrCreate()
        )
    return _spark_session


class SparkIcebergEngine(TableEngine):
    """Production TableEngine backed by Spark + Iceberg + HMS."""

    def __init__(self, spark: SparkSession | None = None):
        self._spark = spark if spark is not None else _get_or_create_spark()

    def current_snapshot_id(self, table: str) -> int:
        row = self._spark.sql(
            f"SELECT snapshot_id FROM iceberg.{table}.snapshots "
            "ORDER BY committed_at DESC LIMIT 1"
        ).first()
        return int(row["snapshot_id"])

    def rollback_to_snapshot(self, table: str, snapshot_id: int) -> None:
        self._spark.sql(
            f"CALL iceberg.system.rollback_to_snapshot('{table}', {int(snapshot_id)})"
        )

    def merge(self, table: str, source_path: str, primary_key: str) -> int:
        schema = table.split(".", 1)[0]
        self._spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{schema}")

        (
            self._spark.read.format("csv")
            .option("header", True)
            .option("inferSchema", True)
            .load(source_path)
            .createOrReplaceTempView("staging")
        )

        # First-run safety net: MERGE INTO needs the target table to exist.
        # CTAS-with-LIMIT-0 stamps an empty Iceberg table with the staging
        # schema, then the MERGE adds every row. Idempotent on subsequent runs.
        self._spark.sql(
            f"CREATE TABLE IF NOT EXISTS iceberg.{table} "
            "USING iceberg AS SELECT * FROM staging LIMIT 0"
        )

        self._spark.sql(
            f"""
            MERGE INTO iceberg.{table} AS target
            USING staging AS source
            ON target.{primary_key} = source.{primary_key}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
        return self.current_snapshot_id(table)

    def count_duplicates(self, table: str, primary_key: str) -> int:
        row = self._spark.sql(
            f"""
            SELECT COUNT(*) AS dup_keys FROM (
                SELECT {primary_key}
                FROM iceberg.{table}
                GROUP BY {primary_key}
                HAVING COUNT(*) > 1
            )
            """
        ).first()
        return int(row["dup_keys"])

    def row_count(self, table: str) -> int:
        row = self._spark.sql(
            f"SELECT COUNT(*) AS n FROM iceberg.{table}"
        ).first()
        return int(row["n"])

    def null_rate(self, table: str, column: str) -> float:
        row = self._spark.sql(
            f"""
            SELECT
              SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) * 1.0
              / NULLIF(COUNT(*), 0) AS rate
            FROM iceberg.{table}
            """
        ).first()
        rate = row["rate"]
        return float(rate) if rate is not None else 0.0
