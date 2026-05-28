#!/usr/bin/env python3
"""Reset the workshop to T10's known-good state.

Restores source/ from seed/, truncates the ledger, drops the warehouse
tables (the next merge will recreate them via CTAS in spark_engine), and
clears the alerts/ directory.

Safe to re-run. Returns nonzero if anything went wrong.
"""
from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import psycopg2

from chaos._voice import alerts_dir, data_root, error, info, maria_says


TABLES = ("users", "orders", "events")


def restore_source_from_seed(root: Path) -> None:
    """Wipe source/<table>/ and copy seed/<table>.csv back into it."""
    for name in TABLES:
        src_dir = root / "source" / name
        seed_file = root / "seed" / f"{name}.csv"

        if src_dir.exists():
            shutil.rmtree(src_dir)
        src_dir.mkdir(parents=True, exist_ok=True)

        if seed_file.exists():
            shutil.copy(seed_file, src_dir / f"{name}.csv")
            info(f"restored source/{name}/{name}.csv from seed")
        else:
            info(f"no seed file for {name} — skipped")


def truncate_ledger(dsn: str) -> None:
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE snapshot_ledger")
    info("truncated snapshot_ledger")


def clear_alerts(directory: Path) -> None:
    if not directory.exists():
        directory.mkdir(parents=True, exist_ok=True)
        return
    for f in directory.iterdir():
        if f.is_file():
            f.unlink()
    info(f"cleared {directory}")


def drop_warehouse_tables() -> None:
    """Drop the Iceberg tables in the Hive Metastore catalog so the next
    merge starts clean. Imported lazily so unit tests don't need pyspark.
    """
    from internal_etl_package.engines.spark_engine import _get_or_create_spark

    spark = _get_or_create_spark()
    for name in TABLES:
        spark.sql(f"DROP TABLE IF EXISTS iceberg.prd.{name}")
        info(f"dropped iceberg.prd.{name}")


def _ledger_dsn() -> str:
    user = os.environ.get("POSTGRES_USER", "airflow")
    password = os.environ.get("POSTGRES_PASSWORD", "airflow")
    db = os.environ.get("POSTGRES_DB", "airflow")
    host = os.environ.get("POSTGRES_HOST", "postgres")
    return f"postgresql://{user}:{password}@{host}/{db}"


def main(*, skip_spark: bool = False) -> int:
    maria_says("Resetting the lab to a clean state. Snapshots, alerts, source files — all back to T10.")

    try:
        restore_source_from_seed(data_root())
        try:
            truncate_ledger(_ledger_dsn())
        except psycopg2.errors.UndefinedTable:
            info("snapshot_ledger does not exist yet — skipping truncate")
        clear_alerts(alerts_dir())
        if not skip_spark and os.environ.get("CHAOS_SKIP_SPARK") != "1":
            drop_warehouse_tables()
        else:
            info("skipped Spark warehouse cleanup (env)")
    except Exception as exc:
        error(f"reset failed: {exc}")
        return 1

    print()
    maria_says("Lab is clean. Trigger the DAG to see the happy path.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
