from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

spark = (
    SparkSession.builder.appName("silver_cdc")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "rest")
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181")
    .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse/")
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .getOrCreate()
)

bronze = spark.read.format("iceberg").load("iceberg.cdc.bronze_customers")

# Deduplicate: keep latest event per primary key
w = Window.partitionBy(col("after.id")).orderBy(col("ts_ms").desc())

latest = (
    bronze.withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")
)

# Prepare MERGE target
silver_table = "iceberg.cdc.silver_customers"

# Create table if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver_table} (
    id INT,
    name STRING,
    email STRING
)
USING iceberg
""")

# MERGE logic
latest.createOrReplaceTempView("updates")

spark.sql(f"""
MERGE INTO {silver_table} t
USING updates u
ON t.id = u.after.id

WHEN MATCHED AND u.op = 'd' THEN DELETE

WHEN MATCHED AND u.op IN ('u', 'c', 'r') THEN UPDATE SET
    id = u.after.id,
    name = u.after.name,
    email = u.after.email

WHEN NOT MATCHED AND u.op IN ('u', 'c', 'r') THEN INSERT *
""")

spark.stop()
