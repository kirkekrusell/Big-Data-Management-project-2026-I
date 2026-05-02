from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

S3_ENDPOINT = "http://minio:9000"

spark = (
    SparkSession.builder
    .appName("Taxi-Bronze")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "rest")
    .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181")
    .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.lakehouse.s3.endpoint", S3_ENDPOINT)
    .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
    .config("spark.sql.defaultCatalog", "lakehouse")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.taxi")

df = spark.read.parquet("/data/taxi/*.parquet")

bronze_df = df.withColumn("trip_id", monotonically_increasing_id())

table = "lakehouse.taxi.bronze_trips"

if not spark.catalog.tableExists(table):
    bronze_df.writeTo(table).create()
else:
    bronze_df.writeTo(table).append()

spark.stop()
