from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, schema_of_json

# Spark session with Iceberg + Kafka
spark = (
    SparkSession.builder.appName("bronze_cdc")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "rest")
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181")
    .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse/")
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .getOrCreate()
)

topic = "dbserver1.public.customers"

df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .load()
)

# Debezium messages are JSON strings
json_df = df.selectExpr("CAST(value AS STRING) as json_str")

# Infer schema dynamically
schema = schema_of_json(json_df.select("json_str").first()[0])

parsed = json_df.select(from_json(col("json_str"), schema).alias("data"))

bronze = parsed.select(
    col("data.payload.op").alias("op"),
    col("data.payload.before").alias("before"),
    col("data.payload.after").alias("after"),
    col("data.payload.ts_ms").alias("ts_ms"),
    col("data.payload.source.table").alias("table"),
)

bronze.writeTo("iceberg.cdc.bronze_customers").append()

spark.stop()
