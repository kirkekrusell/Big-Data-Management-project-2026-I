from pyspark.sql import SparkSession
from pyspark.sql import functions as F

S3_ENDPOINT = "http://minio:9000"

spark = (
    SparkSession.builder
    .appName("Taxi-Gold")
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

silver = spark.table("lakehouse.taxi.silver_trips")

gold = (
    silver
    .withColumn("pickup_hour", F.date_trunc("hour", "pickup_ts"))
    .groupBy("pickup_zone_name", "pickup_hour")
    .agg(
        F.count("*").alias("trips_count"),
        F.avg("fare_amount").alias("avg_fare_amount"),
        F.avg("trip_distance").alias("avg_trip_distance"),
    )
)

table = "lakehouse.taxi.gold_trips_by_zone_hour"

if not spark.catalog.tableExists(table):
    (
        gold
        .writeTo(table)
        .partitionedBy("pickup_hour")
        .create()
    )
else:
    (
        gold
        .writeTo(table)
        .overwritePartitions()
    )

spark.stop()

