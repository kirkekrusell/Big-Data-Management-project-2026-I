from pyspark.sql import SparkSession
from pyspark.sql import functions as F

S3_ENDPOINT = "http://minio:9000"

spark = (
    SparkSession.builder
    .appName("Taxi-Silver")
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

bronze = spark.table("lakehouse.taxi.bronze_trips")

clean = (
    bronze
    .withColumn("pickup_ts", F.to_timestamp("pickup_datetime"))
    .withColumn("dropoff_ts", F.to_timestamp("dropoff_datetime"))
    .withColumn("passenger_count", F.col("passenger_count").cast("int"))
    .withColumn("trip_distance", F.col("trip_distance").cast("double"))
    .withColumn("fare_amount", F.col("fare_amount").cast("double"))
)

clean = clean.filter(
    (F.col("pickup_ts").isNotNull()) &
    (F.col("dropoff_ts").isNotNull()) &
    (F.col("pickup_ts") < F.col("dropoff_ts")) &
    (F.col("trip_distance") > 0) &
    (F.col("fare_amount") > 0) &
    (F.col("passenger_count") > 0) &
    (F.col("passenger_count") <= 6)
)

zones = spark.table("lakehouse.taxi.zones")

silver = (
    clean
    .join(
        zones.withColumnRenamed("zone_id", "pickup_zone_id_dim"),
        clean["pickup_zone_id"] == F.col("pickup_zone_id_dim"),
        "left"
    )
    .withColumnRenamed("zone_name", "pickup_zone_name")
    .drop("pickup_zone_id_dim")
    .join(
        zones.withColumnRenamed("zone_id", "dropoff_zone_id_dim"),
        clean["dropoff_zone_id"] == F.col("dropoff_zone_id_dim"),
        "left"
    )
    .withColumnRenamed("zone_name", "dropoff_zone_name")
    .drop("dropoff_zone_id_dim")
    .withColumn("pickup_date", F.to_date("pickup_ts"))
)

table = "lakehouse.taxi.silver_trips"

if not spark.catalog.tableExists(table):
    (
        silver
        .writeTo(table)
        .partitionedBy("pickup_date")
        .create()
    )
else:
    (
        silver
        .writeTo(table)
        .overwritePartitions()
    )

spark.stop()
