from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

df = spark.read.table("lakehouse.taxi.bronze_trips")

df = df.filter(
    (col("fare_amount") > 0) &
    (col("trip_distance") > 0)
)

df.write.mode("overwrite").saveAsTable("lakehouse.taxi.silver_trips")

print("Taxi Silver done")
