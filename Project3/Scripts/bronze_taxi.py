from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/home/jovyan/project/data/")

df = df.withColumn("trip_id", monotonically_increasing_id())

df.write.mode("append").saveAsTable("lakehouse.taxi.bronze_trips")

print("Taxi Bronze done")
