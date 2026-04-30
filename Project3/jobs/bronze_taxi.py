from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

def main():
    spark = SparkSession.builder \
        .appName("bronze_taxi") \
        .getOrCreate()

    df = spark.read.parquet("data/")

    # required synthetic key for later join
    df = df.withColumn("trip_id", monotonically_increasing_id())

    df.write \
        .format("iceberg") \
        .mode("append") \
        .save("lakehouse.taxi.bronze_trips")

    spark.stop()

if __name__ == "__main__":
    main()
