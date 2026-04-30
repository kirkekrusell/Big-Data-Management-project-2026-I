from pyspark.sql import SparkSession
from pyspark.sql.functions import date_trunc, col, avg, count

def main():
    spark = SparkSession.builder \
        .appName("gold_taxi") \
        .getOrCreate()

    df = spark.read.format("iceberg").load("lakehouse.taxi.silver_trips")

    gold = df.groupBy(
        "PULocationID",
        date_trunc("hour", col("tpep_pickup_datetime")).alias("hour")
    ).agg(
        count("*").alias("trips"),
        avg("fare_amount").alias("avg_fare")
    )

    gold.write \
        .format("iceberg") \
        .mode("overwrite") \
        .save("lakehouse.taxi.gold_trips")

    spark.stop()

if __name__ == "__main__":
    main()
