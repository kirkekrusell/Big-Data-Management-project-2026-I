from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("silver_taxi") \
        .getOrCreate()

    df = spark.read.format("iceberg").load("lakehouse.taxi.bronze_trips")

    # cleaning rules for NYC TLC dataset
    df_clean = df \
        .filter(col("fare_amount") > 0) \
        .filter(col("trip_distance") > 0) \
        .filter(col("passenger_count") > 0)

    # type safety (important for Iceberg stability)
    df_clean = df_clean \
        .withColumn("fare_amount", col("fare_amount").cast("double")) \
        .withColumn("trip_distance", col("trip_distance").cast("double"))

    df_clean.write \
        .format("iceberg") \
        .mode("overwrite") \
        .save("lakehouse.taxi.silver_trips")

    spark.stop()

if __name__ == "__main__":
    main()
