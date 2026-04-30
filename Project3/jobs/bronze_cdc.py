from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

def main():
    spark = SparkSession.builder \
        .appName("bronze_cdc") \
        .getOrCreate()

    schema = StructType([
        StructField("payload", StructType([
            StructField("op", StringType()),
            StructField("before", StringType()),
            StructField("after", StringType()),
            StructField("ts_ms", LongType())
        ]))
    ])

    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "dbserver1.public.customers") \
        .load()

    parsed = df.select(from_json(col("value").cast("string"), schema).alias("json"))

    bronze = parsed.select(
        col("json.payload.op").alias("op"),
        col("json.payload.before").alias("before"),
        col("json.payload.after").alias("after"),
        col("json.payload.ts_ms").alias("ts_ms")
    )

    bronze.write \
        .format("iceberg") \
        .mode("append") \
        .save("lakehouse.cdc.bronze_customers")

    spark.stop()

if __name__ == "__main__":
    main()
