from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "dbserver1.public.customers,dbserver1.public.drivers") \
        .load()

    bronze = df.selectExpr(
        "CAST(value AS STRING) as payload",
        "topic",
        "partition",
        "offset",
        "timestamp"
    )

    bronze.writeTo("lakehouse.cdc.bronze").append()

if __name__ == "__main__":
    main()
