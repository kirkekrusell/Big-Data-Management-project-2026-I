from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

def main():
    spark = SparkSession.builder.getOrCreate()

    bronze = spark.read.table("lakehouse.cdc.bronze")

    # oletame et sul on id + ts_ms olemas JSON-is
    df = bronze.withColumn("id", col("payload").getItem("id")) \
               .withColumn("ts_ms", col("timestamp"))

    w = Window.partitionBy("id").orderBy(col("ts_ms").desc())

    latest = df.withColumn("rn", row_number().over(w)) \
               .filter("rn = 1") \
               .drop("rn")

    latest.createOrReplaceTempView("updates")

    spark.sql("""
    MERGE INTO lakehouse.cdc.silver t
    USING updates s
    ON t.id = s.id
    WHEN MATCHED AND s.payload LIKE '%"op":"d"%' THEN DELETE
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

if __name__ == "__main__":
    main()
