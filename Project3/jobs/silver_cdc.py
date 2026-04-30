from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

def main():
    spark = SparkSession.builder \
        .appName("silver_cdc") \
        .getOrCreate()

    bronze = spark.read.format("iceberg").load("lakehouse.cdc.bronze_customers")

    window = Window.partitionBy("after.id").orderBy(col("ts_ms").desc())

    dedup = bronze.withColumn("rn", row_number().over(window)) \
                  .filter(col("rn") == 1)

    # extract current state
    current = dedup.filter(col("op").isin("c", "u", "r")) \
                   .selectExpr("after.*")

    # DELETE handling
    deletes = dedup.filter(col("op") == "d") \
                   .select(col("before.id").alias("id"))

    # overwrite strategy (simpler than MERGE for class project)
    current.write \
        .format("iceberg") \
        .mode("overwrite") \
        .save("lakehouse.cdc.silver_customers")

    spark.stop()

if __name__ == "__main__":
    main()
