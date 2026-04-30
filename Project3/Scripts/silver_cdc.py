from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

spark = SparkSession.builder.getOrCreate()

df = spark.read.table("lakehouse.cdc.bronze_customers")

w = Window.partitionBy("entity_id").orderBy(col("ts_ms").desc())

deduped = df.withColumn("rn", row_number().over(w)) \
            .filter("rn = 1") \
            .drop("rn")

deduped.write.mode("overwrite").saveAsTable("lakehouse.cdc.silver_customers")

print("Silver CDC done")
