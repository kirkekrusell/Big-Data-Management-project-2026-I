import os
import psycopg2
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

DB_USER = os.getenv("PG_USER", "cdc_user")
DB_PASS = os.getenv("PG_PASSWORD", "cdc_pass")

PG_CONN = {
    "host": "postgres",
    "port": 5432,
    "dbname": "sourcedb",
    "user": DB_USER,
    "password": DB_PASS,
}

def pg_count(table):
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    cur.execute(f"SELECT count(*) FROM {table}")
    n = cur.fetchone()[0]
    cur.close()
    conn.close()
    return n

S3_ENDPOINT = "http://minio:9000"

spark = (
    SparkSession.builder
    .appName("CDC-Validate")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "rest")
    .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181")
    .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.lakehouse.s3.endpoint", S3_ENDPOINT)
    .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
    .config("spark.sql.defaultCatalog", "lakehouse")
    .getOrCreate()
)

silver_customers = spark.table("lakehouse.cdc.silver_customers").count()
silver_drivers = spark.table("lakehouse.cdc.silver_drivers").count()

pg_customers = pg_count("public.customers")
pg_drivers = pg_count("public.drivers")

print(f"Postgres customers: {pg_customers}, Silver customers: {silver_customers}")
print(f"Postgres drivers:   {pg_drivers}, Silver drivers:   {silver_drivers}")

if pg_customers != silver_customers or pg_drivers != silver_drivers:
    raise RuntimeError("CDC validation failed: row counts do not match")

spark.stop()
