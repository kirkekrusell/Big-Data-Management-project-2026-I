import hashlib
import psycopg
from pgvector.psycopg import register_vector
import boto3
import os

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://rico:rico@postgres:5432/rico")
MINIO_URL = os.getenv("MINIO_URL", "http://minio:9000")
MINIO_KEY = os.getenv("MINIO_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET", "minioadmin")
MINIO_BUCKET = "rico-raw"


def get_db():
    conn = psycopg.connect(
        "dbname=rico user=rico password=rico host=postgres port=5432",
        autocommit=True # Ülioluline laienduste loomiseks ja registreerimiseks
    )
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    register_vector(conn)
    return conn


def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_URL,
        aws_access_key_id=MINIO_KEY,
        aws_secret_access_key=MINIO_SECRET,
        region_name="us-east-1",
    )


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
