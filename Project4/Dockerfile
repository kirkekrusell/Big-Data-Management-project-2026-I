FROM apache/airflow:2.10.2

USER root
RUN apt-get update && apt-get install -y --no-install-recommends gcc g++ && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
# Paigaldame kõik kerged paketid otse pildi sisse
RUN pip install --no-cache-dir \
    minio \
    "psycopg[binary]" \
    pgvector \
    pillow \
    requests \
    boto3