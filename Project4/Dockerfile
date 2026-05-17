FROM apache/airflow:2.10.2

USER root
RUN apt-get update && apt-get install -y --no-install-recommends gcc g++ && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
# Paigaldame kõik paketid otse pildi sisse (lisatud open-clip-torch)
RUN pip install --no-cache-dir \
    minio \
    "psycopg[binary]" \
    pgvector \
    pillow \
    requests \
    boto3 \
    datasets \
    transformers \
    sentence-transformers \
    open-clip-torch>=2.30