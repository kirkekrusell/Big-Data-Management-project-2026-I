FROM apache/airflow:2.10.2

USER root
RUN apt-get update && apt-get install -y --no-install-recommends gcc g++ && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
# Paigaldame kõik paketid otse pildi sisse (lisatud open-clip-torch)
RUN pip install --no-cache-dir \
    minio \
    "psycopg[binary]>=3.2" \
    "pgvector>=0.3" \
    "pillow>=10.0" \
    "requests>=2.32" \
    "boto3>=1.35" \
    "datasets<3" \
    "huggingface_hub<0.24" \
    "fsspec<2024.4" \
    "torch>=2.4" \
    "sentence-transformers>=3.0" \
    "open-clip-torch>=2.30"