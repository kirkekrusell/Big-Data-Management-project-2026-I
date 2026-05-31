from io import BytesIO
from pipeline.utils import get_db, get_s3, MINIO_BUCKET, sha256_bytes

def run_ingest(limit, **context):
    from datasets import load_dataset
    from PIL import Image

    run_id = context["ti"].xcom_pull(key="run_id")
    s3 = get_s3()

    # 1. Garanteerime MinIO bucketi olemasolu (S3-ga timeouti muret pole)
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
    except Exception:
        s3.create_bucket(Bucket=MINIO_BUCKET)

    # 2. Avame andmebaasi ühenduse HETKEKS, et luua tabel, ja sulgeme kohe
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS screens_metadata (
                    screen_id INT PRIMARY KEY,
                    app_package TEXT,
                    category TEXT,
                    png_path TEXT,
                    hierarchy_json_path TEXT,
                    run_id TEXT,
                    source_fingerprint TEXT,
                    extraction_payload JSONB,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
            """)

    # 3. Laadime andmed Hugging Face'ist (see võtab aega, DB ühendus on praegu kinni)
    ds = load_dataset("rootsautomation/RICO-Screen2Words", split="train", streaming=True)

    chosen = []
    for row in ds:
        chosen.append(row)
        if len(chosen) == limit:
            break

    # 4. Avame uue, VÄRSKE andmebaasi ühenduse salvestamise ajaks
    with get_db() as conn:
        with conn.cursor() as cur:
            for row in chosen:
                sid = int(row["screenId"])

                # PNG fail MinIO-sse
                png_buf = BytesIO()
                row["image"].save(png_buf, format="PNG")
                png_bytes = png_buf.getvalue()
                png_key = f"screens/{sid}.png"
                s3.put_object(Bucket=MINIO_BUCKET, Key=png_key, Body=png_bytes)

                # JSON fail MinIO-sse
                json_bytes = row["view_hierarchy"].encode("utf-8")
                json_key = f"screens/{sid}.json"
                s3.put_object(Bucket=MINIO_BUCKET, Key=json_key, Body=json_bytes)

                fingerprint = sha256_bytes(png_bytes)

                cur.execute(
                    """
                    INSERT INTO screens_metadata
                    (screen_id, app_package, category, png_path, hierarchy_json_path, run_id, source_fingerprint)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (screen_id) DO NOTHING
                    """,
                    (sid, row.get("package_name", "unknown"), row.get("category", "unknown"), png_key, json_key, run_id, fingerprint)
                )