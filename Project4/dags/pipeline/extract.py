import requests
from pipeline.utils import get_db, get_s3, MINIO_BUCKET, sha256_bytes

OLLAMA_URL = "http://ollama:11434"
MODEL = "qwen2.5:3b"
PROMPT_VERSION = "v1"


def run_extract(**context):
    run_id = context["ti"].xcom_pull(key="run_id")
    conn = get_db()

    with conn, conn.cursor() as cur:
        cur.execute("SELECT screen_id, extraction_payload FROM screens_metadata WHERE run_id = %s", (run_id,))
        rows = cur.fetchall()

        for sid, payload in rows:
            text = payload.get("text_rep", "")

            prompt = f"""
            You are an extractor. Return JSON with fields:
            title, elements, confidence.
            Text: {text}
            """

            resp = requests.post(
                f"{OLLAMA_URL}/api/generate",
                json={"model": MODEL, "prompt": prompt},
                timeout=60,
            ).json()

            output = resp["response"]
            fingerprint = sha256_bytes(text.encode())

            cur.execute(
                """
                UPDATE screens_metadata
                SET extraction_payload = jsonb_set(extraction_payload, '{llm}', %s::jsonb),
                    prompt_version = %s,
                    confidence = (extraction_payload->>'confidence')::float,
                    updated_at = NOW(),
                    run_id = %s,
                    source_fingerprint = %s
                WHERE screen_id = %s
                """,
                (output, PROMPT_VERSION, run_id, fingerprint, sid),
            )
