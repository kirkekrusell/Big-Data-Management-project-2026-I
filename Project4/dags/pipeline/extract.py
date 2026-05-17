import requests
import json
from pipeline.utils import get_db, get_s3, MINIO_BUCKET, sha256_bytes

OLLAMA_URL = "http://ollama:11434"
MODEL = "qwen2.5:3b"
PROMPT_VERSION = "v1"

def run_extract(**context):
    run_id = context["ti"].xcom_pull(key="run_id")

    # Automaatne mudeli kontroll: kui mudelit pole, laeb Ollama selle ise taustal alla
    try:
        requests.post(f"{OLLAMA_URL}/api/pull", json={"name": MODEL, "stream": False}, timeout=300)
    except Exception as e:
        print(f"Mudeli kontrolli hoiatus: {e}")

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT screen_id, extraction_payload FROM screens_metadata WHERE run_id = %s", 
                (run_id,)
            )
            rows = cur.fetchall()

    for sid, payload in rows:
        text = payload.get("texts", "") if payload else ""

        prompt = f"You are an extractor. Return JSON with fields: title, elements, confidence. Text: {text}"

        res = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": MODEL, "prompt": prompt, "stream": False, "format": "json"},
            timeout=60,
        )
        
        if res.status_code != 200:
            raise RuntimeError(f"Ollama API viga: {res.text}")
            
        resp = res.json()
        output_raw = resp["response"]
        fingerprint = sha256_bytes(text.encode())

        try:
            parsed_output = json.loads(output_raw)
            confidence = float(parsed_output.get("confidence", 0.0))
        except Exception:
            confidence = 0.0

        # Pakime kõik andmed ühte JSON objekti, et mitte sõltuda puuduvatest andmebaasi tulpadest
        llm_data = {
            "output": output_raw,
            "prompt_version": PROMPT_VERSION,
            "confidence": confidence
        }

        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE screens_metadata
                    SET extraction_payload = jsonb_set(extraction_payload, '{llm}', %s::jsonb),
                        updated_at = NOW(),
                        source_fingerprint = %s
                    WHERE screen_id = %s
                    """,
                    (json.dumps(llm_data), fingerprint, sid),
                )