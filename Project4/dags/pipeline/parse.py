import json
from pipeline.utils import get_db, get_s3, MINIO_BUCKET


def parse_hierarchy(raw_json: str):
    tree = json.loads(raw_json)
    root = tree.get("activity", {}).get("root", tree)
    stack = [root]
    elements = []

    while stack:
        node = stack.pop()
        if not isinstance(node, dict):
            continue

        text = (node.get("text") or "").strip()
        cls = (node.get("class") or "").strip()
        bounds = node.get("bounds") or [0, 0, 0, 0]

        if text or cls:
            elements.append((cls, text, bounds))

        children = node.get("children")
        if isinstance(children, list):
            stack.extend(reversed(children))

    return elements


def text_representation(elements):
    with_text = [e for e in elements if e[1]]
    ordered = sorted(with_text, key=lambda e: (e[2][1], e[2][0]))
    return " ".join(e[1] for e in ordered)


def run_parse(**context):
    run_id = context["ti"].xcom_pull(key="run_id")
    s3 = get_s3()

    # 1. Küsime töötlemiseks vajalikud read ja sulgeme ühenduse kohe
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT screen_id, hierarchy_json_path FROM screens_metadata WHERE run_id = %s", 
                (run_id,)
            )
            rows = cur.fetchall()

    # 2. Töötleme andmed tsüklis (andmebaasi ühendust ei hoita MinIO päringute ajal lahti)
    for sid, json_path in rows:
        raw = s3.get_object(Bucket=MINIO_BUCKET, Key=json_path)["Body"].read().decode()
        elements = parse_hierarchy(raw)
        text_rep = text_representation(elements)

        # 3. Salvestame tulemuse värske lühiajalise ühendusega, vältides timeouti
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE screens_metadata
                    SET extraction_payload = jsonb_build_object('texts', %s::text),  -- text_rep on string, seega ::text
                        updated_at = NOW()
                    WHERE screen_id = %s
                """, (text_rep, sid))  # Kasutame korrektset muutujat text_rep