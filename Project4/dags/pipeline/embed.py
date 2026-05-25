from PIL import Image
from io import BytesIO
from pipeline.utils import get_db, get_s3, MINIO_BUCKET, sha256_bytes

CLIP_ARCH = "ViT-B-32"
CLIP_PRETRAINED = "laion2b_s34b_b79k"
CLIP_MODEL_VERSION = f"open-clip-{CLIP_ARCH}-{CLIP_PRETRAINED}"
SBERT_MODEL_VERSION = "sentence-transformers/all-MiniLM-L6-v2"


def init_embeddings_table(cur):
    """Abifunktsioon, mis tagab, et tabel ja laiendus on enne päringuid olemas."""
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS screens_embeddings (
            screen_id TEXT,
            model_name TEXT,
            model_version TEXT,
            embedding_kind TEXT,
            embedding vector,
            run_id UUID,
            source_fingerprint TEXT,
            CONSTRAINT screens_embeddings_pk PRIMARY KEY (screen_id, model_name, model_version, embedding_kind)
        );
    """)


def run_embed_image(**context):
    import torch
    import open_clip

    run_id = context["ti"].xcom_pull(key="run_id")
    s3 = get_s3()
    conn = get_db()

    model, _, preprocess = open_clip.create_model_and_transforms(CLIP_ARCH, pretrained=CLIP_PRETRAINED)
    model.eval()

    with conn, conn.cursor() as cur:
        # Tagame, et tabel on olemas
        init_embeddings_table(cur)

        cur.execute("SELECT screen_id, png_path FROM screens_metadata")
        rows = cur.fetchall()

        for sid, png_path in rows:
            blob = s3.get_object(Bucket=MINIO_BUCKET, Key=png_path)["Body"].read()
            img = Image.open(BytesIO(blob)).convert("RGB")
            tensor = preprocess(img).unsqueeze(0)

            with torch.no_grad():
                vec = model.encode_image(tensor)
                vec = vec / vec.norm(dim=-1, keepdim=True)
                vec = vec.cpu().numpy().astype("float32")[0]

            fingerprint = sha256_bytes(blob)

            cur.execute(
                """
                INSERT INTO screens_embeddings 
                (screen_id, model_name, model_version, embedding_kind, embedding, run_id, source_fingerprint)
                VALUES (%s, %s, %s, 'image', %s, %s, %s)
                ON CONFLICT (screen_id, model_name, model_version, embedding_kind) DO NOTHING
                """,
                (sid, "open-clip", CLIP_MODEL_VERSION, vec.tolist(), run_id, fingerprint),
            )


def run_embed_text(**context):
    from sentence_transformers import SentenceTransformer

    run_id = context["ti"].xcom_pull(key="run_id")
    conn = get_db()

    sbert = SentenceTransformer(SBERT_MODEL_VERSION)

    with conn, conn.cursor() as cur:
        # Tagame, et tabel on olemas
        init_embeddings_table(cur)

        cur.execute("SELECT screen_id, extraction_payload FROM screens_metadata")
        rows = cur.fetchall()

        for sid, payload in rows:
            # Vaatame igaks juhuks nii 'text_rep' kui ka 'texts' võtmeid, et vältida tühja stringi embeddingut
            text = ""
            if payload:
                text = payload.get("text_rep") or payload.get("texts") or ""

            vec = sbert.encode([text], normalize_embeddings=True)[0].astype("float32")
            fingerprint = sha256_bytes(text.encode())

            cur.execute(
                """
                INSERT INTO screens_embeddings 
                (screen_id, model_name, model_version, embedding_kind, embedding, run_id, source_fingerprint)
                VALUES (%s, %s, %s, 'text', %s, %s, %s)
                ON CONFLICT (screen_id, model_name, model_version, embedding_kind) DO NOTHING
                """,
                (sid, "sbert", SBERT_MODEL_VERSION, vec.tolist(), run_id, fingerprint),
            )
