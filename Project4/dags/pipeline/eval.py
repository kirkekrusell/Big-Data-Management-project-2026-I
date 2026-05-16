from pipeline.utils import get_db


def run_eval(**context):
    # Minimal eval to satisfy Project 4
    conn = get_db()
    with conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM screens_embeddings")
        total = cur.fetchone()[0]
        print(f"Eval: total embeddings = {total}")
