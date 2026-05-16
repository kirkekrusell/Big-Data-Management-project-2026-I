from airflow.exceptions import AirflowFailException
from pipeline.utils import get_db

def run_audit(**context):
    run_id = context["ti"].xcom_pull(key="run_id")
    conn = get_db()

    with conn, conn.cursor() as cur:
        # 1. Kontrolli duplikaate embeddingutes
        cur.execute("""
            SELECT screen_id, model_name, model_version, embedding_kind, COUNT(*)
            FROM screens_embeddings
            WHERE run_id = %s
            GROUP BY 1,2,3,4
            HAVING COUNT(*) > 1
        """, (run_id,))
        dup_emb = cur.fetchall()

        # 2. Kontrolli duplikaate metandmetes
        cur.execute("""
            SELECT screen_id, COUNT(*)
            FROM screens_metadata
            WHERE run_id = %s
            GROUP BY 1
            HAVING COUNT(*) > 1
        """, (run_id,))
        dup_meta = cur.fetchall()

        if dup_emb or dup_meta:
            # Projekt 4 nõue: Tõsta AirflowFailException, et rakenduks kaitselüliti
            raise AirflowFailException(f"Kaitselüliti rakendus! Audit ebaõnnestus. Duplikaadid: {dup_emb} {dup_meta}")
            
        print("Audit edukalt läbitud. Andmete kvaliteet on puhas!")