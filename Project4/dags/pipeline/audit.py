import json

from airflow.exceptions import AirflowFailException
from pipeline.slack import send_slack_message
from pipeline.utils import get_db


def run_audit(**context):
    run_id = context["ti"].xcom_pull(key="run_id")

    with get_db() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT screen_id, COUNT(*)
                FROM screens_embeddings
                GROUP BY screen_id
                HAVING COUNT(*) > 2
            """)
            dup_emb = cur.fetchall()

            cur.execute("""
                SELECT screen_id, COUNT(*)
                FROM screens_metadata
                GROUP BY screen_id
                HAVING COUNT(*) > 1
            """)
            dup_meta = cur.fetchall()

            if dup_emb or dup_meta:

                cur.execute(
                    """
                    INSERT INTO audit_results
                    (
                        run_id,
                        audit_name,
                        audit_status,
                        details
                    )
                    VALUES (%s, %s, %s, %s::jsonb)
                    """,
                    (
                        run_id,
                        "duplicate_check",
                        "failed",
                        json.dumps(
                            {
                                "duplicate_embeddings": len(dup_emb),
                                "duplicate_metadata": len(dup_meta),
                            }
                        ),
                    ),
                )
                
                send_slack_message(
                    f"""
                ❌ Audit failed

                run_id: {run_id}

                duplicate_embeddings: {len(dup_emb)}
                duplicate_metadata: {len(dup_meta)}
                """
                )

                raise AirflowFailException(
                    f"Audit ebaõnnestus: duplicate embeddings={len(dup_emb)}, duplicate metadata={len(dup_meta)}"
                )

            cur.execute(
                """
                INSERT INTO audit_results
                (
                    run_id,
                    audit_name,
                    audit_status,
                    details
                )
                VALUES (%s, %s, %s, %s::jsonb)
                """,
                (
                    run_id,
                    "duplicate_check",
                    "passed",
                    json.dumps(
                        {
                            "duplicate_embeddings": 0,
                            "duplicate_metadata": 0,
                        }
                    ),
                ),
            )

    print("Audit edukalt läbitud.")