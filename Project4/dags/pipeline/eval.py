from pipeline.utils import get_db


def run_eval(**context):
    run_id = context["ti"].xcom_pull(key="run_id")

    with get_db() as conn:
        with conn.cursor() as cur:

            # Metadata rows
            cur.execute(
                """
                SELECT COUNT(*)
                FROM screens_metadata
                """
            )
            metadata_count = cur.fetchone()[0]

            # Embedding rows
            cur.execute(
                """
                SELECT COUNT(*)
                FROM screens_embeddings
                """
            )
            embedding_count = cur.fetchone()[0]

            # Distinct apps
            cur.execute(
                """
                SELECT COUNT(DISTINCT app_package)
                FROM screens_metadata
                """
            )
            distinct_apps = cur.fetchone()[0]

            # Distinct categories
            cur.execute(
                """
                SELECT COUNT(DISTINCT category)
                FROM screens_metadata
                """
            )
            distinct_categories = cur.fetchone()[0]

            # % extraction payload populated
            cur.execute(
                """
                SELECT
                    100.0 * COUNT(*) FILTER (
                        WHERE extraction_payload IS NOT NULL
                    ) / NULLIF(COUNT(*), 0)
                FROM screens_metadata
                """
            )
            extraction_pct = cur.fetchone()[0] or 0

            # % rows in review queue
            cur.execute(
                """
                SELECT
                    100.0 *
                    (
                        SELECT COUNT(*)
                        FROM screens_review_queue
                    )
                    / NULLIF(
                        (
                            SELECT COUNT(*)
                            FROM screens_metadata
                        ),
                        0
                    )
                """
            )
            review_queue_pct = cur.fetchone()[0] or 0

            # Total run duration
            cur.execute(
                """
                SELECT
                    EXTRACT(EPOCH FROM (ended_at - started_at))
                FROM pipeline_runs
                WHERE run_id = %s
                """,
                (run_id,),
            )
            run_duration_seconds = cur.fetchone()[0] or 0

            metrics = [
                ("metadata_rows", metadata_count),
                ("embedding_rows", embedding_count),
                ("distinct_app_packages", distinct_apps),
                ("distinct_categories", distinct_categories),
                ("extraction_payload_pct", extraction_pct),
                ("review_queue_pct", review_queue_pct),
                ("run_duration_seconds", run_duration_seconds),
            ]

            for metric_name, metric_value in metrics:
                cur.execute(
                    """
                    INSERT INTO pipeline_metrics
                    (run_id, metric_name, metric_value)
                    VALUES (%s, %s, %s)
                    """,
                    (run_id, metric_name, metric_value),
                )

    print(
        f"""
================ PIPELINE SUMMARY ================
run_id                = {run_id}

metadata_rows         = {metadata_count}
embedding_rows        = {embedding_count}

distinct_app_packages = {distinct_apps}
distinct_categories   = {distinct_categories}

extraction_payload_%  = {extraction_pct:.2f}
review_queue_%        = {review_queue_pct:.2f}

run_duration_sec      = {run_duration_seconds:.2f}

status                = succeeded
==================================================
"""
    )