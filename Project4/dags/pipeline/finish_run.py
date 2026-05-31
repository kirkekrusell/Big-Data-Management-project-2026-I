from pipeline.utils import get_db
from pipeline.slack import send_slack_message


def run_finish(**context):
    run_id = context["ti"].xcom_pull(key="run_id")

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_runs
                SET ended_at = NOW(),
                    status = 'succeeded'
                WHERE run_id = %s
                """,
                (run_id,),
            )

    send_slack_message(
        f"""
✅ Pipeline finished

run_id: {run_id}
status: succeeded
"""
    )