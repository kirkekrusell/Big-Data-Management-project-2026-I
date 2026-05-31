from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import uuid

# Kuna pipeline on dags kausta sees, leiab Airflow need impordid kohe üles:
from pipeline.ingest import run_ingest
from pipeline.parse import run_parse
from pipeline.embed import (
    run_embed_image,
    run_embed_text,
    CLIP_MODEL_VERSION,
    SBERT_MODEL_VERSION,
)
from pipeline.extract import run_extract, MODEL, PROMPT_VERSION
from pipeline.load import run_load
from pipeline.audit import run_audit
from pipeline.eval import run_eval
from pipeline.finish_run import run_finish
from pipeline.slack import send_slack_message
from pipeline.utils import get_db



DEFAULT_LIMIT = 5

def init_run(**context):
    run_id = str(uuid.uuid4())
    git_sha = "unknown_commit"  # Git teegi puudumisel konteineris kasutame staatilist väärtust

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs
                (
                    run_id,
                    dag_run_id,
                    started_at,
                    status,
                    limit_param,
                    git_sha,
                    clip_model_version,
                    sbert_model_version,
                    llm_model_version,
                    prompt_version
                )
                VALUES
                (%s,%s,NOW(),'running',%s,%s,%s,%s,%s,%s)
                """,
                (
                    run_id,
                    context["dag_run"].run_id,
                    DEFAULT_LIMIT,
                    git_sha,
                    CLIP_MODEL_VERSION,
                    SBERT_MODEL_VERSION,
                    MODEL,
                    PROMPT_VERSION,
                ),
            )

    context["ti"].xcom_push("run_id", run_id)
    context["ti"].xcom_push("git_sha", git_sha)

    send_slack_message(
        f"""
    🚀 Pipeline started

    run_id: {run_id}
    dag_run_id: {context['dag_run'].run_id}
    limit: {DEFAULT_LIMIT}
    """
    )

    print(f"Started pipeline run with ID: {run_id}")

with DAG(
    dag_id="rico_pipeline",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "kirke",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
) as dag:

    init = PythonOperator(
        task_id="init_run",
        python_callable=init_run,
    )

    ingest = PythonOperator(
        task_id="ingest",
        python_callable=run_ingest,
        op_kwargs={"limit": DEFAULT_LIMIT},
    )

    parse = PythonOperator(
        task_id="parse",
        python_callable=run_parse,
    )

    embed_image = PythonOperator(
        task_id="embed_image",
        python_callable=run_embed_image,
    )

    embed_text = PythonOperator(
        task_id="embed_text",
        python_callable=run_embed_text,
    )

    extract = PythonOperator(
        task_id="extract",
        python_callable=run_extract,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=run_load,
    )

    audit = PythonOperator(
        task_id="audit",
        python_callable=run_audit,
    )

    evaluate = PythonOperator(
        task_id="eval",
        python_callable=run_eval,
    )

    finish = PythonOperator(
    task_id="finish_run",
    python_callable=run_finish,
    )

    # Taskide ahel
    init >> ingest >> parse >> [embed_image, embed_text, extract] >> load >> audit >> evaluate >> finish