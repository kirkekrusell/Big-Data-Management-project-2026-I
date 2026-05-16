from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import uuid

# Kuna pipeline on dags kausta sees, leiab Airflow need impordid kohe üles:
from pipeline.ingest import run_ingest
from pipeline.parse import run_parse
from pipeline.embed import run_embed_image, run_embed_text
from pipeline.extract import run_extract
from pipeline.load import run_load
from pipeline.audit import run_audit
from pipeline.eval import run_eval

DEFAULT_LIMIT = 5

def init_run(**context):
    run_id = str(uuid.uuid4())
    git_sha = "unknown_commit"  # Git teegi puudumisel konteineris kasutame staatilist väärtust

    context["ti"].xcom_push("run_id", run_id)
    context["ti"].xcom_push("git_sha", git_sha)
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

    # Taskide ahel
    init >> ingest >> parse >> [embed_image, embed_text, extract] >> load >> audit >> evaluate