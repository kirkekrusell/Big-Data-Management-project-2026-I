from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import requests

default_args = {
    "owner": "project3",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# ---------- helper to run spark jobs ----------
def run_spark_job(script_path):
    def _run():
        subprocess.run(
            [
                "spark-submit",
                script_path
            ],
            check=True
        )
    return _run

# ---------- Debezium health check ----------
def check_connector():
    r = requests.get("http://connect:8083/connectors/cdc-connector/status")
    status = r.json()["connector"]["state"]
    assert status == "RUNNING", f"Connector is {status}"

# ---------- DAG ----------
with DAG(
    dag_id="project3_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule="*/15 * * * *",
    catchup=False,
    tags=["project3"]
) as dag:

    # --- HEALTH CHECK (CDC dependency gate) ---
    health_check = PythonOperator(
        task_id="connector_health",
        python_callable=check_connector,
    )

    # --- CDC PIPELINE ---
    bronze_cdc = PythonOperator(
        task_id="bronze_cdc",
        python_callable=run_spark_job("/home/jovyan/project/jobs/bronze_cdc.py"),
    )

    silver_cdc = PythonOperator(
        task_id="silver_cdc",
        python_callable=run_spark_job("/home/jovyan/project/jobs/silver_cdc.py"),
    )

    # --- TAXI PIPELINE (independent branch) ---
    bronze_taxi = PythonOperator(
        task_id="bronze_taxi",
        python_callable=run_spark_job("/home/jovyan/project/jobs/bronze_taxi.py"),
    )

    silver_taxi = PythonOperator(
        task_id="silver_taxi",
        python_callable=run_spark_job("/home/jovyan/project/jobs/silver_taxi.py"),
    )

    gold_taxi = PythonOperator(
        task_id="gold_taxi",
        python_callable=run_spark_job("/home/jovyan/project/jobs/gold_taxi.py"),
    )

    # ---------- dependencies ----------
    health_check >> bronze_cdc >> silver_cdc
    bronze_taxi >> silver_taxi >> gold_taxi
