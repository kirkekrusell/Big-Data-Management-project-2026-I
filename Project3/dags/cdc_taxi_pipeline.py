from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "julie",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="cdc_taxi_pipeline",
    start_date=datetime(2026, 5, 1),
    schedule_interval="*/15 * * * *",   # 15‑minute SLA
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:

    start = EmptyOperator(task_id="start")

    connector_health = HttpSensor(
        task_id="connector_health",
        http_conn_id="kafka_connect",  # define this connection in Airflow UI
        endpoint="/connectors/pg-cdc-connector/status",
        response_check=lambda r: r.json()["connector"]["state"] == "RUNNING",
        poke_interval=20,
        timeout=300,
    )

    bronze_cdc = BashOperator(
        task_id="bronze_cdc",
        bash_command="spark-submit /opt/airflow/jobs/bronze_cdc.py",
    )

    bronze_taxi = BashOperator(
        task_id="bronze_taxi",
        bash_command="spark-submit /opt/airflow/jobs/bronze_taxi.py",
    )

    silver_cdc = BashOperator(
        task_id="silver_cdc",
        bash_command="spark-submit /opt/airflow/jobs/silver_cdc.py",
    )

    silver_taxi = BashOperator(
        task_id="silver_taxi",
        bash_command="spark-submit /opt/airflow/jobs/silver_taxi.py",
    )

    gold_taxi = BashOperator(
        task_id="gold_taxi",
        bash_command="spark-submit /opt/airflow/jobs/gold_taxi.py",
    )

    validate = BashOperator(
        task_id="validate",
        bash_command="spark-submit /opt/airflow/jobs/validate_cdc_counts.py",
    )

    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    start >> connector_health
    connector_health >> [bronze_cdc, bronze_taxi]
    bronze_cdc >> silver_cdc >> validate
    bronze_taxi >> silver_taxi >> gold_taxi
    [validate, gold_taxi] >> end
