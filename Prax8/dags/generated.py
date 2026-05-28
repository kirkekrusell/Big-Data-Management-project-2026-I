from internal_etl_package.config_loader import load_tables
from internal_etl_package.dag_factory import build_dag


for table_config in load_tables("/opt/airflow/config/tables.yaml"):
    dag = build_dag(table_config)
    globals()[dag.dag_id] = dag
