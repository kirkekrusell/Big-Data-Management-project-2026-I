"""Structural tests for the DAG factory.

We do not execute the DAG here — DAG correctness end-to-end is proven in T10.
These tests pin the structural contract: id format, task graph, schedule,
catchup, on_failure_callback, and the engine registry.
"""
from internal_etl_package.alerting import slack_callback_on_failure
from internal_etl_package.config_loader import TableConfig
from internal_etl_package.dag_factory import ENGINE_REGISTRY, build_dag
from internal_etl_package.engines.spark_engine import SparkIcebergEngine
from internal_etl_package.engines.trino_engine import TrinoIcebergEngine


def test_engine_registry_has_spark_and_trino():
    assert ENGINE_REGISTRY == {
        "spark": SparkIcebergEngine,
        "trino": TrinoIcebergEngine,
    }


def test_build_dag_id_is_replicate_schema_table(users_config):
    dag = build_dag(users_config)
    assert dag.dag_id == "replicate__prd__users"


def test_build_dag_has_three_tasks(users_config):
    dag = build_dag(users_config)
    assert {t.task_id for t in dag.tasks} == {"pre_audit", "merge", "post_audit"}


def test_build_dag_task_graph_is_pre_then_merge_then_post(users_config):
    dag = build_dag(users_config)
    merge = dag.get_task("merge")
    post = dag.get_task("post_audit")
    pre = dag.get_task("pre_audit")
    assert merge.upstream_task_ids == {"pre_audit"}
    assert post.upstream_task_ids == {"merge"}
    assert pre.upstream_task_ids == set()


def test_build_dag_wires_on_failure_callback(users_config):
    dag = build_dag(users_config)
    for task in dag.tasks:
        assert task.on_failure_callback is slack_callback_on_failure, task.task_id


def test_build_dag_catchup_disabled(users_config):
    dag = build_dag(users_config)
    assert dag.catchup is False


def test_build_dag_no_retries(users_config):
    dag = build_dag(users_config)
    assert dag.default_args["retries"] == 0


def test_build_dag_tags_include_engine_and_severity(users_config):
    dag = build_dag(users_config)
    assert "spark" in dag.tags
    assert "critical" in dag.tags
    assert "replication" in dag.tags


def test_build_dag_for_trino_uses_trino_in_tags():
    cfg = TableConfig(
        name="prd.orders",
        primary_key="order_id",
        source_path="/opt/data/source/orders/",
        critical_columns=["order_id", "user_id"],
        freshness_sla_hours=12,
        severity="warning",
        engine="trino",
    )
    dag = build_dag(cfg)
    assert dag.dag_id == "replicate__prd__orders"
    assert "trino" in dag.tags
    assert "warning" in dag.tags


def test_generated_dags_module_registers_dag_in_globals():
    """`dags/generated.py` must put each built DAG into globals so Airflow's
    DagBag picks it up. The cleanest verification is to import it and read
    the globals."""
    import importlib.util
    import sys
    from pathlib import Path

    path = Path("/opt/airflow/dags/generated.py")
    spec = importlib.util.spec_from_file_location("generated_dags_test", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["generated_dags_test"] = mod
    try:
        spec.loader.exec_module(mod)
        assert "replicate__prd__users" in vars(mod)
    finally:
        sys.modules.pop("generated_dags_test", None)
