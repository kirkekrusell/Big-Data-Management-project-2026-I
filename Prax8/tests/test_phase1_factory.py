"""Phase 1 — the "add a table = ship a pipeline" tests.

These tests prove the factory + YAML loader pipeline holds together. The
schema-validation tests run regardless of phase state. The
three-DAGs-from-three-entries test only runs once participants have added
prd.orders + prd.events to tables.yaml.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from internal_etl_package.config_loader import load_tables
from internal_etl_package.dag_factory import build_dag


CONFIG_PATH = Path("/opt/airflow/config/tables.yaml")


# ─── Always-on schema validation ────────────────────────────────────────────


def test_yaml_schema_validation_rejects_missing_pk(tmp_path):
    """A typo'd YAML must surface a clear pydantic ValidationError, not a
    deep stacktrace from inside the factory."""
    bad = tmp_path / "tables.yaml"
    bad.write_text(
        yaml.safe_dump(
            {
                "tables": [
                    {
                        "name": "prd.orders",
                        # primary_key intentionally omitted
                        "source_path": "/opt/data/source/orders/",
                        "critical_columns": ["order_id"],
                        "freshness_sla_hours": 6,
                        "severity": "warning",
                    }
                ]
            }
        )
    )
    with pytest.raises(ValidationError) as exc:
        load_tables(bad)
    assert "primary_key" in str(exc.value)


def test_each_built_dag_has_three_tasks(tmp_path):
    """Whatever tables.yaml looks like, every DAG the factory builds is a
    three-task pipeline."""
    cfg = tmp_path / "tables.yaml"
    cfg.write_text(
        yaml.safe_dump(
            {
                "tables": [
                    {
                        "name": "prd.demo",
                        "primary_key": "id",
                        "source_path": "/opt/data/source/demo/",
                        "critical_columns": ["id"],
                        "freshness_sla_hours": 6,
                        "severity": "warning",
                    }
                ]
            }
        )
    )
    [config] = load_tables(cfg)
    dag = build_dag(config)
    assert {t.task_id for t in dag.tasks} == {"pre_audit", "merge", "post_audit"}


# ─── Phase 1 fix verification ───────────────────────────────────────────────


def _configs_count() -> int:
    return len(load_tables(CONFIG_PATH))


_phase1_done = pytest.mark.skipif(
    _configs_count() < 3,
    reason="tables.yaml not expanded yet — finish Phase 1 to enable these tests",
)


@_phase1_done
def test_factory_loads_three_dags_after_yaml_edit():
    """Once the participant has added prd.orders + prd.events, the factory
    builds three DAGs."""
    configs = load_tables(CONFIG_PATH)
    dag_ids = {build_dag(c).dag_id for c in configs}
    assert dag_ids == {
        "replicate__prd__users",
        "replicate__prd__orders",
        "replicate__prd__events",
    }


@_phase1_done
def test_each_phase1_table_keeps_its_severity():
    """Severity is per-table, set in YAML, and surfaces on the DAG tags."""
    configs = {c.name: c for c in load_tables(CONFIG_PATH)}
    assert "prd.orders" in configs
    assert "prd.events" in configs
    for name in ("prd.users", "prd.orders", "prd.events"):
        dag = build_dag(configs[name])
        assert configs[name].severity in dag.tags
