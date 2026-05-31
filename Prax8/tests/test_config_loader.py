"""Tests for the Pydantic config loader.

The factory loads tables.yaml on every scheduler scan, so config errors are
participant-facing: a typo in the YAML must produce an error message they
can act on, not a stack trace deep in Pydantic.
"""
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from internal_etl_package.config_loader import TableConfig, load_tables


CONFIG_PATH = Path("/opt/airflow/config/tables.yaml")


def _write(tmp_path, payload):
    p = tmp_path / "tables.yaml"
    p.write_text(yaml.safe_dump(payload))
    return p


def _minimal_entry(**overrides):
    base = {
        "name": "prd.users",
        "primary_key": "user_id",
        "source_path": "/opt/data/source/users/",
        "critical_columns": ["user_id", "email"],
        "freshness_sla_hours": 6,
        "severity": "critical",
    }
    base.update(overrides)
    return base


def test_loads_starter_yaml_with_single_users_entry():
    configs = load_tables(CONFIG_PATH)
    assert len(configs) == 1
    cfg = configs[0]
    assert cfg.name == "prd.users"
    assert cfg.primary_key == "user_id"
    assert cfg.engine == "spark"
    assert cfg.severity == "critical"


def test_engine_defaults_to_spark(tmp_path):
    payload = {"tables": [{k: v for k, v in _minimal_entry().items()}]}
    payload["tables"][0].pop("engine", None)
    p = _write(tmp_path, payload)
    [cfg] = load_tables(p)
    assert cfg.engine == "spark"


def test_engine_must_be_known_value(tmp_path):
    p = _write(tmp_path, {"tables": [_minimal_entry(engine="duckdb")]})
    with pytest.raises(ValidationError) as exc:
        load_tables(p)
    assert "engine" in str(exc.value)


def test_severity_must_be_critical_or_warning(tmp_path):
    p = _write(tmp_path, {"tables": [_minimal_entry(severity="nice-to-have")]})
    with pytest.raises(ValidationError) as exc:
        load_tables(p)
    assert "severity" in str(exc.value)


def test_missing_primary_key_raises(tmp_path):
    entry = _minimal_entry()
    del entry["primary_key"]
    p = _write(tmp_path, {"tables": [entry]})
    with pytest.raises(ValidationError) as exc:
        load_tables(p)
    assert "primary_key" in str(exc.value)


def test_critical_columns_must_be_nonempty(tmp_path):
    p = _write(tmp_path, {"tables": [_minimal_entry(critical_columns=[])]})
    with pytest.raises(ValidationError):
        load_tables(p)


def test_freshness_sla_hours_must_be_positive(tmp_path):
    p = _write(tmp_path, {"tables": [_minimal_entry(freshness_sla_hours=0)]})
    with pytest.raises(ValidationError):
        load_tables(p)


def test_empty_tables_yields_empty_list(tmp_path):
    p = _write(tmp_path, {"tables": []})
    assert load_tables(p) == []


def test_multiple_entries_round_trip(tmp_path):
    p = _write(
        tmp_path,
        {
            "tables": [
                _minimal_entry(name="prd.users"),
                _minimal_entry(name="prd.orders", primary_key="order_id"),
            ]
        },
    )
    configs = load_tables(p)
    assert [c.name for c in configs] == ["prd.users", "prd.orders"]


def test_trino_engine_is_accepted(tmp_path):
    p = _write(tmp_path, {"tables": [_minimal_entry(engine="trino")]})
    [cfg] = load_tables(p)
    assert cfg.engine == "trino"
