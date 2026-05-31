"""Pydantic-validated YAML config loader.

config/tables.yaml is the only place a table is declared. Adding a new entry
makes the dag_factory emit a new DAG on next scheduler scan — that's the
"add a row, ship a pipeline" experience Phase 1 is built around.
"""
from __future__ import annotations

from pathlib import Path
from typing import Literal, Union

import yaml
from pydantic import BaseModel, Field


Severity = Literal["critical", "warning"]
EngineName = Literal["spark", "trino"]


class TableConfig(BaseModel):
    """One row in tables.yaml.

    All fields except `engine` are required. Validation errors here surface in
    the dag_factory's load step — Airflow renders them in the UI's import
    errors tab, which is what participants see when they break the YAML.
    """

    name: str = Field(..., description="Fully qualified table name, e.g. prd.users.")
    primary_key: str = Field(..., description="Column to dedupe on during merge.")
    source_path: str = Field(..., description="Filesystem path the merger reads CSVs from.")
    critical_columns: list[str] = Field(..., min_length=1)
    freshness_sla_hours: int = Field(..., gt=0)
    severity: Severity
    engine: EngineName = "spark"


def load_tables(path: Union[str, Path]) -> list[TableConfig]:
    raw = yaml.safe_load(Path(path).read_text())
    entries = (raw or {}).get("tables", [])
    return [TableConfig(**entry) for entry in entries]
