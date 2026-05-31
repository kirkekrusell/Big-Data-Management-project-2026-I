"""Shared pytest fixtures.

Engine fixtures live nowhere — we never unit-test engine adapters in this repo.
Their correctness is validated by the end-to-end smoke test (T10). What we do
need is a connection to the workshop Postgres for ledger tests (T05+).
"""
import os

import psycopg2
import pytest

from internal_etl_package.config_loader import TableConfig
from internal_etl_package.ledger import LEDGER_TABLE, Ledger


@pytest.fixture
def pg_dsn() -> str:
    """Connection string for the workshop Postgres.

    Reads the same env vars Airflow uses so tests run identically inside the
    airflow-scheduler container and on a host with the stack up.
    """
    user = os.environ.get("POSTGRES_USER", "airflow")
    password = os.environ.get("POSTGRES_PASSWORD", "airflow")
    db = os.environ.get("POSTGRES_DB", "airflow")
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


@pytest.fixture
def users_config() -> TableConfig:
    """Canonical prd.users TableConfig for unit tests."""
    return TableConfig(
        name="prd.users",
        primary_key="user_id",
        source_path="/opt/data/source/users/",
        critical_columns=["user_id", "email", "created_at"],
        freshness_sla_hours=6,
        severity="critical",
        engine="spark",
    )


@pytest.fixture
def ledger(pg_dsn) -> Ledger:
    """A Ledger with an empty ledger table.

    The ledger table is shared with production Airflow tasks in this lab — the
    fixture truncates it before each test so the assertions don't see crosstalk.
    """
    ledger = Ledger(pg_dsn)
    ledger._ensure_schema()
    with psycopg2.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {LEDGER_TABLE}")
    return ledger
