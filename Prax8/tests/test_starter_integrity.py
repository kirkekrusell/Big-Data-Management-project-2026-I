"""Pin the starter state.

These tests are the inverse of the phase tests — they pass when the live
merger.py / quality.py / tables.yaml are in the participant-facing starter
state, and skip once solutions are applied. CI on `main` should run them
in starter mode so a refactor never accidentally ships participants a
broken starting position.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from internal_etl_package.config_loader import load_tables


PACKAGE_ROOT = Path("/opt/airflow/internal_etl_package")
CONFIG_PATH = Path("/opt/airflow/config/tables.yaml")
DATA_SEED = Path("/opt/data/seed")
DAGS_DIR = Path("/opt/airflow/dags")


def _starter_state() -> bool:
    merger = (PACKAGE_ROOT / "merger.py").read_text()
    quality = (PACKAGE_ROOT / "quality.py").read_text()
    return "# TODO" in merger and "# TODO" in quality


pytestmark = pytest.mark.skipif(
    not _starter_state(),
    reason="live files have solutions applied — starter integrity is irrelevant",
)


def test_correct_number_of_todos_present():
    """Exactly 5 TODOs across the two starter files: 2 in merger, 3 in quality."""
    merger_todos = (PACKAGE_ROOT / "merger.py").read_text().count("# TODO")
    quality_todos = (PACKAGE_ROOT / "quality.py").read_text().count("# TODO")
    assert merger_todos == 2, f"merger.py has {merger_todos} TODOs (expected 2)"
    assert quality_todos == 3, f"quality.py has {quality_todos} TODOs (expected 3)"


def test_no_todos_in_engines_or_dag_factory():
    """The architecture layer ships complete — no participant edits there."""
    factory = (PACKAGE_ROOT / "dag_factory.py").read_text()
    assert "# TODO" not in factory
    for engine_file in (PACKAGE_ROOT / "engines").rglob("*.py"):
        assert "# TODO" not in engine_file.read_text(), engine_file


def test_only_users_dag_visible_at_start():
    """tables.yaml ships with only prd.users registered."""
    configs = load_tables(CONFIG_PATH)
    assert [c.name for c in configs] == ["prd.users"]


def test_generated_dags_imports_load_tables_from_canonical_path():
    """dags/generated.py reads /opt/airflow/config/tables.yaml — verify."""
    src = (DAGS_DIR / "generated.py").read_text()
    assert "load_tables" in src
    assert "/opt/airflow/config/tables.yaml" in src


def test_seed_data_row_counts():
    """Seed CSVs land at 1000 / 3000 / 5000 rows (excluding header)."""
    expected = {"users.csv": 1000, "orders.csv": 3000, "events.csv": 5000}
    for name, n in expected.items():
        path = DATA_SEED / name
        lines = path.read_text().splitlines()
        assert len(lines) - 1 == n, f"{name}: expected {n} rows, got {len(lines) - 1}"
