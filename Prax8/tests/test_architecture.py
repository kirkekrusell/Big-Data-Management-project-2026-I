"""Architectural tripwires.

These tests don't check behavior — they check that the *shape* of the codebase
matches the lesson. If a future contributor reaches across the engine seam,
hardcodes Spark in the merger, or sneaks a fifth engine into the registry
without updating it, these tests fire with a useful message.

Four guards:

  1. No code outside engines/ may import pyspark or trino.
  2. ENGINE_REGISTRY has exactly {"spark", "trino"} and every value is a
     TableEngine subclass.
  3. The merger works with any TableEngine — proved by injecting a
     MagicMock(spec=TableEngine) and asserting nothing engine-specific is
     called. This is the Liskov payoff.
  4. Adding a new engine touches only engines/<engine>.py and dag_factory.py
     (registry). The grep for `TrinoIcebergEngine` proves Open/Closed.
"""
from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from internal_etl_package import merger
from internal_etl_package.dag_factory import ENGINE_REGISTRY
from internal_etl_package.engines.base import TableEngine
from internal_etl_package.ledger import Ledger


PACKAGE_ROOT = Path("/opt/airflow/internal_etl_package")
ENGINES_DIR = PACKAGE_ROOT / "engines"


# ─── 1. Engine seam ─────────────────────────────────────────────────────────


_IMPORT_PATTERNS = {
    "pyspark": re.compile(r"^\s*(?:from\s+pyspark|import\s+pyspark)\b", re.MULTILINE),
    "trino": re.compile(r"^\s*(?:from\s+trino|import\s+trino)\b", re.MULTILINE),
}


def test_no_engine_imports_outside_engines_dir():
    """No file in internal_etl_package/ outside engines/ may import pyspark or trino."""
    violations: list[str] = []
    for py_file in PACKAGE_ROOT.rglob("*.py"):
        if ENGINES_DIR in py_file.parents:
            continue
        source = py_file.read_text()
        for lib, pattern in _IMPORT_PATTERNS.items():
            if pattern.search(source):
                violations.append(
                    f"{py_file.relative_to(PACKAGE_ROOT)} imports {lib} "
                    f"directly. Use TableEngine instead."
                )
    assert not violations, "Engine seam breached:\n  " + "\n  ".join(violations)


# ─── 2. Engine registry contract ────────────────────────────────────────────


def test_engine_registry_contains_exactly_spark_and_trino():
    assert set(ENGINE_REGISTRY.keys()) == {"spark", "trino"}


def test_every_registry_value_is_a_table_engine_subclass():
    for name, cls in ENGINE_REGISTRY.items():
        assert issubclass(cls, TableEngine), (
            f"ENGINE_REGISTRY[{name!r}] is {cls!r}, not a TableEngine subclass"
        )


# ─── 3. Liskov payoff ───────────────────────────────────────────────────────


def test_merger_depends_only_on_abstract_engine(users_config):
    """Merger logic is engine-agnostic: a MagicMock(spec=TableEngine) is
    enough to drive merge_table to completion. If the merger reached for an
    engine-specific method, the spec'd mock would raise AttributeError.

    Uses the no-prior-snapshot branch so the assertions hold in both starter
    and solution states. The rollback-target assertion lives in
    test_phase2_stateful_merge.py.
    """
    fake_engine = MagicMock(spec=TableEngine)
    fake_engine.merge.return_value = 999
    fake_ledger = MagicMock(spec=Ledger)
    fake_ledger.get_snapshot_before.return_value = None

    merger.merge_table(users_config, date(2026, 5, 11), fake_engine, fake_ledger)

    fake_engine.merge.assert_called_once()
    fake_ledger.record_snapshot.assert_called_once()


def test_merger_does_not_call_any_concrete_engine_method(users_config):
    """A second Liskov guard: every engine method called by the merger must
    be one of the six on the TableEngine ABC. Detected by mocking with spec=
    and inspecting which attributes were accessed."""
    fake_engine = MagicMock(spec=TableEngine)
    fake_engine.merge.return_value = 999
    fake_ledger = MagicMock(spec=Ledger)
    fake_ledger.get_snapshot_before.return_value = None

    merger.merge_table(users_config, date(2026, 5, 11), fake_engine, fake_ledger)

    abc_methods = set(TableEngine.__abstractmethods__)
    used = {c[0] for c in fake_engine.method_calls}
    extra = used - abc_methods
    assert not extra, f"merger called non-ABC engine methods: {extra}"


# ─── 4. Open/Closed enforcement ─────────────────────────────────────────────


def test_trino_engine_referenced_only_in_engines_and_factory():
    """Open/Closed: TrinoIcebergEngine appears in exactly two files inside
    internal_etl_package/: engines/trino_engine.py (defines it) and
    dag_factory.py (registers it). Anything else means a participant
    rewrote business logic to dispatch on engine type — the seam is broken.
    """
    allowed = {
        PACKAGE_ROOT / "engines" / "trino_engine.py",
        PACKAGE_ROOT / "dag_factory.py",
    }
    offenders: list[str] = []
    for py_file in PACKAGE_ROOT.rglob("*.py"):
        if py_file in allowed:
            continue
        if "TrinoIcebergEngine" in py_file.read_text():
            offenders.append(str(py_file.relative_to(PACKAGE_ROOT)))
    assert not offenders, (
        "TrinoIcebergEngine leaked outside its module:\n  "
        + "\n  ".join(offenders)
    )


def test_spark_engine_referenced_only_in_engines_and_factory():
    """Symmetric check for SparkIcebergEngine."""
    allowed = {
        PACKAGE_ROOT / "engines" / "spark_engine.py",
        PACKAGE_ROOT / "dag_factory.py",
    }
    offenders: list[str] = []
    for py_file in PACKAGE_ROOT.rglob("*.py"):
        if py_file in allowed:
            continue
        if "SparkIcebergEngine" in py_file.read_text():
            offenders.append(str(py_file.relative_to(PACKAGE_ROOT)))
    assert not offenders, (
        "SparkIcebergEngine leaked outside its module:\n  "
        + "\n  ".join(offenders)
    )
