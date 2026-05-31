"""Pin the TableEngine seam.

These tests are intentionally pedantic. Every later test, the architecture
lint, and the entire reliability library assume this contract is exactly six
methods named exactly these names. If somebody adds a seventh, they should be
forced through this gate.
"""
import inspect

import pytest

from internal_etl_package.engines.base import TableEngine


EXPECTED_METHODS = {
    "current_snapshot_id",
    "rollback_to_snapshot",
    "merge",
    "count_duplicates",
    "row_count",
    "null_rate",
}


def test_table_engine_is_abstract():
    with pytest.raises(TypeError):
        TableEngine()  # type: ignore[abstract]


def test_table_engine_exposes_exactly_the_six_abstract_methods():
    assert set(TableEngine.__abstractmethods__) == EXPECTED_METHODS


def test_table_engine_methods_are_documented():
    for name in EXPECTED_METHODS:
        method = getattr(TableEngine, name)
        assert (method.__doc__ or "").strip(), f"{name} is missing a docstring"


def test_method_signatures_match_spec():
    expected_params = {
        "current_snapshot_id": ["self", "table"],
        "rollback_to_snapshot": ["self", "table", "snapshot_id"],
        "merge": ["self", "table", "source_path", "primary_key"],
        "count_duplicates": ["self", "table", "primary_key"],
        "row_count": ["self", "table"],
        "null_rate": ["self", "table", "column"],
    }
    for name, params in expected_params.items():
        sig = inspect.signature(getattr(TableEngine, name))
        assert list(sig.parameters) == params, f"{name} has wrong signature: {sig}"


def test_partial_subclass_cannot_be_instantiated():
    class HalfBakedEngine(TableEngine):
        def current_snapshot_id(self, table):
            return 0

        def rollback_to_snapshot(self, table, snapshot_id):
            pass

        def merge(self, table, source_path, primary_key):
            return 0

        def count_duplicates(self, table, primary_key):
            return 0

        def row_count(self, table):
            return 0

        # null_rate intentionally omitted.

    with pytest.raises(TypeError) as exc:
        HalfBakedEngine()  # type: ignore[abstract]
    assert "null_rate" in str(exc.value)


def test_complete_subclass_can_be_instantiated():
    class FakeEngine(TableEngine):
        def current_snapshot_id(self, table):
            return 1

        def rollback_to_snapshot(self, table, snapshot_id):
            pass

        def merge(self, table, source_path, primary_key):
            return 2

        def count_duplicates(self, table, primary_key):
            return 0

        def row_count(self, table):
            return 0

        def null_rate(self, table, column):
            return 0.0

    engine = FakeEngine()
    assert isinstance(engine, TableEngine)
