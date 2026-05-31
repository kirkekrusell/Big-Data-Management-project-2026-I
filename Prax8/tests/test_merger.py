"""Tests for the stateful merger.

All tests use MagicMock(spec=TableEngine) and MagicMock(spec=Ledger). No real
Spark or Postgres — the ledger has its own tests in test_ledger.py and engine
correctness is proven end-to-end in T10.

These assert the Phase 2 *fix*, so they're skipped when the live merger.py
still has its starter TODOs. Run `python scripts/apply_solutions.py` (or
fill in the TODOs by hand) to take the skip off.
"""
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from internal_etl_package.engines.base import TableEngine
from internal_etl_package.ledger import Ledger
from internal_etl_package.merger import merge_table


_MERGER_PATH = Path(__file__).resolve().parent.parent / "internal_etl_package" / "merger.py"
pytestmark = pytest.mark.skipif(
    "# TODO" in _MERGER_PATH.read_text(),
    reason="merger.py is in starter state — apply solutions first",
)


def _mock_engine_and_ledger(snapshot_before=None, new_snapshot=99):
    engine = MagicMock(spec=TableEngine)
    engine.merge.return_value = new_snapshot
    ledger = MagicMock(spec=Ledger)
    ledger.get_snapshot_before.return_value = snapshot_before
    return engine, ledger


def test_merger_looks_up_prior_snapshot_before_rolling_back(users_config):
    engine, ledger = _mock_engine_and_ledger(snapshot_before=42)
    merge_table(users_config, date(2026, 5, 10), engine, ledger)
    ledger.get_snapshot_before.assert_called_once_with("prd.users", date(2026, 5, 10))
    engine.rollback_to_snapshot.assert_called_once_with("prd.users", 42)


def test_merger_skips_rollback_when_no_prior_snapshot(users_config):
    engine, ledger = _mock_engine_and_ledger(snapshot_before=None)
    merge_table(users_config, date(2026, 5, 10), engine, ledger)
    engine.rollback_to_snapshot.assert_not_called()


def test_merger_records_new_snapshot_after_merge(users_config):
    engine, ledger = _mock_engine_and_ledger(snapshot_before=None, new_snapshot=99)
    merge_table(users_config, date(2026, 5, 10), engine, ledger)
    ledger.record_snapshot.assert_called_once_with(
        "prd.users", date(2026, 5, 10), 99
    )


def test_merger_calls_engine_merge_with_config_values(users_config):
    engine, ledger = _mock_engine_and_ledger(snapshot_before=None)
    merge_table(users_config, date(2026, 5, 10), engine, ledger)
    engine.merge.assert_called_once_with(
        table="prd.users",
        source_path="/opt/data/source/users/",
        primary_key="user_id",
    )


def test_merger_calls_in_order_rollback_then_merge_then_record(users_config):
    """rollback must happen before merge, and the new snapshot must be the one
    that is recorded (not the prior one)."""
    parent = MagicMock()
    engine, ledger = _mock_engine_and_ledger(snapshot_before=42, new_snapshot=99)
    parent.attach_mock(engine.rollback_to_snapshot, "rollback")
    parent.attach_mock(engine.merge, "merge")
    parent.attach_mock(ledger.record_snapshot, "record")

    merge_table(users_config, date(2026, 5, 10), engine, ledger)

    names = [c[0] for c in parent.method_calls]
    assert names == ["rollback", "merge", "record"]
    # And the snapshot ledger sees is the new one, not the prior one.
    assert parent.method_calls[-1].args[2] == 99


def test_merger_does_not_record_when_engine_merge_raises(users_config):
    engine, ledger = _mock_engine_and_ledger(snapshot_before=None)
    engine.merge.side_effect = RuntimeError("spark broke")

    try:
        merge_table(users_config, date(2026, 5, 10), engine, ledger)
    except RuntimeError:
        pass

    ledger.record_snapshot.assert_not_called()
