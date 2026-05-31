"""Phase 2 — the stateful merger fix.

Scenario-framed tests: simulate the "Oct 8 had good data, now we're backfilling
Oct 7" situation and verify the merger asks the ledger for the right prior
snapshot, rolls the table back to it, then re-merges the corrected source.

Mock-only — engine and ledger are MagicMocks. The real-Spark integration check
lives in T19's test_end_to_end. These tests skip until the participant has
filled the Phase 2 TODOs in merger.py.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from internal_etl_package.engines.base import TableEngine
from internal_etl_package.ledger import Ledger
from internal_etl_package.merger import merge_table


_MERGER_PATH = Path("/opt/airflow/internal_etl_package/merger.py")
pytestmark = pytest.mark.skipif(
    "# TODO" in _MERGER_PATH.read_text(),
    reason="merger.py still has Phase 2 TODOs — fill them in to enable",
)


def _wire(snapshot_before: int | None, new_snapshot: int = 999):
    engine = MagicMock(spec=TableEngine)
    engine.merge.return_value = new_snapshot
    ledger = MagicMock(spec=Ledger)
    ledger.get_snapshot_before.return_value = snapshot_before
    return engine, ledger


def test_rollback_uses_correct_prior_snapshot_id(users_config):
    """The Phase 2 fix: read 'snapshot before Oct 8' from the ledger and
    rollback the table to it before applying the Oct 7 backfill source."""
    engine, ledger = _wire(snapshot_before=12345)

    merge_table(users_config, date(2026, 5, 11), engine, ledger)

    ledger.get_snapshot_before.assert_called_once_with("prd.users", date(2026, 5, 11))
    engine.rollback_to_snapshot.assert_called_once_with("prd.users", 12345)


def test_ledger_records_new_snapshot_after_merge(users_config):
    engine, ledger = _wire(snapshot_before=None, new_snapshot=77777)
    merge_table(users_config, date(2026, 5, 11), engine, ledger)
    ledger.record_snapshot.assert_called_once_with(
        "prd.users", date(2026, 5, 11), 77777
    )


def test_backfill_does_not_corrupt_newer_data(users_config):
    """End-to-end scenario sketch: ledger has snapshot S1 from yesterday's
    good run; today we backfill Oct 7. The merger must (a) ask the ledger,
    (b) roll back to S1, (c) merge the backfill source, (d) record the
    new snapshot. If any step is missing, the lesson breaks."""
    engine, ledger = _wire(snapshot_before=1001, new_snapshot=1002)

    merge_table(users_config, date(2026, 5, 11), engine, ledger)

    assert ledger.get_snapshot_before.called
    assert engine.rollback_to_snapshot.called
    assert engine.merge.called
    assert ledger.record_snapshot.called

    # Order: rollback strictly before merge; record after merge.
    method_names = [c[0] for c in engine.method_calls]
    assert method_names.index("rollback_to_snapshot") < method_names.index("merge")
    record_args = ledger.record_snapshot.call_args.args
    assert record_args[-1] == 1002, "must record the NEW snapshot, not the rollback target"
