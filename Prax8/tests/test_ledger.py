"""Pin the ledger contract.

Participants read ledger.py during Phase 2 and trust it. These tests are what
gives them the right to trust it.
"""
import time
from datetime import date

import psycopg2

from internal_etl_package.ledger import LEDGER_TABLE, Ledger


def test_record_then_fetch_round_trip(ledger):
    ledger.record_snapshot("prd.users", date(2026, 5, 10), 100)
    assert ledger.get_snapshot_before("prd.users", date(2026, 5, 11)) == 100


def test_returns_none_when_no_prior_record_exists(ledger):
    assert ledger.get_snapshot_before("prd.users", date(2026, 5, 11)) is None


def test_returns_latest_strictly_prior_date(ledger):
    ledger.record_snapshot("prd.users", date(2026, 5, 1), 1)
    ledger.record_snapshot("prd.users", date(2026, 5, 5), 2)
    ledger.record_snapshot("prd.users", date(2026, 5, 9), 3)
    assert ledger.get_snapshot_before("prd.users", date(2026, 5, 10)) == 3


def test_excludes_records_dated_on_the_input_date(ledger):
    """get_snapshot_before is strictly less-than, not less-than-or-equal."""
    ledger.record_snapshot("prd.users", date(2026, 5, 10), 100)
    assert ledger.get_snapshot_before("prd.users", date(2026, 5, 10)) is None


def test_filters_by_table_name(ledger):
    ledger.record_snapshot("prd.users", date(2026, 5, 10), 1)
    ledger.record_snapshot("prd.orders", date(2026, 5, 10), 2)
    assert ledger.get_snapshot_before("prd.users", date(2026, 5, 11)) == 1
    assert ledger.get_snapshot_before("prd.orders", date(2026, 5, 11)) == 2


def test_multiple_entries_same_date_pick_latest_recorded(ledger):
    ledger.record_snapshot("prd.users", date(2026, 5, 10), 100)
    time.sleep(0.01)
    ledger.record_snapshot("prd.users", date(2026, 5, 10), 200)
    assert ledger.get_snapshot_before("prd.users", date(2026, 5, 11)) == 200


def test_ddl_is_idempotent(pg_dsn):
    """Calling _ensure_schema twice (or on a fresh Ledger over the same DB)
    must not error — participants are expected to construct fresh Ledger
    instances from each task without coordination."""
    Ledger(pg_dsn)._ensure_schema()
    Ledger(pg_dsn)._ensure_schema()


def test_ledger_table_has_expected_columns(ledger, pg_dsn):
    with psycopg2.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = %s ORDER BY ordinal_position",
            (LEDGER_TABLE,),
        )
        cols = [row[0] for row in cur.fetchall()]
    assert cols == ["table_name", "execution_date", "snapshot_id", "recorded_at"]
