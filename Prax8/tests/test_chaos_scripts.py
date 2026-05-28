"""Unit tests for the chaos scripts.

Spark-touching steps are gated on `CHAOS_SKIP_SPARK=1` (or the explicit
`skip_spark=True` keyword) so these tests run in <1s without the cluster.
End-to-end behavior (corruption visible in the warehouse) is verified by
the §T11 manual smoke test, not here.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from chaos import corrupt_users, inject_duplicates, reset
from internal_etl_package.ledger import LEDGER_TABLE, Ledger


@pytest.fixture
def chaos_data_root(tmp_path, monkeypatch):
    """Stand up a self-contained data/ tree with a tiny seed file."""
    root = tmp_path / "data"
    (root / "seed").mkdir(parents=True)
    (root / "source").mkdir()

    seed_lines = ["user_id,email,created_at"]
    for uid in range(1, 11):
        # Half of these are >= 2024-10-08 so corrupt_users drops some.
        day = "2024-10-09" if uid > 5 else "2024-10-01"
        seed_lines.append(f"{uid},u{uid:04d}@example.com,{day} 00:00:00")
    (root / "seed" / "users.csv").write_text("\n".join(seed_lines) + "\n")

    monkeypatch.setenv("DATA_ROOT", str(root))
    monkeypatch.setenv("CHAOS_SKIP_SPARK", "1")
    return root


@pytest.fixture
def chaos_alerts_dir(tmp_path, monkeypatch):
    alerts = tmp_path / "alerts"
    alerts.mkdir()
    monkeypatch.setenv("ALERT_DIR", str(alerts))
    return alerts


# ─── corrupt_users ──────────────────────────────────────────────────────────


def test_corrupt_users_drops_late_rows_from_source(chaos_data_root):
    rc = corrupt_users.main(skip_spark=True)
    assert rc == 0

    written = (chaos_data_root / "source" / "users" / "users.csv").read_text().splitlines()
    # header + only the rows with created_at < 2024-10-08 (uid 1..5)
    assert written[0] == "user_id,email,created_at"
    kept_uids = [line.split(",")[0] for line in written[1:]]
    assert kept_uids == ["1", "2", "3", "4", "5"]


def test_corrupt_users_is_idempotent(chaos_data_root):
    assert corrupt_users.main(skip_spark=True) == 0
    first = (chaos_data_root / "source" / "users" / "users.csv").read_bytes()
    assert corrupt_users.main(skip_spark=True) == 0
    second = (chaos_data_root / "source" / "users" / "users.csv").read_bytes()
    assert first == second


def test_corrupt_users_returns_nonzero_when_seed_missing(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("CHAOS_SKIP_SPARK", "1")
    assert corrupt_users.main(skip_spark=True) == 1


# ─── inject_duplicates ──────────────────────────────────────────────────────


def test_inject_duplicates_writes_dup_csv(chaos_data_root):
    rc = inject_duplicates.main(skip_spark=True)
    assert rc == 0

    dup_file = chaos_data_root / "source" / "users" / inject_duplicates.DUP_FILE_NAME
    lines = dup_file.read_text().splitlines()
    assert lines[0] == "user_id,email,created_at"
    # 10 deterministic duplicate user_ids.
    uids = [line.split(",")[0] for line in lines[1:]]
    assert uids == [str(i) for i in inject_duplicates.DUPLICATE_USER_IDS]


def test_inject_duplicates_is_idempotent(chaos_data_root):
    assert inject_duplicates.main(skip_spark=True) == 0
    first = (chaos_data_root / "source" / "users" / inject_duplicates.DUP_FILE_NAME).read_bytes()
    assert inject_duplicates.main(skip_spark=True) == 0
    second = (chaos_data_root / "source" / "users" / inject_duplicates.DUP_FILE_NAME).read_bytes()
    assert first == second


# ─── reset ──────────────────────────────────────────────────────────────────


def test_reset_restores_source_from_seed(chaos_data_root):
    # Pre-corrupt the source so we can prove the reset replaces it.
    src_dir = chaos_data_root / "source" / "users"
    src_dir.mkdir(parents=True, exist_ok=True)
    (src_dir / "users.csv").write_text("user_id,email,created_at\n")

    rc = reset.main(skip_spark=True)
    assert rc == 0

    restored = (src_dir / "users.csv").read_text()
    assert restored == (chaos_data_root / "seed" / "users.csv").read_text()


def test_reset_clears_alerts(chaos_data_root, chaos_alerts_dir):
    (chaos_alerts_dir / "slack.log").write_text("[STALE] noise\n")
    (chaos_alerts_dir / "pager.log").write_text("buzz\n")

    rc = reset.main(skip_spark=True)
    assert rc == 0
    assert list(chaos_alerts_dir.iterdir()) == []


def test_reset_truncates_ledger(chaos_data_root, chaos_alerts_dir, pg_dsn):
    from datetime import date

    Ledger(pg_dsn).record_snapshot("prd.users", date(2026, 5, 11), 12345)
    rc = reset.main(skip_spark=True)
    assert rc == 0

    import psycopg2

    with psycopg2.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {LEDGER_TABLE}")
        (n,) = cur.fetchone()
    assert n == 0


def test_reset_is_idempotent(chaos_data_root, chaos_alerts_dir):
    assert reset.main(skip_spark=True) == 0
    assert reset.main(skip_spark=True) == 0


def test_reset_handles_missing_alerts_dir(tmp_path, chaos_data_root, monkeypatch):
    missing = tmp_path / "nope"
    monkeypatch.setenv("ALERT_DIR", str(missing))
    assert reset.main(skip_spark=True) == 0
    assert missing.exists()


# ─── voice/idempotency smoke ────────────────────────────────────────────────


def test_each_script_prints_a_maria_message(chaos_data_root, chaos_alerts_dir, capsys):
    corrupt_users.main(skip_spark=True)
    inject_duplicates.main(skip_spark=True)
    reset.main(skip_spark=True)
    out = capsys.readouterr().out
    # corrupt_users and inject_duplicates print once each; reset prints twice
    # (intro + outro).
    assert out.count("Maria (via Slack)") == 4
