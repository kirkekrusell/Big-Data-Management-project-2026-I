"""Reference-solution end-to-end smoke test.

What this test proves, in one go, against the running Docker stack:

  Stage 1 — Baseline clean-data DAG run succeeds; warehouse has 1000 rows.
  Stage 2 — corrupt_users injects an Oct-7 backfill and a warehouse
            DELETE. The DAG runs successfully and the stateful merger
            rolls back, so total row count stays at 1000 — the Phase 2
            payoff.
  Stage 3 — inject_duplicates pushes 10 duplicate user_ids into the
            warehouse and adds dups.csv to source. The DAG run ends in
            `failed` state and alerts/pager.log has a CRITICAL entry
            mentioning prd.users — the Phase 3 payoff.

Total wall-clock target: under 5 minutes. The first DAG run pays the
~40s Spark cold-start cost; later runs in the same scheduler instance
reuse the cluster's executors and finish in ~30s each.

Skipped when the live merger/quality have starter TODOs — only the
reference solution exercises the rollback + circuit-breaker chains.
Marked `integration` so `make test-fast` skips it and `make smoke-test`
picks it up.
"""
from __future__ import annotations

import json
import re
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest


DAG_ID = "replicate__prd__users"
PAGER_LOG = Path("/opt/airflow/alerts/pager.log")

_MERGER = Path("/opt/airflow/internal_etl_package/merger.py")
_QUALITY = Path("/opt/airflow/internal_etl_package/quality.py")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        "# TODO" in _MERGER.read_text() or "# TODO" in _QUALITY.read_text(),
        reason="starter state — apply solutions before running the smoke test",
    ),
]


# ─── helpers ────────────────────────────────────────────────────────────────


def _run(cmd: list[str], timeout: int = 240) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout, check=False
    )


def _airflow(*args: str, timeout: int = 240) -> subprocess.CompletedProcess:
    return _run(["airflow", *args], timeout=timeout)


def _trigger_and_wait(logical_date: str | None = None, deadline_sec: int = 240) -> str:
    """Trigger a fresh DAG run (optionally back-dated so the stateful merger
    can find a prior snapshot to roll back to), poll its specific run_id
    until success/failed.

    We do *not* `airflow dags delete` between stages — that races with the
    scheduler re-parsing generated.py. We also don't poll `runs[0]` — a
    coincident scheduled run can occupy that slot. We poll the run_id
    Airflow assigns to our specific manual trigger.
    """
    args = ["dags", "trigger", DAG_ID]
    if logical_date:
        # Airflow 2.x accepts -e / --exec-date as ISO-8601. Append +00:00
        # only if the caller didn't pre-format it (datetime.isoformat()
        # output already includes hh:mm:ss).
        suffix = "" if "T" in logical_date else "T00:00:00"
        args.extend(["-e", f"{logical_date}{suffix}+00:00"])
    proc = _airflow(*args, timeout=60)
    assert proc.returncode == 0, f"airflow dags trigger failed: {proc.stderr}"

    # The run_id is deterministic: `manual__<execution-date-ISO>`.
    if logical_date:
        suffix = "" if "T" in logical_date else "T00:00:00"
        target_run_id = f"manual__{logical_date}{suffix}+00:00"
    else:
        target_run_id = None

    end = time.time() + deadline_sec
    while time.time() < end:
        proc = _airflow(
            "dags", "list-runs", "--dag-id", DAG_ID, "--output", "json", timeout=30
        )
        try:
            runs = json.loads(proc.stdout or "[]")
        except json.JSONDecodeError:
            runs = []

        if target_run_id:
            matches = [r for r in runs if r.get("run_id") == target_run_id]
            state = matches[0]["state"] if matches else "no_runs"
        else:
            state = runs[0]["state"] if runs else "no_runs"

        if state in {"success", "failed"}:
            return state
        time.sleep(10)
    raise TimeoutError(f"DAG {DAG_ID} did not finish within {deadline_sec}s")


def _row_count() -> int | None:
    """COUNT(*) for prd.users via Trino. Returns None when the query fails
    (e.g. the table doesn't exist yet). Retries briefly because HMS's
    write may not be visible to Trino's iceberg connector on the very
    first poll after a successful DAG run."""
    import trino  # local import: trino client only needed inside the container

    last_exc = None
    for _ in range(6):
        try:
            conn = trino.dbapi.connect(
                host="trino", port=8080, user="smoke-test", catalog="iceberg_datalake"
            )
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM iceberg_datalake.prd.users")
            ((n,),) = cur.fetchall()
            return int(n)
        except Exception as e:  # noqa: BLE001
            last_exc = e
            time.sleep(2)
    print(f"[_row_count] gave up after retries; last error: {last_exc!r}")
    return None


# ─── the test ───────────────────────────────────────────────────────────────


def test_three_phase_smoke_against_solutions():
    """Single integration scenario: baseline → Phase 2 rollback survives →
    Phase 3 duplicates fail the DAG and page.

    The stateful merger looks up the snapshot *strictly before* the run's
    logical_date. To exercise that path we use three back-dated runs (3
    days ago, 2 days ago, 1 day ago) — all in the past so Airflow doesn't
    leave them stuck in `queued`, and each strictly later than the previous
    so `get_snapshot_before` finds the prior stage's snapshot.
    """
    # Use full timestamps (not just date) so re-runs of this test don't
    # collide on run_id with prior attempts. Each stage's logical_date is
    # still strictly less than the next stage's so get_snapshot_before
    # works as the lesson intends.
    now = datetime.utcnow().replace(microsecond=0)
    stage1_date = (now - timedelta(days=3)).isoformat()
    stage2_date = (now - timedelta(days=2)).isoformat()
    stage3_date = (now - timedelta(days=1)).isoformat()

    # Stage 0 — clean slate. The DAG is @hourly with catchup=False so the
    # scheduler may fire its own run at the current hour boundary while we
    # poll; we tolerate that by polling our SPECIFIC run_id (not runs[0])
    # and checking warehouse state immediately after.
    proc = _run(["python", "/opt/chaos/reset.py"], timeout=120)
    assert proc.returncode == 0, f"reset failed: {proc.stderr}"
    _test_body(stage1_date, stage2_date, stage3_date)


def _test_body(stage1_date, stage2_date, stage3_date):

    # ─── Stage 1: baseline ──────────────────────────────────────────────────
    state = _trigger_and_wait(logical_date=stage1_date)  # noqa: E501
    assert state == "success", f"baseline DAG ended in {state}"
    rows_baseline = _row_count()
    assert rows_baseline is not None and rows_baseline >= 1000, (
        f"baseline row count looks wrong: {rows_baseline}"
    )

    # ─── Stage 2: Phase 2 — corrupt_users + stateful rollback ───────────────
    proc = _run(
        ["python", "/opt/chaos/corrupt_users.py"], timeout=120
    )
    assert proc.returncode == 0, f"corrupt_users failed: {proc.stderr}"

    state = _trigger_and_wait(logical_date=stage2_date)
    assert state == "success", (
        f"Phase 2 DAG ended in {state} — the merger should roll back to the "
        f"baseline snapshot before applying the bad source"
    )
    rows_after_phase2 = _row_count()
    assert rows_after_phase2 == rows_baseline, (
        f"Phase 2 rollback didn't protect row count: was {rows_baseline}, "
        f"became {rows_after_phase2}"
    )

    # ─── Stage 3: Phase 3 — inject_duplicates + circuit breaker ─────────────
    # Reset alert logs so we can tell THIS run's pager entry from prior noise.
    PAGER_LOG.unlink(missing_ok=True)
    proc = _run(
        ["python", "/opt/chaos/inject_duplicates.py"], timeout=120
    )
    assert proc.returncode == 0, f"inject_duplicates failed: {proc.stderr}"

    state = _trigger_and_wait(logical_date=stage3_date)
    assert state == "failed", (
        f"Phase 3 DAG ended in {state} — duplicates should have failed the "
        f"task and the circuit breaker should have fired"
    )

    # Pager log should mention prd.users + CRITICAL (either from post_audit's
    # explicit send_slack_alert OR from on_failure_callback's adapter).
    assert PAGER_LOG.exists(), "pager.log was never written during Phase 3"
    pager_text = PAGER_LOG.read_text()
    pager_text_no_ansi = re.sub(r"\x1b\[[0-9;]*m", "", pager_text)
    assert "CRITICAL" in pager_text_no_ansi, (
        f"pager.log has no CRITICAL line: {pager_text_no_ansi!r}"
    )
    assert "prd.users" in pager_text_no_ansi or DAG_ID in pager_text_no_ansi, (
        f"pager.log doesn't mention the failing table: {pager_text_no_ansi!r}"
    )
