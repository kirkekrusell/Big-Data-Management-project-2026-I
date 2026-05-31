# Hey team — Phase 2 handover (stateful merge)

Sorry to bail mid-task. Analyst pinged me about the Oct 7th
backfill data being incomplete. If we just re-run the merge naively
on the corrected source, it'll trample Oct 8–9 records that already
landed — I learned this the hard way last quarter 😅.

The fix is in `internal_etl_package/merger.py`. I left two
`# TODO` markers. The pattern:

1. Look up the snapshot from before the bad date in the **ledger**
   (snapshots are recorded after every merge — see `ledger.py`).
2. Roll the table back to that snapshot via the **engine** abstraction.
3. Let the existing `engine.merge(...)` re-apply the corrected source.
4. The `ledger.record_snapshot(...)` line at the bottom is already
   wired — don't touch it.

Important: you're talking to `engine`, not `spark`. That's by design —
the same code will work the day someone wires Trino in.

`./check.sh` (Phase 2) or `pytest tests/test_phase2_stateful_merge.py`
verify the fix. Good luck! — Maria 💚
