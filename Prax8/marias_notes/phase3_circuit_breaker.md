# Hey team — Phase 3 handover (circuit breaker + Slack)

Last bit, I promise. We had an incident last month where a
duplicate-laden export silently overwrote `prd.users` and broke
three downstream dashboards before anyone noticed. The post-merge
audit is supposed to catch this, but it's currently a no-op.

The fix is in `internal_etl_package/quality.py`, inside `post_audit`.
Three small TODOs:

1. Count primary-key collisions with `engine.count_duplicates(...)`.
2. If any exist, page the team via `send_slack_alert(...)` — use
   `severity="CRITICAL"` so it routes to `#data-incidents` *and*
   the pager log (the "wake oncall" channel).
3. Raise `DataQualityError(...)` so Airflow marks the task FAILED.
   That's the circuit breaker — downstream consumers never read
   the poisoned snapshot.

When you've got it, `make inject-duplicates` should produce a red
row in the dashboard, a `*BUZZ BUZZ*` line in `alerts/pager.log`,
and a FAILED DAG run.

`./check.sh` (Phase 3) or `pytest tests/test_phase3_quality_alerts.py`
when you want the verdict. — Maria 💚
