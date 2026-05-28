# Phase Guides

A more structured companion to the per-phase handover notes
(`phase1_factory.md`, `phase2_stateful_merge.md`,
`phase3_circuit_breaker.md`). Each phase below has the same shape
— *Why this matters*, *Where to look*, *How to verify*. Read the
matching handover note first; come here if you want depth.

---

## Phase 1 — Factory (15 minutes)

### Why this matters

A replication system that requires a code change every time someone
asks for a new table is the kind of system that becomes a bottleneck
in six months. The factory makes "add a row, ship a pipeline" literal.

### Where to look

- [`config/tables.yaml`](../config/tables.yaml) — the only file you edit.
- [`internal_etl_package/dag_factory.py`](../internal_etl_package/dag_factory.py) — the factory that reads the YAML. Zero TODOs; just read it.
- [`internal_etl_package/config_loader.py`](../internal_etl_package/config_loader.py) — the Pydantic schema. Validates the YAML.

### How to verify

```bash
./check.sh                                            # Phase 1 should flip to ✅ PASS
docker compose exec airflow-scheduler airflow dags list   # 3 DAGs now
```

Within ~15 seconds of saving the YAML, the Airflow UI's DAGs page
shows three rows: `replicate__prd__users`, `replicate__prd__orders`,
`replicate__prd__events`. Magic moment.

---

## Phase 2 — Stateful merge (20 minutes)

### Why this matters

Backfills are the most common cause of silent data corruption in
analytics warehouses. A naive `MERGE INTO` against a corrected
historical source can wipe out newer good data that was already
applied. The fix is to use the snapshot ledger to rewind first,
then re-apply.

### Where to look

- [`internal_etl_package/merger.py`](../internal_etl_package/merger.py) — **2 TODOs**.
- [`internal_etl_package/ledger.py`](../internal_etl_package/ledger.py) — the API you call (no TODOs here).
- [`internal_etl_package/engines/base.py`](../internal_etl_package/engines/base.py) — the six engine methods. You'll use one.

### How to verify

```bash
./check.sh                                  # Phase 2 should flip to ✅ PASS
make reset                                  # clean baseline
make corrupt-users                          # simulate the bad backfill
# Trigger replicate__prd__users in the Airflow UI
# After it runs, the dashboard should still show 1000 rows (intact).
```

---

## Phase 3 — Circuit breaker + Slack (20 minutes)

### Why this matters

A post-merge data-quality audit that detects a duplicate-key
collision but doesn't alert anyone *and* doesn't stop the pipeline
is worse than no audit at all — it gives a false sense of safety
while downstream dashboards keep reading corrupted snapshots.
The fix: count, page, then raise.

### Where to look

- [`internal_etl_package/quality.py`](../internal_etl_package/quality.py) — **3 TODOs**, all inside `post_audit`.
- [`internal_etl_package/alerting.py`](../internal_etl_package/alerting.py) — `send_slack_alert` is already imported for you. Routing is automatic.
- [`internal_etl_package/__init__.py`](../internal_etl_package/__init__.py) — `DataQualityError` lives here.

### How to verify

```bash
./check.sh                                  # Phase 3 should flip to ✅ PASS
make reset
make inject-duplicates                      # simulate the bad export
# Trigger replicate__prd__users in the Airflow UI.
# Watch the dashboard flip the row red. tail alerts/pager.log — you'll see *BUZZ BUZZ*.
```

---

## After all three phases

Run the full smoke test against your fixes:

```bash
make smoke-test
```

It exercises the same end-to-end path the integration tests cover
in T19. Takes ~90 seconds.

If everything stays green, [`take_home_trino.md`](take_home_trino.md) is the bonus round (worth **+2 marks**).
