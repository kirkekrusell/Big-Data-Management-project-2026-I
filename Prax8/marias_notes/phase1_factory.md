# Hey team — Phase 1 handover (factory)

Sorry to dump this on you. I'm out for the rest of the day; the
analyst team has been asking for `prd.orders` and `prd.events`
replicated for a week and I never got around to wiring them up.

Good news: the factory in `internal_etl_package/dag_factory.py`
already does the heavy lifting. **You don't have to write Python.**
Adding a table = adding one YAML entry in `config/tables.yaml`.
The scheduler picks new entries up within ~15 seconds — no restart.

Schemas (so you don't have to guess):

- `data/seed/orders.csv` — `order_id, user_id, amount, created_at`. PK is `order_id`.
- `data/seed/events.csv` — `event_id, user_id, event_type, occurred_at`. PK is `event_id`.

Look at the existing `prd.users` entry for the shape. Pick whatever
severity / SLA hours feel right per table; I trust your judgement.

When you're done, `./check.sh` should show Phase 1 ✅ PASS, or
`pytest tests/test_phase1_factory.py` if you want the long form.

— Maria 💚
