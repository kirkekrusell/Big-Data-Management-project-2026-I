# Maria's Reliability Lab

> A 1-hour hands-on workshop on data-pipeline reliability. You'll fix
> three real failure modes — silent corruption from a careless
> backfill, duplicate primary keys, and missing-data on a freeze-out —
> against an honest-to-goodness Airflow + Spark + Iceberg + Hive
> Metastore + Trino stack.

## The night before

> **🌙 Run `make warm` the night before the session.** It pulls and
> builds ~5 GB of images so the morning-of `./setup.sh` finishes in
> under a minute.

## Prereqs

- Docker (with Compose v2). On Docker Desktop 4.30+ works well.
- ~16 GB free disk
- A spare 8 GB RAM you can give Docker for the duration
- No host Python required — everything runs in containers

## First-time setup

```bash
./setup.sh        # copies .env, pulls + builds images, starts stack, verifies
```

`./setup.sh` is idempotent: if anything is already running it just
re-runs `make verify-stack` at the end.

You'll see green checkmarks for postgres, catalog, storage, airflow-web,
airflow-sched, spark, and trino. That's the green light.

## The story

You're picking up where Maria left off. **Everything you need is in
[`marias_notes/`](marias_notes/).** Open the files in this order:

| # | File | What it is |
|---|---|---|
| 1 | [`marias_notes/phase1_factory.md`](marias_notes/phase1_factory.md) | Phase 1: factory (add YAML for `prd.orders` + `prd.events`) |
| 2 | [`marias_notes/phase2_stateful_merge.md`](marias_notes/phase2_stateful_merge.md) | Phase 2: stateful merge (rollback before backfill) |
| 3 | [`marias_notes/phase3_circuit_breaker.md`](marias_notes/phase3_circuit_breaker.md) | Phase 3: circuit breaker + Slack (duplicates fail fast) |
| 4 | [`marias_notes/phase_guides.md`](marias_notes/phase_guides.md) | Structured *why / where / how-to-verify* companion to the phase notes |

### 🎁 Bonus exercise (+2 marks)

[`marias_notes/take_home_trino.md`](marias_notes/take_home_trino.md) —
wire up the Trino engine stub. Worth **2 marks** above the base
workshop. It's the Open/Closed payoff: fill in one file, change
nothing else, and the same DAGs run on a different compute engine.

## Watching the work

```bash
./check.sh                      # per-phase PASS / FAIL / NOT ATTEMPTED
make dashboard                  # live row counts + alerts (project this!)
docker compose logs airflow-scheduler   # if something looks off
```

## Cheat codes (for the curious)

| Command | What it does |
|---|---|
| `./setup.sh` | First-time bring-up (idempotent) |
| `make warm` | Pre-pull + build images (run the night before) |
| `make up` / `make down` | Start / stop the stack |
| `make verify-stack` | Health-check every service |
| `make test-fast` | Run the unit suite (<2s) |
| `make smoke-test` | Full end-to-end DAG run (~90s) |
| `make check` | Same as `./check.sh` |
| `make hint-phaseN-L` | Escalating hints (`L=1,2,3`) |
| `make reset` | Restore lab to clean baseline |
| `make corrupt-users` | Apply the Phase 2 chaos scenario |
| `make inject-duplicates` | Apply the Phase 3 chaos scenario |
| `make dashboard` | Live status UI |
| `make logs` | Tail logs for all services |

## Service endpoints

| Service | URL | Login |
|---|---|---|
| Airflow UI | http://localhost:8080 | `airflow` / `airflow` |
| MinIO Console | http://localhost:9001 | `admin` / `password` |
| Hive Metastore (thrift) | host: `catalog:9083` (internal only) | — |
| Trino UI | http://localhost:8082 | — |
| Spark Master UI | http://localhost:8081 | — |

## When things break

See [`SETUP.md`](SETUP.md) for troubleshooting.

## Architecture
![Lab 8.png](Lab%208.png)