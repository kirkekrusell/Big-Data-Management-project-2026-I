# Troubleshooting

When something doesn't come up clean, work through this list. The fixes
are ordered by how often each one bites.

## "Some services are not yet healthy" after `./setup.sh`

First-boot is slow — Spark + Trino + Airflow + HMS all have ~30s
warmups. Wait a minute and try again:

```bash
make verify-stack
```

If you're still red after two minutes:

```bash
make logs            # tail everything
docker compose ps    # which container is unhealthy?
```

## `make warm` was slow / I have a tiny laptop

The Docker images total ~5 GB. The Spark image alone is ~2 GB.
Doing this the night before is not optional. If you can't pre-pull,
expect 5–10 minutes for `./setup.sh` to finish.

## Port conflicts

Default exposed ports: `5432, 8080, 8081, 8082, 9000, 9001, 7077`. The
Hive Metastore (`9083`) is internal-only. If something on your machine
is already bound to one of the above:

```bash
docker compose down -v
# Edit docker-compose.yaml to remap the host port (e.g. "9999:8080")
./setup.sh
```

## Airflow UI returns 500 / DAG not showing up

The scheduler can take ~15s to re-parse `dags/generated.py` after a
YAML edit. Refresh the page. If still missing:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
# Anything here = a Python error in dag_factory.py or generated.py.
```

## Phase 1: I edited `tables.yaml` but `./check.sh` still says NOT ATTEMPTED

`./check.sh` is detecting the *number* of entries in your YAML. If
you copied prd.users but didn't change the `name`, the loader sees
duplicates and the count check is off. Make sure each entry has a
unique `name`.

## Phase 2 / 3: my fix looks right but the DAG run failed anyway

Always run `make reset` between attempts. The ledger and the
warehouse hold state across runs — a clean baseline avoids confusion.

## "Java gateway exited" or "InvalidClassException"

This used to bite during T10 development. If it shows up:

- pyspark version mismatch — check `airflow/requirements.txt` says
  `pyspark==3.5.6` (matches the cluster).
- airflow-scheduler memory — should be `mem_limit: 2g` in compose.
- `AIRFLOW__CORE__EXECUTE_TASKS_NEW_PYTHON_INTERPRETER` should be
  `"True"`.

If you suspect one of those flipped, rebuild:

```bash
docker compose build airflow-scheduler
docker compose up -d airflow-scheduler
```

## Full reset

The blunt instrument:

```bash
docker compose down -v          # drops all volumes
./setup.sh                       # fresh stack
```

This will wipe the ledger, the warehouse, and any chaos artifacts.
