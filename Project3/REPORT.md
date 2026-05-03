# Project 3 REPORT

## Setup

### 1. Configure credentials

```bash
cp .env.example .env
# Edit .env — change all default passwords before starting the stack
```

The `.env` file is git-ignored and never committed.
You need to change all the default secrets, and provide them in `REPORT.md` section 8 in your project submission.

### 2. Place the data files

Same taxi data as Project 2:

```
project_3/
└── data/
    ├── yellow_tripdata_2025-01.parquet
    ├── yellow_tripdata_2025-02.parquet
    └── taxi_zone_lookup.parquet
```

### 3. Start the stack

```bash
docker compose up -d
```

Allow ~30 seconds for all services to become ready. PostgreSQL, Kafka, Kafka Connect,
MinIO, Iceberg catalog, Airflow, and Jupyter all need to start in order.

### 4. Verify services

```bash
docker ps
```

You should see these services running:

| Container | Role |
|-----------|------|
| `kafka` | Message broker (KRaft, no ZooKeeper) |
| `postgres` | OLTP source database for CDC |
| `connect` | Kafka Connect with Debezium PostgreSQL connector |
| `minio` | S3-compatible object storage for Iceberg |
| `minio_init` | One-shot bucket creation (exited is OK) |
| `iceberg-rest` | Iceberg REST catalog |
| `airflow-webserver` | Airflow UI |
| `airflow-scheduler` | Airflow DAG scheduler |
| `jupyter` | Jupyter + PySpark |

### 5. Seed the PostgreSQL source

```bash
docker exec jupyter python /home/jovyan/project/seed.py
```

This creates the source tables and inserts initial data. Verify in the Jupyter notebook:

```python
pg_execute("SELECT * FROM customers ORDER BY id;", fetch=True)
```

### 6. Start the taxi producer (same as Project 2)

```bash
docker exec jupyter python /home/jovyan/project/produce.py --loop
```

### 7. Start the change simulator

```bash
docker exec jupyter python /home/jovyan/project/simulate.py
```

This continuously makes random inserts, updates, and deletes to the PostgreSQL
source tables, simulating a live application.

### 8. Open services

| Service | URL | Credentials |
|---------|-----|-------------|
| Jupyter | http://localhost:8888 | token: `JUPYTER_TOKEN` from `.env` |
| Airflow | http://localhost:8080 | `AIRFLOW_USER` / `AIRFLOW_PASSWORD` from `.env` |
| Spark UI | http://localhost:4040 | — |
| MinIO Console | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` from `.env` |
| Kafka Connect API | http://localhost:8083 | — |
| Iceberg REST API | http://localhost:8181/v1/namespaces | — |

### 9. Stop the stack

```bash
docker compose down          # keeps MinIO data (named volume)
docker compose down -v       # also deletes stored Iceberg tables
```



## What is graded

Create a report (`REPORT.md`, max ~3 pages). Use the template provided.

### 1. CDC correctness

- Show that silver mirrors PostgreSQL (compare row counts and spot-check specific rows).
- Show that deletes in PostgreSQL are reflected in silver.
- Show that the pipeline is idempotent — running the DAG twice with no new changes produces the same state.

### 2. Lakehouse design

- Describe the schema of bronze CDC, silver CDC, bronze taxi, silver taxi, and gold tables.
- Show Iceberg snapshot history for the silver CDC table.
- Explain how you would roll back a bad MERGE using Iceberg time travel.

### 3. Orchestration design

- Include a screenshot of your Airflow DAG (graph view).
- Explain the task dependency chain and why tasks are in this order.
- Describe your scheduling strategy and what freshness SLA it supports.
- Describe retry and failure handling. Show at least one example of a failed task and how the DAG handled it.
- Show DAG run history — at least 3 successful consecutive runs.
- Explain how backfill works for your DAG.

### 4. Streaming pipeline (taxi)

- Show that the taxi bronze/silver/gold pipeline works correctly (same criteria as Project 2).
- Show improvements over Project 2 based on feedback.

### 5. Custom scenario

- Explain and/or show how you solved the custom scenario from the GitHub issue.



## Grading checklist (self-review before submission)

- [ ] `docker compose up` + seed + simulate + produce + run DAG end-to-end without errors
- [ ] Debezium connector is registered and RUNNING
- [ ] Bronze CDC table contains raw Debezium events with correct op, before, after fields
- [ ] Silver CDC table matches PostgreSQL source (row count + spot check)
- [ ] Deletes in PostgreSQL are reflected in silver CDC
- [ ] Running the DAG twice produces the same silver state (idempotent)
- [ ] Taxi bronze/silver/gold tables are correct (improved from Project 2)
- [ ] Airflow DAG is visible in the UI with correct task dependencies
- [ ] At least 3 successful DAG runs shown
- [ ] Retry/failure handling configured and documented
- [ ] Iceberg snapshot history shown in REPORT.md
- [ ] Custom scenario implemented and documented
- [ ] REPORT.md answers all required sections
- [ ] `.env` values provided in REPORT.md section 8

---

## Troubleshooting

**Debezium connector FAILED**
Check `docker compose logs connect` for errors. Common causes: PostgreSQL not reachable,
wrong credentials, `wal_level` not set to `logical`, replication slot already exists from
a previous run.

**CDC events have all NULL fields**
You are parsing from the top level instead of `$.payload.*`. Debezium wraps events in a
`{"schema": {...}, "payload": {...}}` envelope. Extract from `$.payload.op`,
`$.payload.after.id`, etc.

**PostgreSQL replication slot growing**
If the Debezium connector is stopped for a long time, PostgreSQL retains WAL segments.
Check with: `SELECT slot_name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) FROM pg_replication_slots;`

**Airflow DAG not appearing**
Place your DAG `.py` file in the `dags/` directory. The scheduler scans this folder.
Check `docker compose logs airflow-scheduler` for import errors.

**`Failed to find data source: kafka`**
Check `PYSPARK_SUBMIT_ARGS` in `compose.yml` — versions must match your Spark version.

**Iceberg table not found after restart**
Tables are stored in MinIO (persistent named volume). They survive container restarts
unless you run `docker compose down -v`.
