# Project 3 — CDC + Orchestrated Lakehouse Pipeline

Build a CDC pipeline that captures changes from a PostgreSQL database using Debezium,
lands them in an Apache Iceberg lakehouse (bronze → silver), **and** continues the
streaming taxi pipeline from Project 2 — now orchestrated end-to-end with Apache Airflow.

Your group's custom scenario is (or will be :-)) described in your repository's GitHub issue.

---

## What's in this template

| Path | Description |
|------|-------------|
| `compose.yml` | Kafka, MinIO, Iceberg REST catalog, PostgreSQL, Kafka Connect (Debezium), Airflow, Jupyter/PySpark |
| `produce.py` | Replays taxi parquet rows as JSON into the `taxi-trips` Kafka topic (same as Project 2) |
| `seed.py` | Creates source tables in PostgreSQL and inserts initial data |
| `simulate.py` | Continuously makes changes (INSERT, UPDATE, DELETE) to PostgreSQL, simulating a live OLTP workload |
| `dags/` | Place your Airflow DAG file(s) here — auto-loaded by the Airflow scheduler |
| `REPORT.md` | Template for the report you need to hand in |
| `.env.example` | Template for credentials — copy to `.env` and fill in values |
| `data/` | **Not in git.** Place the same taxi parquet files from Project 1 & 2 here |

The `data/` directory is git-ignored. You will use the same files as in previous projects:

| File | Description |
|------|-------------|
| `data/yellow_tripdata_2025-01.parquet` | NYC Yellow Taxi trips — January 2025 |
| `data/yellow_tripdata_2025-02.parquet` | NYC Yellow Taxi trips — February 2025 |
| `data/taxi_zone_lookup.parquet` | Pickup/dropoff zone names |

---

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

---

## What to build

Your pipeline has **two data paths**, both orchestrated by a single Airflow DAG:

```
┌─ Path A: CDC ──────────────────────────────────────────────────┐
│  PostgreSQL → Debezium → Kafka → Bronze (CDC) → Silver (MERGE) │
└────────────────────────────────────────────────────────────────┘

┌─ Path B: Streaming (from Project 2) ──────────────────────────┐
│  Taxi producer → Kafka → Bronze (taxi) → Silver → Gold         │
└────────────────────────────────────────────────────────────────┘

┌─ Airflow DAG orchestrates both paths ─────────────────────────┐
│  health_check → [bronze_cdc, bronze_taxi] → [silver_cdc,      │
│  silver_taxi] → gold_taxi → validation                         │
└────────────────────────────────────────────────────────────────┘
```

### Path A — CDC Pipeline (new)

#### 1. Debezium CDC connector

- Register a Debezium PostgreSQL connector via the Kafka Connect REST API.
- Configure it to capture changes from the source tables using log-based CDC (WAL).
- Verify that CDC events appear in Kafka topics (`dbserver1.public.<table>`).
- Handle the initial snapshot — document which snapshot mode you chose and why.

#### 2. Bronze CDC layer (raw CDC events)

- Read CDC events from Kafka using Spark.
- Parse the Debezium envelope correctly (extract from `$.payload.*` — the `schema + payload` wrapper).
- Write all events as-is to a bronze Iceberg table. Append-only — never update or delete rows in bronze.
- Include Kafka metadata (offset, partition, timestamp) alongside CDC fields (op, before, after, ts_ms).
- Handle tombstone messages (null-value records from deletes).

#### 3. Silver CDC layer (current-state mirror)

- Read from the bronze CDC table incrementally (only new events since last run).
- Deduplicate: keep only the latest event per primary key (`ROW_NUMBER` over `ts_ms DESC`).
- Apply `MERGE INTO` to the silver Iceberg table:
  - `op = 'd'` → DELETE the row
  - `op IN ('u', 'c', 'r')` → UPDATE if exists, INSERT if not
- After MERGE, silver should mirror the current state of the PostgreSQL source.
- Document your MERGE logic and explain why it is idempotent.

### Path B — Streaming Taxi Pipeline (improved from Project 2)

#### 4. Bronze → Silver → Gold for taxi data

- Same medallion pipeline as Project 2: raw Kafka events → cleaned/enriched → aggregated.
- Improve upon your Project 2 implementation based on feedback received.
- **New requirement:** this pipeline must now be triggered by Airflow, not run as a standalone streaming job.

### Orchestration (ties both paths together)

#### 5. Airflow DAG

- Create an Airflow DAG that orchestrates **both pipelines** end-to-end.
- The DAG should include at minimum:
  - A **health-check task** to verify the Debezium connector is running (REST API call).
  - **Bronze ingestion tasks** for both CDC and taxi data.
  - **Silver tasks** for both pipelines (MERGE for CDC, clean/enrich for taxi).
  - A **gold task** for the taxi aggregation.
  - A **validation task** that confirms silver CDC matches PostgreSQL source.
- **Scheduling:** Configure a reasonable schedule interval. Justify your choice — what freshness SLA does it support?
- **Retries and failure handling:** Configure task-level retries. If the MERGE task fails, downstream tasks should not run. If the connector health check fails, the DAG should alert and stop.
- **SLAs:** Set a time limit on the DAG. Configure at least one failure notification mechanism.
- **Idempotent re-runs:** Running the DAG twice for the same interval must produce the same result. This is critical for backfill scenarios.

### Bonus (not required)

#### 6. Schema evolution

- Add a column to the PostgreSQL source table while the pipeline is running.
- Show that Debezium detects the change, bronze stores both old and new events, silver evolves via `ALTER TABLE ADD COLUMN`.

---

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

---

## Deliverables

GitHub repository containing:

- **Code:** Spark notebooks or Python scripts, Airflow DAG file(s), connector configuration, seed/simulate scripts. Must run end-to-end.
- **Report** (`REPORT.md`).
- The Iceberg tables must be queryable after running the pipeline.
- The Airflow DAG must be visible and runnable in the Airflow UI.

## How it will be checked

The grading process:

1. Clone your repository.
2. Run `docker compose up`, `seed.py`, `simulate.py`, and `produce.py`.
3. Trigger the Airflow DAG or wait for the scheduled run.
4. Verify bronze CDC contains raw CDC events, silver CDC mirrors PostgreSQL.
5. Verify bronze/silver/gold taxi tables contain correct data.
6. Stop the simulator, make a manual change in PostgreSQL, trigger the DAG, verify silver reflects the change.
7. Check Airflow for DAG run history, task logs, and retry behavior.
8. Manually fail a task (e.g., stop Kafka Connect) and verify the DAG handles it correctly.

**Share your GitHub repository link in Moodle under Module 3 as soon as possible so custom scenarios can be assigned.**

---

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