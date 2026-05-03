# Project 3 REPORT

## Setup


### 1. Place the data files

Same taxi data as Project 2:

```
project_3/
└── data/
    ├── yellow_tripdata_2025-01.parquet
    ├── yellow_tripdata_2025-02.parquet
    └── taxi_zone_lookup.parquet
```

### 2. Start the stack

```bash
docker compose up -d
```

### 3. Verify services

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

### 4. Seed the PostgreSQL source

```bash
docker exec jupyter python /home/jovyan/project/seed.py
```

### 5. Start the simulator (keep it running in a separate terminal):

```bash
docker exec jupyter python /home/jovyan/project/simulate.py
```

### 6. Start the taxi producer (same as Project 2)

```bash
docker exec jupyter python /home/jovyan/project/produce.py --loop
```
OR do it in Jupyter terminal (in Docker)

```bash
python project/produce.py             
python project/produce.py --loop     
python project/produce.py --rate 20   
python project/produce.py --data data/yellow_tripdata_2025-02.parquet  
```

### 7. Register the Debezium Connector

```bash
curl.exe -X POST http://localhost:8083/connectors -H "Content-Type: application/json" --data-binary "@cdc.json"
```

### 8. Check the Connector Health

```bash
Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:8083/connectors/cdc-connector/status" | Select-Object -ExpandProperty Content
```


### 9. Verify Data in Kafka (Manual check)

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic dbserver1.public.customers --from-beginning --max-messages 3
```

### 10. Open services

| Service | URL | Credentials |
|---------|-----|-------------|
| Jupyter | http://localhost:8888 | token: `JUPYTER_TOKEN` from `.env` |
| Airflow | http://localhost:8080 | `AIRFLOW_USER` / `AIRFLOW_PASSWORD` from `.env` |
| Spark UI | http://localhost:4040 | — |
| MinIO Console | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` from `.env` |
| Kafka Connect API | http://localhost:8083 | — |
| Iceberg REST API | http://localhost:8181/v1/namespaces | — |

### 11. If airflow doesn't open with password in env, overwrite it with the same password (replace USER and PASS with username and password in .env file.)

```bash 
docker compose exec airflow airflow users reset-password --username USER --password PASS
```

### 12. Stop the stack

```bash
docker compose down          # keeps MinIO data (named volume)
docker compose down -v       # also deletes stored Iceberg tables
```

### Notes
If connector_health fails due to airflow.exceptions.AirflowNotFoundException: The conn_id `debezium_connect` isn't defined then do the following in the Airflow UI:
1. Open your Airflow Web UI (typically http://localhost:8080).

2. Navigate to Admin > Connections.

3. Click the + (plus) icon to add a new connection.

4. Enter the following details:

  -   Connection Id: debezium_connect

  -   Connection Type: HTTP

  -   Host: connect (this is the service name for Kafka Connect)

  -   Port: 8083

5. Click Save.


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
