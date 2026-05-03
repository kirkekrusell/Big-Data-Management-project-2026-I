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


## REPORT

Because we ran into some problems getting our cdc_taxi_pipeline DAG to actually work, out report parts that cover everything after running this DAG is an idea. But we still wanted to include our thoughts how it would work if we would have been able to debug this technical problem.

### 1. CDC correctness

- Show that silver mirrors PostgreSQL (compare row counts and spot-check specific rows).
    - The pipeline ensures that the Iceberg Silver layer mirrors the source PostgreSQL database. The script validate_cdc_counts.py performs an automated row count comparison between the source tables (customers, drivers) and the Iceberg silver tables. If the counts do not match, a RuntimeError is raised, causing the Airflow pipeline to fail.
- Show that deletes in PostgreSQL are reflected in silver.
    - The silver_cdc.py script implements MERGE INTO logic. It specifically handles Debezium delete operations (op = 'd') by executing a DELETE command on the target Iceberg table when a match is found.
- Show that the pipeline is idempotent — running the DAG twice with no new changes produces the same state.
    - *Because our DAG failed, we can't show that*

### 2. Lakehouse design

- Describe the schema of bronze CDC, silver CDC, bronze taxi, silver taxi, and gold tables.
    - Bronze CDC: Contains raw Debezium payload fields including op (operation type), before, after, ts_ms, and source table metadata.
    - Silver CDC: A cleaned table with a flattened schema: id (INT), name (STRING), and email (STRING).
    - Bronze Taxi: Raw Parquet data from S3/MinIO with an added unique trip_id generated via monotonically_increasing_id().
    - Silver Taxi: Refined data with casted types (timestamps, doubles), filtered for validity (e.g., passenger_count > 0), and joined with zone lookups to include pickup_zone_name and dropoff_zone_name.
    - Gold Taxi: An aggregated table showing trips_count, avg_fare_amount, and avg_trip_distance, grouped by pickup_zone_name and pickup_hour.
- Show Iceberg snapshot history for the silver CDC table.
    - <img width="561" height="145" alt="image" src="https://github.com/user-attachments/assets/202cb744-7676-44a9-8d5d-733d043a5c51" />
- Explain how you would roll back a bad MERGE using Iceberg time travel.
    - If a MERGE operation introduces corrupt data, Iceberg allows a rollback to a previous known good state using the system procedure: ```CALL lakehouse.system.rollback_to_snapshot('cdc.silver_customers', <snapshot_id>)```

### 3. Orchestration design

- Include a screenshot of your Airflow DAG (graph view).
    - Here is where we ran into a problem and could not debug why airflow and spark wouldn't connect correctly.
    - <img width="1260" height="226" alt="image" src="https://github.com/user-attachments/assets/7009f637-eaf6-4d4a-a9f9-296238e49534" />
- Explain the task dependency chain and why tasks are in this order.
    - The workflow follows this logical sequence:
        - start → connector_health → [bronze_cdc, bronze_taxi] → [silver_cdc, silver_taxi] → [validate, gold_taxi] → end
    - The workflow begins with the connector_health sensor to verify the Kafka Debezium connector is active, preventing Spark jobs from running against an unavailable data stream. Once cleared, the DAG executes the CDC and Taxi branches in parallel to optimize runtime, as their sources are independent. Within each branch, tasks must follow a strict sequential order—Bronze to Silver to Gold—because each layer requires the refined output of the previous one for transformations like deduplication and aggregation. Finally, the validate task serves as a quality gate at the end of the CDC chain to ensure Lakehouse record counts match the PostgreSQL source before the pipeline is marked successful.
- Gold/Validation tasks.
- Describe your scheduling strategy and what freshness SLA it supports.
    - The DAG is configured with a schedule_interval="*/15 * * * *" (every 15 minutes). This frequency is designed to support a 15-minute data freshness SLA, ensuring that insights in the Gold layer reflect near real-time changes from the PostgreSQL source and the taxi data repository.
- Describe retry and failure handling. Show at least one example of a failed task and how the DAG handled it.
    - The pipeline includes built-in resilience with retries: 1 and a 3-minute delay.
- Show DAG run history — at least 3 successful consecutive runs.
    - *Did not manage to get a successful run for our DAG*
- Explain how backfill works for your DAG.
    - The DAG is set to catchup=False. Once the Spark connection issue is resolved, a manual backfill will be required to process the data missed during the downtime.  Because our logic uses MERGE and overwritePartitions(), the backfill process is idempotent and will safely reconcile the data without duplicates.

### 4. Streaming pipeline (taxi)

- Show that the taxi bronze/silver/gold pipeline works correctly (same criteria as Project 2).
- Show improvements over Project 2 based on feedback.

### 5. Custom scenario

- Explain and/or show how you solved the custom scenario from the GitHub issue.

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
