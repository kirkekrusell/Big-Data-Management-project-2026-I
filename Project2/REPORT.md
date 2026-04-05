# Project 2: Streaming Lakehouse Pipeline

## 1. Medallion layer schemas

### Bronze
_Table DDL or DataFrame schema. Explain what is stored and why it is kept as-is._
Schema (Iceberg table lakehouse.taxi.bronze):

The Bronze layer stores the raw Kafka events as-is, without parsing or transformation. Keeping the original JSON and Kafka metadata (topic, partition, offset) allows:

    full replay/debugging from the original source,
    auditability of what exactly was ingested,
    support for multi-topic scenarios (we can see which topic each row came from).
  
### Silver

_Table DDL or DataFrame schema. Explain what changed compared to bronze and why._
What changed vs Bronze and why:

    JSON parsed: value is parsed into typed columns (ints, doubles, timestamps) for analytics.
    Timestamps cast: pickup_ts and dropoff_ts are proper TIMESTAMP columns for windowing and time-based queries.
    Cleaning applied: invalid/null/duplicate rows are removed to improve data quality.
    Enrichment added: human-readable pickup/dropoff zones and boroughs are joined from the static lookup table.
    Kafka metadata kept: topic/partition/offset remain for traceability.

### Gold

_Table DDL or DataFrame schema. Explain the aggregation logic._

Aggregation logic:

    Read from lakehouse.taxi.silver as a stream.

    Use pickup_ts as event time.

    Apply a 1-hour tumbling window on pickup_ts.

    Group by window(pickup_ts, '1 hour') and pickup_zone.

    Compute:

        trip_count = count(*)

        avg_fare = avg(fare_amount)

    Select pickup_zone, window.start as pickup_hour, trip_count, avg_fare.

This produces a compact, business-friendly Gold table for hourly demand and pricing analysis by pickup zone.

## 2. Cleaning rules and enrichment

_List each cleaning rule (nulls, invalid values, deduplication key) with a brief justification._

Rule 1: Valid timestamps

    Condition:
    pickup_ts IS NOT NULL AND dropoff_ts IS NOT NULL

    Justification:
    Trips without valid pickup/dropoff times cannot be used for time-based analytics or windowing.

Rule 2: Positive trip distance

    Condition:
    trip_distance IS NOT NULL AND trip_distance > 0

    Justification:
    Zero or negative distances indicate invalid or corrupted records.

Rule 3: Non-negative monetary amounts

    Condition:
    total_amount IS NOT NULL AND total_amount >= 0

    (Optionally also fare_amount >= 0)

    Justification:
    Negative totals are usually errors or special cases not relevant for standard revenue analysis.

Rule 4: Reasonable passenger count

    Condition:
    passenger_count IS NOT NULL AND passenger_count > 0 AND passenger_count <= 6

    Justification:
    Filters out impossible values (0 or very large counts) and keeps realistic taxi trips.

Rule 5: Deduplication

    Key:
    VendorID, pickup_ts, dropoff_ts, PULocationID, DOLocationID, total_amount

    Operation:
    .dropDuplicates([...])

    Justification:
    If the same trip is ingested multiple times (e.g., replay, producer restart), this composite key is a good approximation of a unique trip and prevents double-counting.

_Describe the enrichment step (zone lookup join)._

tatic table: data/taxi_zone_lookup.parquet

Join keys:

    PULocationID → LocationID for pickup
    DOLocationID → LocationID for dropoff

Added columns:

    pickup_zone, pickup_borough
    dropoff_zone, dropoff_borough

Implementation:

    Two small dimension DataFrames (zones_pu, zones_do) with broadcast hints for efficient joins.

Justification:
Location IDs alone are not interpretable; zone and borough names make the data usable for business analysis and reporting.

## 3. Streaming configuration

_Describe:_
- _Checkpoint path and what it stores._

Checkpoint path

    Bronze: checkpoints/bronze
    Silver: checkpoints/silver
    Gold: checkpoints/gold

What it stores:

    Kafka offsets (per topic/partition) for the source.
    Progress information for each micro-batch.
    Iceberg commit metadata for the sink.

This allows exactly-once or at least no-duplicate behavior when restarting the streaming queries.
- _Trigger interval and why you chose it._

    Typical configuration:
    .trigger(processingTime="5 seconds")

Why 5 seconds:

    Small enough to keep latency low and show progress quickly in a lab setting.
    Large enough to avoid excessive overhead from too many tiny micro-batches.

- _Output mode (append/update/complete) and why._
    All sinks use: .outputMode("append")

Why append:

    Bronze, Silver, and Gold are append-only tables in this design.
    We are not updating or deleting existing rows, only adding new events and aggregates.
    Append mode is efficient and matches the medallion pattern for streaming ingestion.
    
- _Watermark (if used) and why._
    Applied in Gold layer:
    .withWatermark("pickup_ts", "1 hour")

Why:

    Tells Spark how long to wait for late events before finalizing windowed aggregates.
    Prevents unbounded state growth in the aggregation.
    1 hour is a reasonable tolerance for late-arriving taxi events in this context.
    
## 4. Gold table partitioning strategy

_Explain your partitioning choice. Why this column(s)? What query patterns does it optimize?_
```
CREATE TABLE IF NOT EXISTS lakehouse.taxi.gold (
  pickup_zone  STRING,
  pickup_hour  TIMESTAMP,
  trip_count   BIGINT,
  avg_fare     DOUBLE
)
USING iceberg
PARTITIONED BY (days(pickup_hour));
```
Why partition by days(pickup_hour)

    Most queries on the Gold table are expected to be time-based:

        “Show hourly trips for a given day/week.”

        “Compare average fares across days.”

    Partitioning by day:

        Prunes data efficiently when filtering by date or date range.
        Keeps partition count manageable (one partition per day, not per hour or zone).
        Works well with Iceberg’s hidden partitioning and snapshot management.
    
    Query patterns optimized:

    WHERE pickup_hour >= '2025-01-01' AND pickup_hour < '2025-01-08'
    WHERE date_trunc('day', pickup_hour) = '2025-01-15'
    
_Show the Iceberg snapshot history (query output or screenshot)._
```
SELECT *
FROM lakehouse.taxi.bronze.snapshots;
```
This shows:

    snapshot_id
    committed_at
    operation (e.g., append)
    manifest_list
    summary (including record counts, engine version, etc.)

It demonstrates that the table is managed by Iceberg and that each streaming batch creates a new snapshot.
## 5. Restart proof

    Start the Bronze, Silver, and Gold streaming queries.
    Let the producer (produce.py) run for some time.
    Check row counts:

    ```
    SELECT COUNT(*) FROM lakehouse.taxi.bronze;
    SELECT COUNT(*) FROM lakehouse.taxi.silver;
    SELECT COUNT(*) FROM lakehouse.taxi.gold;
    ```
    Stop the streaming queries (e.g., interrupt notebook cell or stop Spark).
    Restart the same streaming queries using the same checkpoint locations.
    Re-run the COUNT(*) queries.

Expected result:

    Row counts do not increase after restart if no new data was produced.
    This shows that:

        Kafka offsets were restored from the checkpoint.
        The pipeline did not reprocess already committed data.
        No duplicate rows were written to Bronze/Silver/Gold.
  
_Show that stopping and restarting the pipeline does not produce duplicates._
_Include row counts before and after restart._

## 6. Custom scenario
Produce January data to topic taxi-trips-january and February data to topic taxi-trips-february (use the existing --topic and --data flags). Write a single streaming query that subscribes to both using subscribePattern("taxi-trips-.*"). In REPORT.md, show that events from both topics land in the bronze table and explain how Spark tracks offsets across multiple topics.

_Explain and/or show how you solved the custom scenario from the GitHub issue._

Scenario:

    January data → topic taxi-trips-january
    February data → topic taxi-trips-february

Using produce.py:
```
python produce.py --topic taxi-trips-january --data data/yellow_tripdata_2025-01.parquet
python produce.py --topic taxi-trips-february --data data/yellow_tripdata_2025-02.parquet
```
Single streaming query for both topics:
python
```
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribePattern", "taxi-trips-.*")  # matches both topics
    .option("startingOffsets", "earliest")
    .load()
)
```
This feeds into the same Bronze pipeline as before.

Proof that both topics land in Bronze:
```
SELECT topic, COUNT(*) AS cnt
FROM lakehouse.taxi.bronze
GROUP BY topic;
```

How Spark tracks offsets across multiple topics:

    For each topic-partition pair, Spark maintains a separate offset.
    The checkpoint stores a map of offsets keyed by (topic, partition).
    On restart, Spark resumes from the last committed offsets for each topic and partition.
    This ensures exactly-once (or at least no-duplicate) processing even when consuming multiple topics with subscribePattern.
## 7. How to run

```bash
# Step 1: Start infrastructure
docker compose up -d

# Step 2: Start the producer
python produce.py

# January only
python project/produce.py --topic taxi-trips-january --data data/yellow_tripdata_2025-01.parquet

# February only
python project/produce.py --topic taxi-trips-february --data data/yellow_tripdata_2025-02.parquet

# Or loop one of them
python project/produce.py --topic taxi-trips-january --data data/yellow_tripdata_2025-01.parquet --loop

# Step 3: Run the pipeline
In Jupyter (http://localhost:8888):

    Open the notebook for Project 2.

    Run cells in order:

        Spark configuration
        Bronze streaming query
        Silver streaming query
        Gold streaming query

    Optionally open Spark UI at http://localhost:4040 to monitor.

There is no extra command-line step; the pipeline is started by running the notebook cells.
```

_Add any additional steps or dependencies needed to reproduce your results._

_Include the `.env` values the grader should use to run your project._
env values
```bash
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=dbmgroupc
JUPYTER_TOKEN=bdm2
```
