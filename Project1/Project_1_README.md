# 1. Project Goal

The goal of this project is to build an incremental Spark ETL pipeline that processes NYC Yellow Taxi trip data stored as Parquet files. The pipeline runs end‑to‑end, automatically detects and processes new input files, cleans and deduplicates trip records, enriches them with taxi zone information, and produces a continuously growing output dataset.

The ETL job must behave correctly when new files are added and must remain idempotent: running it multiple times must not duplicate data that has already been processed.
# 2. Pipeline Overview

## The ETL pipeline performs the following steps:
### 2.1 Incremental Ingestion

    Reads Parquet files from data/inbox/

    Processes only files that have not been processed before

    Tracks processed files using a manifest file: state/manifest.json

### 2.2 Transformations

    Parses timestamps into Spark TimestampType

    Casts numeric fields where necessary

    Cleans data by removing:

        negative values

        null timestamps

        invalid or corrupted rows

    Deduplicates records using a composite key:
    VendorID + pickup/dropoff timestamps + pickup/dropoff LocationID

### 2.3 Enrichment

    Joins trip data with the lookup table: data/lookup/taxi_zone_lookup.parquet

    Adds pickup and dropoff zone names, boroughs, and service zones

    Uses broadcast joins for performance

### 2.4 Output

    Writes the final enriched dataset to: data/outbox/trips_enriched.parquet

    Uses append mode so the output grows incrementally

    Ensures no duplicates across runs thanks to the manifest

# 3. Manifest (state.json)

The manifest file keeps track of which input files have already been processed.
This prevents reprocessing and ensures idempotency.

Example:
json

{
  "processed_files": [
    "yellow_tripdata_2025-01.parquet",
    "yellow_tripdata_2025-02.parquet"
  ]
}

The manifest must be preserved between runs so the pipeline can continue incrementally.
# 4. Transformations (Details)
## 4.1 Type Parsing

The following fields are cast to Spark TimestampType:

    tpep_pickup_datetime

    tpep_dropoff_datetime

## 4.2 Cleaning Rules

Invalid rows are removed using the following rules:
Rule	Description
trip_distance >= 0	Removes negative distances
passenger_count >= 0	Removes invalid passenger counts
pickup/dropoff timestamps NOT NULL	Ensures valid trip times

Examples of removed “bad rows”:

    trip_distance = -1.2

    passenger_count = -3

    tpep_pickup_datetime = NULL

## 4.3 Deduplication

A trip is considered unique based on:

    VendorID

    tpep_pickup_datetime

    tpep_dropoff_datetime

    PULocationID

    DOLocationID

## 4.4 Derived Fields

    trip_duration_minutes

    pickup_date

## 4.5 Metadata

    source_file — input file name

    ingested_at — ETL timestamp

# 5. Enrichment

The pipeline enriches each trip with zone information from:
```data/lookup/taxi_zone_lookup.parquet```

Pickup Enrichment

    pickup_zone

    pickup_borough

    pickup_service_zone

Dropoff Enrichment

    dropoff_zone

    dropoff_borough

    dropoff_service_zone

Broadcast joins are used to reduce shuffle and improve performance.
# 6. Scenario: Last N Months Filter

The ETL supports a configuration parameter:

```N_MONTHS = <integer or None>```

If set (e.g., N_MONTHS = 3):

    The pipeline finds the latest pickup timestamp

    Computes a cutoff date = latest pickup − N months

    Filters trips where pickup_datetime >= cutoff

If N_MONTHS = None, no filtering is applied.
# 7. Output Dataset

The final dataset is written to:
```data/outbox/trips_enriched.parquet```

Write Mode

The job uses:

```mode("append")```

This ensures:

    New data is added

    Existing data is preserved

    No duplicates occur

Output Fields

    tpep_pickup_datetime

    tpep_dropoff_datetime

    PULocationID

    DOLocationID

    pickup_zone

    dropoff_zone

    passenger_count

    trip_distance

    trip_duration_minutes

    pickup_date

    source_file

    ingested_at

# 8. Correctness Evidence
## 8.1 Row Counts (example)

Stage	Count
Raw input	1,000,000
After cleaning	980,000
After deduplication	975,000
Final output	975,000

## 8.2 Bad Row Examples
Issue	Example
Negative distance	trip_distance = -1.2
Negative passengers	passenger_count = -3
Missing timestamps	tpep_pickup_datetime = NULL

# 9. Performance Evidence
## 9.1 Runtime

Measured using Spark UI and Python timers.
## 9.2 Spark Web UI Screenshots

Two screenshots are required:

    Job / Stage Overview

    Join or Write Stage (shuffle metrics)

## 9.3 Optimizations Implemented

    Broadcast joins

    Append mode output

    Avoiding unnecessary .count() calls

    Repartitioning before write when needed

# 10. How to Run the Pipeline
# Step 1 — Start Jupyter with Docker Compose

```docker compose up -d```

Open:
```http://localhost:8888```

Token:

```bdm```

Spark UI:

```http://localhost:4040```

# Step 2 — Run the Notebook

    Open the jupyter/ folder

    Open project_1.ipynb

    Run → Run All Cells

# 11. Output Location

All processed data is saved in:

```data/outbox/trips_enriched.parquet```

Each run appends only new, unprocessed data.
Deduplication ensures no duplicate records appear even after multiple runs.
