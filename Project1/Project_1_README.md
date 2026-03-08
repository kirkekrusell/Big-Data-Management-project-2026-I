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
## 8.1 Row Counts 

yellow_tripdata_2025-01.parquet

<img width="603" height="675" alt="image" src="https://github.com/user-attachments/assets/ec9e3fef-8520-4567-8f8e-a27ebc1f5a06" />
<img width="1333" height="682" alt="image" src="https://github.com/user-attachments/assets/f2cc4db6-7963-4c30-8f84-eb3d554e5288" />
<img width="1322" height="730" alt="image" src="https://github.com/user-attachments/assets/06ea365d-b569-45d7-9c01-8aeccd059f88" />

yellow_tripdata_2025-02.parquet

<img width="809" height="733" alt="image" src="https://github.com/user-attachments/assets/1e7975b2-3e5a-4ae3-a9c9-e8e2655f37e3" />
<img width="1357" height="711" alt="image" src="https://github.com/user-attachments/assets/eeb0146f-6f98-47f4-aaf5-e722238f37ba" />
<img width="1345" height="721" alt="image" src="https://github.com/user-attachments/assets/876a2d0c-5540-4e0e-a7a3-530d833e469c" />

Final result 

<img width="1338" height="708" alt="image" src="https://github.com/user-attachments/assets/46c1d055-89b6-4e83-be26-939848561948" />

# 9. Performance Evidence
## 9.1 Runtime

Measured using Spark UI and Python timers.
<img width="312" height="35" alt="image" src="https://github.com/user-attachments/assets/a1b765b8-15c8-4df1-a874-1b40e7cf5db0" />

## 9.2 Spark Web UI Screenshots

Two screenshots are required:

    Job / Stage Overview
<img width="1837" height="719" alt="image" src="https://github.com/user-attachments/assets/0108949d-eaf8-4e49-8311-d24777933635" />

    Join or Write Stage (shuffle metrics)

Shuffle write time

<img width="843" height="361" alt="image" src="https://github.com/user-attachments/assets/1dbd4d65-879e-447f-9927-20fc705faafc" />
<img width="818" height="394" alt="image" src="https://github.com/user-attachments/assets/45668828-ddd9-4f48-a32c-6287ba129b60" />

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
