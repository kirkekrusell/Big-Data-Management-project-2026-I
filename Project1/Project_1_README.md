# Project Goal

The goal of this project is to build an incremental Spark ETL pipeline that processes NYC taxi trip data stored as Parquet files. The pipeline is designed to run end-to-end, handle new files automatically, clean and deduplicate data, enrich trips with taxi zone information, and produce a continuously growing output dataset.

The job must work correctly when new files are added and must not duplicate data when it is run multiple times

# Pipeline Overview

The ETL pipeline performs the following steps:
1) Read new files from ` data/inbox/ ` 
* Only processes files that have not been processed before
* Tracks processed files using a ` state.json `  manifest
2) Transform the data
* Parse timestamps (convert to datetime)
* Convert numeric columns (e.g.,` amount `, ` value `)
* Clean data: remove invalid, null, or negative values
* Deduplicate records using a defined key (`id + timestamp`)
3) Enrich trips with taxi zone information
* Merge trip data with `taxi_zone_lookup.parquet`
4) Write output
* Save processed data to `data/outbox/`
* Output grows incrementally, without duplicating records from previous runs

Manifest (`state.json`)
The `state.json` file acts as a manifest, keeping track of which files in `data/inbox/` have already been processed. This prevents reprocessing of the same file multiple times.

Example:
 ```
{
  "processed_files": ["taxi_zone_lookup.parquet", "trip_data_january.parquet"]
}
 ```
# How to run
---

# Step 1: Start Jupyter with Docker Compose
In the folder that contains compose.yml, run:

```
docker compose up -d
```

Then open:
- http://localhost:8888

Token:
- bdm

After running the notebook:
- Spark UI: http://localhost:4040

---

# Step 2: Open and run the notebook

In JupyterLab:
1. Open the jupyter/ folder
2. Open project_1.ipynb
3. Run → Run All Cells

# Output
All processed data is saved in data/outbox/ as Parquet files.
Each new run appends only new, unprocessed data.
Deduplication ensures no duplicate records even after multiple runs.

# Notes
The pipeline is incremental: new files can be added to data/inbox/ at any time. Only unprocessed files will be transformed and added to the output. `state.json` must be kept intact to track progress
