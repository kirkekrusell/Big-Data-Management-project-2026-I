
# Project 4 README
## 1. Quickstart
In order to follow our steps, do the following:
1. clone the repositary
`git clone <repo-url>`
2. open Docker Desktop and Terminal
In terminal: 
- `cd ...` to the project folder
- `make up`
- `make pull-models` - this step may take some time
Access the DAG and trigger runs.
3. Open http://localhost:8080/
4. Trigger the DAG
Run the pipeline with a LIMIT parameter.
    Click the DAG → Trigger DAG
    Set LIMIT=5 for development
    Watch tasks execute in the Graph view
<img width="1353" height="434" alt="image" src="https://github.com/user-attachments/assets/fbe539ab-2299-4bbf-91db-3534f592f969" />
5. Shut everything down
Afterwards `make clean` and all the conteiners are closed and cachew deleted (docker compose down -v)

## 2. Project Structure
```
Project4/
│
├── dags/
│   └── rico_pipeline_dag.py        # Thin DAG orchestration
│
├── pipeline/                       # All business logic
│   ├── ingest.py
│   ├── parse.py
│   ├── embed.py
│   ├── extract.py
│   ├── load.py
│   ├── audit.py
│   ├── eval.py
│   └── utils.py
│
├── migrations/
│   └── 001_init.sql                # pipeline_runs, metrics, audit, table alters
│
├── sql/                            # (optional helper SQL)
│
├── docker-compose.yml
├── Dockerfile
├── Makefile
├── requirements.txt
└── README.md
```
## 3. Schema Overview

Defined in migrations/init.sql.
Tables

screens_metadata 
 ```
Stores raw metadata, parsed text, and LLM extraction results.
Columns include:
screen_id, app_package, category, png_path, hierarchy_json_path,
extraction_payload, prompt_version, confidence, timestamps.
```
screens_embeddings
```
Stores CLIP and SBERT embeddings.
Primary key:
(screen_id, model_name, model_version, embedding_kind)
```
screens_review_queue
```
Screens that require manual review (LLM low confidence, etc).
```
 screens_eval  
 ```
 Stores evaluation results (recall@5, model version, number of queries).
 ```
## 4. Pipeline Stages
The DAG runs:
```
init_run → ingest → parse → [embed_image, embed_text, extract] → load → audit → eval → finish_run
```
What each stage does

    init_run — creates run_id, inserts into pipeline_runs, sends Slack “run started”
    ingest — loads RICO dataset, stores PNG + JSON in MinIO, inserts metadata rows
    parse — parses view hierarchy, extracts text representation
    embed_image — CLIP embeddings (image)
    embed_text — SBERT embeddings (text)
    extract — LLM JSON extraction via Ollama
    load — computes row‑in/out metrics and writes to pipeline_metrics
    audit — duplicate detection (circuit breaker)
    eval — minimal recall@5 self‑test
    finish_run — marks run as success/failure and sends Slack summary

All inserts use ON CONFLICT DO NOTHING for idempotency.
## 5. Audit (Duplicate Detection)

Audit fails the run if:
screens_embeddings

Duplicate:
```
(screen_id, model_name, model_version, embedding_kind)
```
screens_metadata

Duplicate:
```
screen_id
```
If duplicates exist:

    audit task fails loudly
    eval is skipped
    run is marked paused-by-audit
    duplicates are logged and stored in audit_results

## 6. Metrics (Observability)

At end of run, the pipeline writes metrics to pipeline_metrics:
Pipeline health

    per‑task duration
    per‑task row in/out
    retries
    total run duration
    final status

Data quality

    metadata: row count, % extraction_payload, % confidence ≥ 0.5, % in review queue
    embeddings: row count per model, avg dimensionality, % zero‑norm vectors
    distinct app_package + category

A one‑screen summary is logged at the end of the run.

