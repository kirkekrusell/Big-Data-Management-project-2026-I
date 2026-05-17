-- ============================================================
--  Project 4 – Full Schema Initialization
--  Includes: pipeline_runs, audit_results, pipeline_metrics,
--  and traceability fields for existing tables.
-- ============================================================

---------------------------------------------------------------
-- 1. pipeline_runs — every DAG run is recorded here
---------------------------------------------------------------


CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS screens_metadata (
    screen_id TEXT PRIMARY KEY, 
    extraction_payload JSONB, 
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS screens_embeddings (
    screen_id TEXT, 
    model_name TEXT, 
    model_version TEXT, 
    embedding_kind TEXT, 
    embedding vector(384)
);

CREATE TABLE IF NOT EXISTS screens_review_queue (
    screen_id TEXT PRIMARY KEY
);



CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id UUID PRIMARY KEY,
    dag_run_id TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    status TEXT NOT NULL,  -- running / succeeded / failed / paused-by-audit
    limit_param INT NOT NULL,
    git_sha TEXT NOT NULL,
    clip_model_version TEXT NOT NULL,
    sbert_model_version TEXT NOT NULL,
    llm_model_version TEXT NOT NULL,
    prompt_version TEXT NOT NULL
);

---------------------------------------------------------------
-- 2. audit_results — optional but recommended
---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit_results (
    id SERIAL PRIMARY KEY,
    run_id UUID REFERENCES pipeline_runs(run_id),
    audit_name TEXT NOT NULL,
    passed BOOLEAN NOT NULL,
    details TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

---------------------------------------------------------------
-- 3. pipeline_metrics — health + data quality metrics
---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id SERIAL PRIMARY KEY,
    run_id UUID REFERENCES pipeline_runs(run_id),
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION,
    metric_text TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

---------------------------------------------------------------
-- 4. Modify existing tables: add run_id + source_fingerprint
---------------------------------------------------------------

-- screens_metadata
ALTER TABLE screens_metadata
    ADD COLUMN IF NOT EXISTS run_id UUID REFERENCES pipeline_runs(run_id),
    ADD COLUMN IF NOT EXISTS source_fingerprint TEXT NOT NULL;

-- screens_embeddings
ALTER TABLE screens_embeddings
    ADD COLUMN IF NOT EXISTS run_id UUID REFERENCES pipeline_runs(run_id),
    ADD COLUMN IF NOT EXISTS source_fingerprint TEXT NOT NULL;

-- screens_review_queue
ALTER TABLE screens_review_queue
    ADD COLUMN IF NOT EXISTS run_id UUID REFERENCES pipeline_runs(run_id),
    ADD COLUMN IF NOT EXISTS source_fingerprint TEXT NOT NULL;

---------------------------------------------------------------
-- 5. Primary key for screens_embeddings (idempotency + audit)
---------------------------------------------------------------
ALTER TABLE screens_embeddings
    DROP CONSTRAINT IF EXISTS screens_embeddings_pk;

ALTER TABLE screens_embeddings
    ADD CONSTRAINT screens_embeddings_pk
    PRIMARY KEY (screen_id, model_name, model_version, embedding_kind);

---------------------------------------------------------------
-- 6. Optional indexes for performance (recommended)
---------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_screens_metadata_run_id
    ON screens_metadata(run_id);

CREATE INDEX IF NOT EXISTS idx_screens_embeddings_run_id
    ON screens_embeddings(run_id);

CREATE INDEX IF NOT EXISTS idx_screens_review_queue_run_id
    ON screens_review_queue(run_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_run_id
    ON pipeline_metrics(run_id);

---------------------------------------------------------------
-- Done
---------------------------------------------------------------
