CREATE EXTENSION IF NOT EXISTS vector;

-- ==========================================================
-- PIPELINE RUNS
-- ==========================================================

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id UUID PRIMARY KEY,
    dag_run_id TEXT NOT NULL,

    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,

    status TEXT NOT NULL,

    limit_param INTEGER NOT NULL,
    git_sha TEXT NOT NULL,

    clip_model_version TEXT NOT NULL,
    sbert_model_version TEXT NOT NULL,
    llm_model_version TEXT NOT NULL,
    prompt_version TEXT NOT NULL
);

-- ==========================================================
-- SCREEN METADATA
-- ==========================================================

CREATE TABLE IF NOT EXISTS screens_metadata (
    screen_id TEXT PRIMARY KEY,

    app_package TEXT,
    category TEXT,

    png_path TEXT NOT NULL,
    hierarchy_json_path TEXT NOT NULL,

    extraction_payload JSONB,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    run_id UUID REFERENCES pipeline_runs(run_id),

    source_fingerprint TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_screens_metadata_run_id
ON screens_metadata(run_id);

-- ==========================================================
-- EMBEDDINGS
-- ==========================================================

CREATE TABLE IF NOT EXISTS screens_embeddings (
    screen_id TEXT NOT NULL,

    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    embedding_kind TEXT NOT NULL,

    embedding VECTOR,

    run_id UUID REFERENCES pipeline_runs(run_id),

    source_fingerprint TEXT NOT NULL,

    CONSTRAINT screens_embeddings_pk
    PRIMARY KEY (
        screen_id,
        model_name,
        model_version,
        embedding_kind
    )
);

CREATE INDEX IF NOT EXISTS idx_screens_embeddings_run_id
ON screens_embeddings(run_id);

-- ==========================================================
-- REVIEW QUEUE
-- ==========================================================

CREATE TABLE IF NOT EXISTS screens_review_queue (
    screen_id TEXT PRIMARY KEY,

    run_id UUID REFERENCES pipeline_runs(run_id),

    source_fingerprint TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_screens_review_queue_run_id
ON screens_review_queue(run_id);

-- ==========================================================
-- AUDIT RESULTS
-- ==========================================================

CREATE TABLE IF NOT EXISTS audit_results (
    id SERIAL PRIMARY KEY,

    run_id UUID REFERENCES pipeline_runs(run_id),

    audit_name TEXT NOT NULL,
    audit_status TEXT NOT NULL,

    details JSONB,

    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ==========================================================
-- PIPELINE METRICS
-- ==========================================================

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id SERIAL PRIMARY KEY,

    run_id UUID REFERENCES pipeline_runs(run_id),

    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION,
    metric_text TEXT,

    created_at TIMESTAMPTZ DEFAULT NOW()
);