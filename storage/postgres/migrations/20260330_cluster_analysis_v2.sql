ALTER TABLE semantic_clusters
    ADD COLUMN IF NOT EXISTS source_count INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS representative_evidence JSONB DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS related_cluster_ids JSONB DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS lifecycle_state TEXT DEFAULT 'new',
    ADD COLUMN IF NOT EXISTS avg_relevance FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS avg_source_score FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS freshness_score FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS coherence_score FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS explainability JSONB DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS idx_semantic_clusters_lifecycle
    ON semantic_clusters(workspace_id, lifecycle_state, detected_at DESC);

ALTER TABLE trend_clusters
    ADD COLUMN IF NOT EXISTS source_diversity_score FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS freshness_score FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS evidence_strength_score FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS signal_score FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS signal_stage TEXT DEFAULT 'stable',
    ADD COLUMN IF NOT EXISTS source_count INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS semantic_cluster_ids JSONB DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS explainability JSONB DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS idx_trends_signal_stage
    ON trend_clusters(workspace_id, signal_stage, detected_at DESC);

CREATE TABLE IF NOT EXISTS emerging_signals (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    signal_key TEXT NOT NULL,
    title TEXT NOT NULL,
    signal_stage TEXT NOT NULL DEFAULT 'weak' CHECK (signal_stage IN ('weak', 'emerging', 'stable', 'fading')),
    signal_score FLOAT DEFAULT 0.0,
    confidence FLOAT DEFAULT 0.0,
    supporting_semantic_cluster_ids JSONB DEFAULT '[]',
    doc_ids JSONB DEFAULT '[]',
    source_ids JSONB DEFAULT '[]',
    source_count INTEGER DEFAULT 0,
    keywords JSONB DEFAULT '[]',
    evidence JSONB DEFAULT '[]',
    explainability JSONB DEFAULT '{}',
    recommended_watch_action TEXT,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    first_seen_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(workspace_id, signal_key)
);

CREATE INDEX IF NOT EXISTS idx_emerging_signals_workspace
    ON emerging_signals(workspace_id, detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_emerging_signals_stage
    ON emerging_signals(workspace_id, signal_stage, signal_score DESC);

CREATE TABLE IF NOT EXISTS cluster_runs (
    id TEXT PRIMARY KEY,
    workspace_id TEXT,
    stage TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'success', 'error')),
    thresholds JSONB DEFAULT '{}',
    summary JSONB DEFAULT '{}',
    metrics JSONB DEFAULT '{}',
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cluster_runs_workspace
    ON cluster_runs(workspace_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_cluster_runs_stage
    ON cluster_runs(stage, started_at DESC);
