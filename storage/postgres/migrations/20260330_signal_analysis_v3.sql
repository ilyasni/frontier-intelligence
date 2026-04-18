ALTER TABLE trend_clusters
    ADD COLUMN IF NOT EXISTS velocity_score FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS acceleration_score FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS baseline_rate FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS current_rate FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS change_point_count INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS change_point_strength FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS has_recent_change_point BOOLEAN DEFAULT FALSE;

ALTER TABLE emerging_signals
    ADD COLUMN IF NOT EXISTS velocity_score FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS acceleration_score FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS baseline_rate FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS current_rate FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS change_point_count INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS change_point_strength FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS has_recent_change_point BOOLEAN DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS signal_time_series (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    entity_kind TEXT NOT NULL CHECK (entity_kind IN ('semantic', 'trend', 'emerging')),
    entity_id TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    doc_count INTEGER DEFAULT 0,
    source_count INTEGER DEFAULT 0,
    avg_relevance FLOAT DEFAULT 0.0,
    avg_source_score FLOAT DEFAULT 0.0,
    freshness_score FLOAT DEFAULT 0.0,
    window_rate FLOAT DEFAULT 0.0,
    metadata_json JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(workspace_id, entity_kind, entity_id, window_start, window_end)
);

CREATE INDEX IF NOT EXISTS idx_signal_time_series_entity
    ON signal_time_series(workspace_id, entity_kind, entity_id, window_start DESC);
