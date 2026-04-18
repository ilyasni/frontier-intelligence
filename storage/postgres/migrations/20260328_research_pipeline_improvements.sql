ALTER TABLE sources
    ADD COLUMN IF NOT EXISTS source_authority FLOAT DEFAULT 0.5,
    ADD COLUMN IF NOT EXISTS source_score FLOAT,
    ADD COLUMN IF NOT EXISTS source_score_updated_at TIMESTAMPTZ;

ALTER TABLE posts
    ADD COLUMN IF NOT EXISTS semantic_cluster_id TEXT;

CREATE INDEX IF NOT EXISTS idx_posts_semantic_cluster
    ON posts(semantic_cluster_id)
    WHERE semantic_cluster_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS semantic_clusters (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    cluster_key TEXT NOT NULL,
    title TEXT NOT NULL,
    representative_post_id TEXT,
    post_count INTEGER DEFAULT 0,
    doc_ids JSONB DEFAULT '[]',
    source_ids JSONB DEFAULT '[]',
    top_concepts JSONB DEFAULT '[]',
    evidence JSONB DEFAULT '[]',
    time_window TEXT DEFAULT '7d',
    embedding_version TEXT DEFAULT 'EmbeddingsGigaR',
    first_seen_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(workspace_id, cluster_key)
);

CREATE INDEX IF NOT EXISTS idx_semantic_clusters_workspace
    ON semantic_clusters(workspace_id);

CREATE INDEX IF NOT EXISTS idx_semantic_clusters_detected
    ON semantic_clusters(detected_at DESC);
