-- Frontier Intelligence — PostgreSQL Schema
-- Idempotent: safe to re-run

CREATE TABLE IF NOT EXISTS workspaces (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    categories JSONB DEFAULT '[]',
    relevance_weights JSONB DEFAULT '{}',
    design_lenses JSONB DEFAULT '[]',
    cross_workspace_bridges JSONB DEFAULT '[]',
    extra JSONB DEFAULT '{}',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    source_type TEXT NOT NULL CHECK (source_type IN ('telegram', 'rss', 'web', 'api', 'email', 'habr')),
    name TEXT NOT NULL,
    url TEXT,
    tg_channel TEXT,
    tg_account_idx INTEGER DEFAULT 0,
    schedule_cron TEXT DEFAULT '*/5 * * * *',
    is_enabled BOOLEAN DEFAULT TRUE,
    proxy_config JSONB,
    extra JSONB DEFAULT '{}',
    source_authority FLOAT DEFAULT 0.5,
    source_score FLOAT,
    source_score_updated_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sources_workspace ON sources(workspace_id);
CREATE INDEX IF NOT EXISTS idx_sources_enabled ON sources(is_enabled) WHERE is_enabled = TRUE;

CREATE TABLE IF NOT EXISTS source_checkpoints (
    source_id TEXT PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
    cursor_json JSONB DEFAULT '{}',
    etag TEXT,
    last_modified TEXT,
    last_seen_published_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    last_error TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS source_runs (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('running', 'success', 'error')),
    fetched_count INTEGER DEFAULT 0,
    emitted_count INTEGER DEFAULT 0,
    error_text TEXT
);

CREATE INDEX IF NOT EXISTS idx_source_runs_source_started ON source_runs(source_id, started_at DESC);

CREATE TABLE IF NOT EXISTS posts (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    source_id TEXT NOT NULL REFERENCES sources(id),
    external_id TEXT NOT NULL,
    grouped_id TEXT,
    content TEXT NOT NULL,
    category TEXT,
    relevance_score FLOAT,
    has_media BOOLEAN DEFAULT FALSE,
    media_urls JSONB DEFAULT '[]',
    published_at TIMESTAMPTZ,
    tags JSONB DEFAULT '[]',
    semantic_cluster_id TEXT,
    extra JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source_id, external_id)
);

CREATE INDEX IF NOT EXISTS idx_posts_workspace ON posts(workspace_id);
CREATE INDEX IF NOT EXISTS idx_posts_source ON posts(source_id);
CREATE INDEX IF NOT EXISTS idx_posts_published ON posts(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_posts_category ON posts(category) WHERE category IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_posts_grouped ON posts(grouped_id) WHERE grouped_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_posts_semantic_cluster ON posts(semantic_cluster_id) WHERE semantic_cluster_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS indexing_status (
    post_id TEXT PRIMARY KEY REFERENCES posts(id),
    embedding_status TEXT DEFAULT 'pending' CHECK (embedding_status IN ('pending', 'done', 'dropped', 'error')),
    graph_status TEXT DEFAULT 'pending' CHECK (graph_status IN ('pending', 'done', 'skipped', 'error')),
    vision_status TEXT DEFAULT 'pending' CHECK (vision_status IN ('pending', 'done', 'skipped', 'error')),
    retry_count INTEGER DEFAULT 0,
    error_message TEXT,
    qdrant_point_id TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_indexing_embedding_status ON indexing_status(embedding_status);
CREATE INDEX IF NOT EXISTS idx_indexing_retry ON indexing_status(retry_count) WHERE retry_count > 0;

CREATE TABLE IF NOT EXISTS trend_clusters (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    cluster_key TEXT NOT NULL,
    pipeline TEXT NOT NULL CHECK (pipeline IN ('reactive', 'stable')),
    title TEXT NOT NULL,
    insight TEXT,
    opportunity TEXT,
    time_horizon TEXT,
    burst_score FLOAT DEFAULT 0.0,
    coherence FLOAT DEFAULT 0.0,
    novelty FLOAT DEFAULT 0.0,
    source_diversity_score FLOAT DEFAULT 0.0,
    freshness_score FLOAT DEFAULT 0.0,
    evidence_strength_score FLOAT DEFAULT 0.0,
    velocity_score FLOAT DEFAULT 0.0,
    acceleration_score FLOAT DEFAULT 0.0,
    baseline_rate FLOAT DEFAULT 0.0,
    current_rate FLOAT DEFAULT 0.0,
    change_point_count INTEGER DEFAULT 0,
    change_point_strength FLOAT DEFAULT 0.0,
    has_recent_change_point BOOLEAN DEFAULT FALSE,
    signal_score FLOAT DEFAULT 0.0,
    signal_stage TEXT DEFAULT 'stable',
    doc_count INTEGER DEFAULT 0,
    source_count INTEGER DEFAULT 0,
    doc_ids JSONB DEFAULT '[]',
    semantic_cluster_ids JSONB DEFAULT '[]',
    keywords JSONB DEFAULT '[]',
    explainability JSONB DEFAULT '{}',
    category TEXT,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(workspace_id, cluster_key)
);

CREATE INDEX IF NOT EXISTS idx_trends_workspace ON trend_clusters(workspace_id);
CREATE INDEX IF NOT EXISTS idx_trends_pipeline ON trend_clusters(pipeline);
CREATE INDEX IF NOT EXISTS idx_trends_burst ON trend_clusters(burst_score DESC);
CREATE INDEX IF NOT EXISTS idx_trends_detected ON trend_clusters(detected_at DESC);

CREATE TABLE IF NOT EXISTS trend_alerts (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    trend_cluster_id TEXT NOT NULL,
    cluster_key TEXT NOT NULL,
    alert_kind TEXT NOT NULL,
    reason TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'sent', 'error')),
    score FLOAT DEFAULT 0.0,
    message TEXT,
    attempts INTEGER DEFAULT 1,
    last_error TEXT,
    sent_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(workspace_id, cluster_key, alert_kind)
);

CREATE INDEX IF NOT EXISTS idx_trend_alerts_sent_at
    ON trend_alerts(sent_at DESC)
    WHERE status = 'sent';

CREATE INDEX IF NOT EXISTS idx_trend_alerts_cluster
    ON trend_alerts(workspace_id, trend_cluster_id, alert_kind);

CREATE TABLE IF NOT EXISTS semantic_clusters (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    cluster_key TEXT NOT NULL,
    title TEXT NOT NULL,
    representative_post_id TEXT,
    post_count INTEGER DEFAULT 0,
    source_count INTEGER DEFAULT 0,
    doc_ids JSONB DEFAULT '[]',
    source_ids JSONB DEFAULT '[]',
    top_concepts JSONB DEFAULT '[]',
    evidence JSONB DEFAULT '[]',
    representative_evidence JSONB DEFAULT '{}',
    related_cluster_ids JSONB DEFAULT '[]',
    lifecycle_state TEXT DEFAULT 'new',
    avg_relevance FLOAT DEFAULT 0.0,
    avg_source_score FLOAT DEFAULT 0.0,
    freshness_score FLOAT DEFAULT 0.0,
    coherence_score FLOAT DEFAULT 0.0,
    explainability JSONB DEFAULT '{}',
    time_window TEXT DEFAULT '7d',
    embedding_version TEXT DEFAULT 'EmbeddingsGigaR',
    first_seen_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(workspace_id, cluster_key)
);

CREATE INDEX IF NOT EXISTS idx_semantic_clusters_workspace ON semantic_clusters(workspace_id);
CREATE INDEX IF NOT EXISTS idx_semantic_clusters_detected ON semantic_clusters(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_semantic_clusters_lifecycle ON semantic_clusters(workspace_id, lifecycle_state, detected_at DESC);

CREATE TABLE IF NOT EXISTS emerging_signals (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    signal_key TEXT NOT NULL,
    title TEXT NOT NULL,
    signal_stage TEXT NOT NULL DEFAULT 'weak' CHECK (signal_stage IN ('weak', 'emerging', 'stable', 'fading')),
    signal_score FLOAT DEFAULT 0.0,
    confidence FLOAT DEFAULT 0.0,
    velocity_score FLOAT DEFAULT 0.0,
    acceleration_score FLOAT DEFAULT 0.0,
    baseline_rate FLOAT DEFAULT 0.0,
    current_rate FLOAT DEFAULT 0.0,
    change_point_count INTEGER DEFAULT 0,
    change_point_strength FLOAT DEFAULT 0.0,
    has_recent_change_point BOOLEAN DEFAULT FALSE,
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

CREATE INDEX IF NOT EXISTS idx_emerging_signals_workspace ON emerging_signals(workspace_id, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_emerging_signals_stage ON emerging_signals(workspace_id, signal_stage, signal_score DESC);

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
    metadata_json JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(workspace_id, entity_kind, entity_id, window_start, window_end)
);

CREATE INDEX IF NOT EXISTS idx_signal_time_series_entity
    ON signal_time_series(workspace_id, entity_kind, entity_id, window_start DESC);

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

CREATE INDEX IF NOT EXISTS idx_cluster_runs_workspace ON cluster_runs(workspace_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_cluster_runs_stage ON cluster_runs(stage, started_at DESC);

CREATE TABLE IF NOT EXISTS admin_manual_jobs (
    id TEXT PRIMARY KEY,
    job_name TEXT NOT NULL,
    workspace_id TEXT,
    status TEXT NOT NULL,
    trigger TEXT DEFAULT 'manual',
    summary JSONB DEFAULT '{}',
    result JSONB,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_admin_manual_jobs_lookup
    ON admin_manual_jobs(job_name, workspace_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_admin_manual_jobs_status
    ON admin_manual_jobs(status, created_at DESC);

CREATE TABLE IF NOT EXISTS missing_signals (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    topic TEXT NOT NULL,
    gap_score FLOAT DEFAULT 0.0,
    opportunity TEXT,
    searxng_frequency FLOAT DEFAULT 0.0,
    frontier_frequency FLOAT DEFAULT 0.0,
    evidence_urls JSONB DEFAULT '[]',
    category TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_missing_signals_workspace ON missing_signals(workspace_id);
CREATE INDEX IF NOT EXISTS idx_missing_signals_gap ON missing_signals(gap_score DESC);

CREATE TABLE IF NOT EXISTS post_enrichments (
    id TEXT PRIMARY KEY,
    post_id TEXT NOT NULL REFERENCES posts(id),
    kind TEXT NOT NULL CHECK (kind IN ('concepts', 'vision', 'tags', 'crawl', 'valence')),
    data JSONB NOT NULL,
    s3_key TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_enrichments_post ON post_enrichments(post_id);
CREATE INDEX IF NOT EXISTS idx_enrichments_kind ON post_enrichments(kind);

CREATE TABLE IF NOT EXISTS media_objects (
    sha256 TEXT PRIMARY KEY,
    s3_key TEXT NOT NULL,
    mime_type TEXT,
    size_bytes BIGINT,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS media_groups (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    grouped_id TEXT NOT NULL,
    source_id TEXT NOT NULL REFERENCES sources(id),
    item_count INTEGER DEFAULT 0,
    assembled BOOLEAN DEFAULT FALSE,
    vision_summary_s3_key TEXT,
    vision_labels JSONB DEFAULT '[]',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_media_groups_workspace ON media_groups(workspace_id);
CREATE INDEX IF NOT EXISTS idx_media_groups_grouped ON media_groups(grouped_id);
