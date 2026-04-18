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
