-- Provenance / independence proxy (docs/provenance-independence-layer.md, Layer 1+P2).
-- Additive: raw source_count / source_diversity_score are left untouched so scoring
-- and promotion gates keep their current behavior; these columns are the honest
-- de-syndicated companions read by the signal-nature lens / MCP.
--
--   deduped_source_count : distinct feeds after collapsing same-artifact echoes
--   distinct_voices      : number of same-artifact groups (one voice each)
--   echo_ratio           : (posts - voices) / posts, fraction that are re-syndication
--   arrival_dispersion   : distinct arrival-days / voices (low + many voices = spike)
--   distinct_originators  : distinct named actors (org/person); NULL until Neo4j fill
--   independence_score   : uncalibrated heuristic in [0,1]; treat as Гипотеза

ALTER TABLE semantic_clusters
    ADD COLUMN IF NOT EXISTS deduped_source_count INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS distinct_voices INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS echo_ratio FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS arrival_dispersion FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS distinct_originators INTEGER,
    ADD COLUMN IF NOT EXISTS independence_score FLOAT DEFAULT 0.0;

ALTER TABLE trend_clusters
    ADD COLUMN IF NOT EXISTS deduped_source_count INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS distinct_voices INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS echo_ratio FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS arrival_dispersion FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS distinct_originators INTEGER,
    ADD COLUMN IF NOT EXISTS independence_score FLOAT DEFAULT 0.0;

ALTER TABLE emerging_signals
    ADD COLUMN IF NOT EXISTS deduped_source_count INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS distinct_voices INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS echo_ratio FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS arrival_dispersion FLOAT DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS distinct_originators INTEGER,
    ADD COLUMN IF NOT EXISTS independence_score FLOAT DEFAULT 0.0;
