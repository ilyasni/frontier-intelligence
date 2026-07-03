-- Russian display title for trend clusters, generated via wormsoft.
-- The English `title` stays authoritative for term-overlap merging; `title_ru`
-- is a presentation-only rephrase shown in alerts and MCP output.
ALTER TABLE trend_clusters
    ADD COLUMN IF NOT EXISTS title_ru TEXT;
