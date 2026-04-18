ALTER TABLE post_enrichments
    DROP CONSTRAINT IF EXISTS post_enrichments_kind_check;

ALTER TABLE post_enrichments
    ADD CONSTRAINT post_enrichments_kind_check
    CHECK (kind IN ('concepts', 'vision', 'tags', 'crawl', 'valence'));
