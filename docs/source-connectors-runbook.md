# Source Connectors Runbook

This document describes the current source-ingest model in Frontier Intelligence and the operational rules that matter in production.

## Supported Source Types

`source_type` is now a connector type, not a publisher-specific label:

- `telegram`
- `rss`
- `web`
- `api`
- `email`

`habr` is still accepted in the database for backward compatibility, but operationally it should be treated as an RSS preset rather than a dedicated connector family.

## Source Model

The `sources` table remains the primary source registry.

- `url` is the canonical endpoint for the connector.
  - `rss`: feed URL
  - `web`: listing or section page URL
  - `api`: base endpoint
  - `email`: may be `NULL`
- `extra` stores connector-specific config.
- `proxy_config` applies to Telegram and HTTP-based connectors.

Normalized `extra` shape:

```json
{
  "preset": "medium_future",
  "fetch": {
    "lookback_hours": 24,
    "max_items_per_run": 50,
    "timeout_sec": 45,
    "use_conditional_get": true
  },
  "filters": {
    "include_keywords": [],
    "exclude_keywords": [],
    "lang_allow": ["en"]
  },
  "parse": {
    "full_content": false,
    "extract_author": true,
    "extract_tags": true,
    "listing_selector": "",
    "link_selector": "",
    "title_selector": "",
    "date_selector": "",
    "article_selector": ""
  },
  "dedupe": {
    "strategy": "guid_or_url",
    "canonicalize_url": true
  }
}
```

## Runtime Tables

Two runtime tables back observability and incremental ingest:

- `source_checkpoints`
  - `cursor_json`
  - `etag`
  - `last_modified`
  - `last_seen_published_at`
  - `last_success_at`
  - `last_error`
- `source_runs`
  - `started_at`
  - `finished_at`
  - `status`
  - `fetched_count`
  - `emitted_count`
  - `error_text`

Operational behavior:

- At ingest startup, stale `running` rows older than 180 minutes are automatically closed as `error`.
- When a source starts a new run, older unfinished `running` rows for the same source are automatically closed as `error`.
- This prevents `source_runs` from accumulating misleading historical `running` rows after restarts or hard container recreation.

## Connector Lifecycle

Structured connectors follow one lifecycle:

`fetch_index -> normalize_item -> hydrate_item -> to_event`

Internal normalized item fields:

- `external_id`
- `url`
- `title`
- `content`
- `summary`
- `author`
- `published_at`
- `tags`
- `linked_urls`
- `lang`
- `raw_payload`

Rules:

- `external_id`: guid, then canonical URL, then hash of `(url + title + published_at)`
- `content`: hydrated article body, then normalized summary, then title
- `linked_urls`: canonicalized external links from feed body and hydrated content

## HTTP Connector Notes

HTTP-based connectors share one hardened `httpx` profile:

- default `User-Agent`
- default `Accept` and `Accept-Language`
- explicit timeout
- connection limits
- retry wrapper for network errors and `408/429/5xx`
- support for `http`, `https`, and `socks5` proxy config

Security rule:

- `proxy_config` may contain proxy credentials. Keep live values in the database/server `.env` only; do not put real proxy hosts, passwords, MTProxy secrets, or VLESS/Reality IDs in repo examples.

This is the current baseline for `rss`, `web`, and `api`.

## HTML Normalization Rules

Feed HTML fragments are normalized to plain text during ingest.

- `<script>` and `<style>` are removed
- text is extracted with whitespace compaction
- links are preserved into `linked_urls`
- new rows should no longer store Medium-style feed HTML in `posts.content`

Admin-side preview rendering also escapes output, so malformed feed HTML should not break dashboard tables even if old historical rows still exist.

## Production Notes

### Habr source pages

Habr pages can be registered directly as source pages rather than hard-coded RSS URLs.

Current production behavior:

- `RSSSource` can discover RSS/Atom via `<link rel="alternate" ...>` in HTML
- this is used for Habr section and hub pages
- resolved feed URLs are cached in `source_checkpoints.cursor_json.resolved_feed_url`

Examples:

- `https://habr.com/ru/flows/design/articles/`
- `https://habr.com/ru/hubs/artificial_intelligence/`

### Medium

Medium feeds may require VPN/proxy egress depending on the server contour.

Current working production approach:

- use HTTP source proxy config
- route Medium feeds through `xray` via SOCKS5
- keep `parse.full_content=false` for Medium unless there is a specific need for article hydration
- this same pattern also works for `medium.muz.li` and sources that behave similarly from the server contour

Example:

```json
{
  "type": "socks5",
  "host": "xray",
  "port": 10808
}
```

### InsideEVs

InsideEVs article hydration may return `403`.

Current safe approach:

- leave RSS ingestion enabled
- set `parse.full_content=false`
- accept teaser/summary-only ingest unless a browser-backed extractor is introduced

### Hacker News API

Hacker News endpoints are handled as `api` sources with `item_url_template`.

Current supported working patterns:

- `topstories.json`
- `newstories.json`
- `beststories.json`

Operational note:

- do not send a cursor param unless `parse.field_map.next_cursor` is explicitly configured
- HN endpoints are list-style APIs, not cursor-style APIs

### T-Bank / Tinkoff Monitoring

The live `disruption` workspace includes a narrow T-Bank competitor monitoring pack.

Primary sources:

- `rss_tbank_journal`: `https://t-j.ru/feed/`
- `rss_tbank_github`: `https://github.com/TinkoffCreditSystems.atom`
- `web_habr_tbank`: `https://habr.com/ru/companies/tbank/articles/`

Monitoring and context sources:

- `rss_google_news_tbank_ru`
- `rss_google_news_tcs_group_ru`
- `rss_google_news_tinkoff_fintech_en`
- `rss_cbr_events`
- `rss_payspace_magazine`
- `rss_payments_cards_mobile`

Operational notes:

- `journal.tinkoff.ru/feed/` redirects to `https://t-j.ru/feed/`; use the final URL in source config.
- `rss_tbank_journal` is a broad editorial feed, so it must keep banking/auto/fintech/product keyword filters to avoid lifestyle noise.
- Google News feeds require `xray` SOCKS5 routing from the production contour.
- `rss_google_news_tcs_group_ru` should keep strict `exclude_keywords` for stale Oleg Tinkov / historical investor-news noise.
- `rss_tbank_github` currently returns a valid Atom feed with zero entries; `fetched=0` is expected unless GitHub starts publishing public timeline entries for the organization.
- `web_habr_tbank` should use the company page, not Habr RSS variants:
  - `listing_selector: "article"`
  - `link_selector: "a.tm-title__link[href]"`
  - `title_selector: "h2 a.tm-title__link, a.tm-title__link"`
  - `date_selector: "time"`
  - `article_selector: "article, main"`
  - `full_content: true`

### Tesla Blog

Current server contour behavior:

- `https://www.tesla.com/blog/rss.xml` returns `403`
- `https://www.tesla.com/blog` also returns `403` from the production host
- routing through `xray` does not currently bypass this restriction

Operational guidance:

- keep Tesla disabled in the live source pack for now
- do not offer `tesla_blog` in starter bundles
- if Tesla becomes important again, re-evaluate with a browser-backed fetch path or a different egress contour

### Auto.ru Journal

Current server contour behavior:

- `https://auto.ru/journal/rss/` returns `404`
- `https://auto.ru/journal/` also returns `404` from the production host

Operational guidance:

- keep Auto.ru Journal disabled
- do not treat it as a recoverable transient RSS failure until the publisher restores a working endpoint

### Waymo Blog

`web` sources should prefer stable structural selectors and tolerate selector drift.

If a `web` source starts fetching `0` items unexpectedly:

1. inspect the current page DOM
2. adjust `listing_selector` and related selectors
3. keep extraction soft-fail, not run-fail

Current production guidance for `web_waymo_blog`:

- prefer `link_selector: "a[href^='/blog/']"` to avoid nav/share links
- keep `title_selector: "h2, h3"` instead of generic anchors
- keep `full_content=true`; Waymo article pages are stable enough for hydration
- do not downgrade to `listing-only` only because a healthy run emitted `0` new items; for low-frequency blogs this often just means "nothing new"

`parse.full_content` is respected for `web` sources:

- `true`: hydrate each article URL and use article body when available
- `false`: keep listing-only mode and do not force article hydration

Use `full_content=false` for corporate landing/newsroom pages that are useful as signal feeds but brittle as article scrapers.

### Confirmed working web/newsroom sources

The following `web` sources are currently confirmed to run successfully in production:

- `web_waymo_blog`
- `web_nvidia_autonomous_vehicles`
- `web_bosch_software_driven_mobility`
- `web_mobilityhouse_newsroom`
- `web_nngroup_articles`
- `web_ux_journal`

Some of them require `xray` proxy routing to avoid `403` from the server contour.

## Historical Cleanup

If older rows were ingested before feed HTML normalization, use:

- [20260328_source_connectors.sql](../storage/postgres/migrations/20260328_source_connectors.sql) for schema/runtime support
- [backfill_posts_plaintext.py](../scripts/backfill_posts_plaintext.py) for repeatable cleanup of stored HTML-ish post bodies

For emergency one-off cleanup on the server, SQL-only normalization is acceptable for specific source families such as `rss_medium_%`, but the preferred long-term fix is the ingest-side normalization already implemented.

## Live Source Pack

Current live source pack in `disruption` includes, among others:

Core RSS/API:

- `rss_techcrunch`
- `rss_wired_main`
- `rss_wired_ai`
- `rss_wired_business`
- `rss_wired_backchannel`
- `rss_arxiv_cs_ai`
- `rss_arxiv_cs_hc`
- `rss_arxiv_cs_lg`
- `rss_arxiv_cs_ro`
- `rss_arxiv_cs_ai_cs_ro`
- `api_hn_topstories`
- `api_hn_newstories`
- `api_hn_beststories`

Medium and design feeds:

- `rss_medium_ai`
- `rss_medium_future`
- `rss_medium_design`
- `rss_medium_mobility`
- `rss_medium_autonomous_vehicles`
- `rss_medium_product_design`
- `rss_medium_ux`
- `rss_medium_ui`
- `rss_medium_ux_ui`
- `rss_medium_user_experience`
- `rss_muzli`
- `rss_ux_collective`
- `rss_baymard_subscribe`

Mobility and EV:

- `rss_insideevs_all`
- `rss_insideevs_autonomous`
- `rss_insideevs_design`
- `rss_insideevs_ux`
- `rss_electrek`
- `web_waymo_blog`
- `web_nvidia_autonomous_vehicles`
- `web_bosch_software_driven_mobility`
- `web_mobilityhouse_newsroom`

Research and UX:

- `rss_habr_design_articles`
- `rss_habr_ai_hub`
- `web_nngroup_articles`
- `web_ux_journal`

T-Bank competitor monitoring:

- `rss_tbank_journal`
- `rss_tbank_github`
- `web_habr_tbank`
- `rss_google_news_tbank_ru`
- `rss_google_news_tcs_group_ru`
- `rss_google_news_tinkoff_fintech_en`
- `rss_cbr_events`
- `rss_payspace_magazine`
- `rss_payments_cards_mobile`

## Recommended Verification

After adding or changing sources:

1. Check `GET /api/sources?workspace_id=<id>` for `last_run_status`, `last_error`, `last_run_fetched_count`, and `last_run_emitted_count`.
2. Check `GET /api/posts?workspace_id=<id>` for plain-text `preview`.
3. Inspect `source_runs` for unexpected `error` bursts.
4. Inspect `source_checkpoints` for `last_success_at` and `last_seen_published_at`.
5. If the source is external and flaky, test with and without `proxy_config`.
