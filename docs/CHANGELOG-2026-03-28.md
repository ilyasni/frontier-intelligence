# Changelog 2026-03-28

## Connector rollout

Frontier Intelligence ingest was expanded from Telegram-only production use to a normalized multi-connector model.

Implemented connector families:

- `telegram`
- `rss`
- `web`
- `api`
- `email`

Key architectural changes:

- normalized `sources.source_type`
- standardized `extra` connector config
- shared connector lifecycle: `fetch_index -> normalize_item -> hydrate_item -> to_event`
- runtime observability via `source_checkpoints` and `source_runs`

## Database and API

Applied schema/runtime upgrade:

- added `source_checkpoints`
- added `source_runs`
- extended source type validation in admin API
- `GET /api/sources` now returns runtime status fields from checkpoint/run tables

Relevant migration:

- [20260328_source_connectors.sql](./../storage/postgres/migrations/20260328_source_connectors.sql)

## Source implementations

Added or completed:

- `RSSSource`
- `WebSource`
- `APISource`
- `EmailSource`

RSS/source handling improvements:

- Atom/RSS support
- `ETag` / `Last-Modified`
- canonical URL dedupe
- retrying HTTP fetches
- optional full-content hydration
- RSS HTML fragment normalization to plain text
- RSS feed auto-discovery from HTML source pages via `<link rel="alternate">`

## Proxy and network handling

HTTP-based connectors now support `proxy_config`, including:

- `http`
- `https`
- `socks5`

Production note:

- `rss_medium_*` required SOCKS5 routing through `xray`
- current working config is `{"type":"socks5","host":"xray","port":10808}`
- some `web` newsroom sources also required `xray` from the server contour to avoid `403`

## Live sources added

Added into the `disruption` workspace:

- `rss_techcrunch`
- `rss_wired_ai`
- `rss_medium_future`
- `rss_medium_design`
- `rss_medium_mobility`
- `rss_arxiv_cs_ai`
- `rss_arxiv_cs_hc`
- `rss_insideevs_all`
- `rss_insideevs_autonomous`
- `rss_electrek`
- `web_waymo_blog`
- `api_hn_topstories`

Later the live `disruption` pack was expanded with:

- `rss_wired_main`
- `rss_wired_backchannel`
- `rss_medium_ai`
- `rss_medium_autonomous_vehicles`
- `rss_medium_product_design`
- `rss_medium_ux`
- `rss_medium_ui`
- `rss_medium_ux_ui`
- `rss_medium_user_experience`
- `rss_muzli`
- `rss_ux_collective`
- `rss_baymard_subscribe`
- `rss_arxiv_cs_ai_cs_ro`
- `rss_habr_design_articles`
- `rss_habr_ai_hub`
- `rss_insideevs_design`
- `rss_insideevs_ux`
- `api_hn_newstories`
- `api_hn_beststories`
- `web_nvidia_autonomous_vehicles`
- `web_bosch_software_driven_mobility`
- `web_mobilityhouse_newsroom`
- `web_nngroup_articles`
- `web_ux_journal`

T-Bank competitor monitoring was added with:

- `rss_tbank_journal`
- `rss_tbank_github`
- `web_habr_tbank`
- `rss_google_news_tbank_ru`
- `rss_google_news_tcs_group_ru`
- `rss_google_news_tinkoff_fintech_en`
- `rss_cbr_events`
- `rss_payspace_magazine`
- `rss_payments_cards_mobile`

## Production fixes during rollout

Applied on the live server:

- scheduler/runtime propagation of `proxy_config` for HTTP sources
- soft-fail article hydration so one broken page no longer fails the whole source run
- admin API fix for `GET /api/sources?workspace_id=...`
- admin preview sanitization for dashboard/pipeline/post tables
- ingest-side RSS HTML normalization for new rows
- historical Medium content cleanup in `posts`
- automatic cleanup of stale `source_runs.status='running'`
- `RSSSource` support for HTML source pages that expose RSS via `rel="alternate"`
- `WebSource` fallback for blank card titles so a single broken card does not fail the run
- `APISource` fix so list-style APIs like Hacker News do not send invalid cursor parameters on repeat runs
- `rss_tbank_journal` narrowed with banking/auto/fintech/product keyword filters to reduce broad editorial noise
- Google News T-Bank feeds routed through `xray` SOCKS5 from the production contour
- `rss_google_news_tcs_group_ru` tightened with additional `exclude_keywords` for stale personal/investor-history noise

## Current expected behavior

As of this rollout:

- dashboard tables should not break on Medium-style RSS content
- new RSS posts should be stored as plain text, not raw feed HTML
- stale `running` rows should not accumulate in `source_runs`
- runtime health for sources should be visible via `GET /api/sources`

## Operational references

- [source-connectors-runbook.md](./source-connectors-runbook.md)
- [ops-server-troubleshooting.md](./ops-server-troubleshooting.md)
- [pipeline-e2e-checklist.md](./pipeline-e2e-checklist.md)
