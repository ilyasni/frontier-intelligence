# Runtime Monitoring Dashboard

Provisioned Grafana dashboard for runtime resilience signals.

## Dashboard

- Title: `Frontier Runtime`
- UID: `frontier-runtime`
- Grafana path: `/d/frontier-runtime/frontier-runtime`
- Folder: `Frontier Intelligence`

## What it shows

- `Telegram Client Resets (1h)`
- `Crawl Session Recreates (1h)`
- `Rate Limit Events (1h)`
- `Healthy Runtime Targets`
- reset causes over time
- rate limits by `service / upstream / operation`
- GigaChat billable tokens, request volume, cache reuse and escalations
- scrape target health table

## Provisioning files

- Dashboard JSON: [`grafana/dashboards/frontier-runtime.json`](../grafana/dashboards/frontier-runtime.json)
- Dashboard provider: [`grafana/dashboards/dashboard.yml`](../grafana/dashboards/dashboard.yml)
- Prometheus datasource: [`grafana/datasources/prometheus.yml`](../grafana/datasources/prometheus.yml)

## Deploy or refresh

```bash
cd /opt/frontier-intelligence
docker compose --profile monitor up -d --force-recreate grafana
```

Grafana provisions the dashboard and datasource on startup from `/etc/grafana/provisioning`.

Security note: Grafana is currently published by compose on `0.0.0.0:3000`. Keep it behind LAN/VPN/firewall or put it behind authenticated reverse proxy before any internet exposure.

## GigaChat panels

The dashboard now also includes:

- `GigaChat Billable Tokens (1h)`
- `GigaChat Cached Prompt Tokens (1h)`
- `GigaChat Requests (1h)`
- `GigaChat Escalations (1h)`
- token burn rate by `task / model`
- request volume by `task / model / status`
- model escalations by `task / from_model / to_model`

## Interpretation notes

- Normal baseline for `relevance` and `concepts` on the current production contour is `GigaChat-2`.
- `GigaChat-2-Pro` in `relevance` usually means fallback after gray-zone scoring, ambiguous category, or primary-attempt failure.
- `Cached Prompt Tokens` may stay near zero if session cache was auto-disabled because the current SDK/proxy pair does not support `extra_headers`.
- Any runtime appearance of `GigaChat-2-Lite` in model panels is a misconfiguration signal for this server contour.
