#!/usr/bin/env bash
# alert-triage-collect.sh — read-only diagnostic bundle for the daily alert triage loop.
#
# Runs on the server (has loopback access to Prometheus :9090 / Alertmanager :9093).
# Prints one self-contained text bundle to stdout: firing alerts, what fired in the
# last 24h, active Alertmanager alerts, container/host snapshot, and a batch of the
# most useful diagnostic metrics. The Claude triage loop consumes this bundle and only
# drills into `docker compose logs` for the specific alerts that are actually firing.
#
# No arguments. Never mutates anything. Safe to run any number of times.
#
# Invoke CRLF-safely from the client with:
#   ssh frontier-intelligence "cd /opt/frontier-intelligence && tr -d '\r' < scripts/alert-triage-collect.sh | bash"

set -uo pipefail

PROM="http://127.0.0.1:9090"
AM="http://127.0.0.1:9093"
CURL="curl -s --max-time 15"

section() { printf '\n--- SECTION: %s ---\n' "$1"; }

# Instant PromQL query -> labelled JSON line. Failures are reported, never fatal.
pq() {
  local label="$1" query="$2" out
  out=$($CURL -G "${PROM}/api/v1/query" --data-urlencode "query=${query}" 2>/dev/null)
  if [ -z "$out" ]; then
    printf '### %s => <no response>\n' "$label"
  else
    printf '### %s => %s\n' "$label" "$out"
  fi
}

printf '===== FRONTIER ALERT TRIAGE BUNDLE =====\n'
printf 'generated_at_utc: %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
printf 'host: %s\n' "$(hostname)"

# FrontierWatchdog отфильтрован из всех трёх секций. Он firing ВСЕГДА по
# построению (expr: vector(1)) — это dead man's switch, за его отсутствием
# следит scripts/alert-watchdog.sh, а доставка заглушена в blackhole. Без
# фильтра он попадал бы в каждый дайджест, и разбирающий агент диагностировал
# бы синтетический алерт как проблему — каждый день, бесконечно.
WATCHDOG_ALERT="FrontierWatchdog"

section "now_firing (prometheus /api/v1/alerts, state=firing/pending; FrontierWatchdog исключён)"
$CURL "${PROM}/api/v1/alerts" 2>/dev/null \
  | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    print('<unparseable>'); raise SystemExit
a = d.get('data', {}).get('alerts', [])
kept = [x for x in a if x.get('labels', {}).get('alertname') != '${WATCHDOG_ALERT}']
print(json.dumps({'filtered_out_watchdog': len(a) - len(kept), 'alerts': kept},
                 ensure_ascii=False, indent=2))
" 2>/dev/null || echo '<prometheus unreachable>'
printf '\n'

section "fired_last_24h (alertnames that were firing at any point in the last 24h)"
$CURL -G "${PROM}/api/v1/query" \
  --data-urlencode 'query=max by (alertname,severity,service,workspace) (max_over_time(ALERTS{alertstate="firing",alertname!="FrontierWatchdog"}[24h]))' \
  2>/dev/null || echo '<prometheus unreachable>'
printf '\n'

section "alertmanager_active (/api/v2/alerts; FrontierWatchdog исключён)"
$CURL "${AM}/api/v2/alerts" 2>/dev/null \
  | python3 -c "
import sys, json
try:
    a = json.load(sys.stdin)
except Exception:
    print('<unparseable>'); raise SystemExit
kept = [x for x in a if x.get('labels', {}).get('alertname') != '${WATCHDOG_ALERT}']
print(json.dumps(kept, ensure_ascii=False, indent=2))
" 2>/dev/null || echo '<alertmanager unreachable>'
printf '\n'

section "containers (docker compose ps)"
if command -v docker >/dev/null 2>&1; then
  ( cd /opt/frontier-intelligence 2>/dev/null && \
    docker compose ps --format '{{.Service}}\t{{.Status}}' 2>/dev/null ) || echo '<docker compose ps failed>'
else
  echo '<docker not available>'
fi

section "host (disk / load / docker df)"
df -h / 2>/dev/null | sed -n '1,2p'
printf 'loadavg: %s\n' "$(cat /proc/loadavg 2>/dev/null)"
docker system df 2>/dev/null | sed -n '1,5p' || true

section "key_metrics (instant PromQL snapshot)"
pq "targets_down (up==0)"                 'up == 0'
pq "last_post_age_seconds_by_workspace"   'max by (workspace) (frontier_last_post_age_seconds)'
pq "redis_stream_lag"                      'frontier_redis_stream_lag{service="admin"}'
pq "redis_stream_pending"                  'frontier_redis_stream_pending{service="admin"}'
pq "redis_oldest_pending_age_seconds"      'frontier_redis_stream_oldest_pending_age_seconds{service="admin"}'
pq "admin_scheduler_running"               'frontier_admin_scheduler_running{service="admin"}'
pq "gigachat_balance_tokens"               'frontier_gigachat_balance_tokens{service="admin"}'
pq "openrouter_credit_balance"             'frontier_openrouter_credit_balance{service="admin"}'
pq "openrouter_models_quarantined"         'sum by (service) (frontier_openrouter_model_quarantine{service="admin"})'
pq "llm_fallbacks_15m"                     'sum by (from_provider,reason) (increase(frontier_llm_fallbacks_total{service="worker"}[15m])) > 0'
pq "llm_throttle_15m"                      'sum by (provider,reason) (increase(frontier_llm_throttle_events_total{service="worker"}[15m])) > 0'
pq "rate_limit_events_15m"                 'sum by (service,upstream,operation) (increase(frontier_rate_limit_events_total[15m])) > 0'
pq "searxng_errors_15m"                    'sum by (service,mode) (increase(frontier_searxng_requests_total{status="error"}[15m])) > 0'
pq "graph_duplicate_clusters"              'max by (workspace) (frontier_graph_health{metric="duplicate_clusters"})'

printf '\n===== END BUNDLE =====\n'
