# Handoff: migration of operational alerts to ntfy

Date: 2026-09-09
State: `needs-human` — code is synced and validated, runtime cutover is not activated
base_sha: `97194eb85228c9006834cd9aaf967da15440f6f6`
written_by: `codex`

## Understanding

Move operational notifications from Telegram to the shared ntfy service at topic
`home-network`. Urgent trend notifications remain in Telegram. Preserve two independent
critical delivery paths and the host-direct dead-man watchdog.

## Changed files

- Application publisher and settings: `admin/backend/services/ntfy_alerts.py`,
  `shared/config.py`.
- Alert producers: `admin/backend/routers/monitoring.py`,
  `admin/backend/services/gigachat_balance.py`,
  `admin/backend/services/xray_health.py`.
- Alertmanager and rules: `prometheus/alertmanager.yml`, `prometheus/alerts.yml`,
  `prometheus/alerts.test.yml`, `docker-compose.yml`, `.env.example`.
- Host publishers: `scripts/notify_ntfy.py`, `scripts/alert-watchdog.sh`,
  `scripts/alert-triage-deliver.sh`.
- Documentation: `docs/README.md`, `docs/runbooks/alert-triage-daily.md`.
- Contract tests: `tests/test_ntfy_alerts.py`, `tests/test_notify_ntfy_script.py`,
  `tests/test_ntfy_alert_scripts.py`, `tests/test_ntfy_alertmanager_config.py`,
  `tests/test_admin_alert_notifications.py`, `tests/test_admin_monitoring_webhook.py`,
  `tests/test_alert_rules_contract.py`.

## What was done

- Added an HTTPS-only ntfy publisher. Credentials are read from a file, redirects are
  disabled, message size is bounded by UTF-8 bytes, and success requires an ntfy JSON receipt.
- Switched GigaChat balance, Xray health and Alertmanager admin webhook notifications to ntfy.
  Delivery failures remain retryable and do not incorrectly advance cooldown/dedup state.
- Replaced active Prometheus `notify: telegram` labels with `notify: ntfy`; `notify: never`
  and watchdog blackholes are unchanged.
- Kept urgent trend Telegram delivery and its admin environment variables unchanged.
- Kept independent critical delivery: native Alertmanager ntfy webhook plus the admin webhook.
- Kept the host-direct watchdog independent from Docker. Failed recovery delivery no longer
  clears watchdog state.
- Split credentials into three files: app, Alertmanager and watchdog.
- Synced only the 22 task files into `/opt/frontier-intelligence`; unrelated server changes
  and untracked operational files were preserved.

## Validation

- Local targeted pytest: `218 passed, 38 subtests passed`.
- Targeted pytest against a staging tree based on the server working copy:
  `218 passed, 38 subtests passed`.
- Independent review suite: `136 passed, 38 subtests passed`; no Critical, Important or
  Minor findings.
- Full local command `pytest -m "not integration and not e2e" -q`:
  `16 failed, 1464 passed, 5 deselected, 38 subtests passed`. All 16 failures match the
  documented Windows/pre-existing baseline; none is in the ntfy change.
- Targeted Ruff, Python compilation and shell syntax checks passed. Full Ruff only finds
  14 pre-existing `F821` errors in an excluded legacy example under `docs/old_docs`.
- Server Compose render passed.
- Server `promtool check rules`: 91 rules, success.
- Rendered server Alertmanager config passed `amtool check-config` with 3 receivers.
- Route checks: critical -> `ntfy-direct,ntfy-admin`; watchdog -> `blackhole`;
  `notify=never` -> `blackhole`; warning -> `ntfy-admin`.
- ntfy health is reachable from both the server host and the current admin container.
- Admin image build did not complete because Docker Hub metadata lookup timed out. No image
  or running service was changed.

## Not completed

- The three production credential files do not exist yet. Token values were neither requested
  nor read.
- `admin` and `alertmanager` were not rebuilt/recreated, so live notifications still use the
  previous Telegram runtime.
- No synthetic FIRING/RESOLVED notification has been delivered to a real ntfy subscriber.

## Uncertainties and rollback risks

- Recreating the services before credential files exist will fail closed at startup or at the
  bind mount. Provision credentials first.
- Alertmanager native webhook accepts HTTP success semantics; application and host publishers
  additionally validate the ntfy receipt. This difference is documented in the runbook.
- The server checkout had unrelated modifications before this work, including a PostgreSQL
  `shm_size` change in `docker-compose.yml`. Stage that mixed file by hunk and do not absorb
  the pre-existing change into this commit.

## Next step

1. Create three separate write-only ntfy credentials for topic `home-network`.
2. Install them as mode `0600` files in `/etc/frontier-intelligence/credentials/`:
   `ntfy-app`, `ntfy-alertmanager`, `ntfy-watchdog` (parent directory mode `0700`).
3. Put only `NTFY_URL` and the three credential paths in the server `.env`.
4. Retry the admin image build, recreate `admin` and `alertmanager` with valid Compose profiles,
   then run synthetic FIRING and RESOLVED checks and confirm delivery on the subscriber.
