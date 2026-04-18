"""Ingest service — APScheduler-based collector for all sources."""
import asyncio
import datetime
import hashlib
import json
import logging
import os
import sys

from apscheduler.schedulers.asyncio import AsyncIOScheduler

sys.path.insert(0, "/app")

from ingest.account_rotator import AccountRotator, TelegramAccount, env_telegram_proxy_configured
from ingest.scheduler import cron_to_minutes, load_sources
from ingest.source_runtime import SourceRuntimeStore
from ingest.sources.api_source import APISource
from ingest.sources.email_source import EmailSource
from ingest.sources.rss_source import RSSSource
from ingest.sources.web_source import WebSource
from shared.config import get_settings
from shared.metrics import start_metrics_server
from shared.redis_client import RedisClient
from shared.source_definitions import apply_source_preset, canonical_source_type

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# Tracks config hash per job_id to detect source config changes between reloads
_source_config_hashes: dict[str, str] = {}

# Telethon: один клиент на аккаунт — параллельные fetch по каналам рвут соединение (RST / timeouts).
_telegram_run_lock = asyncio.Lock()

try:
    from ingest.sources.telegram_source import TelegramSource
    HAS_TELETHON = True
except ImportError:
    HAS_TELETHON = False
    logger.warning("Telethon not installed — Telegram sources disabled")


def build_rotator(settings) -> "AccountRotator | None":
    if not HAS_TELETHON:
        return None
    accounts = []
    for i in range(2):
        api_id = getattr(settings, f"tg_api_id_{i}", 0)
        api_hash = getattr(settings, f"tg_api_hash_{i}", "")
        if api_id:
            accounts.append(TelegramAccount(
                api_id=api_id,
                api_hash=api_hash,
                session_path=f"/app/sessions/account_{i}",
            ))
    return AccountRotator(accounts) if accounts else None


def build_source_config(row: dict) -> dict:
    """Convert DB row to source config dict."""
    extra = row.get("extra") or {}
    if isinstance(extra, str):
        extra = json.loads(extra)
    proxy_config = row.get("proxy_config") or {}
    if isinstance(proxy_config, str):
        try:
            proxy_config = json.loads(proxy_config)
        except Exception:
            proxy_config = {}

    source_type = canonical_source_type(row.get("source_type", ""))
    config = dict(extra)
    config["source_type"] = source_type
    config["proxy_config"] = proxy_config

    # Telegram
    if row.get("tg_channel"):
        config["channel"] = row["tg_channel"]
        fetch_cfg = config.get("fetch") or {}
        config.setdefault("lookback_hours", fetch_cfg.get("lookback_hours", 24))
        config.setdefault("limit", fetch_cfg.get("max_items_per_run", 200))

    # RSS/Web/Habr
    if row.get("url"):
        config["url"] = row["url"]

    config["url"], config = apply_source_preset(source_type, config.get("url"), config)

    return config


async def run_source(source_row: dict, redis: RedisClient, rotator):
    source_type = canonical_source_type(source_row["source_type"])
    config = build_source_config(source_row)
    runtime_store = None
    settings = get_settings()
    runtime_store = SourceRuntimeStore(settings.database_url)

    cls = None
    if source_type == "telegram" and HAS_TELETHON:
        cls = TelegramSource
    elif source_type == "rss":
        cls = RSSSource
    elif source_type == "web":
        cls = WebSource
    elif source_type == "api":
        cls = APISource
    elif source_type == "email":
        cls = EmailSource

    if cls is None:
        logger.debug("Skipping unsupported source type: %s", source_type)
        return

    kwargs = {
        "source_id": source_row["id"],
        "workspace_id": source_row["workspace_id"],
        "config": config,
        "redis": redis,
        "runtime_store": runtime_store,
    }
    if source_type == "telegram" and rotator:
        # Pass preferred account index from DB; rotator will use it if available
        account_idx = source_row.get("tg_account_idx") or 0
        kwargs["rotator"] = rotator
        kwargs["preferred_account_idx"] = account_idx
        kwargs["proxy_config"] = config.get("proxy_config") or {}

    source = cls(**kwargs)
    if source_type == "telegram" and rotator:
        async with _telegram_run_lock:
            await source.run()
    else:
        await source.run()


async def schedule_all(scheduler: AsyncIOScheduler, redis: RedisClient, rotator, settings):
    sources = await load_sources(settings.database_url)
    logger.info("Loaded %d enabled sources", len(sources))

    for row in sources:
        interval = cron_to_minutes(row.get("schedule_cron") or "*/60 * * * *")
        job_id = f"source_{row['id']}"

        config_hash = source_config_hash(row)
        existing = scheduler.get_job(job_id)
        if existing:
            old_interval = existing.trigger.interval.total_seconds() / 60
            old_hash = _source_config_hashes.get(job_id)
            if old_interval == interval and old_hash == config_hash:
                continue  # nothing changed — keep next_run_time intact
            config_changed = old_hash != config_hash
            # Config changed → run immediately so operator sees effect now.
            # Only-interval change → preserve schedule to avoid unexpected burst.
            next_run = datetime.datetime.now() if config_changed else existing.next_run_time
            scheduler.remove_job(job_id)
        else:
            next_run = datetime.datetime.now()  # new source — run immediately

        scheduler.add_job(
            run_source,
            "interval",
            minutes=interval,
            id=job_id,
            args=[row, redis, rotator],
            max_instances=1,
            coalesce=True,
            next_run_time=next_run,
        )
        _source_config_hashes[job_id] = config_hash
        logger.info("Scheduled %s (%s) every %dm", row["name"], source_type_label(row), interval)


def source_config_hash(row: dict) -> str:
    """Hash of all runtime-relevant source fields to detect config changes."""
    fields = {k: row.get(k) for k in (
        "source_type", "url", "tg_channel", "tg_account_idx",
        "workspace_id", "extra", "proxy_config",
    )}
    return hashlib.md5(json.dumps(fields, sort_keys=True, default=str).encode()).hexdigest()


def source_type_label(row: dict) -> str:
    if row["source_type"] == "telegram":
        return row.get("tg_channel") or "telegram"
    return row.get("url") or row["source_type"]


def _tg_require_proxy_enforced() -> bool:
    return os.environ.get("TG_REQUIRE_PROXY", "").strip().lower() in ("1", "true", "yes", "on")


async def main():
    settings = get_settings()
    start_metrics_server(9091)
    redis = RedisClient(settings.redis_url)
    await redis.connect()
    runtime_store = SourceRuntimeStore(settings.database_url)
    stale_result = await runtime_store.cleanup_stale_runs(max_age_minutes=180)
    logger.info("Source run stale cleanup result: %s", stale_result)

    rotator = build_rotator(settings)

    if (
        HAS_TELETHON
        and rotator
        and _tg_require_proxy_enforced()
        and not env_telegram_proxy_configured()
    ):
        logger.error(
            "TG_REQUIRE_PROXY is set but no Telegram proxy in environment "
            "(TG_SOCKS5 / TG_PROXY_* / WG_SOCKS_* / MTPROXY_*). "
            "Per-source proxy_config alone does not satisfy this check "
            "— set env or unset TG_REQUIRE_PROXY."
        )
        await redis.disconnect()
        sys.exit(1)

    scheduler = AsyncIOScheduler()

    scheduler.add_job(
        schedule_all,
        "interval",
        minutes=5,
        args=[scheduler, redis, rotator, settings],
        id="reload_sources",
        next_run_time=datetime.datetime.now(),
    )

    scheduler.start()
    logger.info("Ingest scheduler started")

    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        scheduler.shutdown()
        if rotator:
            await rotator.close_all()
        await redis.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
