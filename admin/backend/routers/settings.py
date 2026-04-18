"""Read-only operational settings for admin UI."""

from fastapi import APIRouter

from admin.backend.services.gigachat_balance import fetch_gigachat_balance
from admin.backend.services.gigachat_weekly_report import fetch_gigachat_weekly_report
from shared.config import get_settings

router = APIRouter()


@router.get("")
async def get_admin_settings():
    settings = get_settings()
    return {
        "business": {
            "default_relevance_threshold": settings.default_relevance_threshold,
        },
        "runtime": {
            "indexing_batch_size": settings.indexing_batch_size,
            "indexing_max_concurrency": settings.indexing_max_concurrency,
            "indexing_max_retries": settings.indexing_max_retries,
            "indexing_backoff_ms": settings.indexing_backoff_ms,
            "indexing_claim_idle_ms": settings.indexing_claim_idle_ms,
            "indexing_consumer_cleanup_interval": settings.indexing_consumer_cleanup_interval,
            "vision_claim_idle_ms": settings.vision_claim_idle_ms,
            "vision_max_delivery_count": settings.vision_max_delivery_count,
            "vision_dlq_stream": settings.vision_dlq_stream,
            "redis_stream_lag_alert_threshold": settings.redis_stream_lag_alert_threshold,
            "redis_stream_pending_alert_threshold": settings.redis_stream_pending_alert_threshold,
            "redis_stream_oldest_pending_age_alert_seconds": (
                settings.redis_stream_oldest_pending_age_alert_seconds
            ),
            "gigachat_rc_joint_enabled": settings.gigachat_rc_joint_enabled,
            "gigachat_rc_joint_workspaces": settings.gigachat_rc_joint_workspaces,
            "gigachat_rc_joint_sources": settings.gigachat_rc_joint_sources,
            "sparse_vectors_enabled": settings.sparse_vectors_enabled,
            "searxng_enabled": settings.searxng_enabled,
            "searxng_timeout_seconds": settings.searxng_timeout_seconds,
            "searxng_max_results": settings.searxng_max_results,
            "missing_signals_enabled": settings.missing_signals_enabled,
            "missing_signals_topic_limit": settings.missing_signals_topic_limit,
            "missing_signals_min_gap_score": settings.missing_signals_min_gap_score,
            "embed_dim": settings.embed_dim,
        },
        "integrations": {
            "mcp_internal_url": settings.mcp_internal_url,
            "openai_api_base": settings.openai_api_base,
            "qdrant_url": settings.qdrant_url,
            "neo4j_url": settings.neo4j_url,
            "s3_endpoint_url": settings.s3_endpoint_url,
            "s3_bucket_name": settings.s3_bucket_name,
            "paddleocr_url": settings.paddleocr_url,
            "prometheus_url": settings.prometheus_url,
        },
        "secrets": {
            "database_url_set": bool(settings.database_url),
            "gigachat_credentials_set": bool(settings.gigachat_credentials),
            "neo4j_password_set": bool(settings.neo4j_password),
            "s3_access_key_set": bool(settings.s3_access_key_id),
            "s3_secret_key_set": bool(settings.s3_secret_access_key),
            "telegram_bot_token_set": bool(settings.telegram_bot_token),
            "telegram_alert_chat_id_set": bool(settings.telegram_alert_chat_id),
            "telegram_account_0_set": bool(settings.tg_api_id_0 and settings.tg_api_hash_0),
            "telegram_account_1_set": bool(settings.tg_api_id_1 and settings.tg_api_hash_1),
        },
    }


@router.get("/gigachat-balance")
async def get_gigachat_balance():
    return await fetch_gigachat_balance()


@router.get("/gigachat-weekly-report")
async def get_gigachat_weekly_report():
    return await fetch_gigachat_weekly_report()
