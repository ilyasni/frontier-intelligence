import logging

logger = logging.getLogger(__name__)

try:
    from prometheus_client import Counter, Gauge, disable_created_metrics, start_http_server

    disable_created_metrics()

    TELEGRAM_CLIENT_RESETS_TOTAL = Counter(
        "frontier_telegram_client_resets_total",
        "Total Telegram client resets after stale or transport/runtime failures.",
        ["service", "cause"],
    )
    CRAWL_SESSION_RECREATES_TOTAL = Counter(
        "frontier_crawl_session_recreates_total",
        "Total crawl HTTP session recreations.",
        ["service", "cause"],
    )
    RATE_LIMIT_EVENTS_TOTAL = Counter(
        "frontier_rate_limit_events_total",
        "Total upstream rate limit events.",
        ["service", "upstream", "operation"],
    )
    SEARXNG_REQUESTS_TOTAL = Counter(
        "frontier_searxng_requests_total",
        "Total SearXNG requests.",
        ["service", "mode", "status"],
    )
    LLM_PROMPT_TOKENS_TOTAL = Counter(
        "frontier_llm_prompt_tokens_total",
        "Total prompt tokens reported by LLM providers.",
        ["service", "task", "provider", "requested_model", "actual_model"],
    )
    LLM_COMPLETION_TOKENS_TOTAL = Counter(
        "frontier_llm_completion_tokens_total",
        "Total completion tokens reported by LLM providers.",
        ["service", "task", "provider", "requested_model", "actual_model"],
    )
    LLM_BILLABLE_TOKENS_TOTAL = Counter(
        "frontier_llm_billable_tokens_total",
        "Total billable tokens reported by LLM providers.",
        ["service", "task", "provider", "requested_model", "actual_model"],
    )
    LLM_REQUESTS_TOTAL = Counter(
        "frontier_llm_requests_total",
        "Total LLM requests across providers.",
        ["service", "task", "provider", "requested_model", "actual_model", "status"],
    )
    LLM_FALLBACKS_TOTAL = Counter(
        "frontier_llm_fallbacks_total",
        "Total LLM provider/model fallbacks.",
        [
            "service",
            "task",
            "from_provider",
            "from_requested_model",
            "from_actual_model",
            "to_provider",
            "to_model",
            "reason",
        ],
    )
    LLM_COST_ESTIMATE_TOTAL = Counter(
        "frontier_llm_cost_estimate_total",
        "Cumulative estimated LLM cost by provider and task family.",
        ["service", "provider", "task_family", "execution_role"],
    )
    LLM_COST_ACTUAL_TOTAL = Counter(
        "frontier_llm_cost_actual_total",
        "Cumulative actual LLM cost by provider and task family.",
        ["service", "provider", "task_family", "execution_role"],
    )
    LLM_COST_DRIFT_TOTAL = Gauge(
        "frontier_llm_cost_drift_total",
        "Cumulative actual-minus-estimated LLM cost drift by provider and task family.",
        ["service", "provider", "task_family", "execution_role"],
    )
    LLM_FINOPS_RUNTIME_ACTUAL_TOTAL = Gauge(
        "frontier_llm_finops_runtime_actual_total",
        "Current runtime actual LLM cost total by provider from admin finops reconciliation.",
        ["service", "provider"],
    )
    LLM_FINOPS_RUNTIME_ESTIMATED_TOTAL = Gauge(
        "frontier_llm_finops_runtime_estimated_total",
        "Current runtime estimated LLM cost total by provider from admin finops reconciliation.",
        ["service", "provider"],
    )
    LLM_FINOPS_RUNTIME_DRIFT_TOTAL = Gauge(
        "frontier_llm_finops_runtime_drift_total",
        "Current runtime cost drift total by provider from admin finops reconciliation.",
        ["service", "provider"],
    )
    LLM_FINOPS_PUBLISHED_REMAINING = Gauge(
        "frontier_llm_finops_published_remaining",
        "Published remaining budget or balance by provider when available.",
        ["service", "provider"],
    )
    LLM_FINOPS_RECONCILIATION_GAP = Gauge(
        "frontier_llm_finops_reconciliation_gap",
        "Gap between runtime and published provider accounting where units are comparable.",
        ["service", "provider", "kind"],
    )
    LLM_FINOPS_STATUS = Gauge(
        "frontier_llm_finops_status",
        "One-hot finops reconciliation status by provider.",
        ["service", "provider", "status"],
    )
    GIGACHAT_PROMPT_TOKENS_TOTAL = Counter(
        "frontier_gigachat_prompt_tokens_total",
        "Total prompt tokens reported by GigaChat.",
        ["service", "task", "model"],
    )
    GIGACHAT_COMPLETION_TOKENS_TOTAL = Counter(
        "frontier_gigachat_completion_tokens_total",
        "Total completion tokens reported by GigaChat.",
        ["service", "task", "model"],
    )
    GIGACHAT_PRECACHED_PROMPT_TOKENS_TOTAL = Counter(
        "frontier_gigachat_precached_prompt_tokens_total",
        "Total cached prompt tokens reported by GigaChat.",
        ["service", "task", "model"],
    )
    GIGACHAT_BILLABLE_TOKENS_TOTAL = Counter(
        "frontier_gigachat_billable_tokens_total",
        "Total billable tokens reported by GigaChat.",
        ["service", "task", "model"],
    )
    GIGACHAT_REQUESTS_TOTAL = Counter(
        "frontier_gigachat_requests_total",
        "Total GigaChat requests.",
        ["service", "task", "model", "status"],
    )
    GIGACHAT_ESCALATIONS_TOTAL = Counter(
        "frontier_gigachat_escalations_total",
        "Total GigaChat model escalations.",
        ["service", "task", "from_model", "to_model"],
    )
    GIGACHAT_BALANCE_TOKENS = Gauge(
        "frontier_gigachat_balance_tokens",
        "Remaining GigaChat package token balance by usage/model family.",
        ["service", "usage"],
    )
    GIGACHAT_BALANCE_REFRESH_TIMESTAMP = Gauge(
        "frontier_gigachat_balance_refresh_timestamp_seconds",
        "Unix timestamp of the last successful GigaChat balance refresh.",
        ["service"],
    )
    OPENROUTER_VISION_REQUESTS_TOTAL = Counter(
        "frontier_openrouter_vision_requests_total",
        "Total OpenRouter vision requests.",
        ["service", "status"],
    )
    OPENROUTER_VISION_FALLBACKS_TOTAL = Counter(
        "frontier_openrouter_vision_fallbacks_total",
        "Total OpenRouter vision fallbacks to secondary providers.",
        ["service", "to_provider", "reason"],
    )
    OPENROUTER_VISION_QUARANTINE = Gauge(
        "frontier_openrouter_vision_quarantine",
        "Whether OpenRouter free vision is currently quarantined.",
        ["service"],
    )
    OPENROUTER_VISION_RPD_USED = Gauge(
        "frontier_openrouter_vision_rpd_used",
        "Reserved OpenRouter free vision requests for the current UTC day.",
        ["service"],
    )
    POLZA_VISION_REQUESTS_TOTAL = Counter(
        "frontier_polza_vision_requests_total",
        "Total Polza vision requests.",
        ["service", "status"],
    )
    OPENROUTER_CATALOG_AVAILABLE = Gauge(
        "frontier_openrouter_catalog_available",
        "Whether the latest OpenRouter catalog snapshot is usable.",
        ["service"],
    )
    OPENROUTER_CATALOG_REFRESH_TIMESTAMP = Gauge(
        "frontier_openrouter_catalog_refresh_timestamp_seconds",
        "Unix timestamp of the latest OpenRouter catalog snapshot.",
        ["service"],
    )
    OPENROUTER_CATALOG_MODEL_COUNT = Gauge(
        "frontier_openrouter_catalog_model_count",
        "Count of OpenRouter free models grouped by capability snapshot.",
        ["service", "kind"],
    )
    OPENROUTER_KEY_AVAILABLE = Gauge(
        "frontier_openrouter_key_available",
        "Whether the latest OpenRouter key snapshot is usable.",
        ["service"],
    )
    OPENROUTER_KEY_REFRESH_TIMESTAMP = Gauge(
        "frontier_openrouter_key_refresh_timestamp_seconds",
        "Unix timestamp of the latest OpenRouter key snapshot.",
        ["service"],
    )
    OPENROUTER_KEY_LIMIT_REMAINING = Gauge(
        "frontier_openrouter_key_limit_remaining",
        "Remaining OpenRouter credit limit reported by the current key.",
        ["service"],
    )
    OPENROUTER_KEY_USAGE_DAILY = Gauge(
        "frontier_openrouter_key_usage_daily",
        "Current UTC-day usage reported by the OpenRouter key endpoint.",
        ["service", "kind"],
    )
    OPENROUTER_KEY_FREE_TIER = Gauge(
        "frontier_openrouter_key_free_tier",
        "Whether the current OpenRouter key is still considered free tier.",
        ["service"],
    )
    OPENROUTER_HEALTH_SUCCESS_RATE = Gauge(
        "frontier_openrouter_health_success_rate",
        "Rolling OpenRouter model health success rate.",
        ["service", "model_id"],
    )
    OPENROUTER_HEALTH_LATENCY_MS = Gauge(
        "frontier_openrouter_health_latency_ms",
        "Rolling OpenRouter model latency in milliseconds.",
        ["service", "model_id"],
    )
    OPENROUTER_MODEL_QUARANTINE = Gauge(
        "frontier_openrouter_model_quarantine",
        "Whether an OpenRouter model is currently quarantined.",
        ["service", "model_id"],
    )
    OPENROUTER_PICKER_DECISIONS_TOTAL = Counter(
        "frontier_openrouter_picker_decisions_total",
        "Total OpenRouter picker decisions by task family and model.",
        ["service", "task_family", "model_id"],
    )
    OPENROUTER_PICKER_SKIP_TOTAL = Counter(
        "frontier_openrouter_picker_skip_total",
        "Total OpenRouter picker skips by task family and reason.",
        ["service", "task_family", "reason"],
    )
    OPENROUTER_RPD_USED = Gauge(
        "frontier_openrouter_rpd_used",
        "Reserved OpenRouter requests per model for the current UTC day.",
        ["service", "model_id"],
    )
    OPENROUTER_RPM_USED = Gauge(
        "frontier_openrouter_rpm_used",
        "Reserved OpenRouter requests per model for the current UTC minute.",
        ["service", "model_id"],
    )
    WORMSOFT_LIMITS_AVAILABLE = Gauge(
        "frontier_wormsoft_limits_available",
        "Whether the last Wormsoft limits/pricing refresh completed successfully.",
        ["service"],
    )
    WORMSOFT_LIMITS_REFRESH_TIMESTAMP = Gauge(
        "frontier_wormsoft_limits_refresh_timestamp_seconds",
        "Unix timestamp of the last successful Wormsoft limits/pricing refresh.",
        ["service"],
    )
    WORMSOFT_SUBSCRIPTION_LIMIT_CREDITS = Gauge(
        "frontier_wormsoft_subscription_limit_credits",
        "Wormsoft subscription credit allowance per refresh window.",
        ["service", "plan"],
    )
    WORMSOFT_SUBSCRIPTION_WINDOW_SECONDS = Gauge(
        "frontier_wormsoft_subscription_window_seconds",
        "Wormsoft subscription refresh window in seconds.",
        ["service", "plan"],
    )
    WORMSOFT_SUBSCRIPTION_PRICE_RUB = Gauge(
        "frontier_wormsoft_subscription_price_rub",
        "Wormsoft subscription price in RUB per billing period.",
        ["service", "plan"],
    )
    WORMSOFT_MODEL_PRICE_CREDITS_PER_MILLION = Gauge(
        "frontier_wormsoft_model_price_credits_per_million",
        "Wormsoft model pricing in credits per 1M tokens.",
        ["service", "model", "kind"],
    )
    ADMIN_SCHEDULER_RUNNING = Gauge(
        "frontier_admin_scheduler_running",
        "Whether the admin APScheduler is running.",
        ["service"],
    )
    ADMIN_MANUAL_JOBS_RUNNING = Gauge(
        "frontier_admin_manual_jobs_running",
        "Count of running admin manual jobs by job name.",
        ["service", "job_name"],
    )
    ADMIN_MANUAL_JOB_OLDEST_RUNNING_AGE_SECONDS = Gauge(
        "frontier_admin_manual_job_oldest_running_age_seconds",
        "Age of the oldest running admin manual job in seconds.",
        ["service", "job_name"],
    )
    ADMIN_MANUAL_JOBS_RECENT_FAILURES = Gauge(
        "frontier_admin_manual_jobs_recent_failures",
        "Count of admin manual job failures in the recent observation window.",
        ["service", "job_name"],
    )
    REDIS_STREAM_LAG = Gauge(
        "frontier_redis_stream_lag",
        "Redis stream consumer-group lag reported by XINFO GROUPS.",
        ["service", "stream", "group"],
    )
    REDIS_STREAM_PENDING = Gauge(
        "frontier_redis_stream_pending",
        "Redis stream pending message count reported by XPENDING/XINFO GROUPS.",
        ["service", "stream", "group"],
    )
    REDIS_STREAM_OLDEST_PENDING_AGE_SECONDS = Gauge(
        "frontier_redis_stream_oldest_pending_age_seconds",
        "Age of the oldest pending Redis stream message in seconds.",
        ["service", "stream", "group"],
    )
    REDIS_STREAM_CONSUMER_PENDING = Gauge(
        "frontier_redis_stream_consumer_pending",
        "Redis stream pending count per consumer.",
        ["service", "stream", "group", "consumer"],
    )
    REDIS_STREAM_CONSUMER_IDLE_SECONDS = Gauge(
        "frontier_redis_stream_consumer_idle_seconds",
        "Redis stream consumer idle age in seconds.",
        ["service", "stream", "group", "consumer"],
    )
    _PROMETHEUS_AVAILABLE = True
except Exception:  # pragma: no cover - fallback for environments without dependency
    _PROMETHEUS_AVAILABLE = False
    TELEGRAM_CLIENT_RESETS_TOTAL = None
    CRAWL_SESSION_RECREATES_TOTAL = None
    RATE_LIMIT_EVENTS_TOTAL = None
    SEARXNG_REQUESTS_TOTAL = None
    LLM_PROMPT_TOKENS_TOTAL = None
    LLM_COMPLETION_TOKENS_TOTAL = None
    LLM_BILLABLE_TOKENS_TOTAL = None
    LLM_REQUESTS_TOTAL = None
    LLM_FALLBACKS_TOTAL = None
    LLM_COST_ESTIMATE_TOTAL = None
    LLM_COST_ACTUAL_TOTAL = None
    LLM_COST_DRIFT_TOTAL = None
    LLM_FINOPS_RUNTIME_ACTUAL_TOTAL = None
    LLM_FINOPS_RUNTIME_ESTIMATED_TOTAL = None
    LLM_FINOPS_RUNTIME_DRIFT_TOTAL = None
    LLM_FINOPS_PUBLISHED_REMAINING = None
    LLM_FINOPS_RECONCILIATION_GAP = None
    LLM_FINOPS_STATUS = None
    GIGACHAT_PROMPT_TOKENS_TOTAL = None
    GIGACHAT_COMPLETION_TOKENS_TOTAL = None
    GIGACHAT_PRECACHED_PROMPT_TOKENS_TOTAL = None
    GIGACHAT_BILLABLE_TOKENS_TOTAL = None
    GIGACHAT_REQUESTS_TOTAL = None
    GIGACHAT_ESCALATIONS_TOTAL = None
    GIGACHAT_BALANCE_TOKENS = None
    GIGACHAT_BALANCE_REFRESH_TIMESTAMP = None
    OPENROUTER_VISION_REQUESTS_TOTAL = None
    OPENROUTER_VISION_FALLBACKS_TOTAL = None
    OPENROUTER_VISION_QUARANTINE = None
    OPENROUTER_VISION_RPD_USED = None
    POLZA_VISION_REQUESTS_TOTAL = None
    OPENROUTER_CATALOG_AVAILABLE = None
    OPENROUTER_CATALOG_REFRESH_TIMESTAMP = None
    OPENROUTER_CATALOG_MODEL_COUNT = None
    OPENROUTER_KEY_AVAILABLE = None
    OPENROUTER_KEY_REFRESH_TIMESTAMP = None
    OPENROUTER_KEY_LIMIT_REMAINING = None
    OPENROUTER_KEY_USAGE_DAILY = None
    OPENROUTER_KEY_FREE_TIER = None
    OPENROUTER_HEALTH_SUCCESS_RATE = None
    OPENROUTER_HEALTH_LATENCY_MS = None
    OPENROUTER_MODEL_QUARANTINE = None
    OPENROUTER_PICKER_DECISIONS_TOTAL = None
    OPENROUTER_PICKER_SKIP_TOTAL = None
    OPENROUTER_RPD_USED = None
    OPENROUTER_RPM_USED = None
    WORMSOFT_LIMITS_AVAILABLE = None
    WORMSOFT_LIMITS_REFRESH_TIMESTAMP = None
    WORMSOFT_SUBSCRIPTION_LIMIT_CREDITS = None
    WORMSOFT_SUBSCRIPTION_WINDOW_SECONDS = None
    WORMSOFT_SUBSCRIPTION_PRICE_RUB = None
    WORMSOFT_MODEL_PRICE_CREDITS_PER_MILLION = None
    ADMIN_SCHEDULER_RUNNING = None
    ADMIN_MANUAL_JOBS_RUNNING = None
    ADMIN_MANUAL_JOB_OLDEST_RUNNING_AGE_SECONDS = None
    ADMIN_MANUAL_JOBS_RECENT_FAILURES = None
    REDIS_STREAM_LAG = None
    REDIS_STREAM_PENDING = None
    REDIS_STREAM_OLDEST_PENDING_AGE_SECONDS = None
    REDIS_STREAM_CONSUMER_PENDING = None
    REDIS_STREAM_CONSUMER_IDLE_SECONDS = None


def start_metrics_server(port: int) -> None:
    if not _PROMETHEUS_AVAILABLE:
        logger.warning("Prometheus client not available; metrics server disabled")
        return
    start_http_server(port)
    logger.info("Prometheus metrics server started on port %d", port)


def note_telegram_client_reset(service: str, cause: str) -> None:
    if TELEGRAM_CLIENT_RESETS_TOTAL is not None:
        TELEGRAM_CLIENT_RESETS_TOTAL.labels(service=service, cause=cause).inc()


def note_crawl_session_recreate(service: str, cause: str) -> None:
    if CRAWL_SESSION_RECREATES_TOTAL is not None:
        CRAWL_SESSION_RECREATES_TOTAL.labels(service=service, cause=cause).inc()


def note_rate_limit_event(service: str, upstream: str, operation: str) -> None:
    if RATE_LIMIT_EVENTS_TOTAL is not None:
        RATE_LIMIT_EVENTS_TOTAL.labels(
            service=service, upstream=upstream, operation=operation
        ).inc()


def note_searxng_request(service: str, mode: str, status: str) -> None:
    if SEARXNG_REQUESTS_TOTAL is not None:
        SEARXNG_REQUESTS_TOTAL.labels(
            service=service,
            mode=mode,
            status=status,
        ).inc()


def note_llm_usage(
    service: str,
    task: str,
    provider: str,
    requested_model: str,
    actual_model: str,
    *,
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
    billable_tokens: int = 0,
) -> None:
    if LLM_PROMPT_TOKENS_TOTAL is None:
        return
    labels = {
        "service": service,
        "task": task,
        "provider": provider,
        "requested_model": requested_model,
        "actual_model": actual_model,
    }
    LLM_PROMPT_TOKENS_TOTAL.labels(**labels).inc(prompt_tokens)
    LLM_COMPLETION_TOKENS_TOTAL.labels(**labels).inc(completion_tokens)
    LLM_BILLABLE_TOKENS_TOTAL.labels(**labels).inc(billable_tokens)


def note_llm_request(
    service: str,
    task: str,
    provider: str,
    requested_model: str,
    actual_model: str,
    status: str,
) -> None:
    if LLM_REQUESTS_TOTAL is not None:
        LLM_REQUESTS_TOTAL.labels(
            service=service,
            task=task,
            provider=provider,
            requested_model=requested_model,
            actual_model=actual_model,
            status=status,
        ).inc()


def note_llm_fallback(
    service: str,
    task: str,
    *,
    from_provider: str,
    from_requested_model: str,
    from_actual_model: str,
    to_provider: str,
    to_model: str,
    reason: str,
) -> None:
    if LLM_FALLBACKS_TOTAL is not None:
        LLM_FALLBACKS_TOTAL.labels(
            service=service,
            task=task,
            from_provider=from_provider,
            from_requested_model=from_requested_model,
            from_actual_model=from_actual_model,
            to_provider=to_provider,
            to_model=to_model,
            reason=reason,
        ).inc()


def note_llm_cost(
    service: str,
    provider: str,
    task_family: str,
    execution_role: str,
    *,
    estimated_cost: float | None = None,
    actual_cost: float | None = None,
    cost_drift: float | None = None,
) -> None:
    labels = {
        "service": service,
        "provider": provider,
        "task_family": task_family,
        "execution_role": execution_role,
    }
    if LLM_COST_ESTIMATE_TOTAL is not None and estimated_cost is not None:
        LLM_COST_ESTIMATE_TOTAL.labels(**labels).inc(float(estimated_cost))
    if LLM_COST_ACTUAL_TOTAL is not None and actual_cost is not None:
        LLM_COST_ACTUAL_TOTAL.labels(**labels).inc(float(actual_cost))
    if LLM_COST_DRIFT_TOTAL is not None and cost_drift is not None:
        LLM_COST_DRIFT_TOTAL.labels(**labels).inc(float(cost_drift))


def set_llm_finops_snapshot(service: str, payload: dict) -> None:
    reconciliations = list(payload.get("reconciliations") or [])
    if LLM_FINOPS_STATUS is not None:
        LLM_FINOPS_STATUS.clear()
    if LLM_FINOPS_RECONCILIATION_GAP is not None:
        LLM_FINOPS_RECONCILIATION_GAP.clear()
    for item in reconciliations:
        provider = str(item.get("provider") or "")
        if not provider:
            continue
        if LLM_FINOPS_RUNTIME_ACTUAL_TOTAL is not None:
            LLM_FINOPS_RUNTIME_ACTUAL_TOTAL.labels(service=service, provider=provider).set(
                float(item.get("runtime_actual_cost_total") or 0.0)
            )
        if LLM_FINOPS_RUNTIME_ESTIMATED_TOTAL is not None:
            LLM_FINOPS_RUNTIME_ESTIMATED_TOTAL.labels(service=service, provider=provider).set(
                float(item.get("runtime_estimated_cost_total") or 0.0)
            )
        if LLM_FINOPS_RUNTIME_DRIFT_TOTAL is not None:
            LLM_FINOPS_RUNTIME_DRIFT_TOTAL.labels(service=service, provider=provider).set(
                float(item.get("runtime_cost_drift_total") or 0.0)
            )
        if LLM_FINOPS_PUBLISHED_REMAINING is not None:
            LLM_FINOPS_PUBLISHED_REMAINING.labels(service=service, provider=provider).set(
                float(item.get("published_remaining") or 0.0)
            )
        gap_kind = str(item.get("gap_kind") or "none")
        if LLM_FINOPS_RECONCILIATION_GAP is not None:
            LLM_FINOPS_RECONCILIATION_GAP.labels(
                service=service,
                provider=provider,
                kind=gap_kind,
            ).set(float(item.get("gap_value") or 0.0))
        status = str(item.get("status") or "unknown")
        if LLM_FINOPS_STATUS is not None:
            LLM_FINOPS_STATUS.labels(
                service=service,
                provider=provider,
                status=status,
            ).set(1)


def note_gigachat_usage(
    service: str,
    task: str,
    model: str,
    *,
    actual_model: str | None = None,
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
    precached_prompt_tokens: int = 0,
    billable_tokens: int = 0,
) -> None:
    if GIGACHAT_PROMPT_TOKENS_TOTAL is None:
        return
    labels = {"service": service, "task": task, "model": model}
    GIGACHAT_PROMPT_TOKENS_TOTAL.labels(**labels).inc(prompt_tokens)
    GIGACHAT_COMPLETION_TOKENS_TOTAL.labels(**labels).inc(completion_tokens)
    GIGACHAT_PRECACHED_PROMPT_TOKENS_TOTAL.labels(**labels).inc(precached_prompt_tokens)
    GIGACHAT_BILLABLE_TOKENS_TOTAL.labels(**labels).inc(billable_tokens)
    note_llm_usage(
        service,
        task,
        "gigachat",
        model,
        actual_model or model,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        billable_tokens=billable_tokens,
    )


def note_gigachat_request(
    service: str,
    task: str,
    model: str,
    status: str,
    *,
    actual_model: str | None = None,
) -> None:
    if GIGACHAT_REQUESTS_TOTAL is not None:
        GIGACHAT_REQUESTS_TOTAL.labels(
            service=service, task=task, model=model, status=status
        ).inc()
    note_llm_request(service, task, "gigachat", model, actual_model or model, status)


def note_gigachat_escalation(service: str, task: str, from_model: str, to_model: str) -> None:
    if GIGACHAT_ESCALATIONS_TOTAL is not None:
        GIGACHAT_ESCALATIONS_TOTAL.labels(
            service=service, task=task, from_model=from_model, to_model=to_model
        ).inc()


def set_gigachat_balance(service: str, usage: str, value: int) -> None:
    if GIGACHAT_BALANCE_TOKENS is not None:
        GIGACHAT_BALANCE_TOKENS.labels(service=service, usage=usage).set(value)


def note_gigachat_balance_refresh(service: str, timestamp: float) -> None:
    if GIGACHAT_BALANCE_REFRESH_TIMESTAMP is not None:
        GIGACHAT_BALANCE_REFRESH_TIMESTAMP.labels(service=service).set(timestamp)


def note_openrouter_vision_request(service: str, status: str) -> None:
    if OPENROUTER_VISION_REQUESTS_TOTAL is not None:
        OPENROUTER_VISION_REQUESTS_TOTAL.labels(service=service, status=status).inc()


def note_openrouter_vision_fallback(service: str, to_provider: str, reason: str) -> None:
    if OPENROUTER_VISION_FALLBACKS_TOTAL is not None:
        OPENROUTER_VISION_FALLBACKS_TOTAL.labels(
            service=service,
            to_provider=to_provider,
            reason=reason,
        ).inc()


def set_openrouter_vision_quarantine(service: str, is_quarantined: bool) -> None:
    if OPENROUTER_VISION_QUARANTINE is not None:
        OPENROUTER_VISION_QUARANTINE.labels(service=service).set(1 if is_quarantined else 0)


def set_openrouter_vision_rpd_used(service: str, used: int) -> None:
    if OPENROUTER_VISION_RPD_USED is not None:
        OPENROUTER_VISION_RPD_USED.labels(service=service).set(int(used or 0))


def note_polza_vision_request(service: str, status: str) -> None:
    if POLZA_VISION_REQUESTS_TOTAL is not None:
        POLZA_VISION_REQUESTS_TOTAL.labels(service=service, status=status).inc()


def set_openrouter_catalog_snapshot(service: str, payload: dict) -> None:
    if OPENROUTER_CATALOG_AVAILABLE is None:
        return
    models = list(payload.get("models") or [])
    OPENROUTER_CATALOG_AVAILABLE.labels(service=service).set(1 if models else 0)
    fetched_at = payload.get("fetched_at")
    if fetched_at:
        OPENROUTER_CATALOG_REFRESH_TIMESTAMP.labels(service=service).set(float(fetched_at))
    vision_count = sum(1 for model in models if model.get("supports_vision"))
    structured_count = sum(1 for model in models if model.get("supports_structured"))
    tools_count = sum(1 for model in models if model.get("supports_tools"))
    OPENROUTER_CATALOG_MODEL_COUNT.labels(service=service, kind="all").set(len(models))
    OPENROUTER_CATALOG_MODEL_COUNT.labels(service=service, kind="vision").set(vision_count)
    OPENROUTER_CATALOG_MODEL_COUNT.labels(service=service, kind="structured").set(structured_count)
    OPENROUTER_CATALOG_MODEL_COUNT.labels(service=service, kind="tools").set(tools_count)


def set_openrouter_key_snapshot(service: str, payload: dict) -> None:
    if OPENROUTER_KEY_AVAILABLE is None:
        return
    available = bool(payload.get("available"))
    OPENROUTER_KEY_AVAILABLE.labels(service=service).set(1 if available else 0)
    OPENROUTER_KEY_FREE_TIER.labels(service=service).set(1 if payload.get("is_free_tier") else 0)
    fetched_at = payload.get("fetched_at")
    if fetched_at and OPENROUTER_KEY_REFRESH_TIMESTAMP is not None:
        OPENROUTER_KEY_REFRESH_TIMESTAMP.labels(service=service).set(float(fetched_at))
    limit_remaining = payload.get("limit_remaining")
    OPENROUTER_KEY_LIMIT_REMAINING.labels(service=service).set(float(limit_remaining or 0.0))
    OPENROUTER_KEY_USAGE_DAILY.labels(service=service, kind="credits").set(
        float(payload.get("usage_daily") or 0.0)
    )
    OPENROUTER_KEY_USAGE_DAILY.labels(service=service, kind="byok_credits").set(
        float(payload.get("byok_usage_daily") or 0.0)
    )


def set_openrouter_model_health(
    service: str,
    model_id: str,
    *,
    success_rate: float,
    latency_ms: float,
    is_quarantined: bool,
) -> None:
    if OPENROUTER_HEALTH_SUCCESS_RATE is not None:
        OPENROUTER_HEALTH_SUCCESS_RATE.labels(service=service, model_id=model_id).set(
            float(success_rate)
        )
    if OPENROUTER_HEALTH_LATENCY_MS is not None:
        OPENROUTER_HEALTH_LATENCY_MS.labels(service=service, model_id=model_id).set(
            float(latency_ms)
        )
    if OPENROUTER_MODEL_QUARANTINE is not None:
        OPENROUTER_MODEL_QUARANTINE.labels(service=service, model_id=model_id).set(
            1 if is_quarantined else 0
        )


def note_openrouter_picker_decision(service: str, task_family: str, model_id: str) -> None:
    if OPENROUTER_PICKER_DECISIONS_TOTAL is not None:
        OPENROUTER_PICKER_DECISIONS_TOTAL.labels(
            service=service,
            task_family=task_family,
            model_id=model_id,
        ).inc()


def note_openrouter_picker_skip(service: str, task_family: str, reason: str) -> None:
    if OPENROUTER_PICKER_SKIP_TOTAL is not None:
        OPENROUTER_PICKER_SKIP_TOTAL.labels(
            service=service,
            task_family=task_family,
            reason=reason,
        ).inc()


def set_openrouter_model_usage(
    service: str,
    model_id: str,
    *,
    rpd_used: int | None = None,
    rpm_used: int | None = None,
) -> None:
    if rpd_used is not None and OPENROUTER_RPD_USED is not None:
        OPENROUTER_RPD_USED.labels(service=service, model_id=model_id).set(int(rpd_used))
    if rpm_used is not None and OPENROUTER_RPM_USED is not None:
        OPENROUTER_RPM_USED.labels(service=service, model_id=model_id).set(int(rpm_used))


def set_wormsoft_limits_snapshot(service: str, payload: dict) -> None:
    if WORMSOFT_LIMITS_AVAILABLE is None:
        return
    WORMSOFT_LIMITS_AVAILABLE.labels(service=service).set(1 if payload.get("available") else 0)
    fetched_at = payload.get("fetched_at")
    if fetched_at:
        WORMSOFT_LIMITS_REFRESH_TIMESTAMP.labels(service=service).set(float(fetched_at))
    WORMSOFT_SUBSCRIPTION_LIMIT_CREDITS.clear()
    WORMSOFT_SUBSCRIPTION_WINDOW_SECONDS.clear()
    WORMSOFT_SUBSCRIPTION_PRICE_RUB.clear()
    WORMSOFT_MODEL_PRICE_CREDITS_PER_MILLION.clear()
    for item in payload.get("plans") or []:
        plan = str(item.get("id") or "")
        if not plan:
            continue
        WORMSOFT_SUBSCRIPTION_LIMIT_CREDITS.labels(service=service, plan=plan).set(
            int(item.get("amount") or 0)
        )
        WORMSOFT_SUBSCRIPTION_WINDOW_SECONDS.labels(service=service, plan=plan).set(
            int(item.get("seconds") or 0)
        )
        WORMSOFT_SUBSCRIPTION_PRICE_RUB.labels(service=service, plan=plan).set(
            float(item.get("price") or 0.0)
        )
    for model, pricing in (payload.get("pricing") or {}).items():
        for kind in ("input", "output", "cache"):
            value = pricing.get(kind)
            if value is None:
                continue
            WORMSOFT_MODEL_PRICE_CREDITS_PER_MILLION.labels(
                service=service,
                model=str(model),
                kind=kind,
            ).set(float(value))


def set_admin_scheduler_running(service: str, is_running: bool) -> None:
    if ADMIN_SCHEDULER_RUNNING is not None:
        ADMIN_SCHEDULER_RUNNING.labels(service=service).set(1 if is_running else 0)


def set_admin_manual_job_metrics(
    service: str,
    snapshot: list[dict],
) -> None:
    if ADMIN_MANUAL_JOBS_RUNNING is None:
        return
    ADMIN_MANUAL_JOBS_RUNNING.clear()
    ADMIN_MANUAL_JOB_OLDEST_RUNNING_AGE_SECONDS.clear()
    ADMIN_MANUAL_JOBS_RECENT_FAILURES.clear()
    for item in snapshot:
        job_name = str(item.get("job_name") or "")
        if not job_name:
            continue
        ADMIN_MANUAL_JOBS_RUNNING.labels(service=service, job_name=job_name).set(
            int(item.get("running") or 0)
        )
        ADMIN_MANUAL_JOB_OLDEST_RUNNING_AGE_SECONDS.labels(
            service=service,
            job_name=job_name,
        ).set(float(item.get("oldest_running_age_seconds") or 0.0))
        ADMIN_MANUAL_JOBS_RECENT_FAILURES.labels(service=service, job_name=job_name).set(
            int(item.get("recent_failures") or 0)
        )


def set_redis_stream_metrics(service: str, snapshot: dict) -> None:
    if REDIS_STREAM_LAG is None:
        return
    for stream_item in snapshot.get("streams", []):
        stream = str(stream_item.get("stream") or "")
        group = str(stream_item.get("group") or "")
        REDIS_STREAM_LAG.labels(service=service, stream=stream, group=group).set(
            int(stream_item.get("lag") or 0)
        )
        REDIS_STREAM_PENDING.labels(service=service, stream=stream, group=group).set(
            int(stream_item.get("pending") or 0)
        )
        REDIS_STREAM_OLDEST_PENDING_AGE_SECONDS.labels(
            service=service,
            stream=stream,
            group=group,
        ).set(float(stream_item.get("oldest_pending_age_seconds") or 0.0))
        for consumer in stream_item.get("consumers", []):
            consumer_name = str(consumer.get("name") or "")
            labels = {
                "service": service,
                "stream": stream,
                "group": group,
                "consumer": consumer_name,
            }
            REDIS_STREAM_CONSUMER_PENDING.labels(**labels).set(int(consumer.get("pending") or 0))
            REDIS_STREAM_CONSUMER_IDLE_SECONDS.labels(**labels).set(
                float(consumer.get("idle_seconds") or 0.0)
            )
