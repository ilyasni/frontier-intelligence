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
    GIGACHAT_PROMPT_TOKENS_TOTAL = None
    GIGACHAT_COMPLETION_TOKENS_TOTAL = None
    GIGACHAT_PRECACHED_PROMPT_TOKENS_TOTAL = None
    GIGACHAT_BILLABLE_TOKENS_TOTAL = None
    GIGACHAT_REQUESTS_TOTAL = None
    GIGACHAT_ESCALATIONS_TOTAL = None
    GIGACHAT_BALANCE_TOKENS = None
    GIGACHAT_BALANCE_REFRESH_TIMESTAMP = None
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


def note_gigachat_usage(
    service: str,
    task: str,
    model: str,
    *,
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


def note_gigachat_request(service: str, task: str, model: str, status: str) -> None:
    if GIGACHAT_REQUESTS_TOTAL is not None:
        GIGACHAT_REQUESTS_TOTAL.labels(
            service=service, task=task, model=model, status=status
        ).inc()


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
