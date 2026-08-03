from __future__ import annotations

import hashlib
import ipaddress
import json
import logging
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import httpx
from redis.asyncio import Redis

from shared.config import Settings, get_settings
from shared.metrics import note_rate_limit_event, note_searxng_request

logger = logging.getLogger(__name__)

_TRACKING_QUERY_KEYS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "ref",
    "ref_src",
    "source",
    "spm",
    "yclid",
    "_ga",
}
_BLOCKED_HOST_TOKENS = {
    "1337x",
    "casino",
    "porn",
    "torrent",
    "xvideos",
    "xnxx",
}
_PRIVATE_HOSTS = {
    "127.0.0.1",
    "0.0.0.0",
    "admin",
    "crawl4ai",
    "localhost",
    "neo4j",
    "postgres",
    "qdrant",
    "redis",
    "searxng",
    "worker",
    "xray",
}

# ── TTL кэша ─────────────────────────────────────────────────────────────────
#
# Пустая выдача кэшируется, но своим — коротким — TTL.
#
# Кэшировать пустой ответ всё-таки надо: HTTP 200 с нулём результатов почти
# всегда означает, что набор движков лежит, и долбить мёртвый апстрим на каждый
# вызов бессмысленно. Но хранить эту пустоту час — значит растянуть поломку на
# час ПОСЛЕ её устранения: ровно это и наблюдалось 2026-08-03, когда после
# починки searxng/settings.yml те же темы продолжали отдавать [] из кэша и
# починка выглядела как несработавшая.
#
# Отсюда порядок величины — минуты, а не часы. 120 с выбраны по циклу проверки
# руками: правишь конфиг движков → перезапускаешь searxng → перезапускаешь
# анализ и смотришь. Две минуты этот цикл переживает без «почему всё ещё пусто»,
# и при этом мёртвый апстрим по одному и тому же запросу опрашивается не чаще
# ~30 раз в час. Штатный missing_signals ходит по расписанию куда реже двух
# минут, так что в норме этот TTL не стоит ничего: он защищает только от
# горячего цикла ретраев.
_CACHE_TTL_FLOOR_SECONDS = 60
_EMPTY_CACHE_TTL_DEFAULT_SECONDS = 120
_EMPTY_CACHE_TTL_FLOOR_SECONDS = 10


def sanitize_result_url(url: str) -> str | None:
    raw = str(url or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None

    host = (parsed.hostname or "").strip().lower()
    if not host or host in _PRIVATE_HOSTS:
        return None
    if any(token in host for token in _BLOCKED_HOST_TOKENS):
        return None

    try:
        address = ipaddress.ip_address(host)
        if address.is_private or address.is_loopback or address.is_link_local or address.is_reserved:
            return None
    except ValueError:
        pass

    cleaned_query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=False)
        if not key.lower().startswith("utm_") and key.lower() not in _TRACKING_QUERY_KEYS
    ]
    cleaned_path = parsed.path.rstrip("/") or "/"
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            cleaned_path,
            "",
            urlencode(cleaned_query, doseq=True),
            "",
        )
    )


def normalize_searxng_result(item: dict[str, Any]) -> dict[str, Any] | None:
    clean_url = sanitize_result_url(str(item.get("url") or ""))
    if not clean_url:
        return None
    engines = [str(engine).strip() for engine in (item.get("engines") or []) if str(engine).strip()]
    if not engines and item.get("engine"):
        engines = [str(item["engine"]).strip()]
    try:
        score = float(item.get("score") or 0.0)
    except Exception:
        score = 0.0
    return {
        "url": clean_url,
        "title": str(item.get("title") or "").strip(),
        "content": str(item.get("content") or "").strip(),
        "engine": str(item.get("engine") or "").strip(),
        "engines": engines,
        "score": score,
        "published_date": item.get("publishedDate"),
    }


class SearXNGClient:
    def __init__(self, *, settings: Settings | None = None, service_name: str = "worker") -> None:
        self._settings = settings or get_settings()
        self._service_name = service_name

    def _auth(self) -> httpx.BasicAuth | None:
        user = self._settings.searxng_user.strip()
        password = self._settings.searxng_password
        if not user:
            return None
        return httpx.BasicAuth(user, password)

    def _engines_fingerprint(self) -> str:
        """
        Отпечаток набора движков — единственное, что клиент про них знает.

        Набор движков живёт в searxng/settings.yml и монтируется ТОЛЬКО в
        контейнер searxng (`./searxng:/etc/searxng` в docker-compose.yml). Ни
        worker, ни admin, ни mcp этот файл не видят и увидеть не могут. Спросить
        сам сервис можно (GET /config отдаёт список движков), но это лишний
        сетевой запрос перед каждым холодным поиском и новая точка отказа: если
        /config не ответил, отпечаток пришлось бы чем-то подменить — то есть
        подставить значение, которое ПРИТВОРЯЕТСЯ, что отслеживает конфигурацию.

        Поэтому отпечаток объявляет оператор: SEARXNG_ENGINES_FINGERPRINT.
        Он честно отслеживает ровно одно — то, что оператор задекларировал.
        Правило эксплуатации: поменял searxng/settings.yml — поменяй и его
        (удобнее всего класть туда sha256 самого файла). Не поменял —
        инвалидации не будет. Это известное ограничение схемы, а не незаметный
        дефект: сам факт наличия поля в материале ключа виден в коде и в тестах.

        Чтение — прямым обращением к полю, а НЕ через getattr с дефолтом.
        Разница не косметическая. До 2026-08-04 здесь стоял
        `getattr(..., "searxng_engines_fingerprint", "")`, а поля с таким именем
        в shared/config.py не было вовсе; Settings объявлен с extra="ignore",
        поэтому SEARXNG_ENGINES_FINGERPRINT из .env отбрасывался ещё до чтения.
        Правило эксплуатации выше физически не могло сработать: отпечаток был
        константой "" при любом .env, вклад поля в ключ сводился к одноразовому
        бампу префикса v1→v2, и всё это выглядело исправным. Прямое обращение
        превращает «поля нет» из тихого дефолта в AttributeError, а
        tests/test_searxng_client_honesty.py дополнительно пришпиливает
        объявление поля и его алиас к shared/config.py.
        """
        return str(self._settings.searxng_engines_fingerprint or "").strip()

    def _cache_ttl_for(self, results: list[dict[str, Any]]) -> int:
        """TTL записи: пустой выдаче — свой короткий, валидной — полный."""
        full_ttl = max(_CACHE_TTL_FLOOR_SECONDS, int(self._settings.searxng_cache_ttl))
        if results:
            return full_ttl
        # Прямое чтение поля, а не getattr с дефолтом: у Settings extra="ignore",
        # и необъявленное поле означало бы, что SEARXNG_EMPTY_CACHE_TTL из .env молча
        # выбрасывается, а getattr это прячет за «дефолтом». Пусть лучше падает.
        empty_ttl = int(self._settings.searxng_empty_cache_ttl)
        # Пустота не должна жить дольше валидной выдачи ни при какой настройке.
        return min(full_ttl, max(_EMPTY_CACHE_TTL_FLOOR_SECONDS, empty_ttl))

    def _cache_key(
        self,
        *,
        query: str,
        categories: str,
        language: str | None,
        time_range: str | None,
        limit: int,
        engines_fingerprint: str,
    ) -> str:
        digest = hashlib.sha256(
            json.dumps(
                {
                    "query": query,
                    "categories": categories,
                    "language": language,
                    "time_range": time_range,
                    "limit": limit,
                    "engines_fingerprint": engines_fingerprint,
                },
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
        # v2 — в материал ключа добавлен engines_fingerprint. Префикс поднят
        # намеренно: записи v1 (в том числе часовые пустоты, из-за которых
        # починка движков выглядела несработавшей) после выката не читаются.
        return f"searxng:v2:{digest}"

    async def search(
        self,
        query: str,
        *,
        categories: str | None = None,
        language: str | None = None,
        time_range: str | None = None,
        limit: int | None = None,
        mode: str = "general",
    ) -> list[dict[str, Any]]:
        if not self._settings.searxng_enabled:
            return []

        normalized_query = str(query or "").strip()
        if not normalized_query:
            return []

        effective_limit = max(1, min(int(limit or self._settings.searxng_max_results), 10))
        effective_categories = (categories or self._settings.searxng_categories or "general,news").strip()
        effective_language = (language or self._settings.missing_signals_language or "").strip().lower() or None
        if effective_language in {"", "auto", "all"}:
            effective_language = None
        effective_time_range = (time_range or "").strip().lower() or None
        cache_key = self._cache_key(
            query=normalized_query,
            categories=effective_categories,
            language=effective_language,
            time_range=effective_time_range,
            limit=effective_limit,
            engines_fingerprint=self._engines_fingerprint(),
        )

        try:
            async with Redis.from_url(self._settings.redis_url, decode_responses=True) as redis:
                cached = await redis.get(cache_key)
                if cached:
                    cached_items: list[dict[str, Any]] = json.loads(cached)
                    # Отдача пустоты из кэша считается отдельно: при коротком TTL
                    # всплеск запросов по мёртвой теме — это одна запись "empty"
                    # и хвост попаданий, и они не должны маскироваться под
                    # обычные cache_hit по здоровой выдаче.
                    note_searxng_request(
                        self._service_name,
                        mode,
                        "cache_hit" if cached_items else "cache_hit_empty",
                    )
                    return cached_items
        except Exception:
            logger.debug("searxng_cache_read_failed", exc_info=True)

        params: dict[str, Any] = {
            "q": normalized_query,
            "format": "json",
            "categories": effective_categories,
            "safesearch": 0,
        }
        if effective_language:
            params["language"] = effective_language
        if effective_time_range:
            params["time_range"] = effective_time_range

        timeout_value = max(2.0, float(self._settings.searxng_timeout_seconds))
        base_url = self._settings.searxng_url.rstrip("/")
        headers = {
            "Accept": "application/json",
            "User-Agent": "frontier-intelligence/1.0 (+internal searxng client)",
            "X-Forwarded-For": "127.0.0.1",
            "X-Real-IP": "127.0.0.1",
        }

        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(timeout_value, connect=min(timeout_value, 5.0)),
            ) as client:
                response = await client.get(
                    f"{base_url}/search",
                    params=params,
                    headers=headers,
                    auth=self._auth(),
                )
            if response.status_code == 429:
                note_rate_limit_event(self._service_name, "searxng", mode)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                # Тело-не-объект — дефект апстрима, а не удачный запрос. Раньше
                # метрика успевала записать "success" прямо здесь, и только
                # потом вызов падал на payload.get(...) уже вне try: в счётчике
                # такой запрос выглядел успешным.
                raise TypeError("searxng returned a non-object JSON body")
        except Exception:
            note_searxng_request(self._service_name, mode, "error")
            raise

        normalized: list[dict[str, Any]] = []
        seen_urls: set[str] = set()
        for raw_item in payload.get("results") or []:
            if len(normalized) >= effective_limit:
                break
            item = normalize_searxng_result(raw_item if isinstance(raw_item, dict) else {})
            if not item:
                continue
            if item["url"] in seen_urls:
                continue
            seen_urls.add(item["url"])
            normalized.append(item)

        # Статус считается по тому, что реально получил вызывающий, а не по коду
        # ответа: 200 с нулём пригодных результатов — это "empty", отдельная
        # серия. Имена "success"/"error" не трогаем — на них смотрят алерты
        # FrontierSearxngErrorBurst и FrontierSearxngNoSuccessfulQueries.
        note_searxng_request(self._service_name, mode, "success" if normalized else "empty")

        try:
            async with Redis.from_url(self._settings.redis_url, decode_responses=True) as redis:
                await redis.setex(
                    cache_key,
                    self._cache_ttl_for(normalized),
                    json.dumps(normalized, ensure_ascii=False),
                )
        except Exception:
            logger.debug("searxng_cache_write_failed", exc_info=True)

        return normalized
