"""
Честность SearXNG-клиента: пустая выдача, кэш и материал ключа кэша.

Зачем этот файл отдельно от tests/test_searxng_client.py.

Соседний файл проверяет чистые функции (`sanitize_result_url`,
`normalize_searxng_result`) — то, что и так видно глазом. Месяцами тихо ломался
не парсинг, а честность: `SearXNGClient.search()` считает HTTP 200 с НУЛЁМ
результатов обычным успехом и кладёт пустой список в Redis на полный
`searxng_cache_ttl` (по умолчанию 3600 с) под ключом, посчитанным из
{query, categories, language, time_range, limit}. Набора движков в ключе нет —
движки живут на стороне сервиса (`searxng/settings.yml`), клиент их не видит.

Два измеренных следствия, которые здесь и закрепляются:

1. Ноль результатов и десять результатов неразличимы в метрике: оба пишутся как
   `note_searxng_request(..., "success")`. Пустая выдача gap-анализа выглядела
   как исправная работа.
2. После починки конфигурации движков те же темы продолжали возвращать `[]` из
   кэша до часа — ключ не зависит от движков, инвалидации нет. Починка выглядела
   как «не сработала».

Тесты ниже фиксируют ТЕКУЩЕЕ поведение, включая обе болячки. Названия и
докстринги, где поведение считается неправильным, содержат `known_wart` /
`known_defect`: это не «так и задумано», это зафиксированный факт. Когда клиент
починят, эти тесты обязаны покраснеть — красный здесь означает «поведение
изменилось осознанно, обнови контракт», а не «тест сломался».

Ни сети, ни Redis: httpx.AsyncClient и redis.asyncio.Redis подменяются
локально через monkeypatch (см. tests/stub_policy.py — глобальные заглушки
драйверов нежелательны).
"""

from __future__ import annotations

import ast
import inspect
import json
from pathlib import Path
from typing import Any, Callable

import httpx
import pytest

from worker.services import searxng_client as sx
from worker.services.searxng_client import SearXNGClient

pytestmark = pytest.mark.unit

# Корень репозитория — относительно файла теста: прогон идёт и на хосте,
# и внутри образа (маунт репозитория в /src).
REPO_ROOT = Path(__file__).resolve().parents[1]
CLIENT_PY = REPO_ROOT / "worker" / "services" / "searxng_client.py"

QUERY = "policy gap in humanoid robotics"

# Ответ «движки настроены»: две валидные ссылки.
POPULATED_PAYLOAD: dict[str, Any] = {
    "results": [
        {
            "url": "https://example.org/a?utm_source=rss",
            "title": "A",
            "content": "first",
            "engine": "brave",
            "engines": ["brave"],
            "score": 2.0,
        },
        {
            "url": "https://example.net/b",
            "title": "B",
            "content": "second",
            "engine": "duckduckgo",
            "engines": ["duckduckgo"],
            "score": 1.0,
        },
    ]
}

# Ответ «движки отвалились»: HTTP 200, ноль результатов.
EMPTY_PAYLOAD: dict[str, Any] = {"results": []}


# ── фейки ────────────────────────────────────────────────────────────────────


def _response(
    payload: Any = None,
    *,
    status_code: int = 200,
    body: str | None = None,
) -> httpx.Response:
    """Настоящий httpx.Response — чтобы raise_for_status()/json() вели себя как в бою."""
    request = httpx.Request("GET", "http://searxng:8080/search")
    if body is not None:
        return httpx.Response(
            status_code,
            content=body.encode("utf-8"),
            headers={"content-type": "text/html; charset=utf-8"},
            request=request,
        )
    return httpx.Response(status_code, json=payload, request=request)


class FakeRedis:
    """
    Одно хранилище на несколько вызовов search().

    Клиент открывает соединение заново на каждый запрос, поэтому состояние
    обязано жить в фейке, а не в соединении — иначе «кэш между вызовами»
    не воспроизводится и тесты на протухание становятся тавтологией.
    """

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.ttl: dict[str, int] = {}
        self.get_keys: list[str] = []
        self.setex_calls: list[tuple[str, int, str]] = []

    async def get(self, key: str) -> str | None:
        self.get_keys.append(key)
        return self.store.get(key)

    async def setex(self, key: str, ttl: int, value: str) -> bool:
        self.setex_calls.append((key, int(ttl), value))
        self.store[key] = value
        self.ttl[key] = int(ttl)
        return True

    async def __aenter__(self) -> "FakeRedis":
        return self

    async def __aexit__(self, *exc_info: object) -> bool:
        return False


class FakeRedisFactory:
    """Подмена `redis.asyncio.Redis` — нужен только from_url()."""

    def __init__(self, redis: FakeRedis) -> None:
        self._redis = redis
        self.from_url_calls: list[str] = []

    def from_url(self, url: str, *, decode_responses: bool = False) -> FakeRedis:
        self.from_url_calls.append(url)
        return self._redis


class FakeAsyncClient:
    """Подмена httpx.AsyncClient: запоминает запрос, отдаёт заготовленный ответ."""

    def __init__(self, rig: "Rig") -> None:
        self._rig = rig

    async def __aenter__(self) -> "FakeAsyncClient":
        return self

    async def __aexit__(self, *exc_info: object) -> bool:
        return False

    async def get(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        auth: Any = None,
    ) -> httpx.Response:
        request_params = dict(params or {})
        self._rig.requests.append({"url": url, "params": request_params})
        return self._rig.responder(request_params)


class FakeSettings:
    """Заглушка Settings: под pytest настоящий Settings — не pydantic-модель."""

    def __init__(self, **overrides: Any) -> None:
        self.searxng_enabled: bool = True
        self.searxng_url: str = "http://searxng:8080"
        self.searxng_user: str = ""
        self.searxng_password: str = ""
        self.searxng_timeout_seconds: float = 8.0
        self.searxng_cache_ttl: int = 3600
        self.searxng_max_results: int = 5
        self.searxng_categories: str = "general,news"
        self.missing_signals_language: str = "en"
        self.redis_url: str = "redis://fake:6379/0"
        for key, value in overrides.items():
            setattr(self, key, value)


def _empty_responder(params: dict[str, Any]) -> httpx.Response:
    return _response(EMPTY_PAYLOAD)


class Rig:
    """Клиент со всеми внешними зависимостями, подменёнными на фейки."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, **settings_overrides: Any) -> None:
        self.settings = FakeSettings(**settings_overrides)
        self.redis = FakeRedis()
        self.redis_factory = FakeRedisFactory(self.redis)
        self.requests: list[dict[str, Any]] = []
        self.statuses: list[tuple[str, str, str]] = []
        self.rate_limits: list[tuple[str, str, str]] = []
        self.responder: Callable[[dict[str, Any]], httpx.Response] = _empty_responder

        def _note_status(service: str, mode: str, status: str) -> None:
            self.statuses.append((service, mode, status))

        def _note_rate_limit(service: str, upstream: str, operation: str) -> None:
            self.rate_limits.append((service, upstream, operation))

        def _client_factory(*args: Any, **kwargs: Any) -> FakeAsyncClient:
            return FakeAsyncClient(self)

        monkeypatch.setattr(sx, "Redis", self.redis_factory)
        monkeypatch.setattr(sx.httpx, "AsyncClient", _client_factory)
        monkeypatch.setattr(sx, "note_searxng_request", _note_status)
        monkeypatch.setattr(sx, "note_rate_limit_event", _note_rate_limit)

        self.client = SearXNGClient(settings=self.settings, service_name="worker")

    @property
    def http_calls(self) -> int:
        return len(self.requests)

    def respond_with(self, payload: Any) -> None:
        def _responder(params: dict[str, Any]) -> httpx.Response:
            return _response(payload)

        self.responder = _responder

    def respond_raw(self, body: str, *, status_code: int = 200) -> None:
        def _responder(params: dict[str, Any]) -> httpx.Response:
            return _response(body=body, status_code=status_code)

        self.responder = _responder

    def respond_status(self, status_code: int) -> None:
        def _responder(params: dict[str, Any]) -> httpx.Response:
            return _response({"error": "upstream"}, status_code=status_code)

        self.responder = _responder

    async def search(self, query: str = QUERY, **kwargs: Any) -> list[dict[str, Any]]:
        return await self.client.search(query, mode="missing_signals", **kwargs)


@pytest.fixture
def rig(monkeypatch: pytest.MonkeyPatch) -> Rig:
    return Rig(monkeypatch)


# ── самопроверка стенда ──────────────────────────────────────────────────────
# Без неё все тесты ниже могут быть зелёными на неподключённых фейках.


async def test_rig_actually_intercepts_http_and_redis(rig: Rig) -> None:
    rig.respond_with(POPULATED_PAYLOAD)
    await rig.search()
    assert rig.http_calls == 1, "запрос не прошёл через фейковый httpx-клиент"
    assert rig.redis.get_keys, "клиент не читал фейковый Redis"
    assert rig.redis.setex_calls, "клиент не писал в фейковый Redis"
    assert rig.requests[0]["params"]["q"] == QUERY


def test_client_source_file_exists() -> None:
    assert CLIENT_PY.is_file(), f"missing file under test: {CLIENT_PY}"


# ── 1. Непустая выдача ───────────────────────────────────────────────────────


async def test_populated_200_is_returned_and_cached(rig: Rig) -> None:
    """200 с результатами: нормализованный список возвращается и кладётся в кэш."""
    rig.respond_with(POPULATED_PAYLOAD)

    items = await rig.search()

    assert [item["url"] for item in items] == [
        "https://example.org/a",  # utm_* срезан
        "https://example.net/b",
    ]
    assert len(rig.redis.setex_calls) == 1
    key, ttl, value = rig.redis.setex_calls[0]
    assert ttl == 3600
    assert json.loads(value) == items


async def test_populated_second_call_is_served_from_cache(rig: Rig) -> None:
    """Второй одинаковый запрос не ходит в сеть и помечается cache_hit."""
    rig.respond_with(POPULATED_PAYLOAD)

    first = await rig.search()
    second = await rig.search()

    assert second == first
    assert rig.http_calls == 1, "повторный запрос ушёл в сеть — кэш не сработал"
    assert [status for _, _, status in rig.statuses] == ["success", "cache_hit"]


# ── 2. Пустая выдача: зафиксированная болячка ────────────────────────────────


async def test_empty_result_list_is_cached_for_full_ttl_known_wart(rig: Rig) -> None:
    """
    ЗАФИКСИРОВАННАЯ БОЛЯЧКА: 200 с ПУСТЫМ списком результатов кэшируется на
    полный searxng_cache_ttl (3600 с), ровно как валидная выдача.

    Это не одобрение поведения. Пустая выдача — типичный симптом сломанной
    конфигурации движков; храня её час, клиент растягивает поломку на час
    после её устранения. Если появится укороченный TTL для пустых ответов —
    этот тест обязан покраснеть, а контракт обновиться.
    """
    rig.respond_with(EMPTY_PAYLOAD)

    items = await rig.search()

    assert items == []
    assert len(rig.redis.setex_calls) == 1, "пустой ответ не был закэширован — поведение изменилось"
    key, ttl, value = rig.redis.setex_calls[0]
    assert value == "[]"
    assert ttl == 3600, "TTL пустого ответа отличается от TTL валидной выдачи"
    assert rig.redis.store[key] == "[]"


async def test_cached_empty_list_is_a_cache_hit_not_a_miss_known_wart(rig: Rig) -> None:
    """
    ЗАФИКСИРОВАННАЯ БОЛЯЧКА: строка "[]" в Redis — истинная (`if cached:`),
    поэтому пустой кэш перечитывается как попадание, а не как промах.

    Именно поэтому пустота самоподдерживается: повторный запрос в сеть не идёт.
    """
    rig.respond_with(EMPTY_PAYLOAD)
    first = await rig.search()
    rig.respond_with(POPULATED_PAYLOAD)  # апстрим уже здоров

    second = await rig.search()

    assert first == []
    assert second == [], "пустой кэш перестал перечитываться — поведение изменилось"
    assert rig.http_calls == 1, "повторный поход в сеть — пустой кэш больше не hit"
    assert [status for _, _, status in rig.statuses] == ["success", "cache_hit"]


async def test_zero_and_many_results_are_metrically_indistinguishable_known_wart(
    rig: Rig,
) -> None:
    """
    ЗАФИКСИРОВАННАЯ БОЛЯЧКА: ноль результатов и полная выдача пишутся одним
    и тем же статусом метрики "success".

    Пока это так, по frontier_searxng_requests_total нельзя отличить рабочий
    gap-анализ от месяцами пустого. Появится отдельный статус (empty/no_results)
    — тест обязан покраснеть.
    """
    rig.respond_with(EMPTY_PAYLOAD)
    empty_items = await rig.search("topic with no coverage")
    empty_statuses = list(rig.statuses)

    rig.statuses.clear()
    rig.respond_with(POPULATED_PAYLOAD)
    full_items = await rig.search("topic with coverage")
    full_statuses = list(rig.statuses)

    assert empty_items == [] and len(full_items) == 2
    assert empty_statuses == full_statuses == [("worker", "missing_signals", "success")]


# ── 3. Ключ кэша ─────────────────────────────────────────────────────────────


def _cache_key_material() -> tuple[str, ...]:
    """
    Ключи словаря, из которого `_cache_key` считает sha256 — строго через ast.

    Через ast, а не через вызов функции: нужно увидеть именно СОСТАВ материала
    ключа, а не его хэш. Отсутствие словаря — ошибка извлечения, а не «пусто».
    """
    tree = ast.parse(CLIENT_PY.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name != "_cache_key":
            continue
        for inner in ast.walk(node):
            if isinstance(inner, ast.Dict) and inner.keys:
                keys = [
                    k.value
                    for k in inner.keys
                    if isinstance(k, ast.Constant) and isinstance(k.value, str)
                ]
                if keys:
                    return tuple(sorted(keys))
    raise AssertionError(
        "не найден словарь-материал ключа внутри _cache_key: "
        "извлекатель сломан или функцию переписали"
    )


def test_cache_key_material_extraction_is_not_vacuous() -> None:
    assert len(_cache_key_material()) >= 5, "ast-извлекатель вернул неправдоподобно мало полей"


async def test_cache_key_changes_for_every_input_it_claims_to_cover(rig: Rig) -> None:
    """
    Ключ обязан меняться при изменении query / categories / language /
    time_range / limit. Совпадение любых двух ключей = чужой ответ из кэша.
    """
    key = rig.client._cache_key
    base = key(
        query=QUERY,
        categories="general,news",
        language="en",
        time_range="month",
        limit=5,
    )
    variants = {
        "query": key(
            query=QUERY + " 2026",
            categories="general,news",
            language="en",
            time_range="month",
            limit=5,
        ),
        "categories": key(
            query=QUERY,
            categories="science",
            language="en",
            time_range="month",
            limit=5,
        ),
        "language": key(
            query=QUERY, categories="general,news", language="ru", time_range="month", limit=5
        ),
        "time_range": key(
            query=QUERY, categories="general,news", language="en", time_range="day", limit=5
        ),
        "limit": key(
            query=QUERY, categories="general,news", language="en", time_range="month", limit=10
        ),
    }
    collisions = [name for name, value in variants.items() if value == base]
    assert not collisions, f"ключ кэша не зависит от {collisions}: разные запросы делят одну запись"
    assert len(set(variants.values())) == len(variants), "разные входы дали одинаковые ключи"


async def test_search_uses_the_documented_cache_key(rig: Rig) -> None:
    """Ключ, по которому search() реально ходит в Redis, — это _cache_key от нормализованных входов."""
    rig.respond_with(POPULATED_PAYLOAD)

    await rig.search(categories="science", language="RU", time_range="Month", limit=3)

    expected = rig.client._cache_key(
        query=QUERY,
        categories="science",
        language="ru",
        time_range="month",
        limit=3,
    )
    assert rig.redis.get_keys == [expected]
    assert rig.redis.setex_calls[0][0] == expected


def test_cache_key_is_blind_to_the_engine_set_known_defect() -> None:
    """
    ОТВЕТ НА ГЛАВНЫЙ ВОПРОС: НЕТ, ключ кэша НЕ меняется при смене набора движков.

    Набор движков задаётся на стороне сервиса (searxng/settings.yml) и в клиент
    не передаётся вообще: ни `search()`, ни `_cache_key()` не имеют параметра
    про движки, и в материал ключа движки не входят. Следствие — правка
    конфигурации движков не инвалидирует ни одной записи кэша, и до истечения
    searxng_cache_ttl (3600 с) те же темы отдают прежний (в т.ч. пустой) ответ.

    Тест закрепляет дефект как факт. Когда движки внесут в ключ (или добавят
    версионный префикс, зависящий от конфигурации), тест покраснеет — это
    ожидаемый сигнал «дефект закрыт, обнови контракт», а не поломка теста.
    """
    material = _cache_key_material()
    assert material == ("categories", "language", "limit", "query", "time_range")
    assert not [name for name in material if "engine" in name], (
        "движки попали в материал ключа кэша — дефект, похоже, исправлен; "
        "обнови этот тест и docstring"
    )

    key_params = set(inspect.signature(SearXNGClient._cache_key).parameters)
    assert key_params == {"self", "query", "categories", "language", "time_range", "limit"}

    search_params = set(inspect.signature(SearXNGClient.search).parameters)
    assert not [name for name in search_params if "engine" in name], (
        "у search() появился параметр про движки — ключ кэша обязан его учитывать"
    )


async def test_engine_reconfiguration_keeps_serving_stale_empty_results_known_defect(
    rig: Rig,
) -> None:
    """
    Тот же дефект в поведении: сценарий «починили движки — ничего не изменилось».

    Шаг 1: движки сломаны, апстрим отдаёт 200 + [] → пустота уходит в кэш.
    Шаг 2: движки починены, апстрим отдаёт полную выдачу.
    Шаг 3: тот же запрос по-прежнему получает [] и даже не идёт в сеть.
    """
    rig.respond_with(EMPTY_PAYLOAD)
    before_fix = await rig.search()
    cache_key = rig.redis.setex_calls[0][0]

    rig.respond_with(POPULATED_PAYLOAD)  # движки исправлены
    after_fix = await rig.search()

    assert before_fix == []
    assert after_fix == [], "пустой результат перестал переживать починку движков — контракт изменился"
    assert rig.http_calls == 1, "клиент сходил в сеть после починки — инвалидация появилась"
    assert rig.redis.store[cache_key] == "[]"
    assert rig.redis.ttl[cache_key] == 3600


# ── 4. Не-2xx не отравляет кэш ───────────────────────────────────────────────


async def test_http_500_raises_and_writes_nothing_to_cache(rig: Rig) -> None:
    """Ошибка апстрима не должна превращаться в закэшированную пустоту."""
    rig.respond_status(500)

    with pytest.raises(httpx.HTTPStatusError):
        await rig.search()

    assert rig.redis.setex_calls == [], "не-2xx попал в кэш"
    assert rig.redis.store == {}
    assert [status for _, _, status in rig.statuses] == ["error"]


async def test_http_429_is_counted_as_rate_limit_and_not_cached(rig: Rig) -> None:
    """429 отмечается как rate-limit, пробрасывается наверх и ничего не кэширует."""
    rig.respond_status(429)

    with pytest.raises(httpx.HTTPStatusError):
        await rig.search()

    assert rig.rate_limits == [("worker", "searxng", "missing_signals")]
    assert rig.redis.setex_calls == []
    assert [status for _, _, status in rig.statuses] == ["error"]


async def test_failed_call_does_not_block_a_later_successful_cache_write(rig: Rig) -> None:
    """После сбоя следующий успешный запрос кэшируется нормально (нет негативного кэша)."""
    rig.respond_status(503)
    with pytest.raises(httpx.HTTPStatusError):
        await rig.search()

    rig.respond_with(POPULATED_PAYLOAD)
    items = await rig.search()

    assert len(items) == 2
    assert len(rig.redis.setex_calls) == 1
    assert json.loads(rig.redis.setex_calls[0][2]) == items


# ── 5. Кривое тело ответа ────────────────────────────────────────────────────


async def test_non_json_body_raises_out_of_the_client_and_caches_nothing(rig: Rig) -> None:
    """
    ТЕКУЩЕЕ ПОВЕДЕНИЕ: тело не-JSON (200 + HTML заглушки/капчи) поднимает
    исключение наружу из search().

    Ожидание «клиент проглотит и вернёт []» здесь НЕ выполняется — и это лучший
    из вариантов: вызывающий код видит сбой, а в кэш ничего не пишется.
    Обёртывание json() в try/except без отдельного (короткого) TTL превратило бы
    заглушку апстрима в час пустоты — ровно тот отказ, что уже случился.
    """
    rig.respond_raw("<html><body>gateway error</body></html>")

    with pytest.raises(ValueError):  # json.JSONDecodeError — подкласс ValueError
        await rig.search()

    assert rig.redis.setex_calls == [], "кривое тело ответа попало в кэш"
    assert rig.redis.store == {}
    assert [status for _, _, status in rig.statuses] == ["error"]


async def test_json_object_without_results_key_returns_empty_and_caches_it_known_wart(
    rig: Rig,
) -> None:
    """
    ТЕКУЩЕЕ ПОВЕДЕНИЕ (болячка): валидный JSON без ключа 'results' исключения
    не поднимает — `payload.get("results") or []` даёт пустой список, который
    кэшируется на полный TTL наравне с настоящей пустой выдачей.

    Мусор в кэш при этом не попадает — попадает пустота, неотличимая от
    честного «ничего не нашлось».
    """
    rig.respond_with({"query": QUERY, "number_of_results": 0})

    items = await rig.search()

    assert items == []
    assert len(rig.redis.setex_calls) == 1
    _, ttl, value = rig.redis.setex_calls[0]
    assert value == "[]"
    assert ttl == 3600
    assert [status for _, _, status in rig.statuses] == ["success"]


async def test_json_top_level_list_is_counted_as_success_then_raises_known_wart(
    rig: Rig,
) -> None:
    """
    ТЕКУЩЕЕ ПОВЕДЕНИЕ (болячка): если тело — JSON-массив, а не объект, то
    `payload.get(...)` выполняется УЖЕ ВНЕ try/except: наружу летит
    AttributeError, но метрика к этому моменту успела записать "success".

    Т.е. в счётчике такой запрос выглядит успешным, хотя вызов упал.
    В кэш при этом не пишется ничего.
    """
    rig.respond_with([{"url": "https://example.org/a"}])

    with pytest.raises(AttributeError):
        await rig.search()

    assert [status for _, _, status in rig.statuses] == ["success"]
    assert rig.redis.setex_calls == [], "мусорное тело попало в кэш"


async def test_junk_items_are_filtered_and_never_reach_the_cache(rig: Rig) -> None:
    """Мусорные элементы внутри results отбрасываются, в кэш идёт только очищенное."""
    rig.respond_with(
        {
            "results": [
                "not-a-dict",
                {"url": "http://127.0.0.1:8080/private", "title": "SSRF"},
                {"url": "ftp://example.org/file", "title": "wrong scheme"},
                {"url": "", "title": "no url"},
                {"url": "https://example.org/a", "title": "ok", "engines": ["brave"]},
                {"url": "https://example.org/a/", "title": "dup", "engines": ["brave"]},
            ]
        }
    )

    items = await rig.search()

    assert [item["url"] for item in items] == ["https://example.org/a"]
    cached = json.loads(rig.redis.setex_calls[0][2])
    assert cached == items
    assert not [item for item in cached if "127.0.0.1" in item["url"]]
