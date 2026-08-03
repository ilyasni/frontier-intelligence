"""
Честность SearXNG-клиента: пустая выдача, TTL кэша и материал ключа кэша.

Зачем этот файл отдельно от tests/test_searxng_client.py.

Соседний файл проверяет чистые функции (`sanitize_result_url`,
`normalize_searxng_result`) — то, что и так видно глазом. Месяцами тихо ломался
не парсинг, а честность `SearXNGClient.search()`. Три измеренных дефекта,
закрытых 2026-08-03 и закреплённых здесь как контракт:

1. HTTP 200 с НУЛЁМ результатов кэшировался на полный `searxng_cache_ttl`
   (3600 с) наравне с валидной выдачей. После починки набора движков те же темы
   продолжали возвращать `[]` из кэша до часа, и починка выглядела как
   несработавшая. Теперь у пустоты свой короткий TTL
   (`searxng_empty_cache_ttl`, порядок величины — минуты).
2. Ключ кэша считался из {query, categories, language, time_range, limit} и не
   зависел от набора движков, поэтому правка `searxng/settings.yml` не
   инвалидировала ничего. Теперь в материал ключа входит `engines_fingerprint`,
   а префикс ключа поднят до `v2`.
3. Ноль результатов и десять результатов писались одним статусом метрики
   "success" — по `frontier_searxng_requests_total` нельзя было отличить
   рабочий gap-анализ от месяцами пустого. Теперь пустая выдача — отдельный
   статус "empty" (и "cache_hit_empty" при отдаче пустоты из кэша), а имена
   "success"/"error", на которые смотрят алерты, не изменились.

Ревизия 2026-08-04 — две дыры, из-за которых пункты 2 и 3 выше на проде НЕ
работали, хотя все проверки были зелёными:

  4. Отпечаток движков был константной пустой строкой в бою. Поля
     `searxng_engines_fingerprint` в shared/config.py не существовало, а Settings
     объявлен с `extra="ignore"` — значит SEARXNG_ENGINES_FINGERPRINT из .env
     отбрасывался ЕЩЁ ДО чтения, и `getattr(settings, ..., "")` возвращал ""
     всегда. От пункта 2 оставался одноразовый бамп префикса v1→v2 плюс
     константное поле в хэше; правило эксплуатации «поменял settings.yml —
     поменяй отпечаток» физически не могло сработать. Хуже того, прежний тест
     `..._falls_back_to_empty_when_setting_is_absent` УДОСТОВЕРЯЛ это состояние
     как корректное. Теперь поле, его алиас и проброс в compose пришпилены к
     состоянию вне этого файла, а чтение идёт прямым обращением: «поля нет» —
     это AttributeError, а не тихий дефолт.
  5. Правило FrontierSearxngNoSuccessfulQueries (severity critical, notify:
     telegram) фильтровало по `status="ok"` — статусу, которого не эмитил никто
     и никогда. Селектор не выбирал ни одной серии, `пустой вектор == 0` давал
     пустой вектор, и «заземление полностью деградировало» не срабатывало ни при
     какой частоте ошибок. Имена статусов теперь сверяются между клиентом и
     prometheus/alerts.yml напрямую.

Главная ловушка этого файла — тавтология в проверках TTL. Если фикстура настроек
отдаёт то же число, что и продовый дефолт (3600), то `assert ttl == 3600` зелен
и при `max(60, settings.searxng_cache_ttl)`, и при литерале `3600` в коде: тест
не отличает «клиент слушается настройки» от «число зашито». Поэтому фикстура
намеренно отдаёт значения, которых нет ни в одном дефолте
(`FIXTURE_FULL_TTL` / `FIXTURE_EMPTY_TTL`), а
`test_ttl_fixture_values_are_not_the_production_defaults` следит, чтобы они
такими и остались.

Ни сети, ни Redis: httpx.AsyncClient и redis.asyncio.Redis подменяются
локально через monkeypatch (см. tests/stub_policy.py — глобальные заглушки
драйверов нежелательны).
"""

from __future__ import annotations

import ast
import inspect
import json
import re
from pathlib import Path
from typing import Any, Callable

import httpx
import pytest
import yaml

from worker.services import searxng_client as sx
from worker.services.searxng_client import SearXNGClient

pytestmark = pytest.mark.unit

# Корень репозитория — относительно файла теста: прогон идёт и на хосте,
# и внутри образа (маунт репозитория в /src).
REPO_ROOT = Path(__file__).resolve().parents[1]
CLIENT_PY = REPO_ROOT / "worker" / "services" / "searxng_client.py"
CONFIG_PY = REPO_ROOT / "shared" / "config.py"
ALERTS_YML = REPO_ROOT / "prometheus" / "alerts.yml"
COMPOSE_YML = REPO_ROOT / "docker-compose.yml"

# Серия, которую пишет note_searxng_request и читают правила Prometheus.
SEARXNG_METRIC = "frontier_searxng_requests_total"
# Правило «заземление полностью деградировало» — критическое, с notify: telegram.
DEGRADED_ALERT = "FrontierSearxngNoSuccessfulQueries"
# Env-переменная отпечатка движков: единственный инвалидатор кэша.
FINGERPRINT_ENV = "SEARXNG_ENGINES_FINGERPRINT"
# Сервисы compose, которые поднимают SearXNGClient: worker (gap-анализ руками),
# admin (планировщик через manual_jobs), mcp (search_frontier / search_balanced).
FINGERPRINT_SERVICES: tuple[str, ...] = ("admin", "mcp", "worker")

QUERY = "policy gap in humanoid robotics"

# Значения TTL для фикстуры настроек. Оба намеренно НЕ совпадают ни с продовым
# дефолтом searxng_cache_ttl (3600), ни с модульным дефолтом пустого TTL (120):
# иначе assert'ы ниже проходят и на зашитом литерале. См. докстринг модуля.
FIXTURE_FULL_TTL = 1500
FIXTURE_EMPTY_TTL = 90

# Отпечаток набора движков: инвалидатор кэша при правке searxng/settings.yml.
FIXTURE_ENGINES = "engines-keep-only-bing-brave-ddg"

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
    """
    Заглушка Settings: под pytest настоящий Settings — не pydantic-модель.

    searxng_cache_ttl / searxng_empty_cache_ttl здесь НЕ равны продовым дефолтам
    осознанно — см. докстринг модуля и
    test_ttl_fixture_values_are_not_the_production_defaults.
    """

    def __init__(self, **overrides: Any) -> None:
        self.searxng_enabled: bool = True
        self.searxng_url: str = "http://searxng:8080"
        self.searxng_user: str = ""
        self.searxng_password: str = ""
        self.searxng_timeout_seconds: float = 8.0
        self.searxng_cache_ttl: int = FIXTURE_FULL_TTL
        self.searxng_empty_cache_ttl: int = FIXTURE_EMPTY_TTL
        self.searxng_engines_fingerprint: str = FIXTURE_ENGINES
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

    @property
    def status_names(self) -> list[str]:
        return [status for _, _, status in self.statuses]

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


def _settings_field(field_name: str) -> ast.AnnAssign:
    """
    Объявление поля Settings — строго через ast, без импорта shared.config.

    Импортировать нельзя осмысленно: conftest подменяет pydantic_settings, и
    `Settings` под pytest не pydantic-модель (`Field(...)` не обрабатывается,
    алиасы не привязываются). Отсутствие поля — провал теста, а не «нет дефолта»:
    ровно это и было дефектом 2026-08-04.
    """
    tree = ast.parse(CONFIG_PY.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef) or node.name != "Settings":
            continue
        for stmt in node.body:
            if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                if stmt.target.id == field_name:
                    return stmt
    raise AssertionError(
        f"в {CONFIG_PY} нет поля Settings.{field_name}. Настройка, которой нет в модели, "
        "не читается из окружения вообще: model_config стоит на extra=\"ignore\", и "
        "переменная отбрасывается до того, как её кто-либо прочтёт."
    )


def _settings_default(field_name: str) -> Any:
    """Продовый дефолт поля Settings — первый позиционный аргумент Field(...)."""
    value = _settings_field(field_name).value
    if isinstance(value, ast.Call) and value.args:
        first = value.args[0]
        if isinstance(first, ast.Constant):
            return first.value
    raise AssertionError(f"не найден дефолт поля {field_name} в {CONFIG_PY}")


def _settings_aliases(field_name: str) -> tuple[str, ...]:
    """Env-алиасы поля: alias="X" / validation_alias=AliasChoices("X", "Y")."""
    value = _settings_field(field_name).value
    if not isinstance(value, ast.Call):
        return ()
    found: list[str] = []
    for keyword in value.keywords:
        if keyword.arg not in {"alias", "validation_alias"}:
            continue
        node = keyword.value
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            found.append(node.value)
        elif isinstance(node, ast.Call):
            found.extend(
                arg.value
                for arg in node.args
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str)
            )
    return tuple(found)


def _settings_extra_policy() -> str | None:
    """Значение `extra` в Settings.model_config — причина, по которой поле обязано быть."""
    tree = ast.parse(CONFIG_PY.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef) or node.name != "Settings":
            continue
        for stmt in node.body:
            if not isinstance(stmt, ast.Assign) or not isinstance(stmt.value, ast.Call):
                continue
            targets = [t.id for t in stmt.targets if isinstance(t, ast.Name)]
            if "model_config" not in targets:
                continue
            for keyword in stmt.value.keywords:
                if keyword.arg == "extra" and isinstance(keyword.value, ast.Constant):
                    return str(keyword.value.value)
    return None


def _compose_service_environment(service: str) -> tuple[str, ...]:
    """Ключи блока environment сервиса docker-compose.yml."""
    document = yaml.safe_load(COMPOSE_YML.read_text(encoding="utf-8")) or {}
    body = (document.get("services") or {}).get(service) or {}
    environment = body.get("environment")
    if isinstance(environment, dict):
        return tuple(str(key) for key in environment)
    if isinstance(environment, list):
        return tuple(str(item).split("=", 1)[0].strip() for item in environment)
    return ()


def test_ttl_fixture_values_are_not_the_production_defaults() -> None:
    """
    Антитавтология: если фикстура совпадёт с дефолтом, проверки TTL перестанут
    отличать «клиент слушается настройки» от «число зашито в код».

    Именно так этот файл и был сломан до 2026-08-03: FakeSettings отдавал 3600,
    продовый дефолт тоже 3600, и подмена `max(60, settings.searxng_cache_ttl)`
    на литерал `3600` оставляла весь файл зелёным.
    """
    production_full = _settings_default("searxng_cache_ttl")
    assert production_full == 3600, (
        "продовый дефолт searxng_cache_ttl изменился — сверь FIXTURE_FULL_TTL с новым значением"
    )
    assert FIXTURE_FULL_TTL != production_full, (
        "FIXTURE_FULL_TTL совпал с продовым дефолтом: проверки полного TTL стали тавтологией"
    )
    assert FIXTURE_EMPTY_TTL != sx._EMPTY_CACHE_TTL_DEFAULT_SECONDS, (
        "FIXTURE_EMPTY_TTL совпал с модульным дефолтом: проверки пустого TTL стали тавтологией"
    )
    assert FIXTURE_EMPTY_TTL != FIXTURE_FULL_TTL


# ── 1. Непустая выдача ───────────────────────────────────────────────────────


async def test_populated_200_is_returned_and_cached(rig: Rig) -> None:
    """200 с результатами: нормализованный список возвращается и живёт полный TTL."""
    rig.respond_with(POPULATED_PAYLOAD)

    items = await rig.search()

    assert [item["url"] for item in items] == [
        "https://example.org/a",  # utm_* срезан
        "https://example.net/b",
    ]
    assert len(rig.redis.setex_calls) == 1
    key, ttl, value = rig.redis.setex_calls[0]
    assert ttl == FIXTURE_FULL_TTL, "полный TTL взят не из настроек"
    assert json.loads(value) == items
    assert rig.status_names == ["success"]


async def test_populated_second_call_is_served_from_cache(rig: Rig) -> None:
    """Второй одинаковый запрос не ходит в сеть и помечается cache_hit."""
    rig.respond_with(POPULATED_PAYLOAD)

    first = await rig.search()
    second = await rig.search()

    assert second == first
    assert rig.http_calls == 1, "повторный запрос ушёл в сеть — кэш не сработал"
    assert rig.status_names == ["success", "cache_hit"]


async def test_full_ttl_is_read_from_settings_not_hardcoded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Крутим searxng_cache_ttl на заведомо «неродное» число — оно обязано доехать до Redis."""
    rig = Rig(monkeypatch, searxng_cache_ttl=4242)
    rig.respond_with(POPULATED_PAYLOAD)

    await rig.search()

    assert rig.redis.setex_calls[0][1] == 4242


async def test_full_ttl_has_a_lower_floor(monkeypatch: pytest.MonkeyPatch) -> None:
    """Абсурдно малый полный TTL поднимается до пола в 60 с."""
    rig = Rig(monkeypatch, searxng_cache_ttl=5)
    rig.respond_with(POPULATED_PAYLOAD)

    await rig.search()

    assert rig.redis.setex_calls[0][1] == 60


# ── 2. Пустая выдача: свой короткий TTL ──────────────────────────────────────


async def test_empty_result_list_gets_its_own_short_ttl(rig: Rig) -> None:
    """
    200 с ПУСТЫМ списком результатов кэшируется, но по searxng_empty_cache_ttl,
    а не по полному TTL.

    Кэшировать пустоту всё ещё нужно — иначе мёртвый набор движков долбится на
    каждый вызов. Но короткий TTL означает, что починка `searxng/settings.yml`
    становится видна через минуты, а не через час.
    """
    rig.respond_with(EMPTY_PAYLOAD)

    items = await rig.search()

    assert items == []
    assert len(rig.redis.setex_calls) == 1, "пустой ответ перестал кэшироваться вовсе"
    key, ttl, value = rig.redis.setex_calls[0]
    assert value == "[]"
    assert ttl == FIXTURE_EMPTY_TTL, "у пустой выдачи не свой TTL"
    assert ttl < FIXTURE_FULL_TTL, "пустота живёт не короче валидной выдачи"
    assert rig.redis.store[key] == "[]"


async def test_empty_ttl_is_read_from_settings_not_hardcoded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rig = Rig(monkeypatch, searxng_empty_cache_ttl=45)
    rig.respond_with(EMPTY_PAYLOAD)

    await rig.search()

    assert rig.redis.setex_calls[0][1] == 45


async def test_empty_ttl_has_a_lower_floor(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Пол у пустого TTL свой и ниже общего: смысл настройки — секунды-минуты,
    и общий пол в 60 с молча съел бы 30-секундное значение. Но ноль/отрицательное
    в SETEX недопустимы, поэтому пол всё же есть.
    """
    rig = Rig(monkeypatch, searxng_empty_cache_ttl=0)
    rig.respond_with(EMPTY_PAYLOAD)

    await rig.search()

    assert rig.redis.setex_calls[0][1] == sx._EMPTY_CACHE_TTL_FLOOR_SECONDS
    assert sx._EMPTY_CACHE_TTL_FLOOR_SECONDS < sx._CACHE_TTL_FLOOR_SECONDS


async def test_empty_ttl_never_outlives_the_full_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    """Инвариант: пустой ответ не может жить дольше валидного ни при какой настройке."""
    rig = Rig(monkeypatch, searxng_cache_ttl=100, searxng_empty_cache_ttl=9000)
    rig.respond_with(EMPTY_PAYLOAD)

    await rig.search()

    assert rig.redis.setex_calls[0][1] == 100


def test_empty_ttl_is_a_declared_settings_field() -> None:
    """
    Поле обязано существовать в shared/config.py, а не читаться через getattr.

    Здесь раньше стоял тест-близнец, удостоверявший ОБРАТНОЕ: что клиент умеет
    жить без поля, читая его через `getattr(..., default)`. Он был написан, пока
    поля действительно не было, и пережил момент, когда оно появилось, — то есть
    защищал молчаливую деградацию. У Settings `model_config` стоит на
    `extra="ignore"`: pydantic-settings читает окружение ТОЛЬКО по объявленным
    полям, а необъявленный ключ отбрасывает без единого слова. Значит
    `getattr`-с-дефолтом означал бы «SEARXNG_EMPTY_CACHE_TTL из .env не доезжает
    никогда, а выглядит как настроенный» — ровно так до 2026-08-04 жил
    searxng_engines_fingerprint.

    Проверяется состояние ВНЕ этого файла: фикстура FakeSettings задаёт атрибут
    сама и потому увидеть дефект не может в принципе.
    """
    assert _settings_extra_policy() == "ignore", (
        "политика extra у Settings изменилась — пересмотри рассуждение ниже: именно "
        'extra="ignore" делает необъявленное поле невидимым'
    )
    aliases = _settings_aliases("searxng_empty_cache_ttl")
    assert "SEARXNG_EMPTY_CACHE_TTL" in aliases, (
        "поле searxng_empty_cache_ttl не связано с SEARXNG_EMPTY_CACHE_TTL "
        f"(найденные алиасы: {list(aliases)}) — переменная из .env не доедет"
    )
    assert _settings_default("searxng_empty_cache_ttl") == sx._EMPTY_CACHE_TTL_DEFAULT_SECONDS, (
        "дефолт поля разошёлся с модульной константой клиента — одна из сторон отстала"
    )


def test_empty_ttl_has_no_silent_default(rig: Rig) -> None:
    """
    Пропажа поля обязана падать, а не подставлять число.

    Обратная сторона теста выше: вернут `getattr` с дефолтом — удаление поля
    перестанет быть заметным. Метод зовём напрямую: запись в кэш обёрнута в
    `except Exception`, поэтому через search() AttributeError был бы проглочен,
    и тест бы врал.
    """
    source = ast.parse(CLIENT_PY.read_text(encoding="utf-8"))
    for node in ast.walk(source):
        if not isinstance(node, ast.FunctionDef) or node.name != "_cache_ttl_for":
            continue
        for inner in ast.walk(node):
            if isinstance(inner, ast.Call):
                name = getattr(inner.func, "id", None) or getattr(inner.func, "attr", None)
                assert name != "getattr", (
                    "_cache_ttl_for снова читает настройку через getattr: отсутствие "
                    "поля в Settings опять станет тихим дефолтом"
                )
        break
    else:  # pragma: no cover - метод не переименовывали
        raise AssertionError("метод _cache_ttl_for не найден в клиенте")

    delattr(rig.settings, "searxng_empty_cache_ttl")
    with pytest.raises(AttributeError):
        rig.client._cache_ttl_for([])


# ── 3. Метрика различает ноль и не-ноль ──────────────────────────────────────


async def test_zero_and_many_results_are_metrically_distinguishable(rig: Rig) -> None:
    """
    Ноль результатов и полная выдача пишутся РАЗНЫМИ статусами метрики.

    Пока они были одинаковыми ("success"), по frontier_searxng_requests_total
    нельзя было отличить рабочий gap-анализ от месяцами пустого.
    """
    rig.respond_with(EMPTY_PAYLOAD)
    empty_items = await rig.search("topic with no coverage")
    empty_statuses = list(rig.statuses)

    rig.statuses.clear()
    rig.respond_with(POPULATED_PAYLOAD)
    full_items = await rig.search("topic with coverage")
    full_statuses = list(rig.statuses)

    assert empty_items == [] and len(full_items) == 2
    assert empty_statuses == [("worker", "missing_signals", "empty")]
    assert full_statuses == [("worker", "missing_signals", "success")]
    assert empty_statuses != full_statuses


def test_metric_status_names_used_by_alerts_are_untouched() -> None:
    """
    prometheus/alerts.yml фильтрует frontier_searxng_requests_total по
    status="error" (FrontierSearxngErrorBurst) и status="success"
    (FrontierSearxngNoSuccessfulQueries). Новый статус пустоты обязан быть
    ДОБАВЛЕН, а не переименовывать существующие: иначе алерты потеряют серию.

    Проверяем по исходнику клиента, что "success" и "error" из него не пропали.
    """
    source = CLIENT_PY.read_text(encoding="utf-8")
    assert '"success"' in source, "статус success исчез — алерты и дашборды потеряют серию"
    assert '"error"' in source, "статус error исчез — FrontierSearxngErrorBurst ослепнет"
    assert '"empty"' in source, "нет отдельного статуса для пустой выдачи"


# ── 3b. Алерты фильтруют по статусам, которые клиент действительно пишет ─────
#
# Проверка выше сверяла исходник клиента сам с собой: литерал "success" на месте
# — значит «алерты не потеряют серию». Это не проверка, а надежда. Дефект
# 2026-08-04: FrontierSearxngNoSuccessfulQueries (severity critical, notify:
# telegram) фильтровал по status="ok", а такого статуса не эмитил никто и никогда
# — правило было мертво с рождения, и предыдущий проход по этому же файлу назвал
# его живым потребителем метки. Дальше сверяются два ФАЙЛА, а не файл с собой.

_STATUS_SELECTOR = re.compile(
    SEARXNG_METRIC + r"\s*\{([^}]*)\}",
)
_STATUS_MATCHER = re.compile(r'\bstatus\s*(=~|!~|!=|=)\s*"([^"]*)"')
_REGEX_LABEL_OPS = frozenset({"=~", "!~"})


def _client_emitted_statuses() -> frozenset[str]:
    """
    Значения label `status`, которые реально уходят в note_searxng_request.

    Через ast по исходнику клиента — единственного вызывающего этой функции во
    всём репозитории. Тернарники ("success" if normalized else "empty") дают оба
    исхода, поэтому собираются все строковые константы аргумента.
    """
    tree = ast.parse(CLIENT_PY.read_text(encoding="utf-8"))
    statuses: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
        if name != "note_searxng_request":
            continue
        argument: ast.expr | None = node.args[2] if len(node.args) > 2 else None
        for keyword in node.keywords:
            if keyword.arg == "status":
                argument = keyword.value
        if argument is None:
            continue
        for inner in ast.walk(argument):
            if isinstance(inner, ast.Constant) and isinstance(inner.value, str):
                statuses.add(inner.value)
    return frozenset(statuses)


def _alert_status_references() -> tuple[tuple[str, str, str], ...]:
    """(имя правила, оператор, значение) для каждого матчера status по нашей серии."""
    document = yaml.safe_load(ALERTS_YML.read_text(encoding="utf-8")) or {}
    references: list[tuple[str, str, str]] = []
    for group in document.get("groups") or []:
        for raw in (group or {}).get("rules") or []:
            if not isinstance(raw, dict):
                continue
            name = str(raw.get("alert") or raw.get("record") or "?")
            expr = str(raw.get("expr") or "")
            for selector in _STATUS_SELECTOR.findall(expr):
                for operator, pattern in _STATUS_MATCHER.findall(selector):
                    values = (
                        [part for part in pattern.split("|") if part]
                        if operator in _REGEX_LABEL_OPS
                        else [pattern]
                    )
                    references.extend((name, operator, value) for value in values)
    return tuple(references)


def _degraded_alert_expr() -> str:
    document = yaml.safe_load(ALERTS_YML.read_text(encoding="utf-8")) or {}
    for group in document.get("groups") or []:
        for raw in (group or {}).get("rules") or []:
            if isinstance(raw, dict) and raw.get("alert") == DEGRADED_ALERT:
                return str(raw.get("expr") or "")
    raise AssertionError(f"правило {DEGRADED_ALERT} исчезло из {ALERTS_YML}")


def test_status_extraction_is_not_vacuous() -> None:
    """Сломанный извлекатель сделал бы обе проверки ниже зелёными на пустом множестве."""
    emitted = _client_emitted_statuses()
    assert len(emitted) >= 4, f"ast нашёл в клиенте всего {sorted(emitted)} — извлекатель сломан"
    assert {"success", "error", "empty"} <= emitted
    references = _alert_status_references()
    assert len(references) >= 2, (
        f"в {ALERTS_YML} не найдено ни одного матчера status по {SEARXNG_METRIC} — "
        "правила переписали или извлекатель сломан"
    )
    assert {name for name, _, _ in references} >= {DEGRADED_ALERT}


def test_alerts_filter_on_statuses_the_client_actually_emits() -> None:
    """
    Каждое значение status в alerts.yml обязано быть статусом, который клиент пишет.

    Дефект: `status="ok"`. Такого значения не эмитил ни один путь кода —
    note_searxng_request вызывается только из searxng_client.py и только с
    success | error | empty | cache_hit | cache_hit_empty. Селектор не выбирал ни
    одной серии, `sum by (...)` возвращал пустой вектор, и критический алерт «SearXNG
    grounding is fully degraded» не срабатывал ни при какой частоте ошибок. YAML при
    этом валиден, выражение синтаксически цело, promtool ничего не скажет: имена
    меток он не знает.
    """
    emitted = _client_emitted_statuses()
    unknown = sorted(
        {
            (name, value)
            for name, _, value in _alert_status_references()
            if value not in emitted
        }
    )
    assert not unknown, (
        f"правила alerts.yml фильтруют {SEARXNG_METRIC} по статусам, которых клиент не "
        f"пишет: {unknown}. Такой селектор не выбирает ни одной серии — правило молчит "
        f"навсегда. Клиент эмитит: {sorted(emitted)}"
    )


def test_fully_degraded_alert_survives_a_never_created_success_series() -> None:
    """
    «Успехов нет» выражено через `unless`, а не через `== 0`.

    Второй способ умереть, помимо неверного имени статуса. У counter'а
    prometheus_client дочерняя серия создаётся при ПЕРВОМ инкременте с такими
    метками: если движки лежат с момента старта процесса — ровно тот сценарий,
    ради которого правило и написано, — серии {status="success"} для этой пары
    (service, mode) не существует вовсе. `increase(...)` по ней пуст, а `пустой
    вектор == 0` — снова пустой вектор, и `and` гасит всё выражение. `unless`
    вычитает успехи из ошибок и на отсутствующей серии ведёт себя правильно:
    вычитать нечего — левая часть остаётся.
    """
    expr = _degraded_alert_expr()
    statuses = {value for name, _, value in _alert_status_references() if name == DEGRADED_ALERT}
    assert "success" in statuses, (
        f"{DEGRADED_ALERT} больше не смотрит на статус success: {sorted(statuses)}"
    )
    assert "unless" in expr, (
        f"{DEGRADED_ALERT} перестал использовать `unless` — проверь, что отсутствие серии "
        "успехов по-прежнему НЕ гасит алерт"
    )
    assert "==" not in expr.replace(" ", ""), (
        f"{DEGRADED_ALERT} снова сравнивает счётчик успехов с нулём: на отсутствующей "
        "серии это пустой вектор, и алерт не сработает именно тогда, когда должен"
    )


async def test_cached_empty_list_is_still_a_hit_but_counted_separately(rig: Rig) -> None:
    """
    Строка "[]" в Redis остаётся попаданием — это и есть защита мёртвого
    апстрима от долбёжки. Но считается она отдельным статусом: при коротком TTL
    всплеск по мёртвой теме — это одна "empty" и хвост "cache_hit_empty",
    и они не должны маскироваться под попадания по здоровой выдаче.
    """
    rig.respond_with(EMPTY_PAYLOAD)
    first = await rig.search()

    second = await rig.search()

    assert first == [] and second == []
    assert rig.http_calls == 1, "повторный поход в сеть — пустой кэш перестал быть попаданием"
    assert rig.status_names == ["empty", "cache_hit_empty"]


# ── 4. Ключ кэша ─────────────────────────────────────────────────────────────


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
    assert len(_cache_key_material()) >= 6, "ast-извлекатель вернул неправдоподобно мало полей"


async def test_cache_key_changes_for_every_input_it_claims_to_cover(rig: Rig) -> None:
    """
    Ключ обязан меняться при изменении query / categories / language /
    time_range / limit / engines_fingerprint. Совпадение любых двух ключей =
    чужой ответ из кэша.
    """
    key = rig.client._cache_key
    base_kwargs: dict[str, Any] = {
        "query": QUERY,
        "categories": "general,news",
        "language": "en",
        "time_range": "month",
        "limit": 5,
        "engines_fingerprint": FIXTURE_ENGINES,
    }
    base = key(**base_kwargs)
    changes: dict[str, Any] = {
        "query": QUERY + " 2026",
        "categories": "science",
        "language": "ru",
        "time_range": "day",
        "limit": 10,
        "engines_fingerprint": "engines-after-settings-yml-edit",
    }
    variants = {name: key(**{**base_kwargs, name: value}) for name, value in changes.items()}

    collisions = [name for name, value in variants.items() if value == base]
    assert not collisions, f"ключ кэша не зависит от {collisions}: разные запросы делят одну запись"
    assert len(set(variants.values())) == len(variants), "разные входы дали одинаковые ключи"


def test_cache_key_covers_the_engine_fingerprint() -> None:
    """
    Набор движков входит в материал ключа — через объявленный оператором
    отпечаток.

    Честная граница: сам набор движков задаётся на стороне сервиса
    (`searxng/settings.yml` монтируется только в контейнер searxng), клиент его
    не видит и без лишнего запроса к /config увидеть не может. Поэтому в ключ
    входит `engines_fingerprint` — значение, которое объявляет оператор
    (SEARXNG_ENGINES_FINGERPRINT). Оно отслеживает ровно то, что задекларировано,
    и ничего сверх того; правило эксплуатации — менять его вместе с settings.yml.
    """
    material = _cache_key_material()
    assert material == (
        "categories",
        "engines_fingerprint",
        "language",
        "limit",
        "query",
        "time_range",
    )

    key_params = set(inspect.signature(SearXNGClient._cache_key).parameters)
    assert key_params == {
        "self",
        "query",
        "categories",
        "language",
        "time_range",
        "limit",
        "engines_fingerprint",
    }


def test_cache_key_namespace_was_bumped_past_the_blind_version(rig: Rig) -> None:
    """
    Префикс ключа поднят до v2. Это одноразовая инвалидация на выкате: записи
    v1 считались без отпечатка движков, среди них — часовые пустоты, из-за
    которых починка движков выглядела несработавшей. Читать их нельзя.
    """
    key = rig.client._cache_key(
        query=QUERY,
        categories="general,news",
        language="en",
        time_range="month",
        limit=5,
        engines_fingerprint=FIXTURE_ENGINES,
    )
    assert key.startswith("searxng:v2:"), f"версия неймспейса ключа не поднята: {key}"


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
        engines_fingerprint=FIXTURE_ENGINES,
    )
    assert rig.redis.get_keys == [expected]
    assert rig.redis.setex_calls[0][0] == expected


async def test_engine_reconfiguration_invalidates_the_cached_empty_result(rig: Rig) -> None:
    """
    Сценарий «починили движки» — теперь он работает.

    Шаг 1: движки сломаны, апстрим отдаёт 200 + [] → пустота уходит в кэш.
    Шаг 2: оператор правит searxng/settings.yml и бампает отпечаток.
    Шаг 3: тот же запрос считает ДРУГОЙ ключ, идёт в сеть и получает выдачу.
    """
    rig.respond_with(EMPTY_PAYLOAD)
    before_fix = await rig.search()
    stale_key = rig.redis.setex_calls[0][0]

    rig.settings.searxng_engines_fingerprint = "engines-after-settings-yml-edit"
    rig.respond_with(POPULATED_PAYLOAD)
    after_fix = await rig.search()

    assert before_fix == []
    assert [item["url"] for item in after_fix] == [
        "https://example.org/a",
        "https://example.net/b",
    ]
    assert rig.http_calls == 2, "клиент не сходил в сеть после смены отпечатка — инвалидации нет"
    fresh_key = rig.redis.setex_calls[1][0]
    assert fresh_key != stale_key, "смена набора движков не поменяла ключ кэша"


def test_engine_fingerprint_is_a_declared_settings_field() -> None:
    """
    Поле обязано существовать в shared/config.py — иначе отпечаток константен.

    Дефект 2026-08-04, из-за которого весь механизм инвалидации был декорацией:
    поля `searxng_engines_fingerprint` в Settings не было, а `model_config` стоит
    на `extra="ignore"`. Pydantic-settings читает окружение ТОЛЬКО по объявленным
    полям, а лишние ключи при такой политике отбрасывает молча — то есть
    SEARXNG_ENGINES_FINGERPRINT из .env не доезжал до объекта настроек в принципе,
    и `getattr(settings, "searxng_engines_fingerprint", "")` возвращал "" при
    любом содержимом .env. Ни один тест этого не видел: фикстура FakeSettings
    задаёт атрибут сама, поэтому весь файл был зелёным на проде, сидящем в
    мутантном состоянии.

    Поэтому проверяется состояние ВНЕ этого файла: объявление поля, его env-алиас
    (именно то имя, которое названо в .env.example и в докстринге клиента) и
    дефолт. Дефолт обязан быть пустой строкой: «оператор ещё не объявил
    отпечаток» — это отсутствие инвалидации, а не выдуманное значение.
    """
    assert _settings_extra_policy() == "ignore", (
        "политика extra у Settings изменилась — перечитай рассуждение выше: именно "
        "extra=\"ignore\" делает объявление поля обязательным"
    )
    aliases = _settings_aliases("searxng_engines_fingerprint")
    assert FINGERPRINT_ENV in aliases, (
        f"поле searxng_engines_fingerprint не привязано к {FINGERPRINT_ENV} "
        f"(объявленные алиасы: {list(aliases)}) — .env-переменная с этим именем "
        "не будет прочитана"
    )
    assert _settings_default("searxng_engines_fingerprint") == ""


def test_engine_fingerprint_env_var_reaches_the_services_that_search() -> None:
    """
    Объявленного поля мало: docker-compose.yml не пробрасывает .env целиком.

    У сервисов нет `env_file`, каждый получает явный allow-list в `environment:`.
    Переменная, отсутствующая в этом списке, не попадает в контейнер, и поле
    Settings живёт на дефолте — та же тихая смерть, что и отсутствие поля, только
    на слой ниже. Ровно так уже ломались MISSING_SIGNALS_* и SEARXNG_* (см.
    комментарии в .env.example).
    """
    missing = [
        service
        for service in FINGERPRINT_SERVICES
        if FINGERPRINT_ENV not in _compose_service_environment(service)
    ]
    assert not missing, (
        f"{FINGERPRINT_ENV} не пробрасывается сервисам {missing}: они поднимают "
        "SearXNGClient, но получат пустой отпечаток при любом .env"
    )


def test_engine_fingerprint_has_no_silent_default(rig: Rig) -> None:
    """
    Чтение отпечатка — прямое обращение к полю, без getattr с дефолтом.

    Механизм дефекта был именно в этом: `getattr(settings, "...", "")` превращал
    «поля нет в модели» в валидное пустое значение, и разница между «оператор не
    объявил отпечаток» и «настройка физически не может быть прочитана» исчезала.
    Прямое обращение делает вторую ситуацию AttributeError'ом.
    """
    source = ast.parse(CLIENT_PY.read_text(encoding="utf-8"))
    for node in ast.walk(source):
        if not isinstance(node, ast.FunctionDef) or node.name != "_engines_fingerprint":
            continue
        for inner in ast.walk(node):
            if isinstance(inner, ast.Call):
                name = getattr(inner.func, "id", None) or getattr(inner.func, "attr", None)
                assert name != "getattr", (
                    "_engines_fingerprint снова читает настройку через getattr: "
                    "отсутствие поля в Settings опять станет тихой пустой строкой"
                )
        break
    else:  # pragma: no cover - защита от переименования метода
        raise AssertionError("метод _engines_fingerprint не найден в клиенте")

    delattr(rig.settings, "searxng_engines_fingerprint")
    with pytest.raises(AttributeError):
        rig.client._engines_fingerprint()


async def test_engine_fingerprint_value_reaches_the_cache_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Значение из настроек доезжает до ключа как есть — не константа и не хэш «чего-то».

    Отдельно от test_engine_reconfiguration_invalidates_...: там проверяется, что
    ключ ИЗМЕНИЛСЯ, здесь — что он посчитан именно от объявленного отпечатка.
    """
    declared = "sha256-of-settings-yml-2026-08-04"
    rig = Rig(monkeypatch, searxng_engines_fingerprint=declared)
    rig.respond_with(POPULATED_PAYLOAD)

    assert rig.client._engines_fingerprint() == declared
    await rig.search()

    expected = rig.client._cache_key(
        query=QUERY,
        categories="general,news",
        language="en",
        time_range=None,
        limit=5,
        engines_fingerprint=declared,
    )
    assert rig.redis.get_keys == [expected]
    assert rig.redis.setex_calls[0][0] == expected


# ── 5. Не-2xx не отравляет кэш ───────────────────────────────────────────────


async def test_http_500_raises_and_writes_nothing_to_cache(rig: Rig) -> None:
    """Ошибка апстрима не должна превращаться в закэшированную пустоту."""
    rig.respond_status(500)

    with pytest.raises(httpx.HTTPStatusError):
        await rig.search()

    assert rig.redis.setex_calls == [], "не-2xx попал в кэш"
    assert rig.redis.store == {}
    assert rig.status_names == ["error"]


async def test_http_429_is_counted_as_rate_limit_and_not_cached(rig: Rig) -> None:
    """429 отмечается как rate-limit, пробрасывается наверх и ничего не кэширует."""
    rig.respond_status(429)

    with pytest.raises(httpx.HTTPStatusError):
        await rig.search()

    assert rig.rate_limits == [("worker", "searxng", "missing_signals")]
    assert rig.redis.setex_calls == []
    assert rig.status_names == ["error"]


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


# ── 6. Кривое тело ответа ────────────────────────────────────────────────────


async def test_non_json_body_raises_out_of_the_client_and_caches_nothing(rig: Rig) -> None:
    """
    Тело не-JSON (200 + HTML заглушки/капчи) поднимает исключение наружу
    из search().

    Ожидание «клиент проглотит и вернёт []» здесь НЕ выполняется — и это лучший
    из вариантов: вызывающий код видит сбой, а в кэш ничего не пишется. Глотать
    такое и подменять пустым списком нельзя даже теперь, с коротким TTL: это
    заглушка апстрима, а не честное «ничего не нашлось».
    """
    rig.respond_raw("<html><body>gateway error</body></html>")

    with pytest.raises(ValueError):  # json.JSONDecodeError — подкласс ValueError
        await rig.search()

    assert rig.redis.setex_calls == [], "кривое тело ответа попало в кэш"
    assert rig.redis.store == {}
    assert rig.status_names == ["error"]


async def test_json_object_without_results_key_is_counted_as_empty(rig: Rig) -> None:
    """
    Валидный JSON без ключа 'results' исключения не поднимает —
    `payload.get("results") or []` даёт пустой список.

    Это по-прежнему пустота, но теперь честно помеченная: статус "empty" и
    короткий TTL, ровно как у явного `{"results": []}`.
    """
    rig.respond_with({"query": QUERY, "number_of_results": 0})

    items = await rig.search()

    assert items == []
    assert len(rig.redis.setex_calls) == 1
    _, ttl, value = rig.redis.setex_calls[0]
    assert value == "[]"
    assert ttl == FIXTURE_EMPTY_TTL
    assert rig.status_names == ["empty"]


async def test_json_top_level_list_is_counted_as_error_not_success(rig: Rig) -> None:
    """
    Тело-массив вместо объекта — дефект апстрима, и метрика теперь говорит
    именно это.

    Раньше `payload.get(...)` выполнялся УЖЕ ВНЕ try/except: наружу летел
    AttributeError, но счётчик к этому моменту успевал записать "success" —
    упавший вызов выглядел успешным. Теперь тип тела проверяется внутри try,
    статус пишется "error", наружу летит TypeError. Все вызывающие
    (missing_signals, search_frontier, search_balanced) ловят Exception, так
    что смена класса исключения для них ничего не меняет.
    """
    rig.respond_with([{"url": "https://example.org/a"}])

    with pytest.raises(TypeError):
        await rig.search()

    assert rig.status_names == ["error"]
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


async def test_results_that_are_entirely_junk_count_as_empty(rig: Rig) -> None:
    """
    Апстрим вернул элементы, но после фильтрации не осталось ничего — для
    вызывающего это такая же пустота, как и `{"results": []}`.

    Статус считается по тому, что реально ушло наверх, а не по длине сырого
    payload: иначе «десять SSRF-ссылок» выглядели бы в метрике успехом.
    """
    rig.respond_with({"results": [{"url": "http://127.0.0.1:8080/private", "title": "SSRF"}]})

    items = await rig.search()

    assert items == []
    assert rig.status_names == ["empty"]
    assert rig.redis.setex_calls[0][1] == FIXTURE_EMPTY_TTL
