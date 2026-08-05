"""
Маршрутизация краула и чистка извлечённых URL.

Заведён 2026-08-05 по замеру на живом стеке. Гипотеза была «источникам нужен
прокси», и она **не подтвердилась**: из 149 включённых источников ни одному
прокси не нужен сверх уже выданных 37. Отказы краула оказались о другом,
и распались на три несвязанных причины:

  1. `MEDIUM_HOSTS` сравнивался ТОЧНО, поэтому `leohuax.medium.com` и прочие
     поддомены не попадали в браузерный путь (единственный, у которого есть
     прокси) и уходили в обычную сессию. Прямой выход к Medium мёртв на
     TLS-рукопожатии — замер дал ConnectTimeout напрямую и 200 через прокси.
  2. Обычная aiohttp-сессия ходит БЕЗ прокси, и любой хост с мёртвым прямым
     маршрутом отваливался по таймауту заново при каждой попытке: 399
     таймаутов в сутки и ни одного повтора.
  3. URL, выдранные из прозы arXiv-абстрактов, тащили за собой пунктуацию
     и остатки TeX: `github.com/CPS-research-group/ink_bwts.`,
     `github.com/Xia12121/LoCA}{here}.` — гарантированные 404, неотличимые
     в метрике от настоящих мёртвых ссылок.

Из 30 реально отказавших ссылок сменой маршрута оживают 7. Остальное — 403
от Cloudflare на самом Medium (прокси не лечит, это бот-защита сайта, а не
наш адрес выхода) и настоящие 404.
"""

from __future__ import annotations

import pytest

from shared.linked_urls import finalize_linked_urls, strip_url_tail

pytestmark = pytest.mark.unit


# ── Чистка хвостов у извлечённых URL ─────────────────────────────────────────


@pytest.mark.parametrize(
    ("raw", "expected", "why"),
    [
        (
            "https://github.com/CPS-research-group/ink_bwts.",
            "https://github.com/CPS-research-group/ink_bwts",
            "точка конца предложения из абстракта",
        ),
        (
            "https://github.com/Xia12121/LoCA}{here}.",
            "https://github.com/Xia12121/LoCA",
            "остаток TeX-разметки",
        ),
        (
            "https://example.com/page,",
            "https://example.com/page",
            "запятая при перечислении",
        ),
        (
            "https://example.com/page)",
            "https://example.com/page",
            "непарная закрывающая скобка",
        ),
        (
            "https://ru.wikipedia.org/wiki/Foo_(bar)",
            "https://ru.wikipedia.org/wiki/Foo_(bar)",
            "ПАРНЫЕ скобки значащие — трогать нельзя",
        ),
        (
            "https://example.com/a(b)c).",
            "https://example.com/a(b)c",
            "снимается только лишняя скобка, парная остаётся",
        ),
        (
            "https://example.com/path",
            "https://example.com/path",
            "чистый URL не меняется",
        ),
        ("", "", "пустая строка"),
    ],
)
def test_strip_url_tail(raw: str, expected: str, why: str) -> None:
    assert strip_url_tail(raw) == expected, why


def test_finalize_applies_the_cleanup() -> None:
    """Чистка обязана стоять в общем пути, а не только в помощнике."""
    out = finalize_linked_urls(["https://github.com/org/repo."])
    assert out == ["https://github.com/org/repo"]


def test_cleanup_makes_duplicates_collapse() -> None:
    """Побочная польза: ссылка с хвостом и без него — одна ссылка.

    До чистки они считались разными и краулились дважды, причём вторая
    попытка была заведомо обречена.
    """
    out = finalize_linked_urls(
        ["https://example.com/post", "https://example.com/post.", "https://example.com/post,"]
    )
    assert out == ["https://example.com/post"]


# ── Выбор маршрута ───────────────────────────────────────────────────────────


from shared.crawl_routing import DEAD_ROUTE_SET, DEAD_ROUTE_TTL, host_of, is_browser_host


class _FakeRedis:
    def __init__(self, dead: set[str] | None = None, broken: bool = False):
        self.dead = dead or set()
        self.broken = broken
        self.expired = False

    async def sismember(self, key, value):
        if self.broken:
            raise RuntimeError("redis is down")
        return value in self.dead

    async def sadd(self, key, value):
        if self.broken:
            raise RuntimeError("redis is down")
        self.dead.add(value)

    async def expire(self, key, ttl):
        self.expired = True


class _Router:
    """Та же логика выбора маршрута, что в EnrichmentEngine.

    Каталог `crawl4ai/` не пакет (свой Dockerfile, свой WORKDIR), поэтому его
    модули из тестов не импортируются. Сами решения живут в
    `shared/crawl_routing.py` и проверяются здесь напрямую; что движок
    действительно ими пользуется, держит статический тест ниже.
    """

    def __init__(self, redis):
        self._redis = redis

    async def should_use_browser(self, url: str) -> bool:
        if is_browser_host(url):
            return True
        try:
            return bool(await self._redis.sismember(DEAD_ROUTE_SET, host_of(url)))
        except Exception:
            return False

    async def mark_dead(self, url: str) -> None:
        host = host_of(url)
        if not host:
            return
        try:
            await self._redis.sadd(DEAD_ROUTE_SET, host)
            await self._redis.expire(DEAD_ROUTE_SET, DEAD_ROUTE_TTL)
        except Exception:
            pass


def _engine(redis):
    return _Router(redis)


@pytest.mark.parametrize(
    ("url", "expected", "why"),
    [
        ("https://medium.com/@a/b", True, "основной домен"),
        ("https://www.medium.com/x", True, "www"),
        ("https://leohuax.medium.com/x", True, "ПОДДОМЕН — публикации Medium живут на них"),
        ("https://mrspaul999.medium.com/y", True, "поддомен"),
        ("https://medium.com:443/x", True, "порт в netloc не должен ломать разбор"),
        ("https://notmedium.com/x", False, "похожее имя, но другой домен"),
        ("https://evilmedium.com.attacker.net/x", False, "домен только выглядит похожим"),
        ("https://techcrunch.com/x", False, "обычный хост"),
    ],
)
async def test_browser_route_covers_medium_subdomains(url, expected, why) -> None:
    engine = _engine(_FakeRedis())
    assert await engine.should_use_browser(url) is expected, why


async def test_dead_direct_route_switches_host_to_the_browser() -> None:
    """Первый таймаут помечает хост, следующий краул идёт путём с прокси."""
    redis = _FakeRedis()
    engine = _engine(redis)
    url = "https://thegradient.pub/some-post"

    assert await engine.should_use_browser(url) is False
    await engine.mark_dead(url)
    assert await engine.should_use_browser(url) is True
    assert redis.expired, (
        "пометка обязана иметь TTL: маршруты чинятся, и вечная запись "
        "превратила бы разовый сбой в постоянный обход через браузер"
    )


async def test_routing_survives_a_broken_redis() -> None:
    """Наблюдение за маршрутом не имеет права уронить краул.

    Недоступный Redis означает «не знаю» и должен давать прежнее поведение,
    а не исключение посреди выборки.
    """
    engine = _engine(_FakeRedis(broken=True))
    assert await engine.should_use_browser("https://example.com/x") is False
    await engine.mark_dead("https://example.com/x")  # не бросает


async def test_medium_stays_on_browser_even_without_redis() -> None:
    """Medium не зависит от пометок: его маршрут известен статически."""
    engine = _engine(_FakeRedis(broken=True))
    assert await engine.should_use_browser("https://x.medium.com/p") is True


def test_engine_actually_uses_the_shared_routing() -> None:
    """`_Router` выше — копия логики, и копия расходится с оригиналом всегда.

    Единственное, что делает тесты выше осмысленными, — это проверка, что
    движок ходит теми же функциями, а не своей забытой реализацией. Ловит
    возврат локального списка хостов или точного сравнения.
    """
    from pathlib import Path

    source = (
        Path(__file__).resolve().parents[1] / "crawl4ai" / "enrichment_engine.py"
    ).read_text(encoding="utf-8")

    problems = []
    if "from shared.crawl_routing import" not in source:
        problems.append("движок не импортирует shared.crawl_routing")
    for symbol in ("is_browser_host(", "host_of(", "DEAD_ROUTE_SET", "DEAD_ROUTE_TTL"):
        if symbol not in source:
            problems.append(f"движок не использует {symbol}")
    # Прежняя форма: собственный набор хостов и сравнение по вхождению.
    if "MEDIUM_HOSTS" in source:
        problems.append("вернулся локальный MEDIUM_HOSTS с точным сравнением")
    # Пометка мёртвого маршрута обязана стоять на обоих сетевых отказах,
    # иначе хост так и будет отваливаться заново при каждой попытке.
    if source.count("_mark_direct_route_dead(url)") < 2:
        problems.append(
            "пометка мёртвого маршрута стоит не на всех сетевых отказах "
            "(нужны и TimeoutError, и ClientError)"
        )

    assert not problems, "\n  ".join(["маршрутизация краула разъехалась:", *problems])
