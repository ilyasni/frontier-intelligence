"""
Контракт schedule_cron: строка обязана означать то, что в ней написано.

Долг закрыт 2026-08-03: врущих строк в config/sources.yml НЕТ, контракт жёсткий,
списка исключений больше не существует (см. KNOWN_LYING_CRON_BY_SOURCE_ID ниже —
он оставлен пустым как ловушка на попытку завести waiver заново).

Что было измерено 2026-08-03.

`ingest/scheduler.py:cron_to_minutes` не разбирает cron сам: он скармливает строку
croniter, берёт ДВА первых срабатывания и возвращает зазор между ними в минутах.
APScheduler потом вешает IntervalTrigger на это число (ingest/main.py). Следствия:

1. Шаг в поле минут раскрывается по 0-59. `*/75`, `*/90`, `*/120`, `*/180`,
   `*/240`, `*/360` дают ровно одно срабатывание в часе — минуту 0 — то есть
   ИНТЕРВАЛ 60 для всех шести. Источник, заявивший «раз в 6 часов», опрашивается
   раз в час: в шесть раз чаще, чем написано, и никто об этом не узнаёт — ни лога,
   ни метрики, ни ошибки. Ровно тот класс отказа, ради которого написан этот файл.
2. `*/45` раскрывается в минуты [0, 45]. Зазоры в сутках чередуются: 45 и 15.
   Какой из них замерит cron_to_minutes — зависит от минуты настенных часов
   в момент старта ingest. Частота опроса меняется от рестарта к рестарту.
3. Интервал больше 60 выражается ТОЛЬКО через поле часов: `0 */2 * * *` = 120,
   `0 */3 * * *` = 180, `0 */6 * * *` = 360.

Почему модель cron живёт здесь, а не берётся из croniter.

croniter НЕТ в admin-образе (см. admin/Dockerfile — он ставится только в ingest),
а `tests/conftest.py` вдобавок регистрирует `croniter` как MagicMock. Под pytest
настоящий `cron_to_minutes` поэтому падает в свой `except` и возвращает дефолт 60
для ЛЮБОЙ строки — тест поверх него был бы тавтологией «60 == 60».
Поэтому ниже воспроизведены семантика croniter (раскрытие полей) и сам замер
(зазор между первыми двумя срабатываниями). Воспроизведение прикрыто с двух сторон:
  * `test_scheduler_still_measures_the_gap_between_two_fires` — ast-слепок реализации:
    перепишут cron_to_minutes — тест покраснеет и заставит пересобрать модель;
  * `test_model_agrees_with_real_croniter` — сверка с настоящим croniter там, где он
    есть (ingest-образ, dev-машина); в admin-образе честно скипается.
"""

from __future__ import annotations

import ast
import sys
from bisect import bisect_right
from datetime import datetime
from functools import lru_cache
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.unit

# Корень репозитория — относительно файла теста: прогон идёт и на хосте,
# и внутри контейнера с маунтом в произвольную точку (/src).
REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCES_YML = REPO_ROOT / "config" / "sources.yml"
SCHEDULER_PY = REPO_ROOT / "ingest" / "scheduler.py"

MINUTES_PER_DAY = 1440

# ─────────────────────────────────────────────────────────────────────────────
# Долг закрыт 2026-08-03. Таблица ниже пуста и обязана оставаться пустой.
#
# Было: 123 записи config/sources.yml из 168, чей schedule_cron означал не то, что
# в нём написано. Разбивка на момент фиксации:
#   '*/120 * * * *' — 38 шт., реально 60 (в 2 раза чаще заявленного)
#   '*/180 * * * *' — 38 шт., реально 60 (в 3 раза)
#   '*/360 * * * *' — 28 шт., реально 60 (в 6 раз)
#   '*/90  * * * *' —  8 шт., реально 60
#   '*/240 * * * *' —  7 шт., реально 60 (в 4 раза)
#   '*/45  * * * *' —  4 шт., реально 45 ИЛИ 15 — как повезёт при старте ingest
#
# Стало: все 123 переписаны на поле часов ('<m> */2|*/3|*/4|*/6 * * *'). Правило
# пересчёта — ближайший выразимый интервал, при ничьей БОЛЬШИЙ (реже опрашиваем):
# 120/180/240/360 выразимы точно, 90 → 120, 45 → 60. Тай-брейк выбран так, чтобы
# ни один источник не стал опрашиваться ЧАЩЕ, чем до правки. Полный текст правила —
# в шапке config/sources.yml.
#
# Почему словарь оставлен, а не удалён. Пустая таблица + жёсткий
# `test_the_debt_list_stays_empty` — это ловушка на рецидив: вернуть механику waiver'ов
# нельзя, не удалив явный тест. Просто выкинуть константу было бы слабее: следующий
# автор завёл бы свой allowlist «на пару записей», и контракт снова стал бы мягким.
#
# `*/60 * * * *` в долге не было намеренно: шаг тоже схлопывается в минуту 0, но
# результат — 60 — совпадает с заявленным, строка не врала. Такие записи получили
# только фазовый офсет ('<m> * * * *'), частота не изменилась.
# Проверка этого совпадения — test_step_of_exactly_60_collapses_but_does_not_lie.
# ─────────────────────────────────────────────────────────────────────────────
KNOWN_LYING_CRON_BY_SOURCE_ID: dict[str, str] = {}


class CronNotExpressible(ValueError):
    """Строку не берёт ни модель ниже, ни (скорее всего) намерение автора."""


# ── модель croniter ──────────────────────────────────────────────────────────


def _expand_field(field: str, low: int, high: int) -> list[int]:
    """Раскрыть одно поле cron в список значений — как это делает croniter.

    Ключевой момент: шаг применяется к диапазону поля, а не к «времени».
    range(0, 60, 90) — это [0], поэтому `*/90` в минутах и есть «в начале часа».
    """
    values: set[int] = set()
    for part in field.split(","):
        step = 1
        body = part
        if "/" in body:
            body, raw_step = body.split("/", 1)
            if not raw_step.isdigit() or int(raw_step) < 1:
                raise CronNotExpressible(f"bad step in {field!r}")
            step = int(raw_step)
        if body in ("*", "?"):
            start, end = low, high
        elif "-" in body:
            raw_start, raw_end = body.split("-", 1)
            if not (raw_start.isdigit() and raw_end.isdigit()):
                raise CronNotExpressible(f"non-numeric range in {field!r}")
            start, end = int(raw_start), int(raw_end)
        elif body.isdigit():
            start = end = int(body)
        else:
            # L / W / # / MON / @hourly — croniter это умеет, модель ниже нет.
            raise CronNotExpressible(f"unsupported cron syntax: {field!r}")
        if not (low <= start <= high and low <= end <= high and start <= end):
            raise CronNotExpressible(f"value out of range in {field!r}")
        values.update(range(start, end + 1, step))
    if not values:
        raise CronNotExpressible(f"field expands to nothing: {field!r}")
    return sorted(values)


@lru_cache(maxsize=512)
def fire_minutes_of_day(cron: str) -> tuple[int, ...]:
    """Все моменты срабатывания в сутках, в минутах от полуночи."""
    parts = cron.split()
    if len(parts) != 5:
        raise CronNotExpressible(f"expected 5 cron fields, got {len(parts)}: {cron!r}")
    minute, hour, day_of_month, month, day_of_week = parts
    if (day_of_month, month, day_of_week) != ("*", "*", "*"):
        # Все 168 записей суточные. Появится недельная/месячная — модель суток
        # для неё неверна, и это должно быть видно, а не «примерно посчитано».
        raise CronNotExpressible(f"model covers daily crons only: {cron!r}")
    return tuple(
        h * 60 + m
        for h in _expand_field(hour, 0, 23)
        for m in _expand_field(minute, 0, 59)
    )


@lru_cache(maxsize=512)
def observable_intervals(cron: str) -> frozenset[int]:
    """Все зазоры между соседними срабатываниями (по кругу суток), в минутах.

    Ровно множество значений, которые может вернуть cron_to_minutes: какое именно —
    решает datetime.now() в момент старта ingest. Один элемент — детерминированно,
    больше одного — частота опроса плавает от рестарта к рестарту.
    """
    fires = sorted(fire_minutes_of_day(cron))
    count = len(fires)
    return frozenset(
        ((fires[(i + 1) % count] - fires[i]) % MINUTES_PER_DAY) or MINUTES_PER_DAY
        for i in range(count)
    )


def interval_from_base(cron: str, base_hour: int, base_minute: int) -> int:
    """Что вернёт cron_to_minutes, если ingest стартовал в base_hour:base_minute.

    Воспроизводит `it.get_next(); it.get_next(); зазор`. Срабатывание ищется строго
    позже базы — как croniter от datetime.now() с ненулевыми секундами.
    """
    fires = sorted(fire_minutes_of_day(cron))
    base = base_hour * 60 + base_minute
    first = bisect_right(fires, base) % len(fires)
    second = (first + 1) % len(fires)
    return ((fires[second] - fires[first]) % MINUTES_PER_DAY) or MINUTES_PER_DAY


def declared_minute_step(cron: str) -> int | None:
    """N из `*/N` в поле минут — то, что автор строки считает интервалом."""
    minute_field = cron.split()[0]
    if minute_field.startswith("*/") and minute_field[2:].isdigit():
        return int(minute_field[2:])
    return None


def cron_lie(cron: str) -> str | None:
    """Чем строка отличается от собственного обещания. None — не отличается."""
    intervals = observable_intervals(cron)
    if len(intervals) > 1:
        return (
            f"{cron!r}: interval depends on the wall-clock minute ingest starts at, "
            f"one of {sorted(intervals)} min"
        )
    actual = next(iter(intervals))
    step = declared_minute_step(cron)
    if step is not None and step != actual:
        return f"{cron!r}: declares every {step} min, croniter delivers every {actual} min"
    return None


# ── источники данных ─────────────────────────────────────────────────────────


@lru_cache(maxsize=1)
def _yaml_sources() -> tuple[tuple[str, str], ...]:
    """(id, schedule_cron) для каждой записи config/sources.yml."""
    data = yaml.safe_load(SOURCES_YML.read_text(encoding="utf-8")) or {}
    return tuple(
        (str(item["id"]), str(item["schedule_cron"]))
        for item in (data.get("sources") or [])
        if item.get("schedule_cron")
    )


# ── самопроверка: без неё контракт ниже может оказаться пустым множеством ────


def test_inputs_exist() -> None:
    for path in (SOURCES_YML, SCHEDULER_PY):
        assert path.is_file(), f"missing required file: {path}"


def test_source_extraction_is_not_vacuous() -> None:
    sources = _yaml_sources()
    assert len(sources) > 100, f"expected 100+ sources with schedule_cron, got {len(sources)}"
    ids = [source_id for source_id, _ in sources]
    assert len(ids) == len(set(ids)), "duplicate source ids in config/sources.yml"


def test_every_cron_in_sources_yml_is_expressible() -> None:
    """Строка, которую модель не берёт, не проверена ничем — это тоже отказ."""
    broken: dict[str, str] = {}
    for source_id, cron in _yaml_sources():
        try:
            observable_intervals(cron)
        except CronNotExpressible as exc:
            broken[source_id] = str(exc)
    assert not broken, f"schedule_cron values this contract cannot evaluate: {broken}"


# ── семантика croniter ───────────────────────────────────────────────────────


@pytest.mark.parametrize("step", [1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30])
def test_minute_step_that_divides_60_is_honoured(step: int) -> None:
    cron = f"*/{step} * * * *"
    assert observable_intervals(cron) == frozenset({step})
    assert cron_lie(cron) is None


@pytest.mark.parametrize("step", [75, 90, 120, 180, 240, 360])
def test_minute_step_above_59_collapses_to_60(step: int) -> None:
    """Шаг больше диапазона минут даёт ОДНО срабатывание в часе — минуту 0."""
    cron = f"*/{step} * * * *"
    assert fire_minutes_of_day(cron)[:2] == (0, 60)
    assert observable_intervals(cron) == frozenset({60}), (
        f"{cron} must collapse to an hourly poll"
    )
    lie = cron_lie(cron)
    assert lie is not None and f"declares every {step} min" in lie


def test_step_of_exactly_60_collapses_but_does_not_lie() -> None:
    """Почему 25 записей с `*/60` не в списке долга: схлопывание совпало с обещанием."""
    assert _expand_field("*/60", 0, 59) == [0]
    assert observable_intervals("*/60 * * * *") == frozenset({60})
    assert cron_lie("*/60 * * * *") is None


def test_45_minute_step_is_nondeterministic_across_restarts() -> None:
    """`*/45` → минуты [0, 45]; замеренный интервал зависит от минуты старта."""
    cron = "*/45 * * * *"
    assert _expand_field("*/45", 0, 59) == [0, 45]

    started_at_10_50 = interval_from_base(cron, 10, 50)  # ближайшие 11:00 и 11:45
    started_at_10_00 = interval_from_base(cron, 10, 0)  # ближайшие 10:45 и 11:00

    assert started_at_10_50 == 45
    assert started_at_10_00 == 15
    assert started_at_10_50 != started_at_10_00, (
        "the whole point of this test: the same cron string yields two different "
        "polling rates depending on when ingest happened to start"
    )
    assert observable_intervals(cron) == frozenset({15, 45})
    assert cron_lie(cron) is not None


@pytest.mark.parametrize(
    ("cron", "minutes"),
    [
        ("0 * * * *", 60),
        ("37 * * * *", 60),
        ("0 */2 * * *", 120),
        ("17 */2 * * *", 120),
        ("0 */3 * * *", 180),
        ("47 */3 * * *", 180),
        ("0 */6 * * *", 360),
    ],
)
def test_interval_above_60_is_expressible_only_through_the_hour_field(
    cron: str, minutes: int
) -> None:
    assert observable_intervals(cron) == frozenset({minutes})
    assert cron_lie(cron) is None


@pytest.mark.parametrize("cron", ["*/5", "*/5 * * * * *", "@hourly", "0 0 * * MON", "0 0 1 * *"])
def test_unexpressible_crons_raise_instead_of_guessing(cron: str) -> None:
    with pytest.raises(CronNotExpressible):
        observable_intervals(cron)


# ── сцепка модели с реальной реализацией ─────────────────────────────────────


def _real_croniter_works() -> bool:
    """Настоящий croniter импортируется и считает `*/30` как 30 минут."""
    try:
        from croniter import croniter
    except ImportError:
        return False
    try:
        iterator = croniter("*/30 * * * *", datetime(2026, 1, 1, 0, 5, 30))
        first = iterator.get_next(datetime)
        second = iterator.get_next(datetime)
    except Exception:
        # MagicMock ломается ровно здесь (round() от мока → TypeError).
        # Для этого теста «сломался на пробе» == «настоящего croniter нет».
        return False
    if not isinstance(first, datetime) or not isinstance(second, datetime):
        return False
    return (second - first).total_seconds() == 1800


def test_model_agrees_with_real_croniter(monkeypatch: pytest.MonkeyPatch) -> None:
    """Сверка модели с настоящим croniter — там, где он есть.

    conftest регистрирует `croniter` как MagicMock для всего прогона, поэтому
    заглушку здесь снимаем на время теста (monkeypatch вернёт её обратно).
    В admin-образе croniter не установлен вовсе — тест честно скипается,
    а не проходит на моке. В ingest-образе croniter настоящий и сверка идёт.
    """
    monkeypatch.delitem(sys.modules, "croniter", raising=False)
    if not _real_croniter_works():
        pytest.skip("croniter is not installed in this image (admin has none, ingest does)")

    from ingest.scheduler import cron_to_minutes

    checked = {cron for _, cron in _yaml_sources()} | {
        "*/30 * * * *",
        "*/90 * * * *",
        "*/120 * * * *",
        "0 */2 * * *",
    }
    mismatch: dict[str, tuple[int, list[int]]] = {}
    for cron in sorted(checked):
        measured = cron_to_minutes(cron)
        expected = observable_intervals(cron)
        if measured not in expected:
            mismatch[cron] = (measured, sorted(expected))
    assert not mismatch, f"model disagrees with real croniter+cron_to_minutes: {mismatch}"
    assert cron_to_minutes("*/90 * * * *") == 60, "the documented defect disappeared"


def test_scheduler_still_measures_the_gap_between_two_fires() -> None:
    """Слепок реализации cron_to_minutes: модель выше верна, только пока он такой.

    Перепишут функцию (например, начнут считать шаг честно) — тест покраснеет,
    и список долга придётся пересобрать, а не унаследовать молча.
    """
    tree = ast.parse(SCHEDULER_PY.read_text(encoding="utf-8"))
    functions = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "cron_to_minutes"
    ]
    assert len(functions) == 1, "ingest/scheduler.py must define exactly one cron_to_minutes"
    body = functions[0]

    names = {node.id for node in ast.walk(body) if isinstance(node, ast.Name)}
    attrs = [node.attr for node in ast.walk(body) if isinstance(node, ast.Attribute)]
    returns = [
        node.value.value
        for node in ast.walk(body)
        if isinstance(node, ast.Return)
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, int)
    ]

    assert "croniter" in names, "cron_to_minutes no longer delegates to croniter"
    assert attrs.count("get_next") == 2, (
        "cron_to_minutes no longer measures the gap between exactly two fires; "
        "the cron model in tests/test_cron_contract.py assumes it does"
    )
    assert "total_seconds" in attrs, "cron_to_minutes no longer converts a timedelta"
    assert 60 in returns, "the silent 60-minute fallback is gone — re-derive this contract"


# ── контракт config/sources.yml ──────────────────────────────────────────────


def test_no_source_declares_a_cron_that_means_something_else() -> None:
    """Ни одна запись не имеет права врать о частоте опроса. Без исключений.

    Жёстко с 2026-08-03: allowlist'а нет, `cron_lie` не должен срабатывать ни на одной
    из 168 записей. Красное здесь — это либо шаг в поле минут, который не делит 60
    (`*/90`, `*/120`, `*/180`, `*/240`, `*/360` — все они молча означают 60 минут),
    либо плавающий интервал (`*/45` даёт 45 ИЛИ 15 в зависимости от минуты старта
    ingest). Интервал больше часа выражается ТОЛЬКО через поле часов.
    """
    lying: dict[str, str] = {}
    for source_id, cron in _yaml_sources():
        lie = cron_lie(cron)
        if lie is not None:
            lying[source_id] = lie

    assert not lying, (
        "schedule_cron, который означает не то, что в нём написано. "
        "Интервал больше 60 мин выразим только через поле часов "
        "('<m> */2 * * *' = 120, '<m> */3 * * *' = 180, '<m> */4 * * *' = 240, "
        "'<m> */6 * * *' = 360); шаг в поле минут обязан делить 60. "
        f"Нарушители: {lying}"
    )


def test_the_debt_list_stays_empty() -> None:
    """Ловушка на рецидив: waiver-механику нельзя вернуть, не удалив этот тест.

    Долг выплачен целиком 2026-08-03. Любая запись здесь означает, что кто-то снова
    выдал источнику индульгенцию на врущее расписание — и сделал это молча, оставив
    test_no_source_declares_a_cron_that_means_something_else зелёным.
    """
    assert KNOWN_LYING_CRON_BY_SOURCE_ID == {}, (
        "KNOWN_LYING_CRON_BY_SOURCE_ID снова не пуст. Врущий schedule_cron чинится "
        "правкой config/sources.yml, а не строкой в allowlist: "
        f"{sorted(KNOWN_LYING_CRON_BY_SOURCE_ID)}"
    )


def test_every_source_polls_at_a_deterministic_interval() -> None:
    """Позитивная сторона того же контракта: интервал не плавает от рестарта к рестарту.

    `cron_lie` ловит это же через `len(intervals) > 1`, но там оно тонет в общем
    сообщении. Здесь отдельно и явно: 168 записей обязаны давать ровно одно значение
    зазора, иначе частота опроса меняется каждый раз, когда перезапускают ingest.
    """
    floating = {
        source_id: sorted(observable_intervals(cron))
        for source_id, cron in _yaml_sources()
        if len(observable_intervals(cron)) > 1
    }
    assert not floating, (
        "источники, чей интервал зависит от минуты старта ingest (замер идёт по зазору "
        f"между первыми двумя срабатываниями): {floating}"
    )


def test_minute_offsets_are_spread_inside_each_interval_bucket() -> None:
    """Фазовый разнос: записи с одинаковым интервалом не должны падать на одну минуту.

    До 2026-08-03 на минуту 0 приходились ВСЕ 168 источников — шаг в поле минут
    схлопывался в [0], а честные `*/60` и `*/30` и так стартовали с нуля. Теперь у
    каждой записи внутри своего интервального бакета собственная минута.

    Оговорка, важная для чтения этого теста: сегодня минута ДЕКЛАРАТИВНА.
    `cron_to_minutes` возвращает планировщику только интервал, а APScheduler вешает
    IntervalTrigger со случайным стартовым разбросом и jitter (ingest/main.py) — фазу
    задаёт он, а не строка. Тест сторожит намерение, записанное в конфиге, и станет
    нагрузочным в тот день, когда планировщик переведут на CronTrigger.

    Порог само-масштабируется: минут всего 60, поэтому при бакете больше 60 записей
    допускается ceil(N/60) источников на минуту — но не «все на нулевую».
    """
    buckets: dict[tuple[int, str], list[tuple[str, int]]] = {}
    for source_id, cron in _yaml_sources():
        hour_field = cron.split()[1]
        interval = min(observable_intervals(cron))
        # Минута берётся из раскрытия, а не из текста поля: `*/30 * * * *` и
        # `5,35 * * * *` дают один интервал, но разную фазу — 0 и 5.
        phase = min(fire % 60 for fire in fire_minutes_of_day(cron))
        buckets.setdefault((interval, hour_field), []).append((source_id, phase))

    assert len(buckets) >= 4, f"бакетов подозрительно мало, извлекатель сломан: {sorted(buckets)}"

    crowded: dict[str, dict[int, list[str]]] = {}
    for key, members in buckets.items():
        allowed = -(-len(members) // 60)  # ceil
        by_minute: dict[int, list[str]] = {}
        for source_id, minute in members:
            by_minute.setdefault(minute, []).append(source_id)
        over = {minute: ids for minute, ids in by_minute.items() if len(ids) > allowed}
        if over:
            crowded[f"{key[0]}min/{key[1]}"] = over

    assert not crowded, (
        "источники с одинаковым интервалом сидят на одной минуте — при переходе "
        "планировщика на CronTrigger они выстрелят одновременно (все fetch телеграма "
        f"сериализуются одним _telegram_run_lock): {crowded}"
    )


def test_auto_sources_declare_the_interval_they_get() -> None:
    """Батч auto_* починен 2026-08-03 — здесь никаких послаблений.

    Ни одной строки в списке долга: ни один auto_-источник не имеет права
    ни на шаг минут больше 59, ни на плавающий интервал.
    """
    auto = [(sid, cron) for sid, cron in _yaml_sources() if sid.startswith("auto_")]
    assert len(auto) >= 10, f"expected at least 10 auto_* sources, found {len(auto)}"

    lying = {sid: cron_lie(cron) for sid, cron in auto if cron_lie(cron) is not None}
    assert not lying, f"auto_* sources with a lying schedule_cron: {lying}"

    with_minute_step = {sid: cron for sid, cron in auto if declared_minute_step(cron) is not None}
    assert not with_minute_step, (
        "auto_* sources must express their interval through the hour field, never a "
        f"minute step: {with_minute_step}"
    )

    overlap = sorted(sid for sid, _ in auto if sid in KNOWN_LYING_CRON_BY_SOURCE_ID)
    assert not overlap, f"auto_* ids must never appear in the debt allowlist: {overlap}"
