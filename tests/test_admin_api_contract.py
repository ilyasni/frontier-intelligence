"""
Контракт админского API в местах, где отказ раньше выглядел как успех.

Заведён 2026-08-07 по аудиту админки. Четыре инварианта, каждый — из измеренного
дефекта, а не из общих соображений.

1. `POST /api/sources` — это UPSERT, а формы редактирования источника в админке НЕТ:
   «+ Добавить» открывается пустой и де-факто служит редактором. Повторная отправка
   существующего id затирала `proxy_config` пустым `{}` и `schedule_cron` значением
   NULL просто потому, что оператор их не заполнил. Для rss/web-источников за
   Cloudflare прокси xray обязателен — без него источник отдаёт ReadTimeout и ноль
   успехов, а выглядит это как «сайт лёг». Ответ при этом был `{"status": "ok"}`,
   и UI рисовал зелёный тост.

2. `PATCH /api/sources/{id}/toggle` отвечал `{"status": "ok"}`, не проверив, что
   строка вообще есть, и не возвращая новое состояние — фронт угадывал его
   переворотом локального значения. Соседние ручки того же файла (vision,
   telegram-handle) 404 отдают: разъехались две реализации одного и того же.

3. SPA-фолбэк `@app.get("/{path:path}")` объявлен ПОСЛЕ роутеров и потому ловил
   несуществующий GET `/api/...`, отдавая 200 с телом index.html. Фронт на неразбором
   JSON молча кладёт в `data` строку с HTML, `resp.ok` истинно — и вызывающий видит
   «пустой ответ» вместо ошибки маршрута. Сценарий штатный: фронт живёт bind-mount'ом
   и обновляется мгновенно, backend запечён в образ и отстаёт до пересборки.

4. Сжатия ответов не было вовсе: 804 193 B статики летели сырыми при 261 295 B под
   gzip. Замерено `curl -H 'Accept-Encoding: gzip'` — заголовка `content-encoding`
   в ответе не было.

Разбор статический: тесты не поднимают приложение и не ходят в БД. Этого достаточно,
потому что все четыре дефекта видны в тексте — и мутация каждого ловится.
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO = Path(__file__).resolve().parents[1]
SOURCES = REPO / "admin" / "backend" / "routers" / "sources.py"
MAIN = REPO / "admin" / "backend" / "main.py"


@lru_cache(maxsize=8)
def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _function_body(source: str, name: str) -> str:
    """Тело функции от её `async def` до следующего декоратора верхнего уровня."""
    start = source.index(f"async def {name}(")
    rest = source[start:]
    end = rest.find("\n@router.")
    return rest if end == -1 else rest[:end]


# ── 1. UPSERT источника не должен стирать то, чего не прислали ────────────────


def test_source_upsert_preserves_proxy_config_and_schedule() -> None:
    body = _function_body(_text(SOURCES), "create_source")

    assert "ON CONFLICT (id) DO UPDATE" in body, "UPSERT исчез — перечитай тест целиком"

    # Прямое присваивание EXCLUDED в этих двух колонках и есть дефект.
    for column in ("proxy_config", "schedule_cron"):
        naive = re.search(rf"{column}\s*=\s*EXCLUDED\.{column}\s*,", body)
        assert naive is None, (
            f"{column} присваивается из EXCLUDED напрямую: повторная отправка формы "
            "«+ Добавить» сотрёт значение, которого оператор просто не заполнил. "
            "Для rss/web за Cloudflare потеря proxy_config тихо убивает сбор."
        )

    assert "COALESCE(EXCLUDED.schedule_cron, sources.schedule_cron)" in body, (
        "расписание обязано сохраняться, когда его не прислали"
    )
    assert "sources.proxy_config" in body, (
        "прокси обязан сохраняться, когда прислан пустой объект"
    )


def test_source_upsert_tells_the_caller_whether_it_created_or_overwrote() -> None:
    """Без этого признака UI рапортует «Источник сохранён» и на перезаписи тоже."""
    body = _function_body(_text(SOURCES), "create_source")
    assert "xmax = 0" in body, (
        "нет признака создания. `RETURNING (xmax = 0) AS created` — штатный приём "
        "Postgres: у вставленной строки xmax нулевой, у обновлённой там транзакция"
    )
    assert '"created"' in body, "признак created не доезжает до тела ответа"


# ── 2. toggle обязан признавать отсутствие строки ─────────────────────────────


def test_toggle_source_404s_on_unknown_id_and_returns_new_state() -> None:
    body = _function_body(_text(SOURCES), "toggle_source")

    assert "RETURNING is_enabled" in body, (
        "UPDATE без RETURNING не отличает «переключил» от «строки нет»"
    )
    assert "status_code=404" in body, (
        "несуществующий id обязан давать 404, а не 200 «ok»: соседние ручки этого "
        "файла (vision, telegram-handle) так и делают"
    )
    assert '"is_enabled"' in body, (
        "новое состояние обязано возвращаться, иначе фронт угадывает его переворотом "
        "локального значения и расходится с базой при правке из другой вкладки"
    )


# ── 3. SPA-фолбэк не должен отвечать за /api/* ────────────────────────────────


def test_spa_fallback_does_not_swallow_unknown_api_routes() -> None:
    body = _function_body(_text(MAIN), "frontend") if "async def frontend(" in _text(MAIN) else ""
    if not body:
        # frontend объявлен не через @router, а через @app — берём хвост файла.
        source = _text(MAIN)
        body = source[source.index("async def frontend("):]

    assert 'path.startswith("api/")' in body, (
        "catch-all снова отдаёт index.html на неизвестный GET /api/... — фронт получит "
        "200 с HTML и покажет «пусто» вместо ошибки маршрута"
    )
    assert "status_code=404" in body, "неизвестная ручка обязана давать 404"


# ── 4. Сжатие ─────────────────────────────────────────────────────────────────


def test_responses_are_compressed() -> None:
    source = _text(MAIN)
    assert "GZipMiddleware" in source, (
        "сжатие снято: 804 КБ статики полетят сырыми вместо 261 КБ"
    )
    assert "from fastapi.middleware.gzip import GZipMiddleware" in source

    # Порядок важен: GZip обязан быть ВНЕШНИМ, то есть добавленным раньше остальных.
    order = [
        source.index("add_middleware(GZipMiddleware"),
        source.index("add_middleware(\n    CORSMiddleware"),
    ]
    assert order == sorted(order), (
        "GZipMiddleware добавлен после CORS — в стеке Starlette он окажется внутри "
        "и не сожмёт часть ответов"
    )
