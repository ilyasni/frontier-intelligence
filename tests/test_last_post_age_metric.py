"""frontier_last_post_age_seconds не может уйти в минус из-за даты из фида.

Ленты датируют записи вперёд: Mistral AI News на 6.5 ч (08.09.2026), OpenAI на 47 ч
(12.09.2026). При `NOW() - MAX(COALESCE(published_at, created_at))` один такой пост
делал возраст отрицательным, и ни один порог FrontierNoNewPosts для воркспейса не был
достижим, пока часы не догонят дату. Сверка на живой базе 14.09.2026 на момент
12.09 12:00 UTC: ai_products_media старый запрос −129 600 с, новый 3 774 с; у
остальных воркспейсов значения совпали.

Разбор статический: запрос живёт строкой внутри admin/backend/main.py, импорт
которого тянет весь FastAPI-стек.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

MAIN = Path(__file__).resolve().parents[1] / "admin" / "backend" / "main.py"


def _age_query() -> str:
    tree = ast.parse(MAIN.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_refresh_last_post_age_metric":
            strings = [
                n.value
                for n in ast.walk(node)
                if isinstance(n, ast.Constant) and isinstance(n.value, str) and "SELECT" in n.value
            ]
            assert strings, "в _refresh_last_post_age_metric не найден SQL"
            return re.sub(r"\s+", " ", strings[0])
    raise AssertionError("_refresh_last_post_age_metric пропал из admin/backend/main.py")


def test_feed_date_is_capped_at_the_moment_of_ingest() -> None:
    query = _age_query()
    assert "MAX(LEAST(published_at, created_at))" in query, (
        f"{query!r}: дата из фида обязана ограничиваться created_at, иначе пост с "
        "датой вперёд делает возраст отрицательным и глушит FrontierNoNewPosts"
    )


def test_uncapped_coalesce_does_not_come_back() -> None:
    # Кламп результата нулём (GREATEST(0, ...)) не лечит: возраст 0 держится до тех
    # пор, пока часы не догонят дату, и порог так же недостижим.
    query = _age_query()
    assert "COALESCE(published_at" not in query, query
    assert "GREATEST(0" not in query.replace(" ", ""), query
