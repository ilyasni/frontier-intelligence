"""
Инвариант на идентификатор предложения по порогу: он обязан быть уникален
на ПРЕДЛОЖЕНИЕ, а не на пару (воркспейс, ключ порога).

Заведён 2026-08-06 по живому дефекту, который реестр не видел, потому что
смотрел на поверхность одобрения (пункт 2), а сломан был производитель.

Что происходило. `id` собирался как `_digest(f"{ws}|{key}|pending")` — константа
на всё время жизни пары. `ON CONFLICT` в том же INSERT покрывает только частичный
уникальный индекс `uq_threshold_proposals_pending (workspace_id, threshold_key)
WHERE status = 'pending'` (storage/postgres/migrations/20260625_threshold_proposals.sql:29).
Пока предложение висит в `pending`, всё сходится. Но как только оно уходит из
`pending` — а это и есть цель петли, человек его одобряет или отклоняет — строка
остаётся с тем же `id`, и следующий прогон падает на PRIMARY KEY: конфликт по `id`
веткой `ON CONFLICT (workspace_id, threshold_key)` не обрабатывается.

Замер 06.08.2026 на живом сервере: в `threshold_proposals` семь строк, все
`superseded`, самая свежая обновлена 29.06; `run_retrospective_review` падает
каждую ночь на workspace=disruption с `duplicate key value violates unique
constraint "threshold_proposals_pkey" ... thrprop:0bf63509c5b450e6` и роняет
весь прогон по воркспейсу. То есть ретро-петля порогов молчала 41 сутки, а
`FrontierAdminJobFailing` не мог этого показать: он требует ≥3 отказов за 6 часов,
а суточный джоб даёт максимум один.

Проверка тут — не на текст формулы, а на наблюдаемое поведение: два прогона
подряд по одной и той же паре обязаны дать РАЗНЫЕ id. Мутация «вернуть
константный суффикс» ловится.
"""

from __future__ import annotations

from typing import Any

import pytest

from worker.services.relevance_audit import _upsert_relevance_proposal
from worker.services.retrospective import _upsert_proposal

pytestmark = pytest.mark.unit


class _CapturingSession:
    """Минимальная замена AsyncSession: запоминает переданные параметры."""

    def __init__(self) -> None:
        self.params: list[dict[str, Any]] = []

    async def execute(self, _statement: Any, params: dict[str, Any] | None = None) -> None:
        self.params.append(dict(params or {}))


def _prop() -> dict[str, Any]:
    return {
        "threshold_key": "weak_signal_min_source_diversity",
        "current_value": 0.2,
        "proposed_value": 0.1,
        "direction": "lower",
        "evidence": {"n_samples": 4236, "n_vindicated": 183},
    }


@pytest.mark.asyncio
async def test_retrospective_proposal_id_changes_between_runs() -> None:
    session = _CapturingSession()

    await _upsert_proposal(
        session,
        workspace_id="disruption",
        run_id="retro:aaaaaaaaaaaaaaaa",
        threshold_version="thr:v1",
        prop=_prop(),
        rationale="первый прогон",
    )
    await _upsert_proposal(
        session,
        workspace_id="disruption",
        run_id="retro:bbbbbbbbbbbbbbbb",
        threshold_version="thr:v1",
        prop=_prop(),
        rationale="второй прогон",
    )

    first, second = session.params[0]["id"], session.params[1]["id"]
    assert first != second, (
        "id предложения не меняется между прогонами — значит после первого же "
        "перехода строки из pending ночной прогон будет падать на PRIMARY KEY "
        "(так петля порогов и стояла с 26.06.2026)"
    )
    assert first.startswith("thrprop:") and second.startswith("thrprop:")


@pytest.mark.asyncio
async def test_retrospective_proposal_id_is_stable_within_one_run() -> None:
    """Внутри одного прогона id обязан быть воспроизводимым.

    Иначе повторная попытка того же прогона (ретрай, откат транзакции) создала бы
    вторую строку по той же паре и упёрлась бы уже в частичный уникальный индекс.
    """
    session = _CapturingSession()
    for _ in range(2):
        await _upsert_proposal(
            session,
            workspace_id="disruption",
            run_id="retro:cccccccccccccccc",
            threshold_version="thr:v1",
            prop=_prop(),
            rationale="один и тот же прогон",
        )

    assert session.params[0]["id"] == session.params[1]["id"]


@pytest.mark.asyncio
async def test_relevance_proposal_id_changes_between_runs() -> None:
    """Тот же инвариант во второй точке записи — она болела тем же дефектом.

    Здесь он не выстрелил только потому, что relevance-предложений пока не
    создавалось ни одного: `frontier_relevance_audit{metric="audited_30d"}` = 0
    у всех шести воркспейсов, ручных вердиктов в relevance_decisions нет.
    """
    session = _CapturingSession()
    prop = {
        "current_value": 0.6,
        "proposed_value": 0.55,
        "direction": "lower",
        "evidence": {
            "n_audited": 40,
            "n_false_negative": 9,
            "fn_rate": 0.225,
            "recovered_at_proposed": 6,
        },
    }

    await _upsert_relevance_proposal(
        session, workspace_id="design", run_id="relaudit:1111111111111111", prop=prop
    )
    await _upsert_relevance_proposal(
        session, workspace_id="design", run_id="relaudit:2222222222222222", prop=prop
    )

    assert session.params[0]["id"] != session.params[1]["id"]
