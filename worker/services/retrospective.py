"""Контур A (RSI) — ретроспективная петля weak-signal детектора.

Смотрит на старые снимки weak-кандидатов (см. weak_signal_snapshots), проверяет,
оправдались ли они потом (выросли в emerging/stable/trend или подняли burst/score),
и предлагает сдвиг порогов, который лучше отделил бы оправдавшихся от угасших.
Предложение пишется в threshold_proposals со статусом pending — применяет его человек
через MCP-гейт. Сама петля НИЧЕГО не меняет автоматически: рекурсия разорвана гейтом.

Чистые функции (label_outcome, propose_threshold, percentile) тестируются на синтетике;
run_retrospective_review — оркестрация ввода-вывода поверх них.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from worker.services.clustering_math import digest as _digest
from worker.services.semantic_clustering import _cluster_settings

logger = logging.getLogger(__name__)

# Снимок считается «зрелым» (можно судить об исходе) в этом окне возраста.
MATURE_AGE_DAYS = 30
MAX_AGE_DAYS = 120

# Признаки вердикта «оправдался».
VINDICATION_BURST_DELTA = 0.15
VINDICATION_SCORE_DELTA = 0.10
# Контур B→A: насколько уверенным должен быть судья, чтобы его underrated засчитать оправданием.
JUDGE_VINDICATION_NOVELTY = 0.7

# Оптимизатор порога.
MIN_SAMPLES = 20          # меньше — данных недостаточно, предложение не формируем
RETAIN_PERCENTILE = 10    # порог-кандидат удерживает ~90% оправдавшихся
MAX_STEP_RATIO = 0.5      # не двигаем порог более чем на 50% за один шаг
MIN_REL_CHANGE = 0.05     # игнорируем косметические сдвиги

# Какие weak-гейты тюним: ключ порога → колонка снимка + границы.
GATE_FEATURES: list[dict[str, Any]] = [
    {"threshold_key": "weak_signal_min_score", "snap_col": "signal_score", "floor": 0.0, "ceil": 1.0, "is_int": False},
    {"threshold_key": "weak_signal_min_confidence", "snap_col": "confidence", "floor": 0.0, "ceil": 1.0, "is_int": False},
    {"threshold_key": "weak_signal_min_source_diversity", "snap_col": "source_diversity", "floor": 0.0, "ceil": 1.0, "is_int": False},
    {"threshold_key": "weak_signal_min_source_count", "snap_col": "source_count", "floor": 1, "ceil": None, "is_int": True},
]


def percentile(values: list[float], pct: float) -> float:
    """Линейно-интерполированный перцентиль (pct в 0..100). Пустой список → 0.0."""
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    rank = (pct / 100.0) * (len(ordered) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    frac = rank - lo
    return float(ordered[lo] + (ordered[hi] - ordered[lo]) * frac)


def _judge_says_vindicated(judge_verdict: dict[str, Any] | None) -> bool:
    """Контур B→A: судья другой семьи (DeepSeek) даёт РАННИЙ сигнал «это настоящее».

    Высоко-уверенный underrated-вердикт (вне распределения primary) считаем оправданием,
    даже если burst ещё не вырос — это и есть пойманная слепота, ради которой судья и нужен.
    Порог строгий (0.7), чтобы либеральный судья не раздувал vindicated.
    """
    if not judge_verdict:
        return False
    try:
        novelty = float(judge_verdict.get("novelty_score") or 0.0)
    except (TypeError, ValueError):
        novelty = 0.0
    return (
        bool(judge_verdict.get("underrated"))
        and bool(judge_verdict.get("out_of_distribution"))
        and novelty >= JUDGE_VINDICATION_NOVELTY
    )


def label_outcome(
    snapshot: dict[str, Any],
    current: dict[str, Any] | None,
    judge_verdict: dict[str, Any] | None = None,
    *,
    burst_delta: float = VINDICATION_BURST_DELTA,
    score_delta: float = VINDICATION_SCORE_DELTA,
) -> str:
    """Вердикт по одному снимку: 'vindicated' (оправдался) или 'faded' (угас).

    Оправдался, если сигнал к моменту обзора стал trend, поднялся до emerging/stable,
    заметно вырос burst_score / signal_score, ЛИБО судья другой семьи уверенно счёл его
    новым (ранний сигнал, см. _judge_says_vindicated). Иначе — угас.
    """
    if current:
        stage = current.get("signal_stage")
        if current.get("is_trend") or stage in ("emerging", "stable"):
            return "vindicated"
        burst_now, burst_then = current.get("burst_score"), snapshot.get("burst_score")
        if burst_now is not None and burst_then is not None and (burst_now - burst_then) >= burst_delta:
            return "vindicated"
        score_now, score_then = current.get("signal_score"), snapshot.get("signal_score")
        if score_now is not None and score_then is not None and (score_now - score_then) >= score_delta:
            return "vindicated"
    if _judge_says_vindicated(judge_verdict):
        return "vindicated"
    return "faded"


def _clamp_step(
    current_value: float,
    candidate: float,
    *,
    max_step_ratio: float,
    floor: float | None,
    ceil: float | None,
    is_int: bool,
) -> float:
    step_cap = abs(current_value) * max_step_ratio
    if step_cap <= 0:
        step_cap = max(0.05, abs(candidate - current_value))
    lo, hi = current_value - step_cap, current_value + step_cap
    candidate = max(lo, min(hi, candidate))
    if floor is not None:
        candidate = max(floor, candidate)
    if ceil is not None:
        candidate = min(ceil, candidate)
    if is_int:
        candidate = float(max(int(round(candidate)), int(floor or 0)))
    else:
        candidate = round(candidate, 4)
    return candidate


def propose_threshold(
    threshold_key: str,
    current_value: float,
    labeled: list[dict[str, Any]],
    *,
    floor: float | None = 0.0,
    ceil: float | None = 1.0,
    is_int: bool = False,
    min_samples: int = MIN_SAMPLES,
    retain_percentile: float = RETAIN_PERCENTILE,
    max_step_ratio: float = MAX_STEP_RATIO,
) -> dict[str, Any] | None:
    """Чистый оптимизатор одного порога.

    labeled: список {feature_value, outcome}. Кандидат-порог = низкий перцентиль
    значений у оправдавшихся (удерживает ~90% из них). Возвращает предложение или None.
    """
    vind = [float(x["feature_value"]) for x in labeled if x["outcome"] == "vindicated"]
    faded = [float(x["feature_value"]) for x in labeled if x["outcome"] == "faded"]
    n = len(vind) + len(faded)
    if n < min_samples or not vind or not faded:
        return None

    candidate = percentile(vind, retain_percentile)
    candidate = _clamp_step(
        current_value, candidate, max_step_ratio=max_step_ratio, floor=floor, ceil=ceil, is_int=is_int
    )

    denom = abs(current_value) if current_value else 1.0
    if abs(candidate - current_value) / denom < MIN_REL_CHANGE:
        return None

    vindicated_cut_current = sum(1 for v in vind if v < current_value)
    vindicated_cut_proposed = sum(1 for v in vind if v < candidate)
    faded_cut_current = sum(1 for v in faded if v < current_value)
    faded_cut_proposed = sum(1 for v in faded if v < candidate)
    vindicated_recovered = vindicated_cut_current - vindicated_cut_proposed
    faded_cut_delta = faded_cut_proposed - faded_cut_current
    # Чистая польза = (удержали больше оправдавшихся) + (отсекли больше угасших). Если ≤0 —
    # сдвиг порога не улучшает разделение (или ухудшает) → не предлагаем (без шумовых no-op).
    if (vindicated_recovered + faded_cut_delta) <= 0:
        return None
    direction = "lower" if candidate < current_value else "raise"
    return {
        "threshold_key": threshold_key,
        "current_value": round(float(current_value), 4),
        "proposed_value": candidate,
        "direction": direction,
        "evidence": {
            "n_samples": n,
            "n_vindicated": len(vind),
            "n_faded": len(faded),
            "vindicated_cut_by_current": vindicated_cut_current,
            "vindicated_cut_by_proposed": vindicated_cut_proposed,
            "vindicated_recovered": vindicated_recovered,
            "faded_cut_by_current": faded_cut_current,
            "faded_cut_by_proposed": faded_cut_proposed,
            "faded_cut_delta": faded_cut_delta,
        },
    }


def _rationale(prop: dict[str, Any], cohort_n: dict[str, int]) -> str:
    ev = prop["evidence"]
    verb = "Понизить" if prop["direction"] == "lower" else "Поднять"
    judge_n = int(cohort_n.get("vindicated_by_judge", 0) or 0)
    judge_note = (
        f" ВНИМАНИЕ: {judge_n} из {ev['n_vindicated']} «оправдавшихся» — РАННИЙ сигнал судьи "
        f"(DeepSeek счёл новым, ещё НЕ подтверждено ростом burst). Предложение предварительное, "
        f"окрепнет по мере созревания снимков."
        if judge_n
        else ""
    )
    return (
        f"{verb} {prop['threshold_key']}: {prop['current_value']} → {prop['proposed_value']}. "
        f"На {ev['n_samples']} weak-кандидатах ({ev['n_vindicated']} оправдались, "
        f"{ev['n_faded']} угасли) текущий порог режет {ev['vindicated_cut_by_current']} "
        f"оправдавшихся; предложенный — {ev['vindicated_cut_by_proposed']} "
        f"(Δ возврата {ev['vindicated_recovered']:+d}), при этом отсекает "
        f"{ev['faded_cut_by_proposed']} угасших (Δ {ev['faded_cut_delta']:+d})."
        + judge_note
    )


# ── Оркестрация ввода-вывода ──────────────────────────────────────────────────


async def _load_mature_snapshots(
    session: AsyncSession, workspace_id: str
) -> list[dict[str, Any]]:
    """Самый ранний снимок на signal_key: зрелый (>=30д, burst-исход) ИЛИ с вердиктом судьи.

    Судья даёт ранний сигнал (Контур B→A), поэтому судённые-но-ещё-незрелые снимки тоже
    берём — это даёт петле данные раньше, не дожидаясь 30 дней.
    """
    now = datetime.now(UTC)
    rows = await session.execute(
        text(
            """
            SELECT DISTINCT ON (signal_key)
                signal_key, stage, suppressed, signal_score, confidence, burst_score,
                source_diversity, source_count, threshold_version, judge_verdict, detected_at
            FROM weak_signal_snapshots
            WHERE workspace_id = :ws
              AND detected_at >= :max_before
              AND (detected_at <= :mature_before OR judge_verdict IS NOT NULL)
            ORDER BY signal_key, detected_at ASC
            """
        ),
        {
            "ws": workspace_id,
            "mature_before": now - timedelta(days=MATURE_AGE_DAYS),
            "max_before": now - timedelta(days=MAX_AGE_DAYS),
        },
    )
    return [dict(r) for r in rows.mappings().all()]


async def _load_current_state(
    session: AsyncSession, workspace_id: str, keys: list[str]
) -> dict[str, dict[str, Any]]:
    if not keys:
        return {}
    rows = await session.execute(
        text(
            """
            SELECT signal_key AS key, signal_stage, signal_score,
                   NULL::double precision AS burst_score, false AS is_trend
            FROM emerging_signals WHERE workspace_id = :ws AND signal_key = ANY(:keys)
            UNION ALL
            SELECT cluster_key AS key, signal_stage, signal_score, burst_score, true AS is_trend
            FROM trend_clusters WHERE workspace_id = :ws AND cluster_key = ANY(:keys)
            """
        ),
        {"ws": workspace_id, "keys": keys},
    )
    by_key: dict[str, dict[str, Any]] = {}
    for r in rows.mappings().all():
        rec = dict(r)
        prev = by_key.get(rec["key"])
        # trend важнее emerging как «текущее состояние» сигнала
        if prev is None or (rec.get("is_trend") and not prev.get("is_trend")):
            by_key[rec["key"]] = rec
    return by_key


async def _upsert_proposal(
    session: AsyncSession,
    *,
    workspace_id: str,
    run_id: str,
    threshold_version: str | None,
    prop: dict[str, Any],
    rationale: str,
) -> None:
    proposal_id = _digest(f"{workspace_id}|{prop['threshold_key']}|pending", "thrprop")
    await session.execute(
        text(
            """
            INSERT INTO threshold_proposals (
                id, workspace_id, threshold_key, current_value, proposed_value, status,
                cohort_days, direction, rationale, evidence, threshold_version, run_id,
                created_by, created_at, updated_at
            ) VALUES (
                :id, :ws, :key, :cur, :prop, 'pending',
                :cohort, :direction, :rationale, CAST(:evidence AS jsonb), :tv, :run_id,
                'retrospective_loop', NOW(), NOW()
            )
            ON CONFLICT (workspace_id, threshold_key) WHERE status = 'pending'
            DO UPDATE SET
                current_value = EXCLUDED.current_value, proposed_value = EXCLUDED.proposed_value,
                cohort_days = EXCLUDED.cohort_days, direction = EXCLUDED.direction,
                rationale = EXCLUDED.rationale, evidence = EXCLUDED.evidence,
                threshold_version = EXCLUDED.threshold_version, run_id = EXCLUDED.run_id,
                updated_at = NOW()
            """
        ),
        {
            "id": proposal_id,
            "ws": workspace_id,
            "key": prop["threshold_key"],
            "cur": prop["current_value"],
            "prop": prop["proposed_value"],
            "cohort": MATURE_AGE_DAYS,
            "direction": prop["direction"],
            "rationale": rationale,
            "evidence": json.dumps(prop["evidence"]),
            "tv": threshold_version,
            "run_id": run_id,
        },
    )


async def run_retrospective_review(workspace_id: str) -> dict[str, Any]:
    """Прогон ретро-петли для одного workspace. Пишет/обновляет pending-предложения."""
    from shared.db import get_session_factory

    run_id = _digest(f"{workspace_id}|{datetime.now(UTC).isoformat()}", "retro")
    async with get_session_factory()() as session:
        cluster_cfg = await _cluster_settings(session, workspace_id)
        snapshots = await _load_mature_snapshots(session, workspace_id)
        if not snapshots:
            return {
                "workspace_id": workspace_id,
                "run_id": run_id,
                "mature_snapshots": 0,
                "proposals": [],
                "note": "нет зрелых снимков (нужно ≥30 дней истории)",
            }

        keys = [s["signal_key"] for s in snapshots]
        current = await _load_current_state(session, workspace_id, keys)

        labels: list[str] = []
        outcomes_by_key: dict[str, str] = {}
        judge_vindicated_n = 0
        for snap in snapshots:
            jv = snap.get("judge_verdict")
            if isinstance(jv, str):
                try:
                    jv = json.loads(jv)
                except Exception:
                    jv = None
            cur = current.get(snap["signal_key"])
            base = label_outcome(snap, cur, None)
            outcome = label_outcome(snap, cur, jv)
            if base != "vindicated" and outcome == "vindicated":
                judge_vindicated_n += 1  # судья дал ранний сигнал там, где burst ещё не вырос
            outcomes_by_key[snap["signal_key"]] = outcome
            labels.append(outcome)
        vindicated_n = sum(1 for o in labels if o == "vindicated")
        threshold_version = next((s.get("threshold_version") for s in snapshots), None)

        proposals_out: list[dict[str, Any]] = []
        for feat in GATE_FEATURES:
            labeled = [
                {"feature_value": float(s[feat["snap_col"]] or 0), "outcome": outcomes_by_key[s["signal_key"]]}
                for s in snapshots
            ]
            prop = propose_threshold(
                feat["threshold_key"],
                float(cluster_cfg.get(feat["threshold_key"], 0.0)),
                labeled,
                floor=feat["floor"],
                ceil=feat["ceil"],
                is_int=feat["is_int"],
            )
            if not prop:
                continue
            rationale = _rationale(
                prop,
                {
                    "snapshots": len(snapshots),
                    "vindicated": vindicated_n,
                    "vindicated_by_judge": judge_vindicated_n,
                },
            )
            await _upsert_proposal(
                session,
                workspace_id=workspace_id,
                run_id=run_id,
                threshold_version=threshold_version,
                prop=prop,
                rationale=rationale,
            )
            proposals_out.append(prop)

        # Снимаем устаревшие pending-предложения петли, которые этот прогон больше не выдал
        # (условия изменились / польза исчезла) — чтобы pending всегда отражал актуальный прогон.
        await session.execute(
            text(
                """
                UPDATE threshold_proposals SET status = 'superseded', updated_at = NOW()
                WHERE workspace_id = :ws AND status = 'pending'
                  AND created_by = 'retrospective_loop'
                  AND threshold_key <> ALL(:keys)
                """
            ),
            {"ws": workspace_id, "keys": [p["threshold_key"] for p in proposals_out]},
        )
        await session.commit()
        return {
            "workspace_id": workspace_id,
            "run_id": run_id,
            "mature_snapshots": len(snapshots),
            "vindicated": vindicated_n,
            "vindicated_by_judge": judge_vindicated_n,
            "faded": len(snapshots) - vindicated_n,
            "proposals": proposals_out,
        }
