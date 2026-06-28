from worker.services.retrospective import (
    JUDGE_VINDICATION_NOVELTY,
    VINDICATION_BURST_DELTA,
    VINDICATION_SCORE_DELTA,
    label_outcome,
    percentile,
    propose_threshold,
)

_JUDGE_HI = {"underrated": True, "out_of_distribution": True, "novelty_score": 0.85}
_JUDGE_LOW = {"underrated": True, "out_of_distribution": True, "novelty_score": 0.62}
_JUDGE_NOT_OOD = {"underrated": True, "out_of_distribution": False, "novelty_score": 0.9}


def test_judge_underrated_vindicates_without_growth() -> None:
    snap = {"burst_score": 0.3, "signal_score": 0.4}
    # no current (faded by burst), but high-confidence judge -> vindicated (early signal)
    assert label_outcome(snap, None, _JUDGE_HI) == "vindicated"
    # weak current, no growth, but judge high -> vindicated
    weak = {"signal_stage": "weak", "burst_score": 0.31, "signal_score": 0.41}
    assert label_outcome(snap, weak, _JUDGE_HI) == "vindicated"


def test_judge_below_threshold_does_not_vindicate() -> None:
    snap = {"burst_score": 0.3, "signal_score": 0.4}
    assert label_outcome(snap, None, _JUDGE_LOW) == "faded"      # novelty < 0.7
    assert label_outcome(snap, None, _JUDGE_NOT_OOD) == "faded"  # not out_of_distribution
    assert label_outcome(snap, None, None) == "faded"


def test_growth_vindicates_regardless_of_judge() -> None:
    snap = {"burst_score": 0.3, "signal_score": 0.4}
    grew = {"signal_stage": "emerging", "burst_score": 0.3, "signal_score": 0.4}
    assert label_outcome(snap, grew, None) == "vindicated"


def test_percentile_interpolates() -> None:
    assert percentile([], 50) == 0.0
    assert percentile([0.5], 10) == 0.5
    assert percentile([0.0, 1.0], 50) == 0.5
    assert round(percentile([0.0, 0.1, 0.2, 0.3, 0.4], 50), 4) == 0.2


def test_label_outcome_absent_is_faded() -> None:
    assert label_outcome({"burst_score": 0.3, "signal_score": 0.4}, None) == "faded"


def test_label_outcome_trend_or_stage_is_vindicated() -> None:
    assert label_outcome({}, {"is_trend": True, "signal_stage": "weak"}) == "vindicated"
    assert label_outcome({}, {"is_trend": False, "signal_stage": "emerging"}) == "vindicated"
    assert label_outcome({}, {"is_trend": False, "signal_stage": "stable"}) == "vindicated"


def test_label_outcome_growth_is_vindicated() -> None:
    snap = {"burst_score": 0.30, "signal_score": 0.40}
    # burst grew enough
    assert label_outcome(snap, {"signal_stage": "weak", "burst_score": 0.50, "signal_score": 0.40}) == "vindicated"
    # score grew enough
    assert label_outcome(snap, {"signal_stage": "weak", "burst_score": 0.30, "signal_score": 0.55}) == "vindicated"


def test_label_outcome_no_growth_is_faded() -> None:
    snap = {"burst_score": 0.30, "signal_score": 0.40}
    assert label_outcome(snap, {"signal_stage": "weak", "burst_score": 0.32, "signal_score": 0.41}) == "faded"


def _labeled(vind_vals, faded_vals):
    return (
        [{"feature_value": v, "outcome": "vindicated"} for v in vind_vals]
        + [{"feature_value": v, "outcome": "faded"} for v in faded_vals]
    )


def test_propose_threshold_insufficient_samples_returns_none() -> None:
    assert propose_threshold("weak_signal_min_source_diversity", 0.5, _labeled([0.3] * 3, [0.1] * 3)) is None


def test_propose_threshold_requires_both_classes() -> None:
    assert propose_threshold("k", 0.5, _labeled([0.3] * 25, [])) is None


def test_propose_threshold_lowers_to_recover_false_negatives() -> None:
    vind = [0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 1.0]
    faded = [0.0, 0.02, 0.05, 0.08, 0.10, 0.12, 0.15, 0.18, 0.20, 0.05, 0.07, 0.09, 0.11, 0.13, 0.16]
    prop = propose_threshold("weak_signal_min_source_diversity", 0.5, _labeled(vind, faded))
    assert prop is not None
    assert prop["direction"] == "lower"
    assert prop["proposed_value"] == 0.37
    assert prop["evidence"]["vindicated_recovered"] == 2
    # faded all sit well below the new threshold, so none extra admitted
    assert prop["evidence"]["faded_cut_delta"] == 0


def test_propose_threshold_ignores_cosmetic_change() -> None:
    prop = propose_threshold("k", 0.5, _labeled([0.5] * 15, [0.1] * 15))
    assert prop is None


def test_propose_threshold_respects_step_cap() -> None:
    # vindicated spread below a too-high gate (0.8); lowering recovers them, step cap bounds to 0.4.
    vind = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.5]
    faded = [0.02] * 15
    prop = propose_threshold("k", 0.8, _labeled(vind, faded))
    assert prop is not None
    assert prop["proposed_value"] >= 0.4  # 0.8 - 50% step cap
    assert prop["direction"] == "lower"
    assert prop["evidence"]["vindicated_recovered"] > 0  # net benefit guard satisfied


def test_propose_threshold_skips_no_benefit_change() -> None:
    # feature does not separate vindicated from faded -> no net benefit -> None
    vind = [0.05] * 13
    faded = [0.9] * 13
    assert propose_threshold("k", 0.8, _labeled(vind, faded)) is None


def test_propose_threshold_int_feature_stays_integer() -> None:
    vind = [3, 4, 5, 6, 7, 8, 3, 4, 5, 6, 7, 8, 4, 5, 6]
    faded = [1, 1, 1, 2, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2]
    prop = propose_threshold("weak_signal_min_source_count", 2.0, _labeled(vind, faded), floor=1, ceil=None, is_int=True)
    if prop is not None:
        assert prop["proposed_value"] == float(int(prop["proposed_value"]))


# ── Контур A: укреплённое recall-first поведение propose_threshold (фикс) ──────


def test_propose_threshold_recall_first_blocks_raise_cutting_vindicated() -> None:
    """КЛЮЧЕВОЙ тест фикса: повышение порога режет 1 оправдавшегося
    (vindicated_recovered < 0), но даёт положительную чистую пользу
    (faded_cut_delta > 0, net > 0). До фикса предложение прошло бы по net-benefit;
    после фикса recall-first дисквалифицирует его — возврат None.
    """
    current = 0.20
    # один vindicated (0.18) ниже current → попадёт под кандидата 0.255 и будет срезан
    vind = [0.18, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75]
    # часть faded в (current, candidate] → faded_cut_delta > 0, чистая польза > 0
    faded = [0.05, 0.06, 0.07, 0.21, 0.23, 0.24, 0.25, 0.24]
    # sanity: net-benefit положительна, но vindicated_recovered отрицателен → None
    assert propose_threshold("weak_signal_min_score", current, _labeled(vind, faded)) is None


def test_propose_threshold_zero_net_benefit_boundary_returns_none() -> None:
    """Граница чистой пользы: vindicated_recovered == 0 и faded_cut_delta == 0
    → net == 0 → не предлагаем (нет улучшения разделения)."""
    current = 0.20
    vind = [0.30, 0.32, 0.34, 0.36, 0.38, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70]
    # все faded строго ниже current → между current и candidate никого, delta == 0
    faded = [0.05, 0.06, 0.07, 0.08, 0.09, 0.10, 0.11, 0.12]
    assert propose_threshold("weak_signal_min_score", current, _labeled(vind, faded)) is None


def test_propose_threshold_valid_raise_without_cutting_vindicated() -> None:
    """Корректный raise: vindicated высоко (ни один не режется,
    vindicated_recovered >= 0), faded между current и candidate (net > 0)
    → предложение есть, direction == 'raise', proposed > current."""
    current = 0.20
    vind = [0.30, 0.32, 0.34, 0.36, 0.38, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70]
    faded = [0.05, 0.08, 0.22, 0.24, 0.25, 0.26, 0.27, 0.28]
    prop = propose_threshold("weak_signal_min_score", current, _labeled(vind, faded))
    assert prop is not None
    assert prop["direction"] == "raise"
    assert prop["proposed_value"] > current
    assert prop["evidence"]["vindicated_recovered"] >= 0


def test_propose_threshold_valid_lower_happy_path() -> None:
    """Корректный lower happy-path: понижение порога возвращает оправдавшихся
    (vindicated_recovered >= 0, тут > 0), faded остаются отрезанными (net > 0)."""
    current = 0.50
    vind = [0.30, 0.32, 0.34, 0.36, 0.38, 0.40, 0.42, 0.44, 0.46, 0.48, 0.55, 0.60]
    faded = [0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09]
    prop = propose_threshold("weak_signal_min_score", current, _labeled(vind, faded))
    assert prop is not None
    assert prop["direction"] == "lower"
    assert prop["proposed_value"] < current
    assert prop["evidence"]["vindicated_recovered"] >= 0


# ── percentile: краевые случаи ────────────────────────────────────────────────


def test_percentile_edges() -> None:
    assert percentile([0.2, 0.5, 0.9], 0) == 0.2     # p=0 → min
    assert percentile([0.2, 0.5, 0.9], 100) == 0.9   # p=100 → max
    assert percentile([], 50) == 0.0                 # пустой → 0.0
    assert percentile([0.42], 73) == 0.42            # один элемент → он сам


# ── label_outcome: границы novelty / underrated / malformed ───────────────────


def test_label_outcome_novelty_boundary_vindicates() -> None:
    """novelty ровно == JUDGE_VINDICATION_NOVELTY (0.7) при underrated+OOD → vindicated."""
    snap = {"burst_score": 0.3, "signal_score": 0.4}
    judge = {
        "underrated": True,
        "out_of_distribution": True,
        "novelty_score": JUDGE_VINDICATION_NOVELTY,
    }
    assert label_outcome(snap, None, judge) == "vindicated"


def test_label_outcome_not_underrated_high_novelty_is_faded() -> None:
    """underrated=False при OOD и высокой novelty → faded (все три условия обязательны)."""
    snap = {"burst_score": 0.3, "signal_score": 0.4}
    judge = {"underrated": False, "out_of_distribution": True, "novelty_score": 0.95}
    assert label_outcome(snap, None, judge) == "faded"


def test_label_outcome_malformed_novelty_does_not_crash() -> None:
    """Некорректный novelty ('abc'/None) → faded, без исключения."""
    snap = {"burst_score": 0.3, "signal_score": 0.4}
    bad_str = {"underrated": True, "out_of_distribution": True, "novelty_score": "abc"}
    bad_none = {"underrated": True, "out_of_distribution": True, "novelty_score": None}
    assert label_outcome(snap, None, bad_str) == "faded"
    assert label_outcome(snap, None, bad_none) == "faded"


def test_label_outcome_delta_exactly_at_threshold_vindicates() -> None:
    """burst_delta / score_delta ровно на пороге (база 0.0, чтобы избежать fp-дрейфа)
    → vindicated (сравнение >=)."""
    snap = {"burst_score": 0.0, "signal_score": 0.0}
    cur_burst = {"signal_stage": "weak", "burst_score": VINDICATION_BURST_DELTA, "signal_score": 0.0}
    assert label_outcome(snap, cur_burst) == "vindicated"
    cur_score = {"signal_stage": "weak", "burst_score": 0.0, "signal_score": VINDICATION_SCORE_DELTA}
    assert label_outcome(snap, cur_score) == "vindicated"


def test_label_outcome_none_delta_fields_do_not_crash() -> None:
    """None в burst/score полях snapshot/current → faded, без исключения."""
    snap = {"burst_score": None, "signal_score": None}
    cur = {"signal_stage": "weak", "burst_score": None, "signal_score": None}
    assert label_outcome(snap, cur) == "faded"
