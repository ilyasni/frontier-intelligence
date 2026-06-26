from worker.services.relevance_audit import propose_relevance_threshold


def _audited(fn_scores, correct_n):
    rows = [{"score": s, "verdict": "false_negative"} for s in fn_scores]
    rows += [{"score": 0.1, "verdict": "correct_reject"} for _ in range(correct_n)]
    return rows


def test_insufficient_audited_returns_none() -> None:
    assert propose_relevance_threshold(0.6, _audited([0.55, 0.58], 3)) is None


def test_insufficient_false_negatives_returns_none() -> None:
    # 20 audited but only 2 FN (< REL_MIN_FN)
    assert propose_relevance_threshold(0.6, _audited([0.55, 0.58], 18)) is None


def test_low_fn_rate_returns_none() -> None:
    # 5 FN out of 100 -> rate 0.05 < 0.15
    assert propose_relevance_threshold(0.6, _audited([0.55] * 5, 95)) is None


def test_proposes_lower_threshold_to_recover_false_negatives() -> None:
    fns = [0.45, 0.50, 0.52, 0.55, 0.57, 0.58]
    prop = propose_relevance_threshold(0.6, _audited(fns, 14))
    assert prop is not None
    assert prop["threshold_key"] == "relevance_threshold"
    assert prop["direction"] == "lower"
    assert prop["proposed_value"] == 0.5
    assert prop["evidence"]["recovered_at_proposed"] == 5
    assert prop["evidence"]["n_false_negative"] == 6


def test_step_cap_bounds_large_drop() -> None:
    # all FN very low -> candidate would crash down, but step capped to 0.2 below current
    fns = [0.05] * 10
    prop = propose_relevance_threshold(0.6, _audited(fns, 20))
    assert prop is not None
    assert prop["proposed_value"] >= 0.4  # 0.6 - 0.2 step cap
