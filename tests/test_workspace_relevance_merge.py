"""Юнит-тесты слияния relevance_weights (admin POST/PATCH)."""
import pytest

from shared.workspace_relevance import merge_relevance_weights, parse_jsonb_object


@pytest.mark.unit
def test_merge_preserves_category_weights():
    existing = {
        "threshold": 0.6,
        "technology": 1.0,
        "design": 1.2,
        "business_models": 0.9,
    }
    out = merge_relevance_weights(existing, new_threshold=0.45)
    assert out["threshold"] == 0.45
    assert out["technology"] == 1.0
    assert out["design"] == 1.2
    assert out["business_models"] == 0.9


@pytest.mark.unit
def test_merge_from_json_string():
    s = '{"threshold": 0.7, "science": 0.85}'
    out = merge_relevance_weights(s, new_threshold=0.5)
    assert out["threshold"] == 0.5
    assert out["science"] == 0.85


@pytest.mark.unit
def test_merge_empty_existing():
    out = merge_relevance_weights(None, new_threshold=0.6)
    assert out == {"threshold": 0.6}


@pytest.mark.unit
def test_parse_jsonb_object_invalid_string():
    assert parse_jsonb_object("not json") == {}


@pytest.mark.unit
def test_merge_no_threshold_update():
    base = {"threshold": 0.6, "design": 1.0}
    out = merge_relevance_weights(base, new_threshold=None)
    assert out == base
