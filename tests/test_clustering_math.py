import math
from datetime import UTC, datetime, timedelta

import pytest

from worker.services.clustering_math import (
    bucket_start,
    centroid,
    cosine,
    digest,
    freshness,
    jaccard,
    json_ready,
    terms,
)


def test_cosine_identical_vectors_is_one():
    assert cosine([1.0, 2.0, 3.0], [1.0, 2.0, 3.0]) == pytest.approx(1.0)


def test_cosine_orthogonal_is_zero():
    assert cosine([1.0, 0.0], [0.0, 1.0]) == pytest.approx(0.0)


def test_cosine_empty_or_zero_vectors():
    assert cosine([], [1.0]) == 0.0
    assert cosine([0.0, 0.0], [1.0, 1.0]) == 0.0


def test_cosine_known_angle():
    # 45 degrees -> cos = 1/sqrt(2)
    assert cosine([1.0, 0.0], [1.0, 1.0]) == pytest.approx(1.0 / math.sqrt(2))


def test_centroid_averages_componentwise():
    assert centroid([[0.0, 0.0], [2.0, 4.0]]) == [1.0, 2.0]


def test_centroid_empty():
    assert centroid([]) == []


def test_freshness_decay_tiers():
    now = datetime.now(UTC)
    assert freshness(now) == 1.0
    assert freshness(now - timedelta(hours=48)) == 0.75
    assert freshness(now - timedelta(hours=120)) == 0.45
    assert freshness(now - timedelta(hours=400)) == 0.2


def test_freshness_future_clamped_to_one():
    assert freshness(datetime.now(UTC) + timedelta(hours=10)) == 1.0


def test_jaccard():
    assert jaccard(set(), set()) == 1.0
    assert jaccard({"a"}, set()) == 0.0
    assert jaccard({"a", "b"}, {"b", "c"}) == pytest.approx(1 / 3)
    assert jaccard({"a", "b"}, {"a", "b"}) == 1.0


def test_bucket_start_sub_day():
    dt = datetime(2026, 6, 12, 13, 37, 45, tzinfo=UTC)
    b = bucket_start(dt, 6)
    assert b == datetime(2026, 6, 12, 12, 0, 0, tzinfo=UTC)


def test_bucket_start_daily():
    dt = datetime(2026, 6, 12, 13, 37, 45, tzinfo=UTC)
    b = bucket_start(dt, 24)
    assert b == datetime(2026, 6, 12, 0, 0, 0, tzinfo=UTC)


def test_terms_tokenizes_and_lowercases():
    out = terms("Hello WORLD foo-bar a")
    assert "hello" in out
    assert "world" in out
    assert "foo-bar" in out
    # tokens shorter than 3 chars are dropped ("a")
    assert "a" not in out


def test_terms_handles_cyrillic():
    out = terms("Тренд Электромобили")
    assert "тренд" in out
    assert "электромобили" in out


def test_terms_empty():
    assert terms("") == []
    assert terms(None) == []


def test_digest_stable_and_prefixed():
    d1 = digest("hello", "post")
    d2 = digest("hello", "post")
    assert d1 == d2
    assert d1.startswith("post:")
    assert len(d1.split(":")[1]) == 16
    assert digest("hello", "post") != digest("world", "post")


def test_json_ready_serializes_datetime():
    dt = datetime(2026, 6, 12, 13, 0, 0, tzinfo=UTC)
    out = json_ready({"when": dt, "n": 1})
    assert out["n"] == 1
    assert isinstance(out["when"], str)
    assert "2026-06-12" in out["when"]
