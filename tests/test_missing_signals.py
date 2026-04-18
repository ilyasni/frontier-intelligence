import pytest

from worker.services.missing_signals import (
    _external_signal_strength,
    _frontier_frequency,
    _gap_score,
)


@pytest.mark.unit
def test_frontier_frequency_matches_titles_and_keywords() -> None:
    semantic = [
        {
            "title": "Spatial interfaces reshape dashboard UX",
            "top_concepts": ["spatial computing", "dashboard ux"],
            "explainability": {"top_terms": ["hud", "cockpit"]},
        }
    ]
    stable = [
        {
            "title": "Automotive HUD becomes mainstream",
            "keywords": ["hud", "automotive", "ux"],
            "explainability": {"top_terms": ["windshield display"]},
        }
    ]
    emerging = []

    frequency = _frontier_frequency(
        topic="automotive hud spatial ux",
        query="automotive HUD spatial interface UX",
        semantic=semantic,
        stable=stable,
        emerging=emerging,
    )

    assert frequency > 0


@pytest.mark.unit
def test_gap_score_prefers_external_strength_when_frontier_is_low() -> None:
    external_strength = _external_signal_strength(
        [
            {"url": "https://a.example.com/1", "score": 2.4},
            {"url": "https://b.example.com/2", "score": 1.8},
            {"url": "https://c.example.com/3", "score": 2.0},
        ],
        max_results=5,
    )
    gap = _gap_score(frontier_frequency=0.2, external_strength=external_strength)

    assert external_strength > 0
    assert gap > 0.3


@pytest.mark.unit
def test_gap_score_drops_when_frontier_already_has_coverage() -> None:
    external_strength = _external_signal_strength(
        [
            {"url": "https://a.example.com/1", "score": 2.4},
            {"url": "https://b.example.com/2", "score": 1.8},
        ],
        max_results=5,
    )

    low_gap = _gap_score(frontier_frequency=0.1, external_strength=external_strength)
    high_coverage_gap = _gap_score(frontier_frequency=4.2, external_strength=external_strength)

    assert low_gap > high_coverage_gap
