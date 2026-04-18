import pytest
from pydantic import ValidationError

from mcp.tools.search_balanced import _parse_intent
from shared.search_contracts import BalancedSearchRequest, SearchRequest


def test_search_request_forbids_extra_fields() -> None:
    with pytest.raises(ValidationError):
        SearchRequest(query="ev", unexpected="x")


def test_search_request_normalizes_entities_and_text_filters() -> None:
    req = SearchRequest(
        query="  used ev market  ",
        lang="RU",
        source_region="RU",
        entities=[" Tesla ", "BYD", ""],
    )
    assert req.query == "used ev market"
    assert req.lang == "ru"
    assert req.source_region == "ru"
    assert req.entities == ["Tesla", "BYD"]


def test_balanced_search_defaults_are_strict_and_synthesized() -> None:
    req = BalancedSearchRequest(query="battery passport")
    assert req.synthesize is True
    assert req.days_back == 7


def test_balanced_search_intent_detects_region_and_signal_type() -> None:
    intent = _parse_intent(
        BalancedSearchRequest(query="Russian EV regulation risks last 30 days")
    )

    assert intent["source_region"] == "ru"
    assert intent["days_back"] == 30
    assert "regulation" in intent["signal_type_hints"]
    assert intent["wants_counter_signals"] is True
    assert intent["confidence"] >= 0.7
