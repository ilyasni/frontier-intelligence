"""Нормализация category из ответа LLM к slug'ам workspace."""
import pytest

from worker.chains.relevance_chain import normalize_relevance_category

ALLOWED = ["technology", "design", "business_models", "society", "science"]


@pytest.mark.unit
def test_normalize_exact_slug():
    assert normalize_relevance_category("design", ALLOWED)[0] == "design"


@pytest.mark.unit
def test_normalize_case_insensitive():
    assert normalize_relevance_category("Technology", ALLOWED)[0] == "technology"


@pytest.mark.unit
def test_normalize_alias_business_to_business_models():
    assert normalize_relevance_category("business", ALLOWED)[0] == "business_models"


@pytest.mark.unit
def test_normalize_russian_design():
    assert normalize_relevance_category("дизайн", ALLOWED)[0] == "design"


@pytest.mark.unit
def test_normalize_other_slugs():
    assert normalize_relevance_category("другое", ALLOWED)[0] == "other"
    assert normalize_relevance_category("other", ALLOWED)[0] == "other"


@pytest.mark.unit
def test_normalize_unknown_returns_other():
    cat, hint = normalize_relevance_category("квантовая_гравитация_xyz", ALLOWED)
    assert cat == "other"
    assert hint is not None


@pytest.mark.unit
def test_normalize_empty_allowed():
    assert normalize_relevance_category("technology", [])[0] == "other"
