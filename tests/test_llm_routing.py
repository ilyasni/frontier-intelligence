from types import SimpleNamespace

from shared.llm_routing import effective_llm_routing


def _settings(**overrides):
    base = {
        "gigachat_model": "GigaChat-2",
        "gigachat_model_lite": "GigaChat-2",
        "gigachat_model_pro": "GigaChat-2-Pro",
        "gigachat_model_max": "GigaChat-2-Max",
        "gigachat_model_relevance": "GigaChat-2",
        "gigachat_model_concepts": "GigaChat-2",
        "gigachat_model_valence": "GigaChat-2",
        "gigachat_model_mcp_synthesis": "GigaChat-2-Pro",
        "wormsoft_model_default": "wormsoft/agent/medium",
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_effective_llm_routing_defaults_use_wormsoft_for_bulk_text() -> None:
    routing = effective_llm_routing(_settings(), "custom", None)

    assert routing.relevance.provider == "wormsoft"
    assert routing.relevance.model == "wormsoft/agent/medium"
    assert routing.relevance.fallback_provider == "gigachat"
    assert routing.relevance.fallback_model == "GigaChat-2"
    assert routing.mcp_synthesis.provider == "gigachat"
    assert routing.mcp_synthesis.model == "GigaChat-2-Pro"


def test_effective_llm_routing_gigachat_only_mode_forces_giga() -> None:
    routing = effective_llm_routing(
        _settings(gigachat_model="GigaChat-2", gigachat_model_pro="GigaChat-2"),
        "gigachat-2-only",
        {
            "relevance": {
                "provider": "wormsoft",
                "model": "wormsoft/agent/medium",
                "fallback_provider": "gigachat",
                "fallback_model": "GigaChat-2",
            }
        },
    )

    assert routing.relevance.provider == "gigachat"
    assert routing.relevance.model == "GigaChat-2"
