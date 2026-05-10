from types import SimpleNamespace

import pytest

from shared.llm_control_plane import (
    POLICY_MODE_MAINTENANCE,
    POLICY_MODE_STRICT,
    CIRCUIT_LEVEL_MODEL,
    CIRCUIT_STATE_OPEN,
    CircuitState,
    default_routing_policy_v2,
    derive_provider_readiness,
    normalize_wormsoft_model_snapshot,
    simulate_routing_decision,
    RoutingCandidate,
    TaskFamilyPolicy,
)


def _settings():
    return SimpleNamespace(
        wormsoft_model_default="wormsoft/agent/medium",
        wormsoft_model_mcp_synthesis="wormsoft/agent/large",
        openrouter_text_model="openrouter/free",
        openrouter_vision_model="openrouter/free",
        polza_text_model="google/gemma-3-12b-it",
        polza_synthesis_model="mistralai/mistral-small-3.1-24b-instruct",
        polza_vision_model="qwen3-vl-30b",
        gigachat_model_pro="GigaChat-2-Pro",
        gigachat_model_vision="GigaChat-2-Pro",
        gigachat_embeddings_model="EmbeddingsGigaR",
        gigachat_model="GigaChat-2",
        gigachat_model_lite="GigaChat-2",
        gigachat_model_relevance="GigaChat-2",
        gigachat_model_concepts="GigaChat-2",
        gigachat_model_valence="GigaChat-2",
        gigachat_model_mcp_synthesis="GigaChat-2-Pro",
    )


def test_default_routing_policy_v2_keeps_wormsoft_first_for_text() -> None:
    policy = default_routing_policy_v2(_settings(), "custom", None)

    assert policy.text_generation.candidates[0].provider == "wormsoft"
    assert policy.text_generation.candidates[0].model == "wormsoft/agent/medium"
    assert policy.embeddings.candidates[0].provider == "gigachat"


def test_simulate_routing_decision_skips_open_model_circuit() -> None:
    policy = default_routing_policy_v2(_settings(), "custom", None)

    decision = simulate_routing_decision(
        policy,
        task="relevance",
        circuits=[
            CircuitState(
                level=CIRCUIT_LEVEL_MODEL,
                provider="wormsoft",
                model="wormsoft/agent/medium",
                state=CIRCUIT_STATE_OPEN,
                reason="wormsoft_429",
            )
        ],
    )

    assert decision.requested_provider == "wormsoft"
    assert decision.selected_provider == "openrouter"
    assert decision.skipped_candidates[0]["reason"] == "wormsoft_429"


def test_simulate_routing_decision_strict_mode_keeps_single_candidate() -> None:
    policy = default_routing_policy_v2(_settings(), "custom", None)
    policy.text_generation.mode = POLICY_MODE_STRICT

    decision = simulate_routing_decision(policy, task="relevance")

    assert decision.mode == POLICY_MODE_STRICT
    assert decision.fallback_allowed is False
    assert len(decision.considered_candidates) == 1
    assert decision.selected_provider == "wormsoft"


def test_simulate_routing_decision_maintenance_prefers_gigachat() -> None:
    policy = default_routing_policy_v2(_settings(), "custom", None)
    policy.text_generation.mode = POLICY_MODE_MAINTENANCE

    decision = simulate_routing_decision(policy, task="relevance")

    assert decision.mode == POLICY_MODE_MAINTENANCE
    assert decision.fallback_allowed is False
    assert len(decision.considered_candidates) == 1
    assert decision.selected_provider == "gigachat"


def test_normalize_wormsoft_model_snapshot_detects_capabilities() -> None:
    models = normalize_wormsoft_model_snapshot(
        {
            "models": [
                {
                    "id": "wormsoft/agent/medium",
                    "modalities": ["text", "image"],
                    "input_modalities": ["text", "image"],
                    "output_modalities": ["text"],
                    "capabilities": ["reasoning"],
                },
                {
                    "id": "wormsoft/embed/small",
                    "capabilities": ["embeddings"],
                },
            ]
        }
    )

    assert models[0].supports_text_generation is True
    assert models[0].supports_vision_generation is True
    assert models[1].supports_embeddings is True


def test_derive_provider_readiness_handles_low_credit_and_unavailable() -> None:
    assert (
        derive_provider_readiness(
            available=True,
            ready=True,
            health_status="ok",
            quota_pressure="low_credit",
        )
        == "degraded"
    )


def test_task_family_policy_requires_enabled_candidate() -> None:
    with pytest.raises(ValueError, match="at least one candidate must be enabled"):
        TaskFamilyPolicy(
            family="text_generation",
            candidates=[
                RoutingCandidate(
                    provider="wormsoft",
                    model="wormsoft/agent/medium",
                    enabled=False,
                )
            ],
        )
    assert (
        derive_provider_readiness(
            available=False,
            ready=False,
            health_status="missing_api_key",
            quota_pressure="unknown",
        )
        == "unavailable"
    )
