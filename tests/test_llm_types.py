from types import SimpleNamespace

import pytest

from worker.llm_types import usage_from_openai_payload, usage_from_openai_response

pytestmark = pytest.mark.unit


def test_usage_from_openai_response_reads_nested_cached_tokens() -> None:
    response = SimpleNamespace(
        usage=SimpleNamespace(
            prompt_tokens=1_000,
            completion_tokens=100,
            total_tokens=1_100,
            prompt_tokens_details=SimpleNamespace(cached_tokens=400),
        )
    )

    usage = usage_from_openai_response(response)

    assert usage.prompt_tokens == 1_000
    assert usage.completion_tokens == 100
    assert usage.precached_prompt_tokens == 400
    assert usage.cached_prompt_tokens == 400


def test_usage_from_openai_payload_supports_responses_token_names() -> None:
    usage = usage_from_openai_payload(
        {
            "usage": {
                "input_tokens": 300,
                "output_tokens": 50,
                "input_tokens_details": {"cached_tokens": 200},
            }
        }
    )

    assert usage.prompt_tokens == 300
    assert usage.completion_tokens == 50
    assert usage.precached_prompt_tokens == 200
    assert usage.total_tokens == 350


def test_direct_precached_tokens_take_precedence_over_nested_details() -> None:
    usage = usage_from_openai_payload(
        {
            "usage": {
                "prompt_tokens": 100,
                "completion_tokens": 10,
                "precached_prompt_tokens": 25,
                "prompt_tokens_details": {"cached_tokens": 90},
            }
        }
    )

    assert usage.precached_prompt_tokens == 25


def test_negative_provider_usage_is_clamped_before_credit_estimation() -> None:
    usage = usage_from_openai_payload(
        {
            "usage": {
                "prompt_tokens": -100,
                "completion_tokens": -10,
                "precached_prompt_tokens": -25,
            }
        }
    )

    assert usage.prompt_tokens == 0
    assert usage.completion_tokens == 0
    assert usage.precached_prompt_tokens == 0
    assert usage.total_tokens == 0
