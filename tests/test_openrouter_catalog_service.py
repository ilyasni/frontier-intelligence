from admin.backend.services.openrouter_catalog import _normalize_model


def test_normalize_model_keeps_free_vision_model() -> None:
    payload = {
        "id": "qwen/qwen2.5-vl-7b-instruct:free",
        "name": "Qwen VL Free",
        "context_length": 32768,
        "pricing": {"prompt": "0", "completion": "0"},
        "architecture": {
            "input_modalities": ["text", "image"],
            "output_modalities": ["text"],
        },
        "supported_parameters": ["tools", "structured_outputs", "temperature"],
        "top_provider": {"max_completion_tokens": 4096, "is_moderated": True},
    }

    normalized = _normalize_model(payload)

    assert normalized is not None
    assert normalized["id"] == "qwen/qwen2.5-vl-7b-instruct:free"
    assert normalized["supports_vision"] is True
    assert normalized["supports_structured"] is True
    assert normalized["supports_tools"] is True


def test_normalize_model_skips_paid_entry() -> None:
    payload = {
        "id": "openai/gpt-4.1",
        "pricing": {"prompt": "0.000002", "completion": "0.000008"},
        "architecture": {"input_modalities": ["text"], "output_modalities": ["text"]},
    }

    assert _normalize_model(payload) is None
