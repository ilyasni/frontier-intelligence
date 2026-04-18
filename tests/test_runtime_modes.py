from types import SimpleNamespace

from shared.runtime_modes import (
    RUNTIME_MODE_GIGACHAT_2_ONLY,
    normalize_runtime_mode,
    runtime_overrides_for_mode,
)
from worker.gigachat_client import GigaChatClient


def test_runtime_mode_aliases_and_gigachat_2_only_overrides() -> None:
    assert normalize_runtime_mode("economy") == RUNTIME_MODE_GIGACHAT_2_ONLY

    overrides = runtime_overrides_for_mode(RUNTIME_MODE_GIGACHAT_2_ONLY)

    assert overrides["vision_enabled"] is False
    assert overrides["gigachat_escalation_enabled"] is False
    assert overrides["gigachat_model_pro"] == "GigaChat-2"
    assert overrides["gigachat_model_max"] == "GigaChat-2"


def test_gigachat_client_setting_value_prefers_runtime_override() -> None:
    client = GigaChatClient.__new__(GigaChatClient)
    client._settings = SimpleNamespace(
        vision_enabled=True,
        gigachat_model_pro="GigaChat-2-Pro",
    )
    client._runtime_overrides = runtime_overrides_for_mode(RUNTIME_MODE_GIGACHAT_2_ONLY)

    assert client.setting_bool("vision_enabled", True) is False
    assert client.setting_str("gigachat_model_pro") == "GigaChat-2"
