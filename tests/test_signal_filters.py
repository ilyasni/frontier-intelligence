from admin.backend.routers.clusters import _normalize_signal_stages as normalize_admin_signal_stages
from mcp.tools.observability import _normalize_signal_stages as normalize_mcp_signal_stages


def test_signal_stage_filters_default_to_emerging() -> None:
    assert normalize_admin_signal_stages(None, default=("emerging",)) == ["emerging"]
    assert normalize_mcp_signal_stages(None, default=("emerging",)) == ["emerging"]


def test_signal_stage_filters_drop_invalid_values() -> None:
    expected = ["weak", "fading"]
    values = ["WEAK", "unknown", "fading", ""]

    assert normalize_admin_signal_stages(values, default=("emerging",)) == expected
    assert normalize_mcp_signal_stages(values, default=("emerging",)) == expected


def test_signal_stage_filters_fallback_when_values_empty() -> None:
    assert normalize_admin_signal_stages(["nope"], default=("emerging", "stable")) == ["emerging", "stable"]
    assert normalize_mcp_signal_stages(["nope"], default=("emerging", "stable")) == ["emerging", "stable"]
