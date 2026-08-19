import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_PATH = ROOT / "grafana" / "dashboards" / "frontier-runtime.json"


def _dashboard() -> dict:
    return json.loads(DASHBOARD_PATH.read_text(encoding="utf-8"))


def test_wormsoft_credit_panels_use_pricing_weighted_metrics() -> None:
    dashboard = _dashboard()
    expressions = [
        str(target.get("expr") or "")
        for panel in dashboard.get("panels", [])
        for target in panel.get("targets", [])
    ]
    joined = "\n".join(expressions)

    assert "frontier_wormsoft_credit_utilization_ratio" in joined
    assert "frontier_wormsoft_credit_window_refresh_timestamp_seconds" in joined
    assert "frontier_wormsoft_credit_window_usage" in joined
    assert "frontier_wormsoft_credits_estimated_total" in joined
    assert "execution_role" not in "\n".join(
        expression
        for expression in expressions
        if "frontier_wormsoft_credit_utilization_ratio" in expression
    )
    assert "local_throttle_share" not in joined


def test_frontier_runtime_panel_ids_are_unique() -> None:
    panel_ids = [panel["id"] for panel in _dashboard().get("panels", [])]

    assert len(panel_ids) == len(set(panel_ids))
