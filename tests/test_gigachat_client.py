from worker.gigachat_client import (
    _parse_vision_payload,
    _should_skip_vision_escalation,
    _summarize_vision_payload,
    _vision_raw_preview,
    _vision_payload_has_signal,
)


def test_parse_vision_payload_extracts_json() -> None:
    raw = 'noise {"labels":["car"],"ocr_text":"","scene":"dashboard","design_signals":[]} tail'
    parsed = _parse_vision_payload(raw)

    assert parsed is not None
    assert parsed["labels"] == ["car"]
    assert parsed["scene"] == "dashboard"


def test_vision_payload_has_signal_requires_meaningful_content() -> None:
    assert _vision_payload_has_signal(None) is False
    assert _vision_payload_has_signal({"labels": [], "ocr_text": "", "scene": "", "design_signals": []}) is False
    assert _vision_payload_has_signal({"labels": ["brand"], "ocr_text": "", "scene": "", "design_signals": []}) is True
    assert _vision_payload_has_signal({"labels": [], "ocr_text": "", "scene": "interior", "design_signals": []}) is True


class _FakeVisionError(Exception):
    def __init__(self, status_code: int):
        super().__init__(f"status={status_code}")
        self.status_code = status_code


def test_should_skip_vision_escalation_for_nonfatal_payload_errors() -> None:
    assert _should_skip_vision_escalation(_FakeVisionError(413)) is True
    assert _should_skip_vision_escalation(_FakeVisionError(422)) is True
    assert _should_skip_vision_escalation(_FakeVisionError(429)) is False


def test_summarize_vision_payload_exposes_reason_flags() -> None:
    summary = _summarize_vision_payload({"labels": [], "ocr_text": "", "scene": "", "design_signals": []})
    assert summary["has_signal"] is False
    assert "no_labels" in summary["flags"]
    assert summary["labels_count"] == 0
    assert summary["ocr_len"] == 0


def test_vision_raw_preview_normalizes_whitespace() -> None:
    assert _vision_raw_preview("  hello \n\n world  ") == "hello world"
