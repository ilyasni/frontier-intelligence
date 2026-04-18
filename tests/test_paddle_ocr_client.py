from worker.paddle_ocr_client import aggregate_paddle_lines


def test_aggregate_paddle_lines() -> None:
    text = aggregate_paddle_lines(
        {
            "lines": [
                {"text": "Hello", "confidence": 0.9},
                {"text": "мир", "confidence": 0.8},
            ]
        }
    )
    assert "Hello" in text and "мир" in text
