"""HTTP-клиент к PaddleOCR-сервису (контракт как в telegram-assistant: POST /v1/ocr/upload)."""
import logging
import threading
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# Не спамить лог при массовых сбоях OCR (graceful degradation — пустая строка)
_OCR_ERR_LOCK = threading.Lock()
_OCR_ERR_LAST_LOG = 0.0
_OCR_ERR_SUPPRESSED = 0
_OCR_LOG_INTERVAL_SEC = 60.0


def aggregate_paddle_lines(payload: dict[str, Any]) -> str:
    """Склеивает поле lines[].text из JSON-ответа /v1/ocr/upload."""
    lines = payload.get("lines") or []
    parts: list[str] = []
    for line in lines:
        if isinstance(line, dict) and line.get("text"):
            t = str(line["text"]).strip()
            if t:
                parts.append(t)
    return " ".join(parts)


def _guess_image_content(image_bytes: bytes) -> tuple[str, str]:
    if image_bytes[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png", "img.png"
    if image_bytes[:4] == b"GIF8":
        return "image/gif", "img.gif"
    if len(image_bytes) > 12 and image_bytes[:4] == b"RIFF" and image_bytes[8:12] == b"WEBP":
        return "image/webp", "img.webp"
    return "image/jpeg", "img.jpg"


async def paddle_ocr_upload(base_url: str, image_bytes: bytes, timeout: float = 120.0) -> str:
    """
    Отправляет изображение в PaddleOCR. Пустая строка, если URL не задан или запрос не удался.
    """
    base = base_url.strip().rstrip("/")
    if not base:
        return ""
    mime, filename = _guess_image_content(image_bytes)
    url = f"{base}/v1/ocr/upload"
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(url, files={"file": (filename, image_bytes, mime)})
            resp.raise_for_status()
            return aggregate_paddle_lines(resp.json())
    except Exception as exc:
        global _OCR_ERR_LAST_LOG, _OCR_ERR_SUPPRESSED
        now = time.monotonic()
        with _OCR_ERR_LOCK:
            if now - _OCR_ERR_LAST_LOG >= _OCR_LOG_INTERVAL_SEC:
                extra = f" (+ещё {_OCR_ERR_SUPPRESSED} сбоёв за интервал)" if _OCR_ERR_SUPPRESSED else ""
                logger.warning(
                    "PaddleOCR POST failed url=%s err=%s%s",
                    url,
                    exc,
                    extra,
                )
                _OCR_ERR_LAST_LOG = now
                _OCR_ERR_SUPPRESSED = 0
            else:
                _OCR_ERR_SUPPRESSED += 1
        return ""
