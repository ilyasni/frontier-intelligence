"""GigaChat client using openai SDK directly (no langchain-openai proxies issues)."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any

from pydantic.fields import FieldInfo

from shared.config import get_settings
from shared.embedding_models import expected_embedding_dim
from shared.metrics import (
    note_gigachat_escalation,
    note_gigachat_request,
    note_gigachat_usage,
    note_rate_limit_event,
)
from shared.runtime_modes import (
    RUNTIME_MODE_REDIS_KEY,
    normalize_runtime_mode,
    runtime_overrides_for_mode,
)
from worker.token_budget import BudgetedText, fit_text_to_token_budget

logger = logging.getLogger(__name__)

EMBED_CACHE_TTL = 7 * 24 * 3600
EMBED_CACHE_PREFIX = "emb:v1:"

VISION_PROMPT = (
    "Проанализируй изображение и верни JSON:\n"
    "{\n"
    '  "labels": ["ключевые объекты, бренды и визуальные сигналы"],\n'
    '  "ocr_text": "текст на изображении или пустая строка",\n'
    '  "scene": "одно короткое предложение",\n'
    '  "design_signals": ["наблюдения о стиле, цвете, типографике и layout"]\n'
    "}\n"
    "Верни только валидный JSON без markdown."
)


@dataclass(frozen=True)
class GigaChatUsage:
    prompt_tokens: int = 0
    completion_tokens: int = 0
    precached_prompt_tokens: int = 0
    total_tokens: int = 0

    @property
    def billable_tokens(self) -> int:
        return self.total_tokens


@dataclass(frozen=True)
class GigaChatResponse:
    content: str
    model: str
    usage: GigaChatUsage = field(default_factory=GigaChatUsage)
    parsed: dict[str, Any] | None = None


def _usage_from_response(resp: Any) -> GigaChatUsage:
    usage = getattr(resp, "usage", None)
    if usage is None:
        return GigaChatUsage()

    def _coerce(name: str) -> int:
        value = getattr(usage, name, 0)
        try:
            return int(value or 0)
        except Exception:
            return 0

    return GigaChatUsage(
        prompt_tokens=_coerce("prompt_tokens"),
        completion_tokens=_coerce("completion_tokens"),
        precached_prompt_tokens=_coerce("precached_prompt_tokens"),
        total_tokens=_coerce("total_tokens"),
    )


def _parse_vision_payload(raw: str) -> dict[str, Any] | None:
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group())
    except json.JSONDecodeError:
        return None


def _vision_payload_has_signal(parsed: dict[str, Any] | None) -> bool:
    if not isinstance(parsed, dict):
        return False
    labels = parsed.get("labels") or []
    design_signals = parsed.get("design_signals") or []
    scene = str(parsed.get("scene") or "").strip()
    ocr_text = str(parsed.get("ocr_text") or "").strip()
    return bool(labels or design_signals or scene or ocr_text)


def _should_skip_vision_escalation(exc: Exception) -> bool:
    status_code = getattr(exc, "status_code", None)
    return status_code in {400, 413, 415, 422}


def _vision_signal_flags(parsed: dict[str, Any] | None) -> list[str]:
    if not isinstance(parsed, dict):
        return ["no_json"]
    flags: list[str] = []
    labels = parsed.get("labels") or []
    design_signals = parsed.get("design_signals") or []
    scene = str(parsed.get("scene") or "").strip()
    ocr_text = str(parsed.get("ocr_text") or "").strip()
    if not labels:
        flags.append("no_labels")
    if not design_signals:
        flags.append("no_design_signals")
    if not scene:
        flags.append("no_scene")
    if not ocr_text:
        flags.append("no_ocr")
    return flags


def _summarize_vision_payload(parsed: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(parsed, dict):
        return {
            "has_signal": False,
            "flags": ["no_json"],
            "labels_count": 0,
            "design_signals_count": 0,
            "scene_present": False,
            "ocr_len": 0,
            "scene_preview": "",
            "labels_preview": [],
        }
    labels = parsed.get("labels") or []
    design_signals = parsed.get("design_signals") or []
    scene = str(parsed.get("scene") or "").strip()
    ocr_text = str(parsed.get("ocr_text") or "").strip()
    return {
        "has_signal": _vision_payload_has_signal(parsed),
        "flags": _vision_signal_flags(parsed),
        "labels_count": len(labels),
        "design_signals_count": len(design_signals),
        "scene_present": bool(scene),
        "ocr_len": len(ocr_text),
        "scene_preview": scene[:120],
        "labels_preview": [str(label)[:60] for label in labels[:5]],
    }


def _vision_raw_preview(raw: str | None, limit: int = 160) -> str:
    text = re.sub(r"\s+", " ", str(raw or "")).strip()
    return text[:limit]


class GigaChatClient:
    """Wrapper around openai SDK pointing to gpt2giga-proxy."""

    def __init__(self, redis=None, *, service_name: str = "worker"):
        import httpx
        from openai import AsyncOpenAI

        settings = get_settings()
        self._settings = settings
        self._redis = redis
        self._runtime_redis = None
        self._runtime_mode = normalize_runtime_mode(getattr(settings, "runtime_mode", "custom"))
        self._runtime_overrides: dict[str, Any] = runtime_overrides_for_mode(self._runtime_mode)
        self._runtime_overrides_loaded_at = 0.0
        self._service_name = service_name
        self._tokens_count_supported = True
        self._session_headers_supported = True
        self._request_sem = asyncio.Semaphore(
            max(1, int(getattr(settings, "gigachat_max_simultaneous_requests", 1) or 1))
        )
        self._min_request_interval_s = max(
            0.0,
            float(int(getattr(settings, "gigachat_min_request_interval_ms", 250) or 250)) / 1000.0,
        )
        self._request_gap_lock = asyncio.Lock()
        self._last_request_started_at = 0.0

        self._http_client = httpx.AsyncClient(timeout=httpx.Timeout(90.0, connect=15.0))
        self._client = AsyncOpenAI(
            base_url=settings.openai_api_base,
            api_key="gigachat",
            http_client=self._http_client,
            max_retries=5,
        )

    async def close(self) -> None:
        if self._runtime_redis is not None:
            await self._runtime_redis.aclose()
        await self._client.close()

    @property
    def runtime_mode(self) -> str:
        return self._runtime_mode

    @property
    def runtime_overrides(self) -> dict[str, Any]:
        return self._runtime_overrides

    async def refresh_runtime_overrides(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._runtime_overrides_loaded_at < 15:
            return
        self._runtime_overrides_loaded_at = now

        mode = normalize_runtime_mode(getattr(self._settings, "runtime_mode", "custom"))
        redis_client = self._redis
        if redis_client is None:
            try:
                import redis.asyncio as aioredis

                self._runtime_redis = self._runtime_redis or aioredis.from_url(
                    self._settings.redis_url,
                    decode_responses=True,
                )
                redis_client = self._runtime_redis
            except Exception as exc:
                logger.debug("runtime_mode_redis_unavailable err=%s", exc)
                redis_client = None

        if redis_client is not None:
            try:
                raw_mode = await redis_client.get(RUNTIME_MODE_REDIS_KEY)
                if isinstance(raw_mode, bytes):
                    raw_mode = raw_mode.decode("utf-8", errors="replace")
                if raw_mode:
                    mode = normalize_runtime_mode(str(raw_mode))
            except Exception as exc:
                logger.debug("runtime_mode_refresh_failed err=%s", exc)

        self._runtime_mode = mode
        self._runtime_overrides = runtime_overrides_for_mode(mode)

    def setting_value(self, name: str, default: Any = None) -> Any:
        if name in self._runtime_overrides:
            return self._runtime_overrides[name]
        return getattr(self._settings, name, default)

    def setting_bool(self, name: str, default: bool = False) -> bool:
        value = self.setting_value(name, default)
        if isinstance(value, FieldInfo):
            return default
        return bool(value)

    def setting_str(self, name: str, default: str = "") -> str:
        value = self.setting_value(name, default)
        if isinstance(value, FieldInfo) or value is None:
            return default
        return str(value)

    def setting_int(self, name: str, default: int) -> int:
        value = self.setting_value(name, default)
        if isinstance(value, FieldInfo):
            return default
        return int(value or default)

    async def _acquire_request_slot(self) -> None:
        await self._request_sem.acquire()
        try:
            async with self._request_gap_lock:
                now = time.monotonic()
                wait_for = self._min_request_interval_s - (now - self._last_request_started_at)
                if wait_for > 0:
                    await asyncio.sleep(wait_for)
                self._last_request_started_at = time.monotonic()
        except Exception:
            self._request_sem.release()
            raise

    def _release_request_slot(self) -> None:
        self._request_sem.release()

    @staticmethod
    def _observe_rate_limit(exc: Exception, operation: str) -> None:
        if getattr(exc, "status_code", None) == 429:
            note_rate_limit_event("worker", "gigachat", operation)

    def _setting_str(self, name: str, default: str = "") -> str:
        return self.setting_str(name, default)

    def _setting_bool(self, name: str, default: bool = False) -> bool:
        return self.setting_bool(name, default)

    def _session_id(self, task: str, system: str, model: str) -> str | None:
        if not self._setting_bool("gigachat_session_cache_enabled", True):
            return None
        base = f"{task}:{model}:{system.strip()[:512]}"
        return hashlib.sha256(base.encode("utf-8")).hexdigest()

    def _with_session_headers(self, session_id: str | None):
        if not session_id or not self._session_headers_supported:
            return self._client
        try:
            return self._client.with_options(extra_headers={"X-Session-ID": session_id})
        except TypeError as exc:
            self._session_headers_supported = False
            logger.warning("gigachat_session_cache_disabled reason=%s", exc)
            return self._client

    @staticmethod
    def _is_model_not_found_error(exc: Exception) -> bool:
        text = str(exc)
        return bool(re.search(r"no such model|model.+not found|unknown model", text, re.IGNORECASE))

    def _fallback_model_for_missing_model(self, model: str, task: str) -> str | None:
        default_chat_model = self._setting_str("gigachat_model", "GigaChat-2").strip() or "GigaChat-2"
        default_pro_model = self._setting_str("gigachat_model_pro", "GigaChat-2-Pro").strip() or "GigaChat-2-Pro"

        configured_candidates = {
            self._setting_str("gigachat_model_lite", default_chat_model).strip(),
            self._setting_str("gigachat_model_relevance").strip(),
            self._setting_str("gigachat_model_concepts").strip(),
            self._setting_str("gigachat_model_valence").strip(),
            self._setting_str("gigachat_model_mcp_synthesis").strip(),
        }
        configured_candidates.discard("")

        if model in configured_candidates and model != default_chat_model:
            return default_chat_model
        if task in {"relevance", "concepts", "mcp_synthesis"} and model != default_chat_model:
            return default_chat_model
        if model != default_pro_model:
            return default_pro_model
        return None

    def _resolve_chat_model(
        self,
        *,
        task: str,
        pro: bool = False,
        model_override: str | None = None,
    ) -> str:
        if model_override and model_override.strip():
            return model_override.strip()

        if task == "relevance":
            return self._setting_str("gigachat_model_relevance").strip() or self._setting_str(
                "gigachat_model_lite", "GigaChat-2"
            )
        if task == "concepts":
            return self._setting_str("gigachat_model_concepts").strip() or self._setting_str(
                "gigachat_model_lite", "GigaChat-2"
            )
        if task == "valence":
            return self._setting_str("gigachat_model_valence").strip() or self._setting_str(
                "gigachat_model_lite", "GigaChat-2"
            )
        if task == "mcp_synthesis":
            return self._setting_str("gigachat_model_mcp_synthesis").strip() or self._setting_str(
                "gigachat_model_lite", "GigaChat-2"
            )
        if pro:
            return self._setting_str("gigachat_model_pro", "GigaChat-2-Pro")
        return self._setting_str("gigachat_model", "GigaChat-2")

    async def count_tokens(self, model: str, text: str) -> int | None:
        if not self._tokens_count_supported:
            return None
        payload = {"model": model, "input": [text]}
        try:
            resp = await self._http_client.post(
                f"{self._settings.gigachat_proxy_url.rstrip('/')}/tokens/count",
                json=payload,
                headers={"Authorization": "Bearer gigachat"},
            )
            if resp.status_code == 404:
                self._tokens_count_supported = False
                logger.info("gigachat_tokens_count_disabled reason=404_not_supported")
                return None
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.debug("tokens_count_unavailable model=%s err=%s", model, exc)
            return None

        if isinstance(data, dict):
            if isinstance(data.get("tokens"), int):
                return int(data["tokens"])
            if isinstance(data.get("count"), int):
                return int(data["count"])
            items = data.get("data") or data.get("input_tokens") or data.get("items")
            if isinstance(items, list) and items:
                first = items[0]
                if isinstance(first, int):
                    return first
                if isinstance(first, dict):
                    for key in ("tokens", "count", "input_tokens"):
                        if isinstance(first.get(key), int):
                            return int(first[key])
        return None

    async def budget_text(self, text: str, model: str, token_budget: int) -> BudgetedText:
        return await fit_text_to_token_budget(text, model, token_budget, self.count_tokens)

    async def embed(self, text: str) -> list[float]:
        """Return embedding vector, using Redis cache and token-aware trimming."""
        await self.refresh_runtime_overrides()
        budgeted = await self.budget_text(
            text,
            self._settings.gigachat_embeddings_model,
            self.setting_int("gigachat_token_budget_embed", 1200),
        )
        embed_text = budgeted.text
        key = EMBED_CACHE_PREFIX + hashlib.sha256(embed_text.encode("utf-8")).hexdigest()

        if self._redis:
            cached = await self._redis.get(key)
            if cached:
                return json.loads(cached)

        try:
            await self._acquire_request_slot()
            try:
                resp = await self._client.embeddings.create(
                    model=self._settings.gigachat_embeddings_model,
                    input=embed_text,
                )
            finally:
                self._release_request_slot()
            usage = _usage_from_response(resp)
            note_gigachat_request(
                self._service_name,
                "embed",
                self._settings.gigachat_embeddings_model,
                "ok",
            )
            note_gigachat_usage(
                self._service_name,
                "embed",
                self._settings.gigachat_embeddings_model,
                prompt_tokens=usage.prompt_tokens,
                completion_tokens=usage.completion_tokens,
                precached_prompt_tokens=usage.precached_prompt_tokens,
                billable_tokens=usage.billable_tokens,
            )
        except Exception as exc:
            self._observe_rate_limit(exc, "embed")
            note_gigachat_request(
                self._service_name,
                "embed",
                self._settings.gigachat_embeddings_model,
                "error",
            )
            raise

        vector = resp.data[0].embedding
        expected_dim = expected_embedding_dim(self._settings.gigachat_embeddings_model)
        actual_dim = len(vector)
        configured_dim = int(getattr(self._settings, "embed_dim", actual_dim) or actual_dim)
        if expected_dim is not None and configured_dim != expected_dim:
            raise ValueError(
                f"Embedding config mismatch: model={self._settings.gigachat_embeddings_model} "
                f"expects dim={expected_dim}, but EMBED_DIM={configured_dim}"
            )
        if actual_dim != configured_dim:
            raise ValueError(
                f"Embedding vector size mismatch: model={self._settings.gigachat_embeddings_model} "
                f"returned dim={actual_dim}, but EMBED_DIM={configured_dim}"
            )

        if self._redis:
            await self._redis.setex(key, EMBED_CACHE_TTL, json.dumps(vector))

        return vector

    async def chat(
        self,
        system: str,
        user: str,
        *,
        task: str = "chat",
        pro: bool = False,
        model_override: str | None = None,
        max_tokens: int = 1024,
    ) -> GigaChatResponse:
        """Chat completion with usage metadata and optional session caching."""
        await self.refresh_runtime_overrides()
        if (
            self._runtime_mode == "gigachat-2-only"
            and task in {"relevance", "concepts", "valence", "mcp_synthesis", "chat"}
        ):
            model_override = self._setting_str("gigachat_model", "GigaChat-2")
            pro = False
        model = self._resolve_chat_model(task=task, pro=pro, model_override=model_override)
        session_id = self._session_id(task, system, model)
        client = self._with_session_headers(session_id)

        try:
            await self._acquire_request_slot()
            try:
                resp = await client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                    temperature=0.1,
                    max_tokens=max_tokens,
                )
            finally:
                self._release_request_slot()
            usage = _usage_from_response(resp)
            note_gigachat_request(self._service_name, task, model, "ok")
            note_gigachat_usage(
                self._service_name,
                task,
                model,
                prompt_tokens=usage.prompt_tokens,
                completion_tokens=usage.completion_tokens,
                precached_prompt_tokens=usage.precached_prompt_tokens,
                billable_tokens=usage.billable_tokens,
            )
        except Exception as exc:
            if self._is_model_not_found_error(exc):
                fallback_model = self._fallback_model_for_missing_model(model, task)
                if fallback_model and fallback_model != model:
                    logger.warning(
                        "gigachat_model_unavailable task=%s requested_model=%s fallback_model=%s",
                        task,
                        model,
                        fallback_model,
                    )
                    await self._acquire_request_slot()
                    try:
                        resp = await client.chat.completions.create(
                            model=fallback_model,
                            messages=[
                                {"role": "system", "content": system},
                                {"role": "user", "content": user},
                            ],
                            temperature=0.1,
                            max_tokens=max_tokens,
                        )
                    finally:
                        self._release_request_slot()
                    usage = _usage_from_response(resp)
                    note_gigachat_request(self._service_name, task, fallback_model, "ok")
                    note_gigachat_usage(
                        self._service_name,
                        task,
                        fallback_model,
                        prompt_tokens=usage.prompt_tokens,
                        completion_tokens=usage.completion_tokens,
                        precached_prompt_tokens=usage.precached_prompt_tokens,
                        billable_tokens=usage.billable_tokens,
                    )
                    return GigaChatResponse(
                        content=resp.choices[0].message.content or "",
                        model=getattr(resp, "model", fallback_model),
                        usage=usage,
                    )
            self._observe_rate_limit(exc, task)
            note_gigachat_request(self._service_name, task, model, "error")
            raise

        return GigaChatResponse(
            content=resp.choices[0].message.content or "",
            model=getattr(resp, "model", model),
            usage=usage,
        )

    async def _vision_request(
        self,
        *,
        model: str,
        mime: str,
        b64: str,
        prompt_text: str,
    ) -> GigaChatResponse:
        session_id = self._session_id("vision", prompt_text, model)
        client = self._with_session_headers(session_id)
        await self._acquire_request_slot()
        try:
            resp = await client.chat.completions.create(
                model=model,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64}"}},
                        {"type": "text", "text": prompt_text},
                    ],
                }],
                temperature=0.1,
                max_tokens=1024,
            )
        finally:
            self._release_request_slot()
        usage = _usage_from_response(resp)
        raw = resp.choices[0].message.content or ""
        parsed = _parse_vision_payload(raw)
        return GigaChatResponse(
            content=raw,
            model=getattr(resp, "model", model),
            usage=usage,
            parsed=parsed,
        )

    async def vision(
        self,
        image_bytes: bytes,
        prompt: str = VISION_PROMPT,
    ) -> GigaChatResponse:
        """Analyze image via GigaChat Vision. Returns parsed JSON and usage metadata."""
        await self.refresh_runtime_overrides()
        b64 = base64.b64encode(image_bytes).decode()
        mime = "image/jpeg"
        if image_bytes[:8] == b"\x89PNG\r\n\x1a\n":
            mime = "image/png"
        elif image_bytes[:4] == b"GIF8":
            mime = "image/gif"
        elif image_bytes[:4] == b"RIFF" and image_bytes[8:12] == b"WEBP":
            mime = "image/webp"

        primary_model = self._setting_str("gigachat_model_vision").strip() or self._setting_str(
            "gigachat_model_pro", "GigaChat-2-Pro"
        )
        fallback_model = self._setting_str("gigachat_model_max", "GigaChat-2-Max").strip() or "GigaChat-2-Max"
        budgeted_prompt = await self.budget_text(
            prompt,
            primary_model,
            self.setting_int("gigachat_token_budget_vision_prompt", 600),
        )

        primary_response: GigaChatResponse | None = None
        try:
            primary_response = await self._vision_request(
                model=primary_model,
                mime=mime,
                b64=b64,
                prompt_text=budgeted_prompt.text,
            )
            note_gigachat_request(self._service_name, "vision", primary_model, "ok")
            note_gigachat_usage(
                self._service_name,
                "vision",
                primary_model,
                prompt_tokens=primary_response.usage.prompt_tokens,
                completion_tokens=primary_response.usage.completion_tokens,
                precached_prompt_tokens=primary_response.usage.precached_prompt_tokens,
                billable_tokens=primary_response.usage.billable_tokens,
            )
        except Exception as exc:
            self._observe_rate_limit(exc, "vision")
            note_gigachat_request(self._service_name, "vision", primary_model, "error")
            if _should_skip_vision_escalation(exc):
                raise
            if not self._setting_bool("gigachat_escalation_enabled", True) or fallback_model == primary_model:
                raise
            note_gigachat_escalation(self._service_name, "vision", primary_model, fallback_model)
            fallback_response = await self._vision_request(
                model=fallback_model,
                mime=mime,
                b64=b64,
                prompt_text=budgeted_prompt.text,
            )
            note_gigachat_request(self._service_name, "vision", fallback_model, "ok")
            note_gigachat_usage(
                self._service_name,
                "vision",
                fallback_model,
                prompt_tokens=fallback_response.usage.prompt_tokens,
                completion_tokens=fallback_response.usage.completion_tokens,
                precached_prompt_tokens=fallback_response.usage.precached_prompt_tokens,
                billable_tokens=fallback_response.usage.billable_tokens,
            )
            return fallback_response

        if (
            primary_response is not None
            and self._setting_bool("gigachat_escalation_enabled", True)
            and fallback_model != primary_model
            and not _vision_payload_has_signal(primary_response.parsed)
        ):
            primary_summary = _summarize_vision_payload(primary_response.parsed)
            logger.info(
                "vision_low_signal_escalation primary_model=%s fallback_model=%s primary_summary=%s primary_raw_preview=%s",
                primary_model,
                fallback_model,
                json.dumps(primary_summary, ensure_ascii=False),
                json.dumps(_vision_raw_preview(primary_response.content), ensure_ascii=False),
            )
            note_gigachat_escalation(self._service_name, "vision", primary_model, fallback_model)
            try:
                fallback_response = await self._vision_request(
                    model=fallback_model,
                    mime=mime,
                    b64=b64,
                    prompt_text=budgeted_prompt.text,
                )
                note_gigachat_request(self._service_name, "vision", fallback_model, "ok")
                note_gigachat_usage(
                    self._service_name,
                    "vision",
                    fallback_model,
                    prompt_tokens=fallback_response.usage.prompt_tokens,
                    completion_tokens=fallback_response.usage.completion_tokens,
                    precached_prompt_tokens=fallback_response.usage.precached_prompt_tokens,
                    billable_tokens=fallback_response.usage.billable_tokens,
                )
                fallback_summary = _summarize_vision_payload(fallback_response.parsed)
                logger.info(
                    "vision_fallback_result primary_model=%s fallback_model=%s fallback_summary=%s fallback_raw_preview=%s",
                    primary_model,
                    fallback_model,
                    json.dumps(fallback_summary, ensure_ascii=False),
                    json.dumps(_vision_raw_preview(fallback_response.content), ensure_ascii=False),
                )
                if _vision_payload_has_signal(fallback_response.parsed):
                    return fallback_response
            except Exception as exc:
                self._observe_rate_limit(exc, "vision")
                note_gigachat_request(self._service_name, "vision", fallback_model, "error")
                logger.warning("Vision fallback to max failed after low-signal primary: %s", exc)

        return primary_response
