"""Vision consumer — processes media from Telegram via GigaChat Vision."""
import asyncio
import gzip
import hashlib
import json
import logging
import uuid
from typing import Optional

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from shared.config import get_settings
from shared.sqlalchemy_pool import ASYNC_ENGINE_POOL_KWARGS
from shared.events.posts_vision_v1 import PostVisionEvent
from shared.reindex import STREAM_POSTS_REINDEX, build_post_reindex_event
from shared.redis_client import RedisClient
from shared.s3 import make_s3_client
from worker.gigachat_client import GigaChatClient
from worker.paddle_ocr_client import paddle_ocr_upload

logger = logging.getLogger(__name__)

STREAM_IN = "stream:posts:vision"
GROUP = "vision_workers"
CONSUMER = f"vision-{uuid.uuid4().hex[:8]}"

_NONFATAL_VISION_STATUS_CODES = {400, 413, 415, 422}
_VIDEO_SUFFIXES = (".mp4", ".mov", ".webm", ".mkv", ".avi")


def _empty_vision_result() -> dict:
    return {"labels": [], "ocr_text": "", "scene": "", "design_signals": []}


def _classify_vision_error(exc: Exception) -> tuple[str, int | None]:
    """
    openai-python exposes APIStatusError subclasses with .status_code.
    We keep the check generic so the task degrades cleanly even if the upstream
    wrapper or tests raise a different exception object with the same attribute.
    """
    status_code = getattr(exc, "status_code", None)
    if status_code in _NONFATAL_VISION_STATUS_CODES:
        return "nonfatal_upstream", status_code
    return "fatal_upstream", status_code


def _vision_error_payload(exc: Exception) -> dict:
    kind, status_code = _classify_vision_error(exc)
    payload = {
        "kind": kind,
        "status_code": status_code,
        "message": str(exc)[:300],
    }
    request_id = getattr(exc, "request_id", None)
    if request_id:
        payload["request_id"] = str(request_id)
    return payload


def _detect_media_mime(s3_key: str, media_bytes: bytes) -> str:
    key = str(s3_key or "").lower()
    if media_bytes[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if media_bytes[:3] == b"GIF":
        return "image/gif"
    if media_bytes[:4] == b"RIFF" and media_bytes[8:12] == b"WEBP":
        return "image/webp"
    if media_bytes[:12].startswith(b"\x00\x00\x00") and b"ftyp" in media_bytes[:16]:
        return "video/mp4"
    if key.endswith(".mp4"):
        return "video/mp4"
    if key.endswith(".mov"):
        return "video/quicktime"
    if key.endswith(".webm"):
        return "video/webm"
    return "image/jpeg"


def _should_skip_model_vision(event: PostVisionEvent, s3_key: str, mime: str, size_bytes: int) -> str | None:
    if event.vision_mode == "ocr_only":
        return "source_vision_mode_ocr_only"
    if mime.startswith("video/") or str(s3_key or "").lower().endswith(_VIDEO_SUFFIXES):
        return "video_media_not_sent_to_gigachat"
    if event.max_media_bytes > 0 and size_bytes > event.max_media_bytes:
        return f"media_too_large>{event.max_media_bytes}"
    return None


def _should_run_paddle_ocr(vision_mode: str, mime: str) -> bool:
    return vision_mode != "skip" and mime.startswith("image/")


class VisionTask:
    def __init__(self):
        settings = get_settings()
        self.settings = settings
        self.redis = RedisClient(settings.redis_url)
        self.engine = create_async_engine(
            settings.database_url,
            pool_size=3,
            max_overflow=5,
            **ASYNC_ENGINE_POOL_KWARGS,
        )
        self.Session = sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)
        self.gigachat: Optional[GigaChatClient] = None
        self._s3 = None
        self._s3_bucket = None

    async def setup(self):
        await self.redis.connect()
        await self.redis.ensure_consumer_group(STREAM_IN, GROUP)
        self.gigachat = GigaChatClient(redis=self.redis.redis)
        self._s3, self._s3_bucket = make_s3_client(self.settings)
        logger.info("VisionTask ready, consumer=%s", CONSUMER)

    async def _send_to_dlq(
        self,
        msg_id: str,
        data: dict,
        error: str,
        delivery_count: int,
    ) -> None:
        payload = {
            "stream": STREAM_IN,
            "group": GROUP,
            "msg_id": msg_id,
            "post_id": str(data.get("post_id") or ""),
            "error": str(error)[:500],
            "delivery_count": str(delivery_count),
            "payload": json.dumps(data, ensure_ascii=False),
        }
        await self.redis.xadd(self.settings.vision_dlq_stream, payload)
        await self.redis.xack(STREAM_IN, GROUP, msg_id)
        post_id = str(data.get("post_id") or "")
        if post_id:
            await self._update_vision_status(post_id, "error")
        logger.error(
            "Vision event sent to DLQ post=%s msg_id=%s delivery_count=%d err=%s",
            post_id[:8] if post_id else "",
            msg_id,
            delivery_count,
            error,
        )

    async def _handle_failed_event(self, msg_id: str, data: dict, exc: Exception) -> None:
        delivery_count = int(data.get("delivery_count", 0) or 0) + 1
        if delivery_count >= max(1, int(self.settings.vision_max_delivery_count or 1)):
            await self._send_to_dlq(msg_id, data, str(exc), delivery_count)
            return
        await self.redis.xack(STREAM_IN, GROUP, msg_id)
        await self.redis.xadd(
            STREAM_IN,
            {**data, "delivery_count": str(delivery_count)},
        )
        logger.warning(
            "Vision event requeued post=%s msg_id=%s delivery_count=%d err=%s",
            str(data.get("post_id") or "")[:8],
            msg_id,
            delivery_count,
            exc,
        )

    async def _download_from_s3(self, s3_key: str) -> Optional[bytes]:
        if not self._s3:
            return None
        try:
            resp = self._s3.get_object(Bucket=self._s3_bucket, Key=s3_key)
            return resp["Body"].read()
        except Exception as exc:
            logger.warning("S3 download failed for %s: %s", s3_key, exc)
            return None

    async def _upsert_media_object(self, workspace_id: str, s3_key: str, data: bytes, mime: str):
        sha256 = hashlib.sha256(data).hexdigest()
        async with self.Session() as session:
            await session.execute(text("""
                INSERT INTO media_objects (sha256, s3_key, mime_type, size_bytes, workspace_id, created_at)
                VALUES (:sha, :key, :mime, :size, :ws, NOW())
                ON CONFLICT (sha256) DO NOTHING
            """), {
                "sha": sha256, "key": s3_key, "mime": mime,
                "size": len(data), "ws": workspace_id,
            })
            await session.commit()

    async def _save_enrichment(self, post_id: str, kind: str, data: dict):
        enrichment_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{post_id}:{kind}"))
        async with self.Session() as session:
            await session.execute(text("""
                INSERT INTO post_enrichments (id, post_id, kind, data, created_at, updated_at)
                VALUES (:id, :post_id, :kind, CAST(:data AS jsonb), NOW(), NOW())
                ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, updated_at = NOW()
            """), {
                "id": enrichment_id, "post_id": post_id, "kind": kind,
                "data": json.dumps(data),
            })
            await session.commit()

    async def _update_vision_status(self, post_id: str, status: str):
        async with self.Session() as session:
            await session.execute(text("""
                INSERT INTO indexing_status (post_id, embedding_status, vision_status, updated_at)
                VALUES (:post_id, 'pending', :status, NOW())
                ON CONFLICT (post_id) DO UPDATE SET
                    vision_status = EXCLUDED.vision_status,
                    updated_at = NOW()
            """), {"post_id": post_id, "status": status})
            await session.commit()

    async def _update_media_group_vision(
        self, workspace_id: str, source_id: str, grouped_id: str,
        all_labels: list[str], all_ocr: str, summary_s3_key: str | None = None,
    ):
        if not grouped_id:
            return
        group_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{source_id}:{grouped_id}"))
        async with self.Session() as session:
            await session.execute(text("""
                UPDATE media_groups
                SET assembled = CASE
                        WHEN CAST(:summary_present AS boolean) THEN TRUE
                        ELSE assembled
                    END,
                    vision_summary_s3_key = COALESCE(
                        CAST(:summary_s3_key AS text),
                        vision_summary_s3_key
                    ),
                    vision_labels = CAST(:labels AS jsonb),
                    updated_at = NOW()
                WHERE id = :id
            """), {
                "id": group_id,
                "labels": json.dumps(all_labels),
                "summary_present": bool(summary_s3_key),
                "summary_s3_key": summary_s3_key,
            })
            await session.commit()

    def _build_album_summary(
        self,
        post_id: str,
        workspace_id: str,
        source_id: str,
        grouped_id: str,
        items: list[dict],
        all_labels: list[str],
        all_ocr: str,
    ) -> dict:
        return {
            "post_id": post_id,
            "workspace_id": workspace_id,
            "source_id": source_id,
            "grouped_id": grouped_id,
            "items": items,
            "all_labels": sorted(set(all_labels)),
            "ocr_text": all_ocr,
        }

    async def _upload_album_summary(self, summary: dict) -> str | None:
        if not self._s3 or not self._s3_bucket:
            return None
        grouped_id = summary.get("grouped_id")
        workspace_id = summary.get("workspace_id")
        if not grouped_id or not workspace_id:
            return None
        summary_key = f"vision/{workspace_id}/albums/{grouped_id}/summary.json.gz"
        try:
            self._s3.put_object(
                Bucket=self._s3_bucket,
                Key=summary_key,
                Body=gzip.compress(json.dumps(summary).encode("utf-8")),
                ContentType="application/json",
                ContentEncoding="gzip",
            )
            return summary_key
        except Exception as exc:
            logger.warning("Album summary upload failed for grouped_id=%s: %s", grouped_id, exc)
            return None

    async def process_event(self, msg_id: str, data: dict):
        try:
            event = PostVisionEvent.model_validate(dict(data))
        except ValidationError as exc:
            logger.warning("Bad vision event %s: %s", msg_id, exc)
            await self.redis.xack(STREAM_IN, GROUP, msg_id)
            return

        if not event.media_s3_keys:
            await self.redis.xack(STREAM_IN, GROUP, msg_id)
            return

        if event.vision_mode == "skip":
            await self._save_enrichment(event.post_id, "vision", {
                "items": [],
                "all_labels": [],
                "ocr_text": "",
                "vision_mode": "skip",
                "vision_skip_reason": "source_vision_mode_skip",
            })
            await self._update_vision_status(event.post_id, "skipped")
            await self.redis.xack(STREAM_IN, GROUP, msg_id)
            logger.info(
                "Vision skipped post=%s source=%s mode=skip",
                event.post_id[:8],
                event.source_id,
            )
            return

        items = []
        all_labels: list[str] = []
        all_ocr_parts: list[str] = []

        for s3_key in event.media_s3_keys:
            image_bytes = await self._download_from_s3(s3_key)
            if not image_bytes:
                logger.warning("Skipping missing media %s for post %s", s3_key, event.post_id[:8])
                continue

            result = _empty_vision_result()
            vision_error = None
            mime = _detect_media_mime(s3_key, image_bytes)
            skip_reason = _should_skip_model_vision(event, s3_key, mime, len(image_bytes))
            if skip_reason:
                result["vision_skip_reason"] = skip_reason
            else:
                try:
                    vision_response = await self.gigachat.vision(image_bytes)
                    if isinstance(vision_response, dict):
                        result = vision_response
                    else:
                        result = vision_response.parsed or _empty_vision_result()
                except Exception as exc:
                    vision_error = _vision_error_payload(exc)
                    kind = vision_error["kind"]
                    status_code = vision_error["status_code"]
                    if kind == "nonfatal_upstream":
                        logger.info(
                            "Vision fallback to OCR-only for %s status=%s request_id=%s err=%s",
                            s3_key,
                            status_code,
                            vision_error.get("request_id"),
                            exc,
                        )
                    else:
                        logger.warning(
                            "Vision failed for %s status=%s request_id=%s err=%s",
                            s3_key,
                            status_code,
                            vision_error.get("request_id"),
                            exc,
                        )

            if _should_run_paddle_ocr(event.vision_mode, mime):
                paddle_txt = await paddle_ocr_upload(self.settings.paddleocr_url, image_bytes)
                if paddle_txt:
                    gc_ocr = (result.get("ocr_text") or "").strip()
                    result = {
                        **result,
                        "paddle_ocr_text": paddle_txt,
                        "ocr_text": " ".join(p for p in (gc_ocr, paddle_txt) if p),
                    }

            await self._upsert_media_object(event.workspace_id, s3_key, image_bytes, mime)

            item = {
                "s3_key": s3_key,
                "mime_type": mime,
                "size_bytes": len(image_bytes),
                "vision_mode": event.vision_mode,
                **result,
            }
            if vision_error:
                item["vision_error"] = vision_error
            items.append(item)
            all_labels.extend(result.get("labels", []))
            if result.get("ocr_text"):
                all_ocr_parts.append(result["ocr_text"])

        if items:
            await self._save_enrichment(event.post_id, "vision", {
                "vision_mode": event.vision_mode,
                "items": items,
                "all_labels": list(set(all_labels)),
                "ocr_text": " ".join(all_ocr_parts),
            })

        album_summary_key = None
        if event.grouped_id and items:
            album_summary = self._build_album_summary(
                post_id=event.post_id,
                workspace_id=event.workspace_id,
                source_id=event.source_id,
                grouped_id=event.grouped_id,
                items=items,
                all_labels=all_labels,
                all_ocr=" ".join(all_ocr_parts),
            )
            album_summary_key = await self._upload_album_summary(album_summary)
            if album_summary_key:
                logger.info(
                    "Album vision summary saved grouped_id=%s key=%s",
                    event.grouped_id,
                    album_summary_key,
                )

        if event.grouped_id:
            await self._update_media_group_vision(
                workspace_id=event.workspace_id,
                source_id=event.source_id,
                grouped_id=event.grouped_id,
                all_labels=list(set(all_labels)),
                all_ocr=" ".join(all_ocr_parts),
                summary_s3_key=album_summary_key,
            )

        vision_status = "done" if items else "skipped"
        await self._update_vision_status(event.post_id, vision_status)
        if vision_status == "done":
            await self.redis.xadd(
                STREAM_POSTS_REINDEX,
                build_post_reindex_event(
                    post_id=event.post_id,
                    workspace_id=event.workspace_id,
                    reason="vision",
                    source="vision_task",
                    extra={"items": len(items), "labels": len(set(all_labels))},
                ),
            )
        await self.redis.xack(STREAM_IN, GROUP, msg_id)
        logger.info("Vision %s post=%s items=%d labels=%d",
                    vision_status, event.post_id[:8], len(items), len(all_labels))

    async def run_loop(self):
        await self.setup()
        await self._startup_reclaim()
        last_cleanup = asyncio.get_event_loop().time()
        logger.info("Starting vision loop, consumer=%s", CONSUMER)
        while True:
            try:
                reclaimed = await self._reclaim_pending()
                if reclaimed:
                    for mid, d in reclaimed:
                        try:
                            await self.process_event(mid, d)
                        except Exception as exc:
                            await self._handle_failed_event(mid, d, exc)

                messages = await self.redis.xreadgroup(
                    STREAM_IN, GROUP, CONSUMER, count=4, block_ms=5000)
                if messages:
                    # Process sequentially to avoid GigaChat Vision rate limits
                    for mid, d in messages:
                        try:
                            await self.process_event(mid, d)
                        except Exception as exc:
                            await self._handle_failed_event(mid, d, exc)

                now = asyncio.get_event_loop().time()
                if now - last_cleanup > self.settings.indexing_consumer_cleanup_interval:
                    await self._cleanup_dead_consumers()
                    last_cleanup = now
            except Exception as exc:
                logger.error("Vision loop error: %s", exc)
                await asyncio.sleep(5)

    async def _startup_reclaim(self) -> None:
        start_id = "0-0"
        total = 0
        while True:
            next_id, messages = await self.redis.xautoclaim(
                STREAM_IN,
                GROUP,
                CONSUMER,
                self.settings.vision_claim_idle_ms,
                start_id=start_id,
                count=50,
            )
            if messages:
                for mid, data in messages:
                    try:
                        await self.process_event(mid, data)
                    except Exception as exc:
                        await self._handle_failed_event(mid, data, exc)
                total += len(messages)
            # Redis may return an empty message batch while only cleaning deleted
            # PEL entries. Keep scanning until the cursor wraps to 0-0.
            if next_id == "0-0":
                break
            start_id = next_id
        if total:
            logger.info("Vision startup reclaim processed %d stale pending messages", total)

    async def _reclaim_pending(self) -> list:
        _, messages = await self.redis.xautoclaim(
            STREAM_IN,
            GROUP,
            CONSUMER,
            self.settings.vision_claim_idle_ms,
            start_id="0-0",
            count=16,
        )
        return messages

    async def _cleanup_dead_consumers(self) -> None:
        try:
            consumers = await self.redis.xinfo_consumers(STREAM_IN, GROUP)
            for consumer in consumers:
                name = consumer.get("name", "")
                idle_ms = int(consumer.get("idle", 0) or 0)
                pending = int(consumer.get("pending", 0) or 0)
                if name == CONSUMER:
                    continue
                if idle_ms > 3_600_000 and pending == 0:
                    await self.redis.xdel_consumer(STREAM_IN, GROUP, name)
                    logger.info(
                        "Deleted dead vision consumer %s (idle=%ds, pending=0)",
                        name,
                        idle_ms // 1000,
                    )
        except Exception as exc:
            logger.warning("Vision consumer cleanup error: %s", exc)

    async def close(self):
        await self.redis.disconnect()
        await self.engine.dispose()
        if self.gigachat:
            await self.gigachat.close()
