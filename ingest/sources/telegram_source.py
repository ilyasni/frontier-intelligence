"""Telegram source — collects posts via Telethon, album-aware with S3 media upload."""
import asyncio
import hashlib
import io
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.types import Message, MessageMediaDocument, MessageMediaPhoto

from ingest.account_rotator import AccountRotator
from ingest.source_runtime import SourceRuntimeStore
from ingest.sources.base import AbstractSource
from shared.config import get_settings
from shared.events.posts_parsed_v1 import PostParsedEvent
from shared.linked_urls import build_linked_urls_for_telegram_messages
from shared.redis_client import RedisClient
from shared.s3 import make_s3_client

logger = logging.getLogger(__name__)

ALBUM_CACHE_TTL = 6 * 3600   # 6 hours
MAX_MEDIA_SIZE = 20 * 1024 * 1024  # 20 MB — skip larger files
# Telegram: до 10 медиа в альбоме; интервал id обычно короткий — не тянуть сотни id в GetMessages
MAX_ALBUM_ITEMS = 10
MAX_ALBUM_ID_SPAN = 48


def _make_s3_client():
    return make_s3_client(get_settings())


def _should_reset_client(exc: Exception) -> bool:
    if isinstance(exc, (RuntimeError, OSError, ConnectionError, asyncio.TimeoutError)):
        return True
    return "another coroutine is already waiting for incoming data" in str(exc).lower()


class TelegramSource(AbstractSource):
    """Collect recent posts from a Telegram channel, with full album support and S3 media upload."""

    def __init__(
        self,
        source_id: str,
        workspace_id: str,
        config: dict[str, Any],
        redis: RedisClient,
        rotator: AccountRotator,
        preferred_account_idx: int = 0,
        proxy_config: dict = None,
        runtime_store: SourceRuntimeStore | None = None,
    ):
        super().__init__(source_id, workspace_id, config, redis, runtime_store=runtime_store)
        self.rotator = rotator
        self.preferred_account_idx = preferred_account_idx
        self.proxy_config = proxy_config or {}
        self.channel = config["channel"]          # e.g. "@channel_name" or numeric id
        self.lookback_hours = config.get("lookback_hours", 24)
        self.limit = config.get("limit", 200)
        self._s3, self._s3_bucket = _make_s3_client()
        # Maps album_cache_key → True; populated after successful stream emit
        self._album_cache_keys: dict[str, str] = {}

    def _has_media(self, msg: Message) -> bool:
        return isinstance(msg.media, (MessageMediaPhoto, MessageMediaDocument))

    def _text(self, msg: Message) -> str:
        return (msg.message or "").strip()

    def _mime_type(self, msg: Message) -> str:
        if isinstance(msg.media, MessageMediaPhoto):
            return "image/jpeg"
        if isinstance(msg.media, MessageMediaDocument):
            doc = msg.media.document
            if doc and doc.mime_type:
                return doc.mime_type
        return "application/octet-stream"

    def _ext(self, mime: str) -> str:
        return {
            "image/jpeg": "jpg",
            "image/png": "png",
            "image/webp": "webp",
            "image/gif": "gif",
            "video/mp4": "mp4",
        }.get(mime, "bin")

    async def _upload_media(self, client: TelegramClient, msg: Message) -> str | None:
        """Download media from Telegram and upload to S3. Returns s3_key or None."""
        if not self._s3 or not self._has_media(msg):
            return None
        try:
            buf = io.BytesIO()
            await asyncio.wait_for(
                client.download_media(msg, file=buf),
                timeout=60.0,
            )
            data = buf.getvalue()
            if not data or len(data) > MAX_MEDIA_SIZE:
                return None
            mime = self._mime_type(msg)
            ext = self._ext(mime)
            sha = hashlib.sha256(data).hexdigest()[:16]
            s3_key = f"media/{self.workspace_id}/{self.source_id}/{msg.id}_{sha}.{ext}"
            loop = asyncio.get_event_loop()
            await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: self._s3.put_object(
                        Bucket=self._s3_bucket,
                        Key=s3_key,
                        Body=data,
                        ContentType=mime,
                    ),
                ),
                timeout=30.0,
            )
            return s3_key
        except Exception as exc:
            logger.warning("[%s] media upload failed for msg %d: %s", self.source_id, msg.id, exc)
            return None

    async def _expand_album_frames(
        self,
        client: TelegramClient,
        grouped_id: str,
        msgs: list[Message],
    ) -> list[Message]:
        """
        Дозагрузка кадров по диапазону message.id, если iter_messages+limit отрезали альбом.
        """
        if len(msgs) >= MAX_ALBUM_ITEMS:
            return sorted(msgs, key=lambda m: m.id)
        msgs_sorted = sorted(msgs, key=lambda m: m.id)
        min_id, max_id = msgs_sorted[0].id, msgs_sorted[-1].id
        span = max_id - min_id + 1
        if span > MAX_ALBUM_ID_SPAN:
            logger.info(
                "[%s] album grouped_id=%s: id span %d > %d, skip expansion",
                self.source_id,
                grouped_id,
                span,
                MAX_ALBUM_ID_SPAN,
            )
            return msgs_sorted
        try:
            batch = await client.get_messages(self.channel, ids=list(range(min_id, max_id + 1)))
        except Exception as exc:
            logger.warning(
                "[%s] album grouped_id=%s get_messages failed: %s",
                self.source_id,
                grouped_id,
                exc,
            )
            return msgs_sorted

        merged: dict[int, Message] = {m.id: m for m in msgs_sorted}
        for m in batch:
            if m is None or not isinstance(m, Message):
                continue
            gid = str(m.grouped_id) if m.grouped_id else None
            if gid != grouped_id:
                continue
            merged[m.id] = m
        out = sorted(merged.values(), key=lambda m: m.id)
        if len(out) > len(msgs_sorted):
            logger.info(
                "[%s] album grouped_id=%s: expanded frames %d → %d",
                self.source_id,
                grouped_id,
                len(msgs_sorted),
                len(out),
            )
        return out

    async def fetch(self) -> list[PostParsedEvent]:
        client = await self.rotator.get_client(
            preferred_idx=self.preferred_account_idx,
            proxy_config=self.proxy_config if self.proxy_config else None,
        )
        if client is None:
            logger.warning("[%s] no available TG account", self.source_id)
            return []

        since = datetime.now(UTC) - timedelta(hours=self.lookback_hours)

        # Collect all messages first (within lookback window)
        # Albums: group by grouped_id, singles: emit as-is
        albums: dict[str, list[Message]] = {}   # grouped_id → [msgs]
        singles: list[Message] = []
        seen_album_ids: set[str] = set()         # albums already in Redis cache

        try:
            async for msg in client.iter_messages(self.channel, limit=self.limit):
                if not isinstance(msg, Message):
                    continue
                if msg.date < since:
                    break

                grouped_id = str(msg.grouped_id) if msg.grouped_id else None

                if grouped_id:
                    # Check Redis cache once per album group
                    if grouped_id not in albums and grouped_id not in seen_album_ids:
                        cache_key = f"album_seen:{self.channel}:{grouped_id}"
                        already_seen = await self.redis.redis.exists(cache_key)
                        if already_seen:
                            seen_album_ids.add(grouped_id)
                            continue
                    if grouped_id in seen_album_ids:
                        continue
                    albums.setdefault(grouped_id, []).append(msg)
                else:
                    singles.append(msg)

        except FloodWaitError as exc:
            logger.warning("[%s] FloodWait %ds", self.source_id, exc.seconds)
            await self.rotator.handle_error(exc)
        except Exception as exc:
            if _should_reset_client(exc):
                await self.rotator.reset_client(
                    preferred_idx=self.preferred_account_idx,
                    reason=f"transport/runtime failure for {self.source_id}: {type(exc).__name__}",
                )
            logger.exception("[%s] fetch error: %s", self.source_id, exc)
            await self.rotator.handle_error(exc)

        events: list[PostParsedEvent] = []
        channel_clean = self.channel.lstrip("@")

        # Process album groups
        for grouped_id, msgs in albums.items():
            msgs_sorted = await self._expand_album_frames(client, grouped_id, msgs)
            # Pick text from whichever message has it
            text = next((self._text(m) for m in msgs_sorted if self._text(m)), "")
            # Канонический пост — самый ранний id в альбоме
            canonical = msgs_sorted[0]

            had_tg_media = any(self._has_media(m) for m in msgs_sorted)
            # Upload all media in parallel
            media_keys = await asyncio.gather(
                *[self._upload_media(client, m) for m in msgs_sorted],
                return_exceptions=True,
            )
            media_urls = [k for k in media_keys if isinstance(k, str)]
            if had_tg_media and not media_urls:
                logger.warning(
                    "[%s] album grouped_id=%s: в Telegram есть медиа, "
                    "но ни один файл не попал в S3",
                    self.source_id,
                    grouped_id,
                )

            ext_id = str(canonical.id)
            cache_key = f"album_seen:{self.channel}:{grouped_id}"
            self._album_cache_keys[ext_id] = cache_key

            events.append(PostParsedEvent(
                source_id=self.source_id,
                workspace_id=self.workspace_id,
                external_id=ext_id,
                grouped_id=grouped_id,
                # Совпадает с условием vision в enrichment: has_media and media_urls
                has_media=bool(media_urls),
                media_urls=media_urls,
                content=text,
                linked_urls=build_linked_urls_for_telegram_messages(msgs_sorted, text),
                url=f"https://t.me/{channel_clean}/{canonical.id}",
                author=self.channel,
                published_at=canonical.date,
            ))

        # Process single messages (not part of any album)
        already_in_albums = {m.id for msgs in albums.values() for m in msgs}
        for msg in singles:
            if msg.id in already_in_albums:
                continue
            tg_media = self._has_media(msg)
            media_urls = []
            if tg_media:
                s3_key = await self._upload_media(client, msg)
                if s3_key:
                    media_urls = [s3_key]
            if tg_media and not media_urls:
                logger.warning(
                    "[%s] msg %d: медиа в Telegram не загрузилось в S3 "
                    "— has_media=false для пайплайна",
                    self.source_id,
                    msg.id,
                )

            txt = self._text(msg)
            events.append(PostParsedEvent(
                source_id=self.source_id,
                workspace_id=self.workspace_id,
                external_id=str(msg.id),
                grouped_id=None,
                has_media=bool(media_urls),
                media_urls=media_urls,
                content=txt,
                linked_urls=build_linked_urls_for_telegram_messages([msg], txt),
                url=f"https://t.me/{channel_clean}/{msg.id}",
                author=self.channel,
                published_at=msg.date,
            ))

        return events

    async def emit_to_stream(self, events: list[PostParsedEvent]) -> int:
        """Push events and mark album cache keys only after successful xadd."""
        pushed = 0
        for event in events:
            try:
                await self.redis.xadd(self.stream_name, event.model_dump(mode="json"))
                pushed += 1
                # Mark album as seen only after confirmed write to stream
                if event.external_id in self._album_cache_keys:
                    cache_key = self._album_cache_keys.pop(event.external_id)
                    await self.redis.redis.setex(cache_key, ALBUM_CACHE_TTL, "1")
            except Exception as exc:
                logger.error("Failed to push event %s: %s", event.external_id, exc)
        return pushed
