"""Unit tests for TelegramSource album deduplication logic."""
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest


def _make_msg(msg_id: int, grouped_id=None, has_media=False, text="hello"):
    """Build a Message-like object that passes isinstance checks."""
    # Import the stub class registered in conftest
    from telethon.tl.types import Message, MessageMediaPhoto

    class _FakeMsg(Message):  # subclass so isinstance(msg, Message) is True
        pass

    msg = _FakeMsg()
    msg.id = msg_id
    msg.grouped_id = grouped_id
    msg.message = text
    msg.date = datetime.now(timezone.utc) - timedelta(minutes=1)
    msg.media = MagicMock(spec=MessageMediaPhoto) if has_media else None
    return msg


def _async_iter(items):
    """Return a *synchronous* MagicMock whose __aiter__/__anext__ produce items."""
    async def _gen():
        for item in items:
            yield item
    return _gen()


@pytest.fixture()
def source(monkeypatch):
    """Build a TelegramSource with all external deps mocked."""
    # conftest подменяет pydantic_settings — get_settings() не заполняет поля; S3 в юнитах не нужен
    monkeypatch.setattr(
        "ingest.sources.telegram_source._make_s3_client",
        lambda: (None, None),
    )
    from ingest.sources.telegram_source import TelegramSource

    redis_mock = MagicMock()
    redis_mock.redis = AsyncMock()
    redis_mock.xadd = AsyncMock(return_value="1-0")

    rotator_mock = MagicMock()

    src = TelegramSource(
        source_id="src-1",
        workspace_id="disruption",
        config={"channel": "@testchannel", "lookback_hours": 24, "limit": 50},
        redis=redis_mock,
        rotator=rotator_mock,
        preferred_account_idx=0,
        proxy_config={},
    )
    return src


@pytest.mark.unit
@pytest.mark.asyncio
async def test_new_album_included_in_events(source):
    """First message of a new album group should be collected."""
    source.redis.redis.exists = AsyncMock(return_value=0)  # not in cache

    msg = _make_msg(101, grouped_id=999)
    client_mock = MagicMock()
    client_mock.iter_messages.return_value = _async_iter([msg])
    client_mock.get_messages = AsyncMock(return_value=[])
    source.rotator.get_client = AsyncMock(return_value=client_mock)

    events = await source.fetch()

    assert len(events) == 1
    assert events[0].external_id == "101"
    assert "101" in source._album_cache_keys


@pytest.mark.unit
@pytest.mark.asyncio
async def test_cached_album_skipped(source):
    """Album group already in Redis cache must be skipped entirely."""
    source.redis.redis.exists = AsyncMock(return_value=1)  # already cached

    msg = _make_msg(102, grouped_id=888)
    client_mock = MagicMock()
    client_mock.iter_messages.return_value = _async_iter([msg])
    client_mock.get_messages = AsyncMock(return_value=[])
    source.rotator.get_client = AsyncMock(return_value=client_mock)

    events = await source.fetch()

    assert len(events) == 0


@pytest.mark.unit
@pytest.mark.asyncio
async def test_duplicate_album_in_same_batch_deduplicated(source):
    """Two messages with same grouped_id in one batch → only first included."""
    source.redis.redis.exists = AsyncMock(return_value=0)

    msg1 = _make_msg(201, grouped_id=777)
    msg2 = _make_msg(202, grouped_id=777)  # same album
    client_mock = MagicMock()
    client_mock.iter_messages.return_value = _async_iter([msg1, msg2])
    client_mock.get_messages = AsyncMock(return_value=[])
    source.rotator.get_client = AsyncMock(return_value=client_mock)

    events = await source.fetch()

    assert len(events) == 1
    assert events[0].external_id == "201"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_setex_called_after_successful_xadd(source):
    """Redis setex must be called only after xadd succeeds."""
    source.redis.redis.exists = AsyncMock(return_value=0)
    source.redis.xadd = AsyncMock(return_value="2-0")
    source.redis.redis.setex = AsyncMock()

    msg = _make_msg(301, grouped_id=666)
    client_mock = MagicMock()
    client_mock.iter_messages.return_value = _async_iter([msg])
    client_mock.get_messages = AsyncMock(return_value=[])
    source.rotator.get_client = AsyncMock(return_value=client_mock)

    events = await source.fetch()
    await source.emit_to_stream(events)

    source.redis.redis.setex.assert_called_once()
    args = source.redis.redis.setex.call_args[0]
    assert "666" in args[0]  # cache key contains grouped_id


@pytest.mark.unit
@pytest.mark.asyncio
async def test_setex_not_called_on_xadd_failure(source):
    """If xadd raises, setex must NOT be called (dedup cache not poisoned)."""
    source.redis.redis.exists = AsyncMock(return_value=0)
    source.redis.xadd = AsyncMock(side_effect=Exception("redis down"))
    source.redis.redis.setex = AsyncMock()

    msg = _make_msg(401, grouped_id=555)
    client_mock = MagicMock()
    client_mock.iter_messages.return_value = _async_iter([msg])
    client_mock.get_messages = AsyncMock(return_value=[])
    source.rotator.get_client = AsyncMock(return_value=client_mock)

    events = await source.fetch()
    await source.emit_to_stream(events)

    source.redis.redis.setex.assert_not_called()
    # cache key should still be in _album_cache_keys (not consumed)
    assert "401" in source._album_cache_keys


@pytest.mark.unit
@pytest.mark.asyncio
async def test_album_expansion_merges_get_messages(source):
    """get_messages по id 501..503 добирает пропущенный кадр 502 (тот же grouped_id)."""
    source.redis.redis.exists = AsyncMock(return_value=0)

    msg1 = _make_msg(501, grouped_id=333)
    msg_mid = _make_msg(502, grouped_id=333)
    msg3 = _make_msg(503, grouped_id=333)
    client_mock = MagicMock()
    # iter_messages «пропустил» средний кадр (краевой кейс limit/window)
    client_mock.iter_messages.return_value = _async_iter([msg1, msg3])
    client_mock.get_messages = AsyncMock(return_value=[msg1, msg_mid, msg3])
    source.rotator.get_client = AsyncMock(return_value=client_mock)

    events = await source.fetch()

    assert len(events) == 1
    assert events[0].external_id == "501"
    client_mock.get_messages.assert_called_once()
    call_kw = client_mock.get_messages.call_args
    assert call_kw[0][0] == "@testchannel"
    assert call_kw[1]["ids"] == [501, 502, 503]
