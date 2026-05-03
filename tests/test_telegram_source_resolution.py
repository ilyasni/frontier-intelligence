from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest


def _async_iter(items):
    async def _gen():
        for item in items:
            yield item
    return _gen()


def _make_msg(msg_id: int, text: str = "hello"):
    from telethon.tl.types import Message
    from datetime import datetime, timezone

    class _FakeMsg(Message):
        pass

    msg = _FakeMsg()
    msg.id = msg_id
    msg.grouped_id = None
    msg.message = text
    msg.date = datetime.now(timezone.utc)
    msg.media = None
    return msg


@pytest.fixture()
def source(monkeypatch):
    monkeypatch.setattr(
        "ingest.sources.telegram_source._make_s3_client",
        lambda: (None, None),
    )
    from ingest.sources.telegram_source import TelegramSource

    redis_mock = MagicMock()
    redis_mock.redis = AsyncMock()
    redis_mock.xadd = AsyncMock(return_value="1-0")

    rotator_mock = MagicMock()

    return TelegramSource(
        source_id="src-telegram",
        workspace_id="disruption",
        config={"channel": "@oldhandle", "lookback_hours": 24, "limit": 50},
        redis=redis_mock,
        rotator=rotator_mock,
        preferred_account_idx=0,
        proxy_config={},
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fetch_uses_cached_entity_id_when_username_changed(source):
    source._checkpoint = {
        "cursor_json": {
            "telegram_peer": {
                "entity_id": 777001,
                "username": "newhandle",
            }
        }
    }
    source.redis.redis.exists = AsyncMock(return_value=0)
    client_mock = MagicMock()
    client_mock.get_input_entity = AsyncMock(return_value="cached-peer")
    client_mock.get_entity = AsyncMock(
        return_value=SimpleNamespace(username="newhandle", id=777001, title="New Handle")
    )
    client_mock.iter_messages.return_value = _async_iter([_make_msg(101, text="post body")])
    source.rotator.get_client = AsyncMock(return_value=client_mock)

    events = await source.fetch()

    assert len(events) == 1
    assert events[0].author == "@newhandle"
    assert events[0].url == "https://t.me/newhandle/101"
    client_mock.get_input_entity.assert_awaited_once_with(777001)
    client_mock.iter_messages.assert_called_once_with("cached-peer", limit=50)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_marks_error_on_semantic_username_failure_without_cached_peer(source):
    runtime_store = SimpleNamespace(
        start_run=AsyncMock(return_value="run-1"),
        finish_run=AsyncMock(),
        load_checkpoint=AsyncMock(return_value={}),
        upsert_checkpoint=AsyncMock(),
    )
    source.runtime_store = runtime_store
    source.redis.redis.exists = AsyncMock(return_value=0)

    client_mock = MagicMock()
    client_mock.get_entity = AsyncMock(side_effect=ValueError('No user has "oldhandle" as username'))
    source.rotator.get_client = AsyncMock(return_value=client_mock)
    source.rotator.handle_error = AsyncMock()
    source.rotator.reset_client = AsyncMock()

    result = await source.run()

    assert result == 0
    finish_kwargs = runtime_store.finish_run.await_args.kwargs
    assert finish_kwargs["status"] == "error"
    assert "telegram_username_unresolved" in finish_kwargs["error_text"]
    checkpoint_kwargs = runtime_store.upsert_checkpoint.await_args.kwargs
    assert "telegram_username_unresolved" in checkpoint_kwargs["last_error"]
