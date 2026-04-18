"""Unit tests for ingest/main.py scheduler logic."""
import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ingest.main import (
    _source_config_hashes,
    build_source_config,
    schedule_all,
    source_config_hash,
)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_new_source_runs_immediately():
    """A source seen for the first time must get next_run_time=now (not paused)."""

    _source_config_hashes.clear()

    source_row = {
        "id": "src-1",
        "name": "Test",
        "source_type": "rss",
        "url": "http://example.com/feed",
        "workspace_id": "disruption",
        "schedule_cron": "*/60 * * * *",
        "tg_channel": None,
        "tg_account_idx": 0,
        "extra": {},
        "proxy_config": {},
    }

    scheduler = MagicMock()
    scheduler.get_job.return_value = None  # source is new
    scheduler.add_job = MagicMock()

    settings = MagicMock()
    redis = MagicMock()

    with patch("ingest.main.load_sources", new=AsyncMock(return_value=[source_row])):
        await schedule_all(scheduler, redis, None, settings)

    assert scheduler.add_job.called
    kwargs = scheduler.add_job.call_args.kwargs
    assert kwargs["next_run_time"] is not None
    # Should be very close to now
    delta = datetime.datetime.now() - kwargs["next_run_time"]
    assert abs(delta.total_seconds()) < 5


@pytest.mark.unit
@pytest.mark.asyncio
async def test_unchanged_source_not_rescheduled():
    """A source with same interval and config hash must not be recreated."""

    source_row = {
        "id": "src-2",
        "name": "Stable",
        "source_type": "rss",
        "url": "http://stable.com/feed",
        "workspace_id": "disruption",
        "schedule_cron": "*/60 * * * *",
        "tg_channel": None,
        "tg_account_idx": 0,
        "extra": {},
        "proxy_config": {},
    }

    job_id = "source_src-2"
    existing_hash = source_config_hash(source_row)
    _source_config_hashes[job_id] = existing_hash

    existing_job = MagicMock()
    existing_job.trigger.interval.total_seconds.return_value = 3600  # 60 min

    scheduler = MagicMock()
    scheduler.get_job.return_value = existing_job
    scheduler.add_job = MagicMock()
    scheduler.remove_job = MagicMock()

    settings = MagicMock()
    redis = MagicMock()

    with patch("ingest.main.load_sources", new=AsyncMock(return_value=[source_row])):
        await schedule_all(scheduler, redis, None, settings)

    scheduler.add_job.assert_not_called()
    scheduler.remove_job.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_config_change_triggers_immediate_run():
    """When source config changes, next_run_time must be now (not preserved)."""

    source_row = {
        "id": "src-3",
        "name": "Changed",
        "source_type": "rss",
        "url": "http://new-url.com/feed",
        "workspace_id": "disruption",
        "schedule_cron": "*/60 * * * *",
        "tg_channel": None,
        "tg_account_idx": 0,
        "extra": {},
        "proxy_config": {},
    }

    job_id = "source_src-3"
    _source_config_hashes[job_id] = "old-hash-different"  # stale hash

    future_run = datetime.datetime.now() + datetime.timedelta(minutes=45)
    existing_job = MagicMock()
    existing_job.trigger.interval.total_seconds.return_value = 3600
    existing_job.next_run_time = future_run

    scheduler = MagicMock()
    scheduler.get_job.return_value = existing_job
    scheduler.add_job = MagicMock()

    settings = MagicMock()
    redis = MagicMock()

    with patch("ingest.main.load_sources", new=AsyncMock(return_value=[source_row])):
        await schedule_all(scheduler, redis, None, settings)

    assert scheduler.add_job.called
    kwargs = scheduler.add_job.call_args.kwargs
    # Should run now, not 45 min from now
    delta = datetime.datetime.now() - kwargs["next_run_time"]
    assert abs(delta.total_seconds()) < 5


@pytest.mark.unit
@pytest.mark.asyncio
async def test_interval_only_change_preserves_schedule():
    """When only the schedule interval changes, existing next_run_time is preserved."""

    source_row = {
        "id": "src-4",
        "name": "Interval",
        "source_type": "rss",
        "url": "http://interval.com/feed",
        "workspace_id": "disruption",
        "schedule_cron": "*/30 * * * *",  # new: 30 min instead of 60
        "tg_channel": None,
        "tg_account_idx": 0,
        "extra": {},
        "proxy_config": {},
    }

    job_id = "source_src-4"
    _source_config_hashes[job_id] = source_config_hash(source_row)  # config unchanged

    future_run = datetime.datetime.now() + datetime.timedelta(minutes=20)
    existing_job = MagicMock()
    existing_job.trigger.interval.total_seconds.return_value = 3600  # old: 60 min
    existing_job.next_run_time = future_run

    scheduler = MagicMock()
    scheduler.get_job.return_value = existing_job
    scheduler.add_job = MagicMock()

    settings = MagicMock()
    redis = MagicMock()

    with patch("ingest.main.load_sources", new=AsyncMock(return_value=[source_row])), \
         patch("ingest.main.cron_to_minutes", return_value=30):  # stub: new schedule = 30min
        await schedule_all(scheduler, redis, None, settings)

    assert scheduler.add_job.called
    kwargs = scheduler.add_job.call_args.kwargs
    # next_run_time should be the original future_run, not now
    assert kwargs["next_run_time"] == future_run


@pytest.mark.unit
def test_build_source_config_preserves_proxy_config_for_http_sources():
    row = {
        "id": "src-proxy",
        "source_type": "rss",
        "url": "https://medium.com/feed/tag/future",
        "workspace_id": "disruption",
        "extra": {"fetch": {"timeout_sec": 45}},
        "proxy_config": {"type": "socks5", "host": "xray", "port": 10808},
    }

    config = build_source_config(row)

    assert config["proxy_config"] == {"type": "socks5", "host": "xray", "port": 10808}
