from types import SimpleNamespace
from unittest.mock import AsyncMock

from worker.tasks.vision_task import VisionTask, _classify_vision_error


class _Vision422Error(Exception):
    def __init__(self, message: str = "Model does not support image"):
        super().__init__(message)
        self.status_code = 422
        self.request_id = "req_vision_422"


async def test_vision_task_falls_back_to_ocr_on_422(monkeypatch) -> None:
    task = VisionTask.__new__(VisionTask)
    task.settings = SimpleNamespace(paddleocr_url="http://paddleocr:8008")
    task.redis = SimpleNamespace(xack=AsyncMock(), xadd=AsyncMock())
    task.gigachat = SimpleNamespace(vision=AsyncMock(side_effect=_Vision422Error()))
    task._download_from_s3 = AsyncMock(return_value=b"\xff\xd8\xffjpeg")
    task._upsert_media_object = AsyncMock()
    task._save_enrichment = AsyncMock()
    task._update_vision_status = AsyncMock()
    task._update_media_group_vision = AsyncMock()

    async def _fake_ocr(_base_url: str, _image_bytes: bytes, timeout: float = 120.0) -> str:
        return "detected text"

    monkeypatch.setattr("worker.tasks.vision_task.paddle_ocr_upload", _fake_ocr)

    await task.process_event(
        "1-0",
        {
            "post_id": "post-1",
            "workspace_id": "disruption",
            "source_id": "aiwizards",
            "grouped_id": "",
            "media_s3_keys": ["media/1.jpg"],
        },
    )

    task._save_enrichment.assert_awaited_once()
    save_args = task._save_enrichment.await_args.args
    assert save_args[0] == "post-1"
    assert save_args[1] == "vision"
    payload = save_args[2]
    assert payload["vision_mode"] == "full"
    assert payload["ocr_text"] == "detected text"
    assert payload["items"][0]["paddle_ocr_text"] == "detected text"
    assert payload["items"][0]["vision_error"]["status_code"] == 422
    assert payload["items"][0]["vision_error"]["kind"] == "nonfatal_upstream"
    assert payload["items"][0]["vision_error"]["request_id"] == "req_vision_422"

    task._update_vision_status.assert_awaited_once_with("post-1", "done")
    task.redis.xadd.assert_awaited_once()
    stream_name, reindex_payload = task.redis.xadd.await_args.args
    assert stream_name == "stream:posts:reindex"
    assert reindex_payload["post_id"] == "post-1"
    assert reindex_payload["reason"] == "vision"
    task.redis.xack.assert_awaited_once()


def test_classify_vision_error_marks_422_nonfatal() -> None:
    kind, status_code = _classify_vision_error(_Vision422Error())
    assert kind == "nonfatal_upstream"
    assert status_code == 422


async def test_vision_task_uploads_album_summary_and_marks_group(monkeypatch) -> None:
    task = VisionTask.__new__(VisionTask)
    task.settings = SimpleNamespace(paddleocr_url="")
    task.redis = SimpleNamespace(xack=AsyncMock(), xadd=AsyncMock())
    task.gigachat = SimpleNamespace(vision=AsyncMock(return_value={
        "labels": ["interface", "dashboard"],
        "ocr_text": "headline",
        "scene": "ui",
        "design_signals": [],
    }))
    task._download_from_s3 = AsyncMock(return_value=b"\x89PNG\r\n\x1a\npng")
    task._upsert_media_object = AsyncMock()
    task._save_enrichment = AsyncMock()
    task._update_vision_status = AsyncMock()
    task._update_media_group_vision = AsyncMock()
    task._upload_album_summary = AsyncMock(
        return_value="vision/disruption/albums/album-1/summary.json.gz"
    )

    async def _fake_ocr(_base_url: str, _image_bytes: bytes, timeout: float = 120.0) -> str:
        return ""

    monkeypatch.setattr("worker.tasks.vision_task.paddle_ocr_upload", _fake_ocr)

    await task.process_event(
        "1-0",
        {
            "post_id": "post-2",
            "workspace_id": "disruption",
            "source_id": "aiwizards",
            "grouped_id": "album-1",
            "media_s3_keys": ["media/album.png"],
        },
    )

    task._upload_album_summary.assert_awaited_once()
    _, kwargs = task._update_media_group_vision.await_args
    assert kwargs["workspace_id"] == "disruption"
    assert kwargs["source_id"] == "aiwizards"
    assert kwargs["grouped_id"] == "album-1"
    assert sorted(kwargs["all_labels"]) == ["dashboard", "interface"]
    assert kwargs["all_ocr"] == "headline"
    assert kwargs["summary_s3_key"] == "vision/disruption/albums/album-1/summary.json.gz"
    task.redis.xadd.assert_awaited_once()


async def test_vision_task_requeues_failed_event_before_dlq_threshold() -> None:
    task = VisionTask.__new__(VisionTask)
    task.settings = SimpleNamespace(
        vision_max_delivery_count=3,
        vision_dlq_stream="stream:posts:vision:dlq",
    )
    task.redis = SimpleNamespace(xack=AsyncMock(), xadd=AsyncMock())

    await task._handle_failed_event(
        "1-0",
        {"post_id": "post-1", "media_s3_keys": "[]", "delivery_count": "1"},
        RuntimeError("boom"),
    )

    task.redis.xack.assert_awaited_once_with("stream:posts:vision", "vision_workers", "1-0")
    task.redis.xadd.assert_awaited_once()
    stream_name, payload = task.redis.xadd.await_args.args
    assert stream_name == "stream:posts:vision"
    assert payload["delivery_count"] == "2"


async def test_vision_task_moves_failed_event_to_dlq_after_threshold() -> None:
    task = VisionTask.__new__(VisionTask)
    task.settings = SimpleNamespace(
        vision_max_delivery_count=2,
        vision_dlq_stream="stream:posts:vision:dlq",
    )
    task.redis = SimpleNamespace(xack=AsyncMock(), xadd=AsyncMock())
    task._update_vision_status = AsyncMock()

    await task._handle_failed_event(
        "2-0",
        {"post_id": "post-2", "media_s3_keys": "[]", "delivery_count": "1"},
        RuntimeError("fatal"),
    )

    task.redis.xack.assert_awaited_once_with("stream:posts:vision", "vision_workers", "2-0")
    task._update_vision_status.assert_awaited_once_with("post-2", "error")
    stream_name, payload = task.redis.xadd.await_args.args
    assert stream_name == "stream:posts:vision:dlq"
    assert payload["post_id"] == "post-2"
    assert payload["delivery_count"] == "2"
    assert payload["stream"] == "stream:posts:vision"
    assert payload["group"] == "vision_workers"


async def test_vision_task_startup_reclaim_continues_after_empty_batch_with_live_cursor() -> None:
    task = VisionTask.__new__(VisionTask)
    task.settings = SimpleNamespace(vision_claim_idle_ms=600_000)
    task.redis = SimpleNamespace(
        xautoclaim=AsyncMock(side_effect=[
            ("1700000000000-0", []),
            ("0-0", [("2-0", {"post_id": "post-2", "media_s3_keys": '["m.jpg"]'})]),
        ])
    )
    task.process_event = AsyncMock()
    task._handle_failed_event = AsyncMock()

    await task._startup_reclaim()

    assert task.redis.xautoclaim.await_count == 2
    task.process_event.assert_awaited_once_with(
        "2-0",
        {"post_id": "post-2", "media_s3_keys": '["m.jpg"]'},
    )


async def test_vision_task_cleanup_dead_consumers_removes_idle_zero_pending_only() -> None:
    task = VisionTask.__new__(VisionTask)
    task.redis = SimpleNamespace(
        xinfo_consumers=AsyncMock(return_value=[
            {"name": "vision-dead", "idle": 3_700_000, "pending": 0},
            {"name": "vision-busy", "idle": 7_200_000, "pending": 2},
        ]),
        xdel_consumer=AsyncMock(),
    )

    await task._cleanup_dead_consumers()

    task.redis.xdel_consumer.assert_awaited_once_with(
        "stream:posts:vision",
        "vision_workers",
        "vision-dead",
    )
