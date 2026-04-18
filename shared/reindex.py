"""Shared Redis stream contract for post reindex events."""
from __future__ import annotations

import json
from typing import Any

STREAM_POSTS_REINDEX = "stream:posts:reindex"
GROUP_POSTS_REINDEX = "reindex_workers"


def build_post_reindex_event(
    *,
    post_id: str,
    workspace_id: str,
    reason: str,
    trace_id: str = "",
    source: str = "",
    extra: dict[str, Any] | None = None,
) -> dict[str, str]:
    payload: dict[str, str] = {
        "post_id": str(post_id),
        "workspace_id": str(workspace_id),
        "reason": str(reason),
    }
    if trace_id:
        payload["trace_id"] = str(trace_id)
    if source:
        payload["source"] = str(source)
    if extra:
        payload["extra"] = json.dumps(extra, ensure_ascii=False)
    return payload
