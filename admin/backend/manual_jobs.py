from __future__ import annotations

import asyncio
import json
import sys

from admin.backend.services.pipeline_jobs import refresh_source_scores
from admin.backend.services.pipeline_jobs import run_semantic_cluster_job
from admin.backend.services.pipeline_jobs import run_signal_analysis_job


async def _dispatch(job_name: str, workspace_id: str | None) -> dict:
    if job_name == "refresh_source_scores":
        return await refresh_source_scores(workspace_id)
    if job_name == "run_semantic_clusters":
        return await run_semantic_cluster_job(workspace_id)
    if job_name == "run_signal_analysis":
        return await run_signal_analysis_job(workspace_id)
    raise ValueError(f"Unsupported manual job: {job_name}")


def main(argv: list[str] | None = None) -> int:
    args = list(argv or sys.argv[1:])
    if len(args) != 2:
        print(
            json.dumps(
                {
                    "status": "error",
                    "error": "usage: python -m admin.backend.manual_jobs <job_name> <workspace_id|__all__>",
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 2

    job_name, raw_workspace_id = args
    workspace_id = None if raw_workspace_id == "__all__" else raw_workspace_id
    try:
        result = asyncio.run(_dispatch(job_name, workspace_id))
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "error",
                    "job_name": job_name,
                    "workspace_id": workspace_id,
                    "error": str(exc),
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 1

    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
