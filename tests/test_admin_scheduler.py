import asyncio

from admin.backend import scheduler as scheduler_module


class _Settings:
    admin_scheduler_enabled = True
    admin_scheduler_timezone = "UTC"


class _FakeProc:
    def __init__(self, stdout: bytes, stderr: bytes, returncode: int) -> None:
        self._stdout = stdout
        self._stderr = stderr
        self.returncode = returncode

    async def communicate(self):
        return self._stdout, self._stderr


def test_scheduler_status_without_running_scheduler(monkeypatch) -> None:
    monkeypatch.setattr(scheduler_module, "get_settings", lambda: _Settings())
    monkeypatch.setattr(scheduler_module, "_scheduler", None)

    status = scheduler_module.scheduler_status()

    assert status["enabled"] is True
    assert status["timezone"] == "UTC"
    assert status["running"] is False
    assert status["jobs"] == []


def test_run_for_active_workspaces_offloads_each_workspace(monkeypatch) -> None:
    """Heavy scheduled jobs must run one child process per active workspace."""
    calls: list[tuple[str, str | None]] = []

    async def _fake_active():
        return ["alpha", "beta"]

    async def _fake_subprocess(job_name, workspace_id):
        calls.append((job_name, workspace_id))
        return {"status": "ok", "workspace_id": workspace_id, "job_name": job_name}

    monkeypatch.setattr(scheduler_module, "list_active_workspace_ids", _fake_active)
    monkeypatch.setattr(scheduler_module, "_run_job_subprocess", _fake_subprocess)

    async def _run():
        return await scheduler_module._run_for_active_workspaces(
            job_name="run_signal_analysis",
            lock=asyncio.Lock(),
        )

    result = asyncio.run(_run())

    assert result["status"] == "ok"
    assert result["workspace_count"] == 2
    assert calls == [("run_signal_analysis", "alpha"), ("run_signal_analysis", "beta")]
    assert [r["status"] for r in result["results"]] == ["ok", "ok"]


def test_run_for_active_workspaces_isolates_workspace_failure(monkeypatch) -> None:
    """A single workspace failing in its subprocess must not abort the others."""

    async def _fake_active():
        return ["alpha", "beta"]

    async def _fake_subprocess(job_name, workspace_id):
        if workspace_id == "alpha":
            raise RuntimeError("subprocess exploded")
        return {"status": "ok", "workspace_id": workspace_id, "job_name": job_name}

    monkeypatch.setattr(scheduler_module, "list_active_workspace_ids", _fake_active)
    monkeypatch.setattr(scheduler_module, "_run_job_subprocess", _fake_subprocess)

    async def _run():
        return await scheduler_module._run_for_active_workspaces(
            job_name="run_signal_analysis",
            lock=asyncio.Lock(),
        )

    result = asyncio.run(_run())

    assert result["status"] == "ok"
    assert result["workspace_count"] == 2
    assert result["results"][0]["status"] == "error"
    assert result["results"][0]["workspace_id"] == "alpha"
    assert result["results"][1]["status"] == "ok"


def test_run_for_active_workspaces_skips_when_locked(monkeypatch) -> None:
    async def _fake_active():
        raise AssertionError("must not query workspaces when lock is held")

    monkeypatch.setattr(scheduler_module, "list_active_workspace_ids", _fake_active)

    async def _run():
        lock = asyncio.Lock()
        await lock.acquire()
        return await scheduler_module._run_for_active_workspaces(
            job_name="run_signal_analysis",
            lock=lock,
        )

    result = asyncio.run(_run())

    assert result["status"] == "skipped"
    assert result["reason"] == "already_running"


def test_run_job_subprocess_parses_stdout_json(monkeypatch) -> None:
    captured: dict[str, tuple] = {}

    async def _fake_exec(*args, **kwargs):
        captured["argv"] = args
        return _FakeProc(b'{"status": "ok", "semantic_clusters": 5}', b"", 0)

    monkeypatch.setattr(scheduler_module.asyncio, "create_subprocess_exec", _fake_exec)

    result = asyncio.run(
        scheduler_module._run_job_subprocess("run_signal_analysis", "disruption")
    )

    assert result == {"status": "ok", "semantic_clusters": 5}
    # The child is the manual_jobs entrypoint with the job name + workspace id.
    assert "admin.backend.manual_jobs" in captured["argv"]
    assert "run_signal_analysis" in captured["argv"]
    assert "disruption" in captured["argv"]


def test_run_job_subprocess_passes_all_sentinel_for_none(monkeypatch) -> None:
    captured: dict[str, tuple] = {}

    async def _fake_exec(*args, **kwargs):
        captured["argv"] = args
        return _FakeProc(b"{}", b"", 0)

    monkeypatch.setattr(scheduler_module.asyncio, "create_subprocess_exec", _fake_exec)

    asyncio.run(scheduler_module._run_job_subprocess("run_signal_analysis", None))

    assert "__all__" in captured["argv"]


def test_run_job_subprocess_raises_on_nonzero_exit(monkeypatch) -> None:
    async def _fake_exec(*args, **kwargs):
        return _FakeProc(b"", b"fatal: boom traceback", 1)

    monkeypatch.setattr(scheduler_module.asyncio, "create_subprocess_exec", _fake_exec)

    error_message = {}

    async def _run():
        try:
            await scheduler_module._run_job_subprocess("run_signal_analysis", "disruption")
        except RuntimeError as exc:
            error_message["msg"] = str(exc)

    asyncio.run(_run())

    assert "boom traceback" in error_message["msg"]
