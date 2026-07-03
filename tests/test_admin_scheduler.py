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


class _HangingProc:
    """A child whose communicate() never returns until it is terminated/killed."""

    def __init__(self) -> None:
        self.pid = 4242
        self.returncode = None
        self.terminated = False
        self.killed = False
        self._exit = asyncio.Event()

    async def communicate(self):
        # Block forever; asyncio.wait_for will cancel this on timeout.
        await self._exit.wait()
        return b"", b""

    def terminate(self) -> None:
        self.terminated = True
        # Simulate a well-behaved child exiting on SIGTERM.
        self.returncode = -15
        self._exit.set()

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9
        self._exit.set()

    async def wait(self):
        await self._exit.wait()
        return self.returncode


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


def test_job_subprocess_timeout_env_override(monkeypatch) -> None:
    monkeypatch.setenv("ADMIN_JOB_SUBPROCESS_TIMEOUT_SEC", "123")
    assert scheduler_module._job_subprocess_timeout_sec() == 123.0

    # Invalid / non-positive values fall back to the safe default.
    monkeypatch.setenv("ADMIN_JOB_SUBPROCESS_TIMEOUT_SEC", "not-a-number")
    assert (
        scheduler_module._job_subprocess_timeout_sec()
        == scheduler_module._DEFAULT_JOB_SUBPROCESS_TIMEOUT_SEC
    )
    monkeypatch.setenv("ADMIN_JOB_SUBPROCESS_TIMEOUT_SEC", "0")
    assert (
        scheduler_module._job_subprocess_timeout_sec()
        == scheduler_module._DEFAULT_JOB_SUBPROCESS_TIMEOUT_SEC
    )


def test_run_job_subprocess_kills_child_and_raises_on_timeout(monkeypatch) -> None:
    """A hung child must be terminated (or killed) and surface a RuntimeError.

    This is the core reliability guarantee: without it, communicate() would await
    forever, the family asyncio.Lock would stay held, and the job family would be
    blocked permanently.
    """
    proc = _HangingProc()

    async def _fake_exec(*args, **kwargs):
        return proc

    monkeypatch.setattr(scheduler_module.asyncio, "create_subprocess_exec", _fake_exec)
    # Tiny timeout so the test is fast.
    monkeypatch.setenv("ADMIN_JOB_SUBPROCESS_TIMEOUT_SEC", "0.05")

    captured: dict[str, str] = {}

    async def _run():
        try:
            await scheduler_module._run_job_subprocess("run_signal_analysis", "disruption")
        except RuntimeError as exc:
            captured["msg"] = str(exc)

    asyncio.run(_run())

    assert "job_subprocess_timeout" in captured["msg"]
    assert proc.terminated is True  # child was reaped, not left as a zombie
    assert proc.returncode is not None


def test_run_job_subprocess_timeout_releases_family_lock(monkeypatch) -> None:
    """After a timeout the caller's ``async with lock`` must free the lock again.

    Reproduces the original bug's failure mode: run one workspace whose child hangs,
    then a second whose child succeeds, sharing one lock — the second must still run.
    """
    monkeypatch.setenv("ADMIN_JOB_SUBPROCESS_TIMEOUT_SEC", "0.05")

    procs = {"alpha": _HangingProc()}

    async def _fake_active():
        return ["alpha", "beta"]

    async def _fake_exec(*args, **kwargs):
        # args = (python, "-m", module, job_name, workspace_id)
        workspace_id = args[-1]
        if workspace_id == "alpha":
            return procs["alpha"]
        return _FakeProc(b'{"status": "ok", "workspace_id": "beta"}', b"", 0)

    monkeypatch.setattr(scheduler_module, "list_active_workspace_ids", _fake_active)
    monkeypatch.setattr(scheduler_module.asyncio, "create_subprocess_exec", _fake_exec)

    lock = asyncio.Lock()

    async def _run():
        result = await scheduler_module._run_for_active_workspaces(
            job_name="run_signal_analysis",
            lock=lock,
        )
        # Lock must be free once the run returns — the hung child did not wedge it.
        assert lock.locked() is False
        return result

    result = asyncio.run(_run())

    # alpha timed out (error, isolated), beta ran to completion.
    assert result["results"][0]["status"] == "error"
    assert result["results"][0]["workspace_id"] == "alpha"
    assert result["results"][1]["status"] == "ok"
    assert procs["alpha"].terminated is True
