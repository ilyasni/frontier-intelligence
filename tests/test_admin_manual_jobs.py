import asyncio

from admin.backend import scheduler as scheduler_module


class _FakeResult:
    def __init__(self, row=None, rows=None) -> None:
        self._row = row
        self._rows = rows or []

    def mappings(self):
        return self

    def first(self):
        return self._row

    def all(self):
        return self._rows


class _FakeSession:
    def __init__(self, responses: list[_FakeResult], statements: list[tuple[str, dict]]) -> None:
        self._responses = responses
        self._statements = statements

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def execute(self, statement, params=None):
        self._statements.append((str(statement), params or {}))
        if self._responses:
            return self._responses.pop(0)
        return _FakeResult()

    async def commit(self):
        return None


def test_launch_manual_job_persists_queued_row(monkeypatch) -> None:
    statements: list[tuple[str, dict]] = []
    responses = [_FakeResult(row=None)]

    async def _noop():
        return None

    def _fake_async_session(_engine):
        return _FakeSession(responses, statements)

    def _fake_create_task(coro):
        coro.close()
        return None

    monkeypatch.setattr(scheduler_module, "ensure_manual_jobs_table", _noop)
    monkeypatch.setattr(scheduler_module, "get_engine", lambda: object())
    monkeypatch.setattr(scheduler_module, "AsyncSession", _fake_async_session)
    monkeypatch.setattr(scheduler_module.asyncio, "create_task", _fake_create_task)

    payload = asyncio.run(
        scheduler_module.launch_manual_job(
            job_name="run_signal_analysis",
            workspace_id="disruption",
            runner=None,
        )
    )

    assert payload["job_name"] == "run_signal_analysis"
    assert payload["workspace_id"] == "disruption"
    assert payload["status"] == "queued"
    assert any("INSERT INTO admin_manual_jobs" in statement for statement, _ in statements)


def test_get_manual_job_serializes_db_row(monkeypatch) -> None:
    row = {
        "id": "manual:test",
        "job_name": "run_signal_analysis",
        "workspace_id": "disruption",
        "status": "success",
        "created_at": scheduler_module._utcnow(),
        "started_at": scheduler_module._utcnow(),
        "finished_at": scheduler_module._utcnow(),
        "trigger": "manual",
        "summary": {"missing_signals": 3},
        "error": None,
        "result": {"status": "ok"},
    }

    async def _noop():
        return None

    def _fake_async_session(_engine):
        return _FakeSession([_FakeResult(row=row)], [])

    monkeypatch.setattr(scheduler_module, "ensure_manual_jobs_table", _noop)
    monkeypatch.setattr(scheduler_module, "get_engine", lambda: object())
    monkeypatch.setattr(scheduler_module, "AsyncSession", _fake_async_session)

    payload = asyncio.run(scheduler_module.get_manual_job("manual:test"))

    assert payload is not None
    assert payload["id"] == "manual:test"
    assert payload["summary"]["missing_signals"] == 3
