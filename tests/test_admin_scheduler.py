from admin.backend import scheduler as scheduler_module


class _Settings:
    admin_scheduler_enabled = True
    admin_scheduler_timezone = "UTC"


def test_scheduler_status_without_running_scheduler(monkeypatch) -> None:
    monkeypatch.setattr(scheduler_module, "get_settings", lambda: _Settings())
    monkeypatch.setattr(scheduler_module, "_scheduler", None)

    status = scheduler_module.scheduler_status()

    assert status["enabled"] is True
    assert status["timezone"] == "UTC"
    assert status["running"] is False
    assert status["jobs"] == []
