from admin.backend import db as admin_db


class _Settings:
    database_url = "postgresql+asyncpg://frontier:secret@postgres:5432/frontier"


def test_get_engine_reuses_singleton(monkeypatch) -> None:
    created = []

    def _fake_create_async_engine(url: str, **kwargs):
        created.append((url, kwargs))
        return object()

    monkeypatch.setattr(admin_db, "_engine", None)
    monkeypatch.setattr(admin_db, "get_settings", lambda: _Settings())
    monkeypatch.setattr(admin_db, "create_async_engine", _fake_create_async_engine)

    first = admin_db.get_engine()
    second = admin_db.get_engine()

    assert first is second
    assert len(created) == 1
    assert created[0][1]["pool_size"] == 3
    assert created[0][1]["max_overflow"] == 2
