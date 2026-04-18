import base64

from admin.backend.routers.monitoring import _parse_basic_auth_password


def test_parse_basic_auth_password_accepts_alertmanager_user() -> None:
    token = "super-secret-token"
    header = "Basic " + base64.b64encode(
        f"alertmanager:{token}".encode("utf-8")
    ).decode("ascii")
    assert _parse_basic_auth_password(header) == token


def test_parse_basic_auth_password_rejects_other_user() -> None:
    header = "Basic " + base64.b64encode(
        b"someone:super-secret-token"
    ).decode("ascii")
    assert _parse_basic_auth_password(header) is None
