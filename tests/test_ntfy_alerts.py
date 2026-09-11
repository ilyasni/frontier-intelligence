from __future__ import annotations

import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest

from admin.backend.services import ntfy_alerts


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        length = int(self.headers["Content-Length"])
        self.server.received.append(  # type: ignore[attr-defined]
            (self.path, dict(self.headers), self.rfile.read(length))
        )
        self.send_response(self.server.status)  # type: ignore[attr-defined]
        if self.server.status == 302:  # type: ignore[attr-defined]
            self.send_header("Location", "/redirect-target")
        self.end_headers()
        self.wfile.write(self.server.response)  # type: ignore[attr-defined]

    def log_message(self, *_args: object) -> None:
        return


@pytest.fixture
def ntfy_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    server.status = 200
    server.response = json.dumps(
        {"id": "receipt-1", "event": "message", "topic": "frontier-alerts"}
    ).encode("utf-8")
    server.received = []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def _configure(monkeypatch, tmp_path, url: str):
    credential_file = tmp_path / "ntfy-publisher"
    credential_file.write_text("unit-test-credential\n", encoding="utf-8")
    settings = SimpleNamespace(
        ntfy_url=url,
        ntfy_credential_file=str(credential_file),
    )
    monkeypatch.setattr(ntfy_alerts, "get_settings", lambda: settings)
    return credential_file


async def _send_loopback_message(text: str, **kwargs) -> bool:
    return await ntfy_alerts._send_ntfy_alert_message(
        text, _allow_http_loopback=True, **kwargs
    )


def test_production_publisher_rejects_http_loopback_before_network(
    monkeypatch, tmp_path, ntfy_server
) -> None:
    url = f"http://127.0.0.1:{ntfy_server.server_port}/frontier-alerts"
    _configure(monkeypatch, tmp_path, url)
    monkeypatch.setenv("NTFY_ALLOW_HTTP_LOOPBACK", "true")
    ntfy_alerts.get_settings().ntfy_allow_http_loopback = True

    with pytest.raises(ntfy_alerts.NtfyDeliveryError, match="HTTPS"):
        asyncio.run(ntfy_alerts.send_ntfy_alert_message("health failed"))

    assert ntfy_server.received == []


def test_private_publisher_requires_explicit_http_loopback_permission(
    monkeypatch, tmp_path, ntfy_server
) -> None:
    url = f"http://127.0.0.1:{ntfy_server.server_port}/frontier-alerts"
    _configure(monkeypatch, tmp_path, url)

    with pytest.raises(ntfy_alerts.NtfyDeliveryError, match="HTTPS"):
        asyncio.run(ntfy_alerts._send_ntfy_alert_message("test"))
    assert ntfy_server.received == []
    assert asyncio.run(
        ntfy_alerts._send_ntfy_alert_message("test", _allow_http_loopback=True)
    ) is True
    assert len(ntfy_server.received) == 1


@pytest.mark.parametrize(
    "url",
    [
        "https://ntfy.example/frontier-alerts?",
        "https://ntfy.example/frontier-alerts#",
        "https://@ntfy.example/frontier-alerts",
        "https://:@ntfy.example/frontier-alerts",
        "https://ntfy.example:invalid/frontier-alerts",
        "https://ntfy.example:65536/frontier-alerts",
        "https://ntfy.example:0/frontier-alerts",
        "https://ntfy.example:443/frontier-alerts",
        "https://ntfy.example:8443/frontier-alerts",
        "https://ntfy.example:/frontier-alerts",
        " https://ntfy.example/frontier-alerts",
        "https://ntfy.example/frontier-alerts\n",
        "https://ntfy.example/frontier.alerts",
        "https://ntfy.example/" + "a" * 65,
        "http://localhost/frontier-alerts",
        "http://[::1]/frontier-alerts",
    ],
)
def test_validator_rejects_unsafe_https_origins_and_topics(url: str) -> None:
    with pytest.raises(ntfy_alerts.NtfyDeliveryError):
        ntfy_alerts._validate_url(url)


def test_publisher_delivers_utf8_plain_text_and_requires_matching_receipt(
    monkeypatch, tmp_path, ntfy_server
) -> None:
    url = f"http://127.0.0.1:{ntfy_server.server_port}/frontier-alerts"
    _configure(monkeypatch, tmp_path, url)

    delivered = asyncio.run(
        _send_loopback_message(
            "Сервис восстановлен 🟢",
            title="Frontier health",
            priority="high",
            tags="white_check_mark,frontier",
        )
    )

    assert delivered is True
    path, headers, body = ntfy_server.received[0]
    assert path == "/frontier-alerts"
    assert body.decode("utf-8") == "Сервис восстановлен 🟢"
    assert headers["Authorization"] == "Bearer unit-test-credential"
    assert headers["Content-Type"] == "text/plain; charset=utf-8"
    assert headers["Title"] == "Frontier health"
    assert headers["Priority"] == "high"
    assert headers["Tags"] == "white_check_mark,frontier"


@pytest.mark.parametrize("status", [201, 302, 401, 403, 429, 500, 503])
def test_publisher_rejects_non_200_and_never_follows_redirects(
    monkeypatch, tmp_path, ntfy_server, status: int
) -> None:
    url = f"http://127.0.0.1:{ntfy_server.server_port}/frontier-alerts"
    _configure(monkeypatch, tmp_path, url)
    ntfy_server.status = status

    with pytest.raises(ntfy_alerts.NtfyDeliveryError):
        asyncio.run(_send_loopback_message("health failed"))

    assert len(ntfy_server.received) == 1


@pytest.mark.parametrize(
    "receipt",
    [
        b"<html>ok</html>",
        b"{}",
        b"[]",
        b'{"id":"x","event":"message","topic":"other-topic"}',
        b'{"id":"","event":"message","topic":"frontier-alerts"}',
        b'{"id":"x","event":"open","topic":"frontier-alerts"}',
    ],
)
def test_publisher_rejects_false_receipts(
    monkeypatch, tmp_path, ntfy_server, receipt: bytes
) -> None:
    url = f"http://127.0.0.1:{ntfy_server.server_port}/frontier-alerts"
    _configure(monkeypatch, tmp_path, url)
    ntfy_server.response = receipt

    with pytest.raises(ntfy_alerts.NtfyDeliveryError):
        asyncio.run(_send_loopback_message("health failed"))


@pytest.mark.parametrize(
    "url_template",
    [
        "http://user:password@127.0.0.1:{port}/frontier-alerts",
        "http://127.0.0.1:{port}/",
        "http://127.0.0.1:{port}/frontier-alerts/extra",
        "http://127.0.0.1:{port}/frontier-alerts?token=secret",
        "http://127.0.0.1:{port}/frontier-alerts?",
        "http://127.0.0.1:{port}/frontier-alerts#fragment",
        "http://127.0.0.1:{port}/frontier-alerts#",
        "http://127.0.0.1:{port}/admin",
        "http://127.0.0.1:{port}/frontier alerts",
        "http://127.0.0.1:0/frontier-alerts",
    ],
)
def test_publisher_rejects_unsafe_urls_before_any_loopback_request(
    monkeypatch, tmp_path, ntfy_server, url_template: str
) -> None:
    url = url_template.format(port=ntfy_server.server_port)
    _configure(monkeypatch, tmp_path, url)

    with pytest.raises(ntfy_alerts.NtfyDeliveryError):
        asyncio.run(_send_loopback_message("health failed"))

    assert ntfy_server.received == []


def test_plain_http_nonapproved_host_is_rejected_before_http_client(
    monkeypatch, tmp_path, ntfy_server
) -> None:
    class _UnexpectedAsyncClient:
        def __init__(self, *_args, **_kwargs) -> None:
            raise AssertionError("HTTP client must not be constructed for an unsafe URL")

    monkeypatch.setattr(ntfy_alerts.httpx, "AsyncClient", _UnexpectedAsyncClient)
    url = f"http://127.0.0.2:{ntfy_server.server_port}/frontier-alerts"
    _configure(monkeypatch, tmp_path, url)

    with pytest.raises(ntfy_alerts.NtfyDeliveryError):
        asyncio.run(_send_loopback_message("health failed"))

    assert ntfy_server.received == []


@pytest.mark.parametrize("credential", ["", "two\nlines", "bad header", "bad\rheader"])
def test_publisher_rejects_invalid_credential_before_network(
    monkeypatch, tmp_path, ntfy_server, credential: str
) -> None:
    url = f"http://127.0.0.1:{ntfy_server.server_port}/frontier-alerts"
    credential_file = _configure(monkeypatch, tmp_path, url)
    credential_file.write_text(credential, encoding="utf-8")

    with pytest.raises(ntfy_alerts.NtfyDeliveryError):
        asyncio.run(_send_loopback_message("health failed"))

    assert ntfy_server.received == []


@pytest.mark.parametrize("message", ["", "я" * 1901, "я" * 2048, "я" * 2049])
def test_publisher_rejects_messages_outside_utf8_byte_limit(
    monkeypatch, tmp_path, ntfy_server, message: str
) -> None:
    url = f"http://127.0.0.1:{ntfy_server.server_port}/frontier-alerts"
    _configure(monkeypatch, tmp_path, url)

    with pytest.raises(ntfy_alerts.NtfyDeliveryError):
        asyncio.run(_send_loopback_message(message))

    assert ntfy_server.received == []


def test_message_cap_is_strictly_below_ntfy_server_limit() -> None:
    # ntfy 2.28.0: util.Peek(body, message-size-limit) → LimitReached при read == limit,
    # т.е. тело РОВНО 4096 байт уже вложение; без attachment-cache это HTTP 400 (40014).
    # Замерено 2026-09-11: 4096 → 400, 4095 → 200. Кэп обязан оставаться строго ниже.
    assert ntfy_alerts._NTFY_SERVER_MESSAGE_SIZE_LIMIT == 4096
    assert ntfy_alerts._MAX_MESSAGE_BYTES < ntfy_alerts._NTFY_SERVER_MESSAGE_SIZE_LIMIT
    assert ntfy_alerts._MAX_MESSAGE_BYTES <= 3800


def test_publisher_accepts_message_at_the_cap(monkeypatch, tmp_path, ntfy_server) -> None:
    url = f"http://127.0.0.1:{ntfy_server.server_port}/frontier-alerts"
    _configure(monkeypatch, tmp_path, url)
    message = "я" * (ntfy_alerts._MAX_MESSAGE_BYTES // 2)
    assert len(message.encode("utf-8")) == ntfy_alerts._MAX_MESSAGE_BYTES

    assert asyncio.run(_send_loopback_message(message)) is True
    assert len(ntfy_server.received[0][2]) == ntfy_alerts._MAX_MESSAGE_BYTES


def test_default_truncation_of_oversized_digest_stays_below_server_limit() -> None:
    # Реальный кейс 11.09.2026: дайджест 5595 байт с кириллицей и эмодзи, суффикс
    # усечения как в scripts/alert-triage-deliver.sh.
    suffix = "\n\n… (обрезано; полный разбор: docs/ops/alert-digests/2026-09-11.md на сервере)"
    digest = "🔎 Frontier alert-triage 2026-09-11 (UTC)\n\n" + "Превышение порога 🟠 — причина\n" * 200
    assert len(digest.encode("utf-8")) > ntfy_alerts._NTFY_SERVER_MESSAGE_SIZE_LIMIT

    result = ntfy_alerts.truncate_ntfy_message(digest, suffix=suffix)

    encoded = result.encode("utf-8")
    assert len(encoded) <= ntfy_alerts._MAX_MESSAGE_BYTES
    assert len(encoded) < ntfy_alerts._NTFY_SERVER_MESSAGE_SIZE_LIMIT
    assert result.endswith(suffix)
    assert result.startswith("🔎 Frontier alert-triage")
    assert encoded.decode("utf-8") == result  # ни одного разрезанного code point


def test_rejection_error_carries_ntfy_json_code_and_error_only(
    monkeypatch, tmp_path, ntfy_server, caplog
) -> None:
    url = f"http://127.0.0.1:{ntfy_server.server_port}/frontier-alerts"
    credential_file = _configure(monkeypatch, tmp_path, url)
    credential_file.write_text("credential-must-stay-secret", encoding="utf-8")
    ntfy_server.status = 400
    ntfy_server.response = json.dumps(
        {
            "code": 40014,
            "http": 400,
            "error": "invalid request: attachments not allowed",
            "link": "https://ntfy.sh/docs/config/#attachments",
        }
    ).encode("utf-8")

    with caplog.at_level("WARNING", logger="admin.backend.services.ntfy_alerts"):
        with pytest.raises(ntfy_alerts.NtfyDeliveryError) as exc_info:
            asyncio.run(_send_loopback_message("message-body-must-stay-secret"))

    error = str(exc_info.value)
    assert "HTTP 400 (code 40014: invalid request: attachments not allowed)" in error
    assert "link" not in error and "ntfy.sh/docs" not in error
    assert "credential-must-stay-secret" not in error
    assert "message-body-must-stay-secret" not in error
    assert "code 40014" in caplog.text
    assert "credential-must-stay-secret" not in caplog.text
    assert "message-body-must-stay-secret" not in caplog.text


@pytest.mark.parametrize(
    "raw",
    [
        b"not json",
        b"[]",
        b'{"code":"40014","error":"str code"}',
        b'{"code":true,"error":"bool code"}',
        b'{"code":40014,"error":123}',
        b'{"http":400}',
    ],
)
def test_describe_error_body_ignores_unstructured_bodies(raw: bytes) -> None:
    assert ntfy_alerts._describe_error_body(raw) == ""


def test_describe_error_body_bounds_and_flattens_error_text() -> None:
    raw = json.dumps({"code": 40014, "error": "a\n b" + "x" * 500}).encode("utf-8")
    described = ntfy_alerts._describe_error_body(raw)
    assert described.startswith(" (code 40014: a b")
    assert len(described) <= len(" (code 40014: ") + 200 + 1


def test_delivery_errors_do_not_expose_credential_message_or_response_body(
    monkeypatch, tmp_path, ntfy_server
) -> None:
    url = f"http://127.0.0.1:{ntfy_server.server_port}/frontier-alerts"
    credential_file = _configure(monkeypatch, tmp_path, url)
    credential_file.write_text("credential-must-stay-secret", encoding="utf-8")
    ntfy_server.status = 500
    ntfy_server.response = b"server-body-must-stay-secret"

    with pytest.raises(ntfy_alerts.NtfyDeliveryError) as exc_info:
        asyncio.run(
            _send_loopback_message("message-body-must-stay-secret")
        )

    error = str(exc_info.value)
    assert "credential-must-stay-secret" not in error
    assert "message-body-must-stay-secret" not in error
    assert "server-body-must-stay-secret" not in error


def test_enabled_requires_both_url_and_credential_file(monkeypatch) -> None:
    for url, credential_file, expected in (
        ("", "", False),
        ("https://ntfy.example/frontier-alerts", "", False),
        ("", "/run/credentials/ntfy-publisher", False),
        (
            "https://ntfy.example/frontier-alerts",
            "/run/credentials/ntfy-publisher",
            True,
        ),
    ):
        monkeypatch.setattr(
            ntfy_alerts,
            "get_settings",
            lambda url=url, credential_file=credential_file: SimpleNamespace(
                ntfy_url=url,
                ntfy_credential_file=credential_file,
            ),
        )
        assert ntfy_alerts.ntfy_alerts_enabled() is expected


def test_truncate_ntfy_message_is_utf8_safe_and_preserves_suffix() -> None:
    suffix = "\n… ещё"
    result = ntfy_alerts.truncate_ntfy_message(
        "Привет 🟢" * 20,
        suffix=suffix,
        max_bytes=31,
    )

    assert result.endswith(suffix)
    assert len(result.encode("utf-8")) <= 31
    assert result.encode("utf-8").decode("utf-8") == result


def test_truncate_ntfy_message_leaves_short_text_unchanged() -> None:
    assert ntfy_alerts.truncate_ntfy_message("коротко", suffix="…") == "коротко"


@pytest.mark.parametrize(
    ("suffix", "max_bytes"),
    [("окончание", 5), ("x", 0)],
)
def test_truncate_ntfy_message_rejects_suffix_that_cannot_fit(
    suffix: str,
    max_bytes: int,
) -> None:
    with pytest.raises(ValueError, match="suffix"):
        ntfy_alerts.truncate_ntfy_message(
            "длинное сообщение",
            suffix=suffix,
            max_bytes=max_bytes,
        )
