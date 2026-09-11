"""Contract tests for the standalone host-side ntfy publisher."""

import importlib.util
import json
import tempfile
import threading
import unittest
from unittest.mock import patch
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "notify_ntfy.py"


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers["Content-Length"])
        self.server.received.append(
            (self.path, dict(self.headers), self.rfile.read(length))
        )
        self.send_response(self.server.status)
        if self.server.status == 302:
            self.send_header("Location", self.server.location)
        self.end_headers()
        self.wfile.write(self.server.response)

    def log_message(self, *args):
        pass


class NotifyNtfyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location("frontier_notify_ntfy", SCRIPT)
        cls.notify = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.notify)
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.credential = Path(self.temp.name) / "credential"
        self.credential.write_text("unit-test-only\n", encoding="utf-8")
        self.url = f"http://127.0.0.1:{self.server.server_port}/frontier-alerts"
        self.server.status = 200
        self.server.location = self.url + "/redirect-target"
        self.server.response = json.dumps(
            {"id": "receipt", "event": "message", "topic": "frontier-alerts"}
        ).encode()
        self.server.received = []

    def publish(self, message="Контур восстановлен 🟢"):
        self.notify._publish(
            self.url, self.credential, message, timeout=2, _allow_http_loopback=True
        )

    def test_production_publisher_rejects_http_loopback_before_network(self):
        with self.assertRaises(self.notify.DeliveryError):
            self.notify.publish(self.url, self.credential, "test", timeout=2)
        self.assertEqual(self.server.received, [])

    def test_cli_rejects_http_loopback_even_with_environment_permission(self):
        with patch.dict(
            self.notify.os.environ,
            {
                "NTFY_URL": self.url,
                "NTFY_CREDENTIAL_FILE": str(self.credential),
                "NTFY_ALLOW_HTTP_LOOPBACK": "true",
            },
        ), patch.object(self.notify.sys, "argv", [str(SCRIPT), "test"]):
            self.assertEqual(self.notify.main(), 1)
        self.assertEqual(self.server.received, [])

    def test_private_publisher_requires_explicit_http_loopback_permission(self):
        with self.assertRaises(self.notify.DeliveryError):
            self.notify._publish(self.url, self.credential, "test", timeout=2)
        self.assertEqual(self.server.received, [])
        self.notify._publish(
            self.url, self.credential, "test", timeout=2, _allow_http_loopback=True
        )
        self.assertEqual(len(self.server.received), 1)

    def test_publish_requires_matching_receipt_and_sends_watchdog_headers(self):
        self.publish()
        path, headers, body = self.server.received[0]
        self.assertEqual(path, "/frontier-alerts")
        self.assertEqual(body.decode("utf-8"), "Контур восстановлен 🟢")
        self.assertEqual(headers["Authorization"], "Bearer unit-test-only")
        self.assertEqual(headers["Content-Type"], "text/plain; charset=utf-8")
        self.assertEqual(headers["Title"], "Frontier watchdog")
        self.assertEqual(headers["Priority"], "high")
        self.assertIn("warning", headers["Tags"])

    def test_rejects_http_errors_redirects_and_false_receipts(self):
        for status in (201, 302, 401, 403, 429, 500, 503):
            with self.subTest(status=status):
                self.server.status = status
                with self.assertRaises(self.notify.DeliveryError):
                    self.publish()
        self.server.status = 200
        for payload in (
            b"<html>ok</html>",
            b"{}",
            b"[]",
            b'{"id":"   ","event":"message","topic":"frontier-alerts"}',
            b'{"id":"x","event":"message","topic":"other"}',
        ):
            with self.subTest(payload=payload):
                self.server.response = payload
                with self.assertRaises(self.notify.DeliveryError):
                    self.publish()

    def test_rejects_bad_credentials_before_network(self):
        for contents in ("", "two\nlines", "bad\rheader"):
            with self.subTest(contents=contents):
                self.credential.write_text(contents, encoding="utf-8")
                with self.assertRaises(self.notify.DeliveryError):
                    self.publish()
        self.assertEqual(self.server.received, [])

    def test_message_limit_is_utf8_bytes_and_strictly_below_ntfy_4096(self):
        # ntfy 2.28.0 считает тело ровно 4096 байт вложением → 400 без attachment-cache.
        self.assertLess(self.notify.MAX_MESSAGE_BYTES, 4096)
        self.publish("я" * (self.notify.MAX_MESSAGE_BYTES // 2))
        with self.assertRaises(self.notify.DeliveryError):
            self.publish("")
        for chars in (self.notify.MAX_MESSAGE_BYTES // 2 + 1, 2048, 2049):
            with self.subTest(chars=chars), self.assertRaises(self.notify.DeliveryError):
                self.publish("я" * chars)

    def test_url_requires_https_and_exactly_one_safe_topic(self):
        invalid = (
            "http://example.net/topic",
            "https://u:p@example.net/topic",
            "https://example.net/",
            "https://example.net/topic/extra",
            "https://example.net/topic?x=1",
            "https://example.net/topic?",
            "https://example.net/topic#x",
            "https://example.net/topic#",
            "https://@example.net/topic",
            "https://:@example.net/topic",
            "https://example.net:invalid/topic",
            "https://example.net:65536/topic",
            "https://example.net:0/topic",
            "https://example.net:443/topic",
            "https://example.net:8443/topic",
            "https://example.net:/topic",
            " https://example.net/topic",
            "https://example.net/topic\n",
            "https://example.net/frontier.alerts",
            "https://example.net/" + "a" * 65,
            "http://localhost/topic",
            "http://[::1]/topic",
            "https://example.net/admin",
        )
        for url in invalid:
            with self.subTest(url=url), self.assertRaises(self.notify.DeliveryError):
                self.notify.validate_url(url)


if __name__ == "__main__":
    unittest.main()
