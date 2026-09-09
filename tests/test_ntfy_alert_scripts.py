"""Behavioral tests for the host alert scripts' ntfy delivery boundary."""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
GIT_BASH = Path(r"C:\Program Files\Git\bin\bash.exe")
BASH = str(GIT_BASH) if GIT_BASH.exists() else shutil.which("bash")


def executable(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8", newline="\n")
    path.chmod(0o755)


def bash_path(path: Path) -> str:
    resolved = path.resolve().as_posix()
    return f"/{resolved[0].lower()}{resolved[2:]}"


def install_fake_admin(repo: Path) -> None:
    services = repo / "admin" / "backend" / "services"
    services.mkdir(parents=True)
    for package in (repo / "admin", repo / "admin" / "backend", services):
        (package / "__init__.py").write_text("", encoding="utf-8")
    (services / "ntfy_alerts.py").write_text(
        """import os
from pathlib import Path


def truncate_ntfy_message(text, suffix=''):
    raw = text.encode('utf-8')
    if len(raw) <= 4096:
        return text
    budget = 4096 - len(suffix.encode('utf-8'))
    head = raw[:budget]
    while True:
        try:
            return head.decode('utf-8') + suffix
        except UnicodeDecodeError:
            head = head[:-1]


async def send_ntfy_alert_message(text):
    raw = text.encode('utf-8')
    if len(raw) > 4096:
        raise ValueError('message exceeds ntfy boundary')
    if os.environ.get('FAKE_SENDER_OK') != '1':
        print('UNSENT diagnostic contains SENT')
        return False
    Path(os.environ['DELIVERED_FILE']).write_bytes(raw)
    return True
""",
        encoding="utf-8",
    )


def install_loopback_watchdog_publisher(repo: Path) -> None:
    """Запустить настоящий CLI с явно включённым private transport seam."""
    executable(
        repo / "scripts" / "notify_ntfy.py",
        "import importlib.util\n"
        "import sys\n"
        "from functools import partial\n"
        "spec = importlib.util.spec_from_file_location(\n"
        f"    'real_notify_ntfy', {str(ROOT / 'scripts' / 'notify_ntfy.py')!r})\n"
        "publisher = importlib.util.module_from_spec(spec)\n"
        "spec.loader.exec_module(publisher)\n"
        "publisher.publish = partial(publisher._publish, _allow_http_loopback=True)\n"
        "sys.exit(publisher.main())\n",
    )


def install_fake_docker(fake_bin: Path, *, allow_http_loopback: bool = False) -> None:
    command = 'exec "$REAL_PYTHON" -c "$1" "$2"\n'
    if allow_http_loopback:
        # Меняем только точку входа тестового процесса; реальный publisher
        # проверяет credential, выполняет HTTP-запрос и валидирует receipt.
        executable(
            fake_bin / "docker-python.py",
            "import sys\n"
            "from functools import partial\n"
            "from admin.backend.services import ntfy_alerts\n"
            "ntfy_alerts.send_ntfy_alert_message = partial(\n"
            "    ntfy_alerts._send_ntfy_alert_message, _allow_http_loopback=True)\n"
            "code = sys.argv.pop(1)\n"
            "sys.argv[0] = '-c'\n"
            "exec(compile(code, '<string>', 'exec'))\n",
        )
        command = 'exec "$REAL_PYTHON" "$(dirname "$0")/docker-python.py" "$1" "$2"\n'
    executable(
        fake_bin / "docker",
        "#!/usr/bin/env bash\n"
        "shift 6\n"
        + command,
    )


class WatchdogHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/prom/-/healthy":
            self.send_response(200 if self.server.prom_healthy else 503)
            self.end_headers()
            return
        if self.path == "/am/-/healthy":
            self.send_response(200)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        if self.path.startswith("/am/api/v2/alerts"):
            payload = [{"labels": {"alertname": "FrontierWatchdog"}}]
        else:
            payload = {"data": {"result": []}}
        self.wfile.write(json.dumps(payload).encode())

    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        self.server.received.append((dict(self.headers), body))
        self.send_response(self.server.ntfy_status)
        self.end_headers()
        receipt = {
            "id": "watchdog-test",
            "event": "message",
            "topic": "frontier-alerts",
        }
        self.wfile.write(json.dumps(receipt).encode())

    def log_message(self, *args):
        pass


@pytest.mark.skipif(BASH is None, reason="bash is unavailable")
def test_watchdog_uses_only_ntfy_delivery_settings_and_preserves_retry_state(tmp_path):
    with tempfile.TemporaryDirectory(dir=tmp_path) as raw_temp:
        temp = Path(raw_temp)
        root = temp / "repo"
        fake_bin = temp / "bin"
        root.mkdir()
        fake_bin.mkdir()
        executable(
            fake_bin / "python3",
            "#!/usr/bin/env bash\nexec \"$REAL_PYTHON\" \"$@\"\n",
        )
        credential = temp / "publisher.cred"
        credential.write_text("do-not-log-this-token", encoding="utf-8")
        (root / "scripts").mkdir()
        install_loopback_watchdog_publisher(root)
        server = ThreadingHTTPServer(("127.0.0.1", 0), WatchdogHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = server.server_port
            (root / ".env").write_text(
                f"NTFY_URL=http://127.0.0.1:{port}/frontier-alerts\n"
                "NTFY_WATCHDOG_CREDENTIAL_FILE=publisher.cred\n"
                "UNRELATED_SECRET=must-not-be-sourced\n",
                encoding="utf-8",
            )
            server.prom_healthy = False
            server.ntfy_status = 200
            server.received = []
            env = os.environ.copy()
            env.update(
                ROOT_DIR="repo",
                STATE_FILE="repo/runtime/watchdog-state",
                TEXTFILE_DIR="repo/prometheus/textfile",
                PROM=f"http://127.0.0.1:{port}/prom",
                ALERTMANAGER=f"http://127.0.0.1:{port}/am",
                REAL_PYTHON=bash_path(Path(sys.executable)),
                PATH=str(fake_bin) + os.pathsep + env["PATH"],
            )

            def run_watchdog():
                return subprocess.run(
                    [BASH, str(ROOT / "scripts" / "alert-watchdog.sh")],
                    cwd=temp,
                    env=env,
                    capture_output=True,
                    encoding="utf-8",
                    errors="replace",
                )

            broken = run_watchdog()
            assert broken.returncode == 1
            assert len(server.received) == 1, (broken.stdout, broken.stderr)
            headers, body = server.received[0]
            assert headers["Authorization"] == "Bearer do-not-log-this-token"
            assert "контур алертинга сломан" in body.decode("utf-8")
            assert "do-not-log-this-token" not in (broken.stdout + broken.stderr)
            state = root / "runtime" / "watchdog-state"
            assert state.read_text(encoding="utf-8").strip()
            metrics_path = root / "prometheus" / "textfile" / "frontier_watchdog.prom"
            assert "frontier_watchdog_problems 1" in metrics_path.read_text(encoding="utf-8")

            cooldown = run_watchdog()
            assert cooldown.returncode == 1
            assert len(server.received) == 1

            server.prom_healthy = True
            server.ntfy_status = 500
            problem_state = state.read_bytes()
            failed_recovery = run_watchdog()
            assert failed_recovery.returncode == 0
            assert len(server.received) == 2
            assert "контур алертинга восстановлен" in server.received[-1][1].decode("utf-8")
            assert state.read_bytes() == problem_state
            assert "frontier_watchdog_problems 0" in metrics_path.read_text(encoding="utf-8")

            server.ntfy_status = 200
            recovered = run_watchdog()
            assert recovered.returncode == 0
            assert len(server.received) == 3
            assert "контур алертинга восстановлен" in server.received[-1][1].decode("utf-8")
            assert not state.read_text(encoding="utf-8")

            assert run_watchdog().returncode == 0
            assert len(server.received) == 3

            server.prom_healthy = False
            server.ntfy_status = 500
            failed_send = run_watchdog()
            assert failed_send.returncode == 1
            assert not state.read_text(encoding="utf-8")
            run_watchdog()
            assert len(server.received) == 5
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


@pytest.mark.skipif(BASH is None, reason="bash is unavailable")
def test_triage_always_saves_and_delivers_a_byte_safe_ntfy_message(tmp_path):
    with tempfile.TemporaryDirectory(dir=tmp_path) as raw_temp:
        temp = Path(raw_temp)
        repo = temp / "repo"
        fake_bin = temp / "bin"
        repo.mkdir()
        fake_bin.mkdir()
        install_fake_docker(fake_bin, allow_http_loopback=True)
        digest = temp / "digest.md"
        digest.write_text("TL;DR\n\n" + "Превышение порога 🟠\n" * 400, encoding="utf-8")
        credential = temp / "publisher.cred"
        credential.write_text("triage-test-token", encoding="utf-8")
        server = ThreadingHTTPServer(("127.0.0.1", 0), WatchdogHandler)
        server.ntfy_status = 200
        server.received = []
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            env = os.environ.copy()
            env.update(
                REPO="repo",
                DATABASE_URL="postgresql://unused:unused@localhost/unused",
                NTFY_URL=f"http://127.0.0.1:{server.server_port}/frontier-alerts",
                NTFY_CREDENTIAL_FILE=str(credential),
                PYTHONPATH=str(ROOT),
                PYTHONUTF8="1",
                REAL_PYTHON=bash_path(Path(sys.executable)),
                PATH=str(fake_bin) + os.pathsep + env["PATH"],
            )
            sent = subprocess.run(
                [BASH, str(ROOT / "scripts" / "alert-triage-deliver.sh"), "digest.md", "send"],
                cwd=temp,
                env=env,
                capture_output=True,
                encoding="utf-8",
                errors="replace",
            )
            assert sent.returncode == 0
            saved = list((repo / "docs" / "ops" / "alert-digests").glob("*.md"))
            assert len(saved) == 1, (sent.stdout, sent.stderr, env["REPO"])
            assert saved[0].read_text(encoding="utf-8") == digest.read_text(encoding="utf-8")
            assert "deliver: ntfy sent" in sent.stdout
            assert len(server.received) == 1, (sent.stdout, sent.stderr)
            headers, delivered_bytes = server.received[0]
            assert headers["Authorization"] == "Bearer triage-test-token"
            assert len(delivered_bytes) <= 4096
            delivered_text = delivered_bytes.decode("utf-8")
            assert delivered_text.startswith("🔎 Frontier alert-triage")
            assert "… (обрезано; полный разбор:" in delivered_text

            skipped = subprocess.run(
                [BASH, str(ROOT / "scripts" / "alert-triage-deliver.sh"), "digest.md", "skip"],
                cwd=temp,
                env=env,
                capture_output=True,
                encoding="utf-8",
                errors="replace",
            )
            assert skipped.returncode == 0
            assert len(server.received) == 1
            assert "deliver: ntfy skipped" in skipped.stdout
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


@pytest.mark.skipif(BASH is None, reason="bash is unavailable")
def test_triage_rejects_invalid_mode_without_sending():
    with tempfile.TemporaryDirectory() as raw_temp:
        temp = Path(raw_temp)
        repo = temp / "repo"
        fake_bin = temp / "bin"
        repo.mkdir()
        fake_bin.mkdir()
        install_fake_admin(repo)
        install_fake_docker(fake_bin)
        (temp / "digest.md").write_text("TL;DR", encoding="utf-8")
        delivered = temp / "delivered.txt"
        env = os.environ.copy()
        env.update(
            REPO="repo",
            DELIVERED_FILE=str(delivered),
            FAKE_SENDER_OK="1",
            PYTHONUTF8="1",
            REAL_PYTHON=bash_path(Path(sys.executable)),
            PATH=str(fake_bin) + os.pathsep + env["PATH"],
        )
        result = subprocess.run(
            [BASH, str(ROOT / "scripts" / "alert-triage-deliver.sh"), "digest.md", "sned"],
            cwd=temp,
            env=env,
            capture_output=True,
            encoding="utf-8",
            errors="replace",
        )
        assert result.returncode != 0
        assert not delivered.exists()


@pytest.mark.skipif(BASH is None, reason="bash is unavailable")
@pytest.mark.parametrize("failure", ("mkdir", "cp"))
def test_triage_does_not_claim_or_send_when_persistence_fails(failure):
    with tempfile.TemporaryDirectory() as raw_temp:
        temp = Path(raw_temp)
        fake_bin = temp / "bin"
        fake_bin.mkdir()
        (temp / "digest.md").write_text("TL;DR", encoding="utf-8")
        delivered = temp / "delivered.txt"
        repo = temp / "repo"
        if failure == "mkdir":
            repo.write_text("not a directory", encoding="utf-8")
        else:
            repo.mkdir()
            install_fake_admin(repo)
            install_fake_docker(fake_bin)
            utc_date = datetime.now(UTC).strftime("%Y-%m-%d")
            collision = repo / "docs" / "ops" / "alert-digests" / f"{utc_date}.md"
            (collision / "digest.md").mkdir(parents=True)
        env = os.environ.copy()
        env.update(
            REPO="repo",
            DELIVERED_FILE=str(delivered),
            FAKE_SENDER_OK="1",
            PYTHONUTF8="1",
            REAL_PYTHON=bash_path(Path(sys.executable)),
            PATH=str(fake_bin) + os.pathsep + env["PATH"],
        )
        result = subprocess.run(
            [BASH, str(ROOT / "scripts" / "alert-triage-deliver.sh"), "digest.md", "send"],
            cwd=temp,
            env=env,
            capture_output=True,
            encoding="utf-8",
            errors="replace",
        )
        assert result.returncode != 0
        assert "saved digest" not in result.stdout
        assert not delivered.exists()


@pytest.mark.skipif(BASH is None, reason="bash is unavailable")
def test_triage_requires_exact_success_marker():
    with tempfile.TemporaryDirectory() as raw_temp:
        temp = Path(raw_temp)
        repo = temp / "repo"
        fake_bin = temp / "bin"
        repo.mkdir()
        fake_bin.mkdir()
        install_fake_admin(repo)
        install_fake_docker(fake_bin)
        (temp / "digest.md").write_text("TL;DR", encoding="utf-8")
        env = os.environ.copy()
        env.update(
            REPO="repo",
            DELIVERED_FILE=str(temp / "delivered.txt"),
            FAKE_SENDER_OK="0",
            PYTHONUTF8="1",
            REAL_PYTHON=bash_path(Path(sys.executable)),
            PATH=str(fake_bin) + os.pathsep + env["PATH"],
        )
        result = subprocess.run(
            [BASH, str(ROOT / "scripts" / "alert-triage-deliver.sh"), "digest.md", "send"],
            cwd=temp,
            env=env,
            capture_output=True,
            encoding="utf-8",
            errors="replace",
        )
        assert result.returncode == 0
        assert "deliver: ntfy sent" not in result.stdout
        assert "deliver: ntfy send FAILED" in result.stderr
