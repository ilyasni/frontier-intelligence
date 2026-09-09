"""Contract tests for ntfy delivery from admin and Alertmanager."""

import os
from pathlib import Path
import re
import shutil
import subprocess

import pytest
import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
ALERTMANAGER_CONFIG = REPO_ROOT / "prometheus" / "alertmanager.yml"
COMPOSE_CONFIG = REPO_ROOT / "docker-compose.yml"
ENV_EXAMPLE = REPO_ROOT / ".env.example"


def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _receiver(config: dict, name: str) -> dict:
    receivers = {
        receiver["name"]: receiver for receiver in config.get("receivers") or []
    }
    assert name in receivers, f"receiver {name!r} is missing"
    return receivers[name]


def _example_value(name: str) -> str:
    text = ENV_EXAMPLE.read_text(encoding="utf-8")
    match = re.search(rf"^# {re.escape(name)}=(.*)$", text, flags=re.MULTILINE)
    assert match, f"{name} is missing from .env.example"
    return match.group(1)


def test_alertmanager_preserves_blackholes_and_delivers_every_page_via_admin() -> None:
    """A routing edit must not page watchdog/opt-out alerts or bypass the admin catch-all."""
    config = _load_yaml(ALERTMANAGER_CONFIG)
    route = config["route"]
    child_routes = route["routes"]

    assert route["receiver"] == "ntfy-admin"
    assert child_routes[0] == {
        "receiver": "blackhole",
        "matchers": ['alertname="FrontierWatchdog"'],
    }
    assert child_routes[1] == {
        "receiver": "blackhole",
        "matchers": ['notify="never"'],
    }
    assert child_routes[-1] == {"receiver": "ntfy-admin"}

    admin = _receiver(config, "ntfy-admin")
    assert admin["webhook_configs"] == [
        {
            "url": "http://admin:8101/api/monitoring/alertmanager/webhook",
            "send_resolved": True,
            "max_alerts": 10,
            "timeout": "60s",
            "http_config": {
                "basic_auth": {
                    "username": "alertmanager",
                    "password": "__ALERTMANAGER_WEBHOOK_TOKEN__",
                }
            },
        }
    ]


def test_critical_pages_also_take_an_independent_authenticated_ntfy_path() -> None:
    """Critical alerts must survive admin failure without putting a bearer token in YAML."""
    config = _load_yaml(ALERTMANAGER_CONFIG)
    critical_routes = [
        route
        for route in config["route"]["routes"]
        if route.get("matchers") == ['severity="critical"']
    ]
    assert critical_routes == [
        {
            "receiver": "ntfy-direct",
            "matchers": ['severity="critical"'],
            "continue": True,
        }
    ]

    direct = _receiver(config, "ntfy-direct")
    assert "telegram_configs" not in direct
    assert direct["webhook_configs"] == [
        {
            "url": "__NTFY_URL__?template=netwatch",
            "send_resolved": True,
            "max_alerts": 3,
            "timeout": "15s",
            "http_config": {
                "follow_redirects": False,
                "authorization": {
                    "type": "Bearer",
                    "credentials_file": "/run/secrets/ntfy-alertmanager",
                },
            },
        }
    ]
    assert not any(
        receiver.get("telegram_configs") for receiver in config["receivers"]
    )


def test_compose_gives_admin_its_ntfy_credential_without_removing_urgent_telegram() -> None:
    """Admin owns ntfy app auth while urgent trend Telegram settings remain available."""
    compose = _load_yaml(COMPOSE_CONFIG)
    admin = compose["services"]["admin"]
    environment = admin["environment"]

    assert environment["NTFY_URL"] == "${NTFY_URL:-https://ntfy.produman.studio/home-network}"
    assert environment["NTFY_CREDENTIAL_FILE"] == "/run/secrets/ntfy-app"
    assert environment["TELEGRAM_BOT_TOKEN"] == "${TELEGRAM_BOT_TOKEN:-}"
    assert environment["ALERT_TELEGRAM_CHAT_ID"] == "${ALERT_TELEGRAM_CHAT_ID:-}"
    assert environment["TELEGRAM_ALERT_CHAT_ID"] == "${TELEGRAM_ALERT_CHAT_ID:-}"
    assert environment["TELEGRAM_ALERT_PROXY_URL"] == (
        "${TELEGRAM_ALERT_PROXY_URL:-socks5://xray:10808}"
    )
    credential_mounts = [
        volume for volume in admin["volumes"] if "/run/secrets/ntfy-" in volume
    ]
    assert credential_mounts == [
        "${NTFY_APP_CREDENTIAL_FILE:-/etc/frontier-intelligence/credentials/ntfy-app}:/run/secrets/ntfy-app:ro"
    ]


def test_compose_isolates_alertmanager_ntfy_auth_from_app_and_telegram_credentials() -> None:
    """Alertmanager gets only its own bearer file and no direct Telegram credentials."""
    compose = _load_yaml(COMPOSE_CONFIG)
    alertmanager = compose["services"]["alertmanager"]
    environment = alertmanager["environment"]

    assert environment == {
        "ALERTMANAGER_WEBHOOK_TOKEN": "${ALERTMANAGER_WEBHOOK_TOKEN:-}",
        "NTFY_URL": "${NTFY_URL:-https://ntfy.produman.studio/home-network}",
    }
    credential_mounts = [
        volume
        for volume in alertmanager["volumes"]
        if "/run/secrets/ntfy-" in volume
    ]
    assert credential_mounts == [
        "${NTFY_ALERTMANAGER_CREDENTIAL_FILE:-/etc/frontier-intelligence/credentials/ntfy-alertmanager}:/run/secrets/ntfy-alertmanager:ro"
    ]

    for service_name, service in compose["services"].items():
        if service_name in {"admin", "alertmanager"}:
            continue
        assert not any(
            "/run/secrets/ntfy-" in volume for volume in service.get("volumes") or []
        ), f"unrelated service {service_name!r} mounts an ntfy credential"

    all_volumes = [
        volume
        for service in compose["services"].values()
        for volume in service.get("volumes") or []
    ]
    assert not any("ntfy-watchdog" in volume for volume in all_volumes)


def test_env_example_declares_three_distinct_host_credential_files() -> None:
    """Operators need separate app, Alertmanager, and host-watchdog bearer files."""
    assert _example_value("NTFY_APP_CREDENTIAL_FILE") == (
        "/etc/frontier-intelligence/credentials/ntfy-app"
    )
    assert _example_value("NTFY_ALERTMANAGER_CREDENTIAL_FILE") == (
        "/etc/frontier-intelligence/credentials/ntfy-alertmanager"
    )
    assert _example_value("NTFY_WATCHDOG_CREDENTIAL_FILE") == (
        "/etc/frontier-intelligence/credentials/ntfy-watchdog"
    )


def _bash_executable() -> str:
    if os.name == "nt":
        for candidate in (
            Path(r"C:\Program Files\Git\bin\bash.exe"),
            Path(r"C:\Program Files\Git\usr\bin\bash.exe"),
            Path(r"C:\Program Files (x86)\Git\bin\bash.exe"),
        ):
            if candidate.is_file():
                return str(candidate)
    discovered = shutil.which("bash")
    if discovered:
        return discovered
    raise AssertionError("bash is required to exercise the Alertmanager entrypoint")


def _shell_path(path: Path) -> str:
    resolved = path.resolve()
    if os.name != "nt":
        return str(resolved)
    drive = resolved.drive.rstrip(":").lower()
    tail = resolved.as_posix().split(":", 1)[1]
    return f"/{drive}{tail}"


def _run_entrypoint(
    script: str,
    tmp_path: Path,
    *,
    webhook_token: str,
    ntfy_url: str,
) -> tuple[subprocess.CompletedProcess[str], bool]:
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    marker = tmp_path / "sed-called"
    fake_sed = fake_bin / "sed"
    fake_sed.write_text(
        '#!/bin/sh\n: > "$SED_MARKER"\nexit "$FAKE_SED_EXIT"\n',
        encoding="utf-8",
    )
    fake_sed.chmod(0o755)

    rendered_script = script.replace("$$", "$")
    wrapped_script = f'PATH="{_shell_path(fake_bin)}:$PATH"\n{rendered_script}'
    environment = os.environ.copy()
    environment.update(
        {
            "ALERTMANAGER_WEBHOOK_TOKEN": webhook_token,
            "NTFY_URL": ntfy_url,
            "SED_MARKER": _shell_path(marker),
            "FAKE_SED_EXIT": "97",
        }
    )
    result = subprocess.run(
        [_bash_executable(), "-c", wrapped_script],
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )
    return result, marker.exists()


@pytest.mark.parametrize(
    "topic",
    [
        "", "home/network", "home-network/", "topic.name", "topic~name",
        "a" * 65, "account", "admin", "app", "docs", "file", "health",
        "metrics", "settings", "static", "v1", "home-network?", "home-network#",
        "home-network?raw=1", "home-network#fragment", "home network", "home\tnetwork",
        "home\nnetwork", "тема", "%61pp", "home&network", "home'network",
        "$(false)", "`false`", "home\\network",
    ],
)
def test_alertmanager_rendering_rejects_invalid_topics_before_sed(tmp_path: Path, topic: str) -> None:
    """Invalid ntfy endpoints must never reach configuration interpolation."""
    script = _load_yaml(COMPOSE_CONFIG)["services"]["alertmanager"]["entrypoint"][2]
    result, sed_called = _run_entrypoint(
        script, tmp_path, webhook_token="safe_token_123",
        ntfy_url=f"https://ntfy.produman.studio/{topic}",
    )
    assert result.returncode == 1, result.stderr
    assert not sed_called, result.stderr


def test_alertmanager_rendering_rejects_unsafe_values_before_sed(tmp_path: Path) -> None:
    """Unsafe values must stop the real shell entrypoint before interpolation."""
    compose = _load_yaml(COMPOSE_CONFIG)
    script = compose["services"]["alertmanager"]["entrypoint"][2]
    unsafe_cases = [
        ("unsafe&token", "https://ntfy.produman.studio/home-network"),
        ("safe_token_123", "http://ntfy.produman.studio/home-network"),
        ("safe_token_123", "https://user@ntfy.produman.studio/home-network"),
        ("safe_token_123", "https://ntfy.produman.studio/home-network?raw=1"),
        ("safe_token_123", "https://ntfy.produman.studio/home/network"),
        ("safe_token_123", "https:///home-network"),
        ("safe_token_123", "https://ntfy.produman.studio:443/home-network"),
        ("safe_token_123", "https://ntfy.produman.studio:8443/home-network"),
        ("safe_token_123", "https://ntfy.produman.studio:invalid/home-network"),
        ("safe_token_123", "https://ntfy.produman.studio /home-network"),
        ("safe_token_123", "https://ntfy.produman.studio\n/home-network"),
        ("safe_token_123", "https://user:password@ntfy.produman.studio/home-network"),
        ("safe_token_123", "https://@ntfy.produman.studio/home-network"),
        ("safe_token_123", "https://ntfy.produman.studio?x/home-network"),
        ("safe_token_123", "https://ntfy.produman.studio#x/home-network"),
        ("safe_token_123", "https://ntfy.produman.studio\\evil/home-network"),
    ]

    for index, (token, url) in enumerate(unsafe_cases):
        case_dir = tmp_path / str(index)
        case_dir.mkdir()
        result, sed_called = _run_entrypoint(
            script,
            case_dir,
            webhook_token=token,
            ntfy_url=url,
        )
        assert result.returncode != 0, result.stderr
        assert not sed_called, result.stderr


@pytest.mark.parametrize("topic", ["home-network", "a", "a" * 64, "Mixed_123-name", "Admin"])
def test_alertmanager_rendering_stops_when_sed_fails(tmp_path: Path, topic: str) -> None:
    """A failed interpolation must not fall through to the Alertmanager exec."""
    compose = _load_yaml(COMPOSE_CONFIG)
    script = compose["services"]["alertmanager"]["entrypoint"][2]
    result, sed_called = _run_entrypoint(
        script,
        tmp_path,
        webhook_token="safe_token_123",
        ntfy_url=f"https://ntfy.produman.studio/{topic}",
    )

    assert sed_called, (
        f"exit={result.returncode}; stdout={result.stdout}; stderr={result.stderr}"
    )
    assert result.returncode == 97, result.stderr
