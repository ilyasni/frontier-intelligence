import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock


def _load_run_proxy_module(monkeypatch):
    run_proxy_path = (
        Path(__file__).resolve().parents[1] / "gpt2giga-proxy" / "run_proxy.py"
    )
    monkeypatch.syspath_prepend(str(run_proxy_path.parent))
    monkeypatch.setitem(sys.modules, "disable_ssl", MagicMock())
    spec = importlib.util.spec_from_file_location("gpt2giga_run_proxy", run_proxy_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_run_proxy_uses_dotted_cli_flags(monkeypatch) -> None:
    module = _load_run_proxy_module(monkeypatch)
    captured: dict = {}

    def _fake_run(args, env=None):
        captured["args"] = args
        captured["env"] = env
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(module.subprocess, "run", _fake_run)
    monkeypatch.setenv("GPT2GIGA_HOST", "0.0.0.0")
    monkeypatch.setenv("GPT2GIGA_PROXY_PORT", "8090")
    monkeypatch.setenv("GPT2GIGA_LOG_LEVEL", "INFO")
    monkeypatch.setenv("GPT2GIGA_TIMEOUT", "600")
    monkeypatch.setenv("GPT2GIGA_EMBEDDINGS", "EmbeddingsGigaR")
    monkeypatch.setenv("GPT2GIGA_PASS_MODEL", "True")
    monkeypatch.setenv("GPT2GIGA_ENABLE_IMAGES", "True")
    monkeypatch.setenv("GIGACHAT_BASE_URL", "https://gigachat.devices.sberbank.ru/api/v1")
    monkeypatch.setenv("GIGACHAT_VERIFY_SSL_CERTS", "False")
    monkeypatch.setenv("GIGACHAT_MODEL", "GigaChat-Max")
    monkeypatch.setenv("GIGACHAT_MAX_RETRIES", "3")
    monkeypatch.setenv("GIGACHAT_RETRY_BACKOFF_FACTOR", "0.5")
    monkeypatch.setenv("GIGACHAT_CREDENTIALS", "secret")
    monkeypatch.setenv("GIGACHAT_SCOPE", "GIGACHAT_API_PERS")

    rc = module.main()

    assert rc == 0
    args = captured["args"]
    assert "--proxy.host" in args
    assert "--proxy.port" in args
    assert "--proxy.pass-model" in args
    assert "--proxy.enable-images" in args
    assert "--proxy.embeddings" in args
    assert "--gigachat.timeout" in args
    assert "--gigachat.base-url" in args
    assert "--gigachat.verify-ssl-certs" in args
    assert "--gigachat.max-retries" in args
    assert "--gigachat.retry-backoff-factor" in args
    assert "--gigachat.model" in args
    assert "--gigachat.credentials" in args
    assert "--gigachat.scope" in args
    assert "--proxy-pass-model" not in args
    assert "--proxy-host" not in args
    assert "--gigachat-base-url" not in args
