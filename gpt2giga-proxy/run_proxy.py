"""
Запуск gpt2giga через subprocess: патч SSL в родительском процессе + флаги CLI для дочернего
(как в telegram-assistant/run_gpt2giga.py; execvp сбрасывает эффект import disable_ssl).
"""
import os
import subprocess
import sys

sys.path.insert(0, "/app")
import disable_ssl  # noqa: F401, E402


def _env_bool(name: str, default: str) -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes")


def main() -> int:
    host = os.getenv("GPT2GIGA_HOST", "0.0.0.0")
    port = os.getenv("GPT2GIGA_PROXY_PORT", "8090")
    log_level = os.getenv("GPT2GIGA_LOG_LEVEL", "INFO")
    timeout = os.getenv("GPT2GIGA_TIMEOUT", "600")
    embeddings = os.getenv("GPT2GIGA_EMBEDDINGS", "EmbeddingsGigaR")
    pass_model = _env_bool("GPT2GIGA_PASS_MODEL", "True")
    enable_images = _env_bool("GPT2GIGA_ENABLE_IMAGES", "True")
    gigachat_model = os.getenv("GIGACHAT_MODEL", "").strip()
    max_retries = os.getenv("GIGACHAT_MAX_RETRIES", "3").strip()
    retry_backoff = os.getenv("GIGACHAT_RETRY_BACKOFF_FACTOR", "0.5").strip()
    base_url = os.getenv(
        "GIGACHAT_BASE_URL",
        "https://gigachat.devices.sberbank.ru/api/v1",
    )
    verify_ssl = _env_bool("GIGACHAT_VERIFY_SSL_CERTS", "False")

    args: list[str] = [
        "gpt2giga",
        "--proxy.host",
        host,
        "--proxy.port",
        port,
        "--proxy.log-level",
        log_level,
        "--proxy.pass-model",
        "True" if pass_model else "False",
        "--proxy.enable-images",
        "True" if enable_images else "False",
        "--gigachat.timeout",
        timeout,
        "--proxy.embeddings",
        embeddings,
        "--gigachat.base-url",
        base_url,
        "--gigachat.verify-ssl-certs",
        "True" if verify_ssl else "False",
        "--gigachat.max-retries",
        max_retries,
        "--gigachat.retry-backoff-factor",
        retry_backoff,
    ]

    if _env_bool("GPT2GIGA_VERBOSE", "False"):
        args.extend(["--proxy.log-level", "DEBUG"])

    creds = os.getenv("GIGACHAT_CREDENTIALS")
    if creds:
        args.extend(["--gigachat.credentials", creds])

    scope = os.getenv("GIGACHAT_SCOPE")
    if scope:
        args.extend(["--gigachat.scope", scope])

    if gigachat_model:
        args.extend(["--gigachat.model", gigachat_model])

    env = os.environ.copy()
    env.setdefault("PYTHONHTTPSVERIFY", "0" if not verify_ssl else "1")
    if not verify_ssl:
        env.setdefault("CURL_CA_BUNDLE", "")
        env.setdefault("REQUESTS_CA_BUNDLE", "")
        env.setdefault("SSL_CERT_FILE", "")
    env["GIGACHAT_VERIFY_SSL_CERTS"] = "True" if verify_ssl else "False"

    proc = subprocess.run(args, env=env)
    return int(proc.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
