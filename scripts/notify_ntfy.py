#!/usr/bin/env python3
"""Опубликовать сообщение Frontier watchdog только с проверенной квитанцией ntfy."""

import argparse
import json
import os
import re
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


class DeliveryError(Exception):
    """Безопасное для журнала описание без сообщения и credential."""


def validate_url(url: str, *, _allow_http_loopback: bool = False) -> str:
    try:
        parts = urlsplit(url)
        authority_suffix = parts.netloc.rsplit("]", 1)[-1]
        valid_transport = (
            parts.scheme == "https" and ":" not in authority_suffix
        ) or (
            _allow_http_loopback
            and parts.scheme == "http"
            and parts.hostname in {"127.0.0.1", "::1", "localhost"}
        )
        topic = parts.path.removeprefix("/")
        reserved = {
            "app",
            "docs",
            "static",
            "file",
            "v1",
            "admin",
            "account",
            "settings",
            "health",
            "metrics",
        }
        if (
            not valid_transport
            or not parts.hostname
            or parts.username is not None
            or parts.password is not None
            or parts.query
            or parts.fragment
            or "?" in url
            or "#" in url
            or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", topic)
            or topic in reserved
            or any(char.isspace() for char in url)
            or parts.netloc.endswith(":")
            or parts.port == 0
        ):
            raise ValueError
    except ValueError:
        raise DeliveryError(
            "Некорректный URL ntfy: нужен HTTPS и один topic"
        ) from None
    return topic


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def publish(
    url: str, credential_file: Path, message: str, timeout: float = 15
) -> None:
    _publish(url, credential_file, message, timeout)


def _publish(
    url: str,
    credential_file: Path,
    message: str,
    timeout: float = 15,
    *,
    _allow_http_loopback: bool = False,
) -> None:
    """HTTP loopback разрешается явно только для локальных transport-тестов."""
    topic = validate_url(url, _allow_http_loopback=_allow_http_loopback)
    body = message.encode("utf-8")
    if not body or len(body) > 4096:
        raise DeliveryError("Сообщение ntfy должно занимать 1–4096 байт UTF-8")
    try:
        credential = credential_file.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError):
        raise DeliveryError("Файл доступа ntfy недоступен") from None
    if not credential or not re.fullmatch(r"[A-Za-z0-9._~-]+", credential):
        raise DeliveryError("Файл доступа ntfy пуст или имеет неверный формат")

    request = Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {credential}",
            "Content-Type": "text/plain; charset=utf-8",
            "Title": "Frontier watchdog",
            "Priority": "high",
            "Tags": "warning,rotating_light",
        },
    )
    try:
        with build_opener(NoRedirect()).open(request, timeout=timeout) as response:
            if response.status != 200:
                raise DeliveryError("Сервер ntfy не подтвердил публикацию")
            raw = response.read(16385)
        receipt = json.loads(raw) if len(raw) <= 16384 else None
    except HTTPError as error:
        raise DeliveryError(
            f"ntfy отклонил публикацию: HTTP {error.code}"
        ) from None
    except (URLError, TimeoutError, OSError):
        raise DeliveryError("Ошибка соединения с ntfy") from None
    except (ValueError, UnicodeError):
        raise DeliveryError("Некорректная квитанция ntfy") from None

    if (
        not isinstance(receipt, dict)
        or not isinstance(receipt.get("id"), str)
        or not receipt["id"].strip()
        or receipt.get("event") != "message"
        or receipt.get("topic") != topic
    ):
        raise DeliveryError("Нет квитанции ntfy для ожидаемого topic")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("message", help="Одно сообщение watchdog")
    args = parser.parse_args()
    credential_file = Path(os.environ.get("NTFY_CREDENTIAL_FILE", ""))
    try:
        publish(os.environ.get("NTFY_URL", ""), credential_file, args.message)
    except DeliveryError as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
