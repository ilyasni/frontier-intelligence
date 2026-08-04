"""Basic-авторизация к admin API для standalone ops-скриптов.

Админка закрыта: всё под `/api/` кроме `/api/health`, `/api/auth/login` и вебхука
Alertmanager отвечает `401 {"detail":"unauthorized"}` (admin/backend/main.py).
Скрипты в `scripts/`, написанные до закрытия, ходили без единого credential —
и деградировали каждый по-своему:

  * `server-reprocess-window.ps1` печатал «reprocess ok N/N» при любом исходе,
    потому что код возврата не проверялся вовсе;
  * `server_apply_vision_chain_policy.py` падал с HTTPError на первом же GET;
  * `reprocess_done_for_sparse.py` логировал ошибку по каждому посту и всё равно
    завершался нулём.

Общее у них одно: инструмент выглядит рабочим до момента, когда он понадобится.
Поэтому доступ к админке из ops-скриптов идёт через один модуль.

Почему не `shared.config`: эти скрипты запускаются на сервере голым `python3`,
вне контейнера и без зависимостей проекта. `Settings` из `pydantic-settings`
там не поднимется, а на `.env` с инлайн-комментарием ещё и упадёт разбором
(грабля 2026-07-14). Здесь нужен ровно разбор двух строк — без импорта мира.
"""

from __future__ import annotations

import base64
import os
from pathlib import Path

DEFAULT_ENV_PATH = Path(
    os.environ.get("FRONTIER_ENV_PATH", "/opt/frontier-intelligence/.env")
)

# Дефолт совпадает с admin/backend/main.py::_admin_user. Держать их врозь нельзя:
# разошлись — и авторизация ломается молча, ровно тем же способом.
DEFAULT_ADMIN_USER = "admin"


class AdminCredentialsMissing(RuntimeError):
    """Пароль админки не найден. Намеренно исключение, а не пустая строка.

    Fail-closed: пустой пароль означает 401 на каждый вызов, то есть скрипт
    отработает вхолостую и отрапортует успех. Лучше отказаться на первом шаге.
    """


def _strip_value(raw: str) -> str:
    value = raw.strip().replace("\r", "")
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        value = value[1:-1]
    return value


def _from_env_file(key: str, env_path: Path) -> str:
    if not env_path.is_file():
        return ""
    for raw_line in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, _, value = line.partition("=")
        if name.strip() == key:
            return _strip_value(value)
    return ""


def read_admin_credentials(env_path: Path | None = None) -> tuple[str, str]:
    """Вернуть (user, password). Окружение приоритетнее файла.

    Приоритет именно такой, потому что внутри контейнера переменные уже выставлены
    compose'ом, а `.env` может быть не смонтирован.
    """
    path = DEFAULT_ENV_PATH if env_path is None else env_path
    user = os.environ.get("ADMIN_USER") or _from_env_file("ADMIN_USER", path)
    password = os.environ.get("ADMIN_PASSWORD") or _from_env_file("ADMIN_PASSWORD", path)
    if not password:
        raise AdminCredentialsMissing(
            f"ADMIN_PASSWORD is empty in the environment and in {path}. "
            "Without it every /api/ call returns 401 and the script would do nothing "
            "while reporting success."
        )
    return (user or DEFAULT_ADMIN_USER), password


def basic_auth_header(env_path: Path | None = None) -> dict[str, str]:
    """Готовый заголовок `Authorization: Basic ...` для httpx/urllib."""
    user, password = read_admin_credentials(env_path)
    token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {token}"}
