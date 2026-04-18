"""Telegram account rotator — switches accounts on FloodWait/SessionRevoked."""
import logging
import os
from dataclasses import dataclass
from typing import Any, Optional, Union

from telethon import TelegramClient
from telethon.errors import FloodWaitError, SessionRevokedError, UserDeactivatedBanError
from telethon.network.connection.tcpmtproxy import ConnectionTcpMTProxyRandomizedIntermediate

from shared.metrics import note_telegram_client_reset

logger = logging.getLogger(__name__)

# SOCKS5 (dict) или MTProxy (отдельные поля для TelegramClient + connection=)
ResolvedTelegramProxy = Union[dict[str, Any], "MtProxySettings", None]


@dataclass(frozen=True)
class MtProxySettings:
    """Параметры MTProto proxy по документации Telethon stable."""

    host: str
    port: int
    secret: str


def socks5_proxy_dict(
    host: str,
    port: int,
    username: Optional[str] = None,
    password: Optional[str] = None,
    rdns: bool = True,
) -> dict[str, Any]:
    """Формат proxy для TelegramClient (рекомендуемый в docs.telethon.dev stable)."""
    d: dict[str, Any] = {
        "proxy_type": "socks5",
        "addr": host,
        "port": port,
        "rdns": rdns,
    }
    if username:
        d["username"] = username
    if password:
        d["password"] = password
    return d


def _parse_socks5_dsn(dsn: str) -> Optional[tuple[str, int, Optional[str], Optional[str]]]:
    """Одна строка host:port[:user[:password]] (password может содержать «:» — берём после третьего «:»)."""
    raw = dsn.strip()
    if not raw:
        return None
    parts = raw.split(":", 3)
    if len(parts) < 2:
        logger.warning("TG_SOCKS5 / TG_PROXY_DSN: need at least host:port")
        return None
    host = parts[0].strip()
    if not host:
        return None
    try:
        port = int(parts[1].strip())
    except ValueError:
        logger.warning("TG_SOCKS5 / TG_PROXY_DSN: invalid port")
        return None
    if len(parts) >= 4:
        u = parts[2].strip() or None
        p = parts[3].strip() or None
        return (host, port, u, p)
    if len(parts) == 3:
        u = parts[2].strip() or None
        return (host, port, u, None)
    return (host, port, None, None)


def env_telegram_proxy_configured() -> bool:
    """True, если из окружения можно собрать прокси (для TG_REQUIRE_PROXY). Per-source в БД не учитывается."""
    return _get_proxy_from_env() is not None


def _get_proxy_from_env() -> ResolvedTelegramProxy:
    """Только переменные окружения: MTPROXY_*; иначе SOCKS5."""
    mt_host = (os.environ.get("MTPROXY_HOST") or "").strip()
    if mt_host:
        secret = (os.environ.get("MTPROXY_SECRET") or "").strip()
        if not secret:
            logger.warning("MTPROXY_HOST set but MTPROXY_SECRET empty — MTProxy env ignored")
        else:
            return MtProxySettings(
                host=mt_host,
                port=int(os.environ.get("MTPROXY_PORT", "443")),
                secret=secret,
            )

    host = (os.environ.get("TG_PROXY_HOST") or "").strip() or (os.environ.get("WG_SOCKS_HOST") or "").strip()
    if host:
        port_str = os.environ.get("TG_PROXY_PORT") or os.environ.get("WG_SOCKS_PORT") or "1080"
        port = int(port_str)
        user = (os.environ.get("TG_PROXY_USER") or os.environ.get("WG_SOCKS_USER") or "").strip()
        password = (os.environ.get("TG_PROXY_PASS") or os.environ.get("WG_SOCKS_PASS") or "").strip()
        return socks5_proxy_dict(host, port, user or None, password or None)

    dsn = (os.environ.get("TG_SOCKS5") or os.environ.get("TG_PROXY_DSN") or "").strip()
    parsed = _parse_socks5_dsn(dsn) if dsn else None
    if parsed:
        h, port, user, password = parsed
        return socks5_proxy_dict(h, port, user, password)
    return None


def _socks5_from_source(proxy_config: dict) -> dict[str, Any]:
    return socks5_proxy_dict(
        proxy_config["host"],
        int(proxy_config.get("port", 1080)),
        (proxy_config.get("user") or "").strip() or None,
        (proxy_config.get("password") or "").strip() or None,
    )


def _mtproxy_from_source(proxy_config: dict) -> Optional[MtProxySettings]:
    host = (proxy_config.get("host") or "").strip()
    secret = (proxy_config.get("secret") or "").strip()
    if not host or not secret:
        logger.warning("proxy_config mtproto: need host and secret")
        return None
    return MtProxySettings(
        host=host,
        port=int(proxy_config.get("port", 443)),
        secret=secret,
    )


def build_resolved_proxy(proxy_config: Optional[dict]) -> ResolvedTelegramProxy:
    """Прокси: сначала proxy_config источника, иначе env."""
    if proxy_config and proxy_config.get("host"):
        if proxy_config.get("type") == "mtproto":
            return _mtproxy_from_source(proxy_config)
        return _socks5_from_source(proxy_config)
    return _get_proxy_from_env()


def _proxy_log_label(resolved: ResolvedTelegramProxy, proxy_config: Optional[dict]) -> None:
    if isinstance(resolved, MtProxySettings):
        src = "per-source proxy_config" if (proxy_config and proxy_config.get("host")) else "env MTPROXY_*"
        logger.info("Telegram proxy: MTProxy %s:%s (%s)", resolved.host, resolved.port, src)
        return
    if isinstance(resolved, dict):
        src = "per-source proxy_config" if (proxy_config and proxy_config.get("host")) else _env_socks_label()
        logger.info(
            "Telegram proxy: SOCKS5 %s:%s (%s)",
            resolved.get("addr"),
            resolved.get("port"),
            src,
        )
        return
    logger.info(
        "Telegram connection: direct to DC "
        "(set TG_SOCKS5 or TG_PROXY_* / WG_SOCKS_* / MTPROXY_* in .env or proxy_config on source)"
    )


def _env_socks_label() -> str:
    if (os.environ.get("TG_PROXY_HOST") or "").strip():
        return "env TG_PROXY_*"
    if (os.environ.get("WG_SOCKS_HOST") or "").strip():
        return "env WG_SOCKS_* (see docs/README)"
    if (os.environ.get("TG_SOCKS5") or os.environ.get("TG_PROXY_DSN") or "").strip():
        return "env TG_SOCKS5 / TG_PROXY_DSN"
    return "env"


@dataclass
class TelegramAccount:
    api_id: int
    api_hash: str
    session_path: str
    client: Optional[TelegramClient] = None
    banned: bool = False
    _lock: object = None

    def __post_init__(self):
        import asyncio

        self._lock = asyncio.Lock()


class AccountRotator:
    def __init__(self, accounts: list[TelegramAccount]):
        self.accounts = [a for a in accounts if a.api_id > 0]
        self._current_idx = 0

    async def _drop_client_locked(self, account: TelegramAccount, reason: str) -> None:
        client = account.client
        account.client = None
        if client is None:
            return
        note_telegram_client_reset("ingest", _normalize_reset_reason(reason))
        try:
            if client.is_connected():
                await client.disconnect()
        except Exception as exc:
            logger.warning("Failed to disconnect Telegram client (%s): %s", reason, exc)
        else:
            logger.info("Telegram client reset (%s)", reason)

    def _build_client_instance(
        self,
        account: TelegramAccount,
        proxy_config: Optional[dict] = None,
    ) -> TelegramClient:
        resolved = self._build_proxy(proxy_config)
        _proxy_log_label(resolved, proxy_config)

        if isinstance(resolved, MtProxySettings):
            return TelegramClient(
                account.session_path,
                account.api_id,
                account.api_hash,
                connection=ConnectionTcpMTProxyRandomizedIntermediate,
                proxy=(resolved.host, resolved.port, resolved.secret),
                system_version="4.16.30-vxCUSTOM",
            )

        return TelegramClient(
            account.session_path,
            account.api_id,
            account.api_hash,
            proxy=resolved,
            system_version="4.16.30-vxCUSTOM",
        )

    @property
    def current(self) -> Optional[TelegramAccount]:
        for i in range(len(self.accounts)):
            idx = (self._current_idx + i) % len(self.accounts)
            if not self.accounts[idx].banned:
                self._current_idx = idx
                return self.accounts[idx]
        return None

    def rotate(self) -> Optional[TelegramAccount]:
        self._current_idx = (self._current_idx + 1) % max(len(self.accounts), 1)
        return self.current

    def _account_for_idx(self, preferred_idx: int) -> Optional[TelegramAccount]:
        """Return preferred account if not banned, otherwise fall back to rotator."""
        if 0 <= preferred_idx < len(self.accounts) and not self.accounts[preferred_idx].banned:
            return self.accounts[preferred_idx]
        return self.current

    def _build_proxy(self, proxy_config: Optional[dict]) -> ResolvedTelegramProxy:
        return build_resolved_proxy(proxy_config)

    async def get_client(
        self,
        preferred_idx: int = 0,
        proxy_config: Optional[dict] = None,
    ) -> Optional[TelegramClient]:
        account = self._account_for_idx(preferred_idx)
        if account is None:
            return None
        async with account._lock:
            if account.client is not None and not account.client.is_connected():
                await self._drop_client_locked(account, "stale/disconnected client")

            if account.client is None:
                account.client = self._build_client_instance(account, proxy_config)
                try:
                    await account.client.connect()
                except Exception:
                    await self._drop_client_locked(account, "connect failure")
                    raise
        return account.client

    async def reset_client(
        self,
        preferred_idx: int = 0,
        reason: str = "reset requested",
    ) -> bool:
        account = self._account_for_idx(preferred_idx)
        if account is None:
            return False
        async with account._lock:
            had_client = account.client is not None
            await self._drop_client_locked(account, reason)
            return had_client

    async def handle_error(self, exc: Exception) -> bool:
        """Returns True if we should retry with new account."""
        if isinstance(exc, FloodWaitError):
            logger.warning("FloodWait %ds on account %d, rotating", exc.seconds, self._current_idx)
            self.rotate()
            return True
        if isinstance(exc, (SessionRevokedError, UserDeactivatedBanError)):
            logger.error("Account %d banned/revoked, marking bad", self._current_idx)
            if self.current:
                self.current.banned = True
            self.rotate()
            return True
        return False

    async def close_all(self):
        for acc in self.accounts:
            async with acc._lock:
                await self._drop_client_locked(acc, "close_all")


def _normalize_reset_reason(reason: str) -> str:
    normalized = (reason or "").lower()
    if "stale" in normalized or "disconnected" in normalized:
        return "stale_client"
    if "connect failure" in normalized:
        return "connect_failure"
    if "transport" in normalized or "runtime" in normalized:
        return "transport_runtime_failure"
    if "close_all" in normalized:
        return "shutdown"
    return "other"
