from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

SUPPORTED_TRANSPORTS = {"reality", "ws-tls"}


def _as_bool(value: Any, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() not in {"", "0", "false", "no", "off"}
    return bool(value)


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _normalize_profile(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("xray_profile_must_be_object")
    name = str(raw.get("name") or "").strip()
    if not name:
        raise ValueError("xray_profile_name_required")
    transport = str(raw.get("transport") or "").strip().lower()
    if transport not in SUPPORTED_TRANSPORTS:
        raise ValueError(f"unsupported_xray_transport:{transport}")
    profile = {
        "name": name,
        "enabled": _as_bool(raw.get("enabled"), True),
        "priority": _as_int(raw.get("priority"), 100),
        "transport": transport,
        "address": str(raw.get("address") or "").strip(),
        "port": _as_int(raw.get("port"), 443),
        "uuid": str(raw.get("uuid") or "").strip(),
        "flow": str(raw.get("flow") or "").strip(),
        "reality": _as_dict(raw.get("reality")),
        "tls": _as_dict(raw.get("tls")),
        "ws": _as_dict(raw.get("ws")),
    }
    if not profile["address"] or not profile["uuid"]:
        raise ValueError(f"incomplete_xray_profile:{name}")
    return profile


def normalize_registry_payload(payload: Any) -> dict[str, Any]:
    if payload is None:
        return {"profiles": []}
    if isinstance(payload, list):
        payload = {"profiles": payload}
    if not isinstance(payload, dict):
        raise ValueError("xray_registry_must_be_object")
    profiles = payload.get("profiles") or []
    if not isinstance(profiles, list):
        raise ValueError("xray_registry_profiles_must_be_list")
    normalized = [_normalize_profile(item) for item in profiles]
    normalized.sort(key=lambda item: (item["priority"], item["name"]))
    return {"profiles": normalized}


def load_registry(path: str | os.PathLike[str]) -> dict[str, Any]:
    registry_path = Path(path)
    if not registry_path.is_file():
        return {"profiles": []}
    raw = registry_path.read_text(encoding="utf-8")
    payload = json.loads(raw)
    return normalize_registry_payload(payload)


def write_json_atomic(path: str | os.PathLike[str], payload: Any) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=f"{target.name}.", dir=str(target.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
            fh.write("\n")
        os.replace(tmp_path, target)
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def write_text_atomic(path: str | os.PathLike[str], value: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=f"{target.name}.", dir=str(target.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(value.strip())
            fh.write("\n")
        os.replace(tmp_path, target)
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def touch_file(path: str | os.PathLike[str]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.touch()


def list_profiles(registry_path: str | os.PathLike[str]) -> list[dict[str, Any]]:
    return load_registry(registry_path)["profiles"]


def enabled_profiles(registry_path: str | os.PathLike[str]) -> list[dict[str, Any]]:
    return [item for item in list_profiles(registry_path) if item.get("enabled")]


def read_active_profile_name(path: str | os.PathLike[str]) -> str:
    active_path = Path(path)
    if not active_path.is_file():
        return ""
    return active_path.read_text(encoding="utf-8").strip()


def find_profile(
    profiles: list[dict[str, Any]],
    profile_name: str,
) -> dict[str, Any] | None:
    wanted = str(profile_name or "").strip()
    if not wanted:
        return None
    for profile in profiles:
        if profile.get("name") == wanted:
            return profile
    return None


def resolve_active_profile(
    registry_path: str | os.PathLike[str],
    active_profile_path: str | os.PathLike[str],
) -> tuple[str, dict[str, Any] | None, list[dict[str, Any]]]:
    profiles = list_profiles(registry_path)
    active_name = read_active_profile_name(active_profile_path)
    active_profile = find_profile(profiles, active_name)
    if active_profile and active_profile.get("enabled"):
        return active_name, active_profile, profiles
    first_enabled = next((item for item in profiles if item.get("enabled")), None)
    if first_enabled:
        return str(first_enabled["name"]), first_enabled, profiles
    return active_name, None, profiles


def profile_runtime_summary(profile: dict[str, Any], *, is_active: bool = False) -> dict[str, Any]:
    return {
        "name": profile.get("name"),
        "enabled": bool(profile.get("enabled")),
        "priority": int(profile.get("priority") or 0),
        "transport": profile.get("transport"),
        "address": profile.get("address"),
        "port": int(profile.get("port") or 0),
        "flow": profile.get("flow") or "",
        "is_active": is_active,
    }


def build_legacy_outbound(env: dict[str, str]) -> dict[str, Any]:
    user: dict[str, Any] = {
        "id": env.get("XRAY_VLESS_ID", "").strip(),
        "encryption": "none",
    }
    flow = env.get("XRAY_VLESS_FLOW", "").strip()
    if flow:
        user["flow"] = flow
    return {
        "tag": "proxy",
        "protocol": "vless",
        "settings": {
            "vnext": [
                {
                    "address": env.get("XRAY_VLESS_ADDRESS", "").strip(),
                    "port": _as_int(env.get("XRAY_VLESS_PORT"), 443),
                    "users": [user],
                }
            ]
        },
        "streamSettings": {
            "network": "tcp",
            "security": "reality",
            "realitySettings": {
                "fingerprint": env.get("XRAY_REALITY_FP", "chrome").strip() or "chrome",
                "serverName": env.get("XRAY_REALITY_SNI", "").strip(),
                "publicKey": env.get("XRAY_REALITY_PBK", "").strip(),
                "shortId": env.get("XRAY_REALITY_SID", "").strip(),
                "spiderX": "/",
            },
        },
    }


def build_outbound_from_profile(profile: dict[str, Any]) -> dict[str, Any]:
    user: dict[str, Any] = {
        "id": str(profile.get("uuid") or "").strip(),
        "encryption": "none",
    }
    flow = str(profile.get("flow") or "").strip()
    if flow:
        user["flow"] = flow

    outbound: dict[str, Any] = {
        "tag": "proxy",
        "protocol": "vless",
        "settings": {
            "vnext": [
                {
                    "address": str(profile.get("address") or "").strip(),
                    "port": _as_int(profile.get("port"), 443),
                    "users": [user],
                }
            ]
        },
    }
    transport = profile.get("transport")
    if transport == "reality":
        reality = _as_dict(profile.get("reality"))
        outbound["streamSettings"] = {
            "network": "tcp",
            "security": "reality",
            "realitySettings": {
                "fingerprint": str(reality.get("fingerprint") or "chrome").strip() or "chrome",
                "serverName": str(reality.get("server_name") or "").strip(),
                "publicKey": str(reality.get("public_key") or "").strip(),
                "shortId": str(reality.get("short_id") or "").strip(),
                "spiderX": str(reality.get("spider_x") or "/").strip() or "/",
            },
        }
        return outbound

    tls = _as_dict(profile.get("tls"))
    ws = _as_dict(profile.get("ws"))
    tls_settings: dict[str, Any] = {}
    server_name = str(
        tls.get("server_name")
        or ws.get("host")
        or profile.get("address")
        or ""
    ).strip()
    if server_name:
        tls_settings["serverName"] = server_name
    alpn = tls.get("alpn")
    if isinstance(alpn, list) and alpn:
        tls_settings["alpn"] = [str(item).strip() for item in alpn if str(item).strip()]
    outbound["streamSettings"] = {
        "network": "ws",
        "security": "tls",
        "tlsSettings": tls_settings,
        "wsSettings": {
            "path": str(ws.get("path") or "/").strip() or "/",
        },
    }
    ws_host = str(ws.get("host") or "").strip()
    if ws_host:
        outbound["streamSettings"]["wsSettings"]["headers"] = {"Host": ws_host}
    return outbound


def build_xray_config(env: dict[str, str]) -> tuple[dict[str, Any], dict[str, Any]]:
    registry_path = env.get("XRAY_PROFILE_REGISTRY_PATH", "/runtime/xray-profiles.json")
    active_profile_path = env.get("XRAY_ACTIVE_PROFILE_PATH", "/runtime/xray-active-profile.txt")
    active_name, active_profile, profiles = resolve_active_profile(registry_path, active_profile_path)
    metadata = {
        "mode": "legacy",
        "active_profile": active_name,
        "profiles": [profile_runtime_summary(item, is_active=item.get("name") == active_name) for item in profiles],
    }
    if active_profile:
        outbound = build_outbound_from_profile(active_profile)
        metadata["mode"] = "registry"
    else:
        outbound = build_legacy_outbound(env)
    config = {
        "log": {
            "loglevel": env.get("XRAY_LOG_LEVEL", "warning").strip() or "warning",
        },
        "inbounds": [
            {
                "tag": "socks-in",
                "listen": "0.0.0.0",
                "port": _as_int(env.get("XRAY_SOCKS_PORT"), 10808),
                "protocol": "socks",
                "settings": {
                    "udp": True,
                },
                "sniffing": {
                    "enabled": True,
                    "destOverride": ["http", "tls", "quic"],
                },
            }
        ],
        "outbounds": [
            outbound,
            {
                "tag": "direct",
                "protocol": "freedom",
                "settings": {},
            },
        ],
    }
    return config, metadata
