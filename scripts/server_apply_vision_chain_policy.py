#!/usr/bin/env python3
"""Применить цепочку vision: Wormsoft (WORMSOFT_VISION_MODEL при ключе) → OpenRouter free → Polza → GigaChat vision/Pro.

Запускать на сервере из /opt/frontier-intelligence (или указать OPENROUTER_* через .env).
Если POLZA_VISION_MODEL пуст — дописывает в .env безопасный дефолт VL-модели Polza.
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from typing import Any
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.admin_api_auth import basic_auth_header  # noqa: E402

ROOT = Path("/opt/frontier-intelligence")
ENV_PATH = ROOT / ".env"
POLICY_URL = "http://127.0.0.1:8101/api/settings/policy"
# Дефолт при отсутствии POLZA_VISION_MODEL: VL-модель Polza (публичная карточка qwen/qwen3-vl-30b-a3b-instruct).
DEFAULT_POLZA_VISION = "qwen/qwen3-vl-30b-a3b-instruct"


def parse_env(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.is_file():
        return out
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key:
            out[key] = val
    return out


def ensure_polza_vision_model(env: dict[str, str]) -> str:
    vm = str(env.get("POLZA_VISION_MODEL") or "").strip()
    if vm:
        return vm
    backup = ENV_PATH.with_suffix(".env.bak.polza_vision")
    text = ENV_PATH.read_text(encoding="utf-8") if ENV_PATH.is_file() else ""
    backup.write_text(text, encoding="utf-8")
    append = f"\n# scripts/server_apply_vision_chain_policy.py — добавлено автоматически\nPOLZA_VISION_MODEL={DEFAULT_POLZA_VISION}\n"
    ENV_PATH.write_text(text.rstrip() + append + "\n", encoding="utf-8")
    print(f"POLZA_VISION_MODEL was empty; set to {DEFAULT_POLZA_VISION} (backup {backup})")
    return DEFAULT_POLZA_VISION


def main() -> None:
    env = parse_env(ENV_PATH)
    polza_vm = ensure_polza_vision_model(env)
    env = parse_env(ENV_PATH)

    giga_vis = str(env.get("GIGACHAT_MODEL_VISION") or "").strip()
    if not giga_vis:
        giga_vis = str(env.get("GIGACHAT_MODEL_PRO") or "GigaChat-2-Pro").strip() or "GigaChat-2-Pro"

    # Админка закрыта авторизацией: без заголовка первый же GET отдаёт 401.
    auth_headers = basic_auth_header()

    req_get = urllib.request.Request(POLICY_URL, method="GET", headers=auth_headers)
    with urllib.request.urlopen(req_get, timeout=60) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    effective = payload.get("effective") or {}
    if not effective:
        raise SystemExit("policy payload missing effective")

    worm_key = str(env.get("WORMSOFT_API_KEY") or "").strip()
    worm_vis = str(env.get("WORMSOFT_VISION_MODEL") or "").strip()
    vision_candidates: list[dict[str, Any]] = []
    if worm_key and worm_vis:
        vision_candidates.append(
            {
                "provider": "wormsoft",
                "model": worm_vis,
                "enabled": True,
                "capability_tags": ["vision", "text"],
                "budget_class": "primary",
                "notes": "",
            }
        )
    vision_candidates.extend(
        [
            {
                "provider": "openrouter",
                "model": "openrouter/free",
                "enabled": True,
                "capability_tags": ["vision", "text"],
                "budget_class": "exploratory",
                "notes": "",
            },
            {
                "provider": "polza",
                "model": polza_vm,
                "enabled": True,
                "capability_tags": ["vision", "text"],
                "budget_class": "fallback",
                "notes": "",
            },
            {
                "provider": "gigachat",
                "model": giga_vis,
                "enabled": True,
                "capability_tags": ["vision", "text"],
                "budget_class": "trusted",
                "notes": "",
            },
        ]
    )

    effective["vision_generation"] = {
        "family": "vision_generation",
        "mode": str(effective.get("vision_generation", {}).get("mode") or "degraded"),
        "fallback_exception_only": bool(
            effective.get("vision_generation", {}).get("fallback_exception_only", True)
        ),
        "candidates": vision_candidates,
    }

    body = {
        "version": effective.get("version", "v2"),
        "default_mode": effective.get("default_mode", "degraded"),
        "text_generation": effective["text_generation"],
        "vision_generation": effective["vision_generation"],
        "embeddings": effective["embeddings"],
        "updated_at": time.time(),
    }

    data = json.dumps(body).encode("utf-8")
    req_post = urllib.request.Request(
        POLICY_URL,
        data=data,
        headers={"Content-Type": "application/json", **auth_headers},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req_post, timeout=120) as resp:
            out = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        err_body = exc.read().decode("utf-8", errors="replace")[:2000]
        raise SystemExit(f"POST policy HTTP {exc.code}: {err_body}") from exc

    eff = out.get("effective") or {}
    vgen = eff.get("vision_generation") or {}
    print("policy_source=", out.get("source"))
    print("vision_generation candidates:")
    for c in vgen.get("candidates") or []:
        print(f"  - {c.get('provider')} / {c.get('model')} enabled={c.get('enabled')}")

    sim_url = "http://127.0.0.1:8101/api/settings/simulate"
    sim_body = json.dumps({"task": "vision"}).encode("utf-8")
    req_sim = urllib.request.Request(
        sim_url,
        data=sim_body,
        headers={"Content-Type": "application/json", **auth_headers},
        method="POST",
    )
    with urllib.request.urlopen(req_sim, timeout=60) as resp:
        sim = json.loads(resp.read().decode("utf-8"))
    dec = (sim.get("decision") or {}) if isinstance(sim, dict) else {}
    print(
        "simulate_vision selected=",
        dec.get("selected_provider"),
        dec.get("selected_model"),
        "considered=",
        len(dec.get("considered_candidates") or []),
    )
    skipped = dec.get("skipped_candidates") or []
    if skipped:
        print("simulate_vision skipped:", skipped[:8])


if __name__ == "__main__":
    main()
