#!/usr/bin/env python3
"""
Проверка .env без вывода значений: только наличие и отсев явных плейсхолдеров.
Запуск на сервере: cd /opt/frontier-intelligence && python3 scripts/check_env.py
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# Совпадение с .env.example / типичными заглушками
PLACEHOLDER_PATTERNS = (
    re.compile(r"your_.*_here", re.I),
    re.compile(r"^changeme", re.I),
    re.compile(r"^placeholder", re.I),
    re.compile(r"^<.*>$"),
)

# Ключи, которые compose подставляет без дефолта (пусто → сломанный стек)
REQUIRED: dict[str, list[str]] = {
    "core": [
        "POSTGRES_PASSWORD",
        "GIGACHAT_CREDENTIALS",
        "NEO4J_PASSWORD",
    ],
    "worker": [
        "POSTGRES_PASSWORD",
        "NEO4J_PASSWORD",
        "S3_ENDPOINT_URL",
        "S3_BUCKET_NAME",
        "S3_ACCESS_KEY_ID",
        "S3_SECRET_ACCESS_KEY",
    ],
    "ingest": [
        "POSTGRES_PASSWORD",
        "TG_API_ID_0",
        "TG_API_HASH_0",
        "S3_ENDPOINT_URL",
        "S3_BUCKET_NAME",
        "S3_ACCESS_KEY_ID",
        "S3_SECRET_ACCESS_KEY",
    ],
    "xray": [
        "XRAY_VLESS_ADDRESS",
        "XRAY_VLESS_PORT",
        "XRAY_VLESS_ID",
        "XRAY_REALITY_SNI",
        "XRAY_REALITY_PBK",
        "XRAY_REALITY_SID",
    ],
}


def parse_env(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.is_file():
        return out
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
            val = val[1:-1]
        out[key] = val
    return out


def is_placeholder(value: str) -> bool:
    v = value.strip()
    if not v:
        return True
    for pat in PLACEHOLDER_PATTERNS:
        if pat.search(v):
            return True
    return False


def main() -> int:
    ap = argparse.ArgumentParser(description="Проверка ключей .env (без печати секретов)")
    ap.add_argument(
        "--env-file",
        type=Path,
        default=Path(".env"),
        help="Путь к .env (по умолчанию ./.env)",
    )
    ap.add_argument(
        "--profiles",
        nargs="*",
        default=["core", "worker", "ingest"],
        help="Профили compose: core worker ingest xray (по умолчанию core worker ingest)",
    )
    args = ap.parse_args()
    env_path: Path = args.env_file
    data = parse_env(env_path)
    if not data:
        print(f"FAIL: файл не найден или пуст: {env_path.resolve()}", file=sys.stderr)
        return 2

    profiles = set(args.profiles)
    keys_needed: set[str] = set()
    for p in profiles:
        for k in REQUIRED.get(p, []):
            keys_needed.add(k)

    problems: list[str] = []
    for key in sorted(keys_needed):
        val = data.get(key, "")
        if key not in data or not val.strip():
            problems.append(f"  отсутствует или пусто: {key}")
        elif is_placeholder(val):
            problems.append(f"  похоже на заглушку: {key}")

    # Частая ошибка переноса: склейка строк в одной линии
    for k, v in data.items():
        if "S3_" in v and k != v and k.upper().startswith(("POSTGRES", "DATABASE", "REDIS")):
            problems.append(
                f"  подозрительная строка (возможна склейка с S3_ в значении другого ключа): проверь {k}"
            )

    if problems:
        print(f"Проблемы в {env_path.resolve()}:")
        print("\n".join(problems))
        return 1

    print(f"OK: для профилей {', '.join(sorted(profiles))} обязательные ключи заданы (значения не показываются).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
