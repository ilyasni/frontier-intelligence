#!/usr/bin/env bash
set -euo pipefail

MODELS="${*:-GigaChat-2-Max GigaChat-Max}"
export MODELS

docker exec -e MODELS="$MODELS" -i frontier-intelligence-worker-1 python - <<'PY'
import json
import os
import urllib.error
import urllib.request

base = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO2X8XcAAAAASUVORK5CYII="
url = "http://gpt2giga-proxy:8090/v1/chat/completions"
models = os.environ.get("MODELS", "").split()
headers = {"Content-Type": "application/json"}

for model in models:
    payload = {
        "model": model,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{base}"}},
                {"type": "text", "text": "Верни пустой JSON объект {}"},
            ],
        }],
        "temperature": 0.1,
        "max_tokens": 128,
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers=headers,
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = resp.read().decode()
            print(f"{model}\t{resp.status}\t{body[:400]}")
    except urllib.error.HTTPError as exc:
        print(f"{model}\t{exc.code}\t{exc.read().decode()[:400]}")
PY
