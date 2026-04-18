# Runtime Modes

These modes are non-secret deployment overlays. They do not replace the real
server `.env`; they only override routing/tuning flags for Docker Compose.

## Modes

| Mode | Overlay | Vision | Text models | Fallbacks |
|---|---|---|---|---|
| `full-vision` | `.env.mode.full-vision.example` | enabled: GigaChat Vision, optional PaddleOCR | `GigaChat-2` first | Pro/Max enabled |
| `no-vision` | `.env.mode.no-vision.example` | disabled globally | `GigaChat-2` first | Pro/Max enabled |
| `gigachat-2-only` | `.env.mode.gigachat-2-only.example` | disabled globally | all chat tasks use `GigaChat-2` | disabled; Pro/Max routes point to `GigaChat-2` |

Per-source `extra.vision.mode` still exists for finer control:
`full`, `ocr_only`, `skip`. The global `VISION_ENABLED=false` wins over
per-source settings because the worker does not start `VisionTask`.

## Apply On Server

From the local workspace:

```powershell
.\scripts\sync-push.ps1
ssh frontier-intelligence
```

Or use the helper that syncs and runs the server command:

```powershell
.\scripts\apply-runtime-mode.ps1 full-vision
.\scripts\apply-runtime-mode.ps1 no-vision
.\scripts\apply-runtime-mode.ps1 gigachat-2-only
```

On the server:

```bash
cd /opt/frontier-intelligence

# Preview only:
bash scripts/server-apply-runtime-mode.sh full-vision --dry-run

# Apply one mode:
bash scripts/server-apply-runtime-mode.sh full-vision
bash scripts/server-apply-runtime-mode.sh no-vision
bash scripts/server-apply-runtime-mode.sh gigachat-2-only
```

The script loads the server-only `.env`, then overlays the selected example
file in-process. It does not write secrets and does not modify `.env`.

## What Changes

### `full-vision`

- `VISION_ENABLED=true`: worker starts `VisionTask`.
- `GPT2GIGA_ENABLE_IMAGES=True`: the proxy accepts image payloads.
- `GIGACHAT_VISION_MODEL=GigaChat-2-Pro`: image understanding goes through
  the configured Vision-capable route.
- `PADDLEOCR_URL=http://paddleocr:8008`: OCR is added when the `paddleocr`
  service is available.

Use this when visual evidence matters: screenshots, interface images,
infographics, charts, product photos, album posts.

### `no-vision`

- Media is still stored, but image analysis is skipped.
- Worker marks media posts with `vision_status=skipped`.
- Text enrichment, embeddings, Qdrant, Neo4j, MCP synthesis continue.
- Pro/Max fallbacks remain available for ambiguous or malformed text tasks.

Use this as the default lower-cost mode when captions/articles are enough.

### `gigachat-2-only`

- Same no-image behavior as `no-vision`.
- `GIGACHAT_ESCALATION_ENABLED=false`.
- `GIGACHAT_MODEL_PRO=GigaChat-2` and `GIGACHAT_MODEL_MAX=GigaChat-2`, so any
  explicit fallback path still stays on regular `GigaChat-2`.
- Embeddings still use `GIGACHAT_EMBEDDINGS_MODEL` such as `EmbeddingsGigaR`.

Expected trade-off: fewer expensive model calls and less rate-limit pressure,
but lower recall/precision on borderline relevance, thinner concept extraction,
and weaker recovery from malformed JSON responses. Image-only Telegram posts
will usually become weak or dropped unless their caption contains enough text.

## Verify

```bash
docker compose ps
docker compose logs -f worker
curl -sS http://127.0.0.1:8101/api/settings | jq '.runtime'
```

Useful metrics:

```promql
sum by (task, model, status) (increase(frontier_gigachat_requests_total[1h]))
sum by (task, from_model, to_model) (increase(frontier_gigachat_escalations_total[1h]))
```
