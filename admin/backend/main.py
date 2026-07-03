"""Admin UI backend — FastAPI serving API + static frontend."""
import base64
import hashlib
import hmac
import logging
import os
import secrets
import sys
import time
from contextlib import asynccontextmanager

sys.path.insert(0, "/app")

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from admin.backend.db import get_engine
from admin.backend.services.llm_finops import fetch_llm_finops_snapshot
from admin.backend.scheduler import scheduler_lifespan
from admin.backend.scheduler import manual_job_metrics_snapshot
from admin.backend.scheduler import scheduler_status
from shared.config import get_settings
from shared.metrics import set_admin_manual_job_metrics
from shared.metrics import set_admin_scheduler_running
from shared.metrics import set_last_post_age
from shared.metrics import set_llm_finops_snapshot
from shared.metrics import set_redis_stream_metrics
from shared.redis_streams import collect_redis_stream_snapshot

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with scheduler_lifespan():
        yield


app = FastAPI(title="Frontier Intelligence Admin", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=get_settings().allowed_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Auth: cookie-сессия (SPA-friendly) ИЛИ HTTP Basic (curl/интеграции). ---
# Статика фронта (/, /static, catch-all) публична — SPA грузится и показывает форму входа.
# Данные /api/* требуют авторизации. 401 отдаётся JSON'ом БЕЗ WWW-Authenticate,
# чтобы браузер не всплывал native-диалогом на XHR (иначе fetch подвисает).
_COOKIE_NAME = "fadmin_session"
_SESSION_TTL = 7 * 24 * 3600


def _admin_user() -> str:
    return os.environ.get("ADMIN_USER", "admin")


def _sign(payload: str) -> str:
    key = os.environ.get("ADMIN_PASSWORD", "").encode()
    return hmac.new(key, payload.encode(), hashlib.sha256).hexdigest()


def _make_session_token(user: str) -> str:
    exp = int(time.time()) + _SESSION_TTL
    payload = f"{user}:{exp}"
    raw = f"{payload}:{_sign(payload)}"
    return base64.urlsafe_b64encode(raw.encode()).decode()


def _verify_session_token(token: str) -> str | None:
    try:
        raw = base64.urlsafe_b64decode(token.encode()).decode()
        user, exp, sig = raw.rsplit(":", 2)
        if not hmac.compare_digest(sig, _sign(f"{user}:{exp}")):
            return None
        if int(exp) < time.time():
            return None
        return user
    except Exception:
        return None


def _check_basic(request: Request, expected_pw: str) -> bool:
    header = request.headers.get("authorization", "")
    if not header.startswith("Basic "):
        return False
    try:
        user, _, pw = base64.b64decode(header[6:]).decode("utf-8").partition(":")
        return secrets.compare_digest(user, _admin_user()) and secrets.compare_digest(pw, expected_pw)
    except Exception:
        return False


def _path_needs_auth(path: str) -> bool:
    if path == "/metrics":
        return False
    if not path.startswith("/api/"):
        return False  # статика/SPA — публичны
    if path == "/api/health" or path == "/api/auth/login":
        return False
    if path == "/api/monitoring/alertmanager/webhook":
        return False
    return True


@app.middleware("http")
async def _auth(request: Request, call_next):
    if request.method == "OPTIONS" or not _path_needs_auth(request.url.path):
        return await call_next(request)

    expected_pw = os.environ.get("ADMIN_PASSWORD", "")
    if not expected_pw:
        logging.getLogger(__name__).error("ADMIN_PASSWORD не задан — admin API заблокирован")
        return Response("admin auth not configured", status_code=503)

    token = request.cookies.get(_COOKIE_NAME)
    if (token and _verify_session_token(token)) or _check_basic(request, expected_pw):
        return await call_next(request)

    return Response('{"detail":"unauthorized"}', status_code=401, media_type="application/json")


@app.middleware("http")
async def _no_cache_frontend(request: Request, call_next):
    # Статика/SPA — no-cache (браузер ревалидирует), чтобы правки фронта подхватывались
    # сразу и не оставался «белый экран» от закэшированной старой версии.
    resp = await call_next(request)
    path = request.url.path
    if not path.startswith("/api/") and path != "/metrics":
        resp.headers["Cache-Control"] = "no-cache, must-revalidate"
    return resp


# API routers
from admin.backend.routers.workspaces import router as ws_router
from admin.backend.routers.sources import router as src_router
from admin.backend.routers.pipeline import router as pipeline_router
from admin.backend.routers.posts import router as posts_router
from admin.backend.routers.albums import router as albums_router
from admin.backend.routers.media import router as media_router
from admin.backend.routers.settings import router as settings_router
from admin.backend.routers.graph import router as graph_router
from admin.backend.routers.search import router as search_router
from admin.backend.routers.clusters import router as clusters_router
from admin.backend.routers.monitoring import router as monitoring_router

app.include_router(ws_router, prefix="/api/workspaces", tags=["workspaces"])
app.include_router(src_router, prefix="/api/sources", tags=["sources"])
app.include_router(pipeline_router, prefix="/api/pipeline", tags=["pipeline"])
app.include_router(posts_router, prefix="/api/posts", tags=["posts"])
app.include_router(albums_router, prefix="/api/albums", tags=["albums"])
app.include_router(media_router, prefix="/api/media", tags=["media"])
app.include_router(settings_router, prefix="/api/settings", tags=["settings"])
app.include_router(graph_router, prefix="/api/graph", tags=["graph"])
app.include_router(search_router, prefix="/api/search", tags=["search"])
app.include_router(clusters_router, prefix="/api/clusters", tags=["clusters"])
app.include_router(monitoring_router, prefix="/api/monitoring", tags=["monitoring"])


@app.get("/api/health")
async def health():
    return {"status": "ok"}


@app.post("/api/auth/login")
async def auth_login(payload: dict, response: Response):
    username = str(payload.get("username", ""))
    password = str(payload.get("password", ""))
    expected_pw = os.environ.get("ADMIN_PASSWORD", "")
    if not expected_pw:
        raise HTTPException(status_code=503, detail="admin auth not configured")
    if secrets.compare_digest(username, _admin_user()) and secrets.compare_digest(password, expected_pw):
        response.set_cookie(
            _COOKIE_NAME, _make_session_token(username),
            httponly=True, samesite="lax", max_age=_SESSION_TTL, path="/",
        )
        return {"ok": True, "user": username}
    raise HTTPException(status_code=401, detail="Неверный логин или пароль")


@app.get("/api/auth/me")
async def auth_me(request: Request):
    # Middleware уже пропустил → авторизован. Отдаём текущего пользователя.
    token = request.cookies.get(_COOKIE_NAME)
    user = _verify_session_token(token) if token else None
    return {"authenticated": True, "user": user or _admin_user()}


@app.post("/api/auth/logout")
async def auth_logout(response: Response):
    response.delete_cookie(_COOKIE_NAME, path="/")
    return {"ok": True}


async def _refresh_last_post_age_metric() -> None:
    """Update frontier_last_post_age_seconds per workspace.

    Isolated in its own try so a Postgres failure never blocks the other
    metric refreshes in /metrics.
    """
    try:
        engine = get_engine()
        async with AsyncSession(engine) as session:
            result = await session.execute(
                text(
                    """
                    SELECT
                        workspace_id,
                        EXTRACT(EPOCH FROM (NOW() - MAX(COALESCE(published_at, created_at)))) AS age_seconds
                    FROM posts
                    GROUP BY workspace_id
                    """
                )
            )
            ages = {
                str(row.workspace_id): float(row.age_seconds)
                for row in result.all()
                if row.age_seconds is not None
            }
        set_last_post_age("admin", ages)
    except Exception:
        logging.getLogger(__name__).exception("Failed to refresh last-post-age metric")


@app.get("/metrics")
async def metrics():
    try:
        scheduler = scheduler_status()
        set_admin_scheduler_running("admin", bool(scheduler.get("running")))
        set_admin_manual_job_metrics(
            "admin",
            await manual_job_metrics_snapshot(),
        )
        set_llm_finops_snapshot("admin", await fetch_llm_finops_snapshot())
        stream_snapshot = await collect_redis_stream_snapshot(get_settings().redis_url)
        set_redis_stream_metrics("admin", stream_snapshot)
        await _refresh_last_post_age_metric()
    except Exception:
        logging.getLogger(__name__).exception("Failed to refresh admin metrics snapshot")
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# Serve frontend
app.mount("/static", StaticFiles(directory="/app/admin/frontend"), name="static")


@app.get("/")
@app.get("/{path:path}")
async def frontend(path: str = ""):
    if path == "metrics":
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
    return FileResponse("/app/admin/frontend/index.html")


if __name__ == "__main__":
    import uvicorn
    from shared.config import get_settings
    settings = get_settings()
    uvicorn.run(app, host="0.0.0.0", port=settings.admin_port)
