"""Admin UI backend — FastAPI serving API + static frontend."""
import logging
import sys
from contextlib import asynccontextmanager

sys.path.insert(0, "/app")

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from admin.backend.scheduler import scheduler_lifespan
from admin.backend.scheduler import manual_job_metrics_snapshot
from admin.backend.scheduler import scheduler_status
from shared.config import get_settings
from shared.metrics import set_admin_manual_job_metrics
from shared.metrics import set_admin_scheduler_running
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


@app.get("/metrics")
async def metrics():
    try:
        scheduler = scheduler_status()
        set_admin_scheduler_running("admin", bool(scheduler.get("running")))
        set_admin_manual_job_metrics(
            "admin",
            await manual_job_metrics_snapshot(),
        )
        stream_snapshot = await collect_redis_stream_snapshot(get_settings().redis_url)
        set_redis_stream_metrics("admin", stream_snapshot)
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
