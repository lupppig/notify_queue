from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from notify_queue.api.routes import router
from notify_queue.config import Settings
from notify_queue.db import create_pool
from notify_queue.redis_client import create_redis

WEB_DIR = Path(__file__).resolve().parents[3] / "web"


def create_app(settings: Settings | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.settings = settings or Settings()
        app.state.pool = await create_pool(app.state.settings.database_url)
        app.state.redis = create_redis(app.state.settings.redis_url)
        yield
        await app.state.redis.aclose()
        await app.state.pool.close()

    app = FastAPI(title="notify_queue", lifespan=lifespan)
    app.include_router(router)
    if WEB_DIR.is_dir():
        app.mount("/", StaticFiles(directory=WEB_DIR, html=True), name="web")
    return app


app = create_app()
