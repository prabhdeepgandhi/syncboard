import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.db.mongodb import connect_db, close_db, get_db
from app.api.v1.router import api_router
from app.services.change_stream_worker import start_change_stream_worker
from app.core.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_change_stream_task = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _change_stream_task
    await connect_db()

    redis_client = None
    try:
        import redis.asyncio as aioredis
        redis_client = aioredis.from_url(settings.redis_url, decode_responses=True)
        await redis_client.ping()
        logger.info("Redis connected.")
    except Exception as e:
        logger.warning(f"Redis unavailable, change stream will log only: {e}")

    db = get_db()
    _change_stream_task = asyncio.create_task(
        start_change_stream_worker(db, redis_client)
    )

    yield

    _change_stream_task.cancel()
    try:
        await _change_stream_task
    except asyncio.CancelledError:
        pass
    await close_db()
    if redis_client:
        await redis_client.close()


app = FastAPI(
    title="SyncBoard API",
    description="Collaborative workspace API with hierarchical documents, RBAC, and real-time sync.",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(api_router)


@app.get("/health")
async def health():
    return {"status": "ok"}
