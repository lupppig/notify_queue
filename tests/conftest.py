import asyncio
import socket
from pathlib import Path

import asyncpg
import pytest
import uvicorn
from fastapi import FastAPI, Request
from httpx import ASGITransport, AsyncClient

from notify_queue.api.app import create_app
from notify_queue.config import Settings
from notify_queue.db import create_pool
from notify_queue.log import setup_logging
from notify_queue.redis_client import create_redis

setup_logging("test", log_file="test.log")

ADMIN_DATABASE_URL = "postgresql://notify:notify@localhost:5433/notifications"
TEST_DATABASE_URL = "postgresql://notify:notify@localhost:5433/notifications_test"
TEST_REDIS_URL = "redis://localhost:6380/1"
TABLES = "webhook_log, dead_letter_queue, job_idempotency, jobs"


def make_settings(**overrides) -> Settings:
    # Timings are shrunk so tests exercise real timeouts without real waits.
    defaults = dict(
        database_url=TEST_DATABASE_URL,
        redis_url=TEST_REDIS_URL,
        rate_limit_per_hour=2,
        delivery_failure_rate=0.0,
        max_attempts=3,
        base_retry_delay_seconds=1,
        scheduler_poll_interval_ms=50,
        scheduler_lookahead_seconds=5,
        queued_requeue_seconds=1,
        heartbeat_interval_seconds=0.2,
        heartbeat_timeout_seconds=1,
        worker_count=1,
        job_lock_ttl_seconds=5,
        webhook_timeout_seconds=2,
        webhook_max_attempts=1,
    )
    defaults.update(overrides)
    return Settings(**defaults)


async def _prepare_database() -> None:
    admin = await asyncpg.connect(ADMIN_DATABASE_URL)
    try:
        await admin.execute("DROP DATABASE IF EXISTS notifications_test")
        await admin.execute("CREATE DATABASE notifications_test")
    finally:
        await admin.close()
    conn = await asyncpg.connect(TEST_DATABASE_URL)
    try:
        migration = Path(__file__).parents[1] / "migrations" / "001_initial.sql"
        await conn.execute(migration.read_text())
    finally:
        await conn.close()


@pytest.fixture(scope="session", autouse=True)
def _database():
    asyncio.run(_prepare_database())


@pytest.fixture
async def pool():
    pool = await create_pool(TEST_DATABASE_URL)
    await pool.execute(f"TRUNCATE {TABLES}")
    yield pool
    await pool.close()


@pytest.fixture
async def redis():
    client = create_redis(TEST_REDIS_URL)
    await client.flushdb()
    yield client
    await client.aclose()


@pytest.fixture
def settings():
    return make_settings()


@pytest.fixture
async def client(pool, redis, settings):
    app = create_app(settings)
    app.state.settings = settings
    app.state.pool = pool
    app.state.redis = redis
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.fixture
def submit(client):
    async def _submit(**overrides):
        body = {
            "recipient": "user@example.com",
            "channel": "email",
            "payload": {"subject": "hi"},
            "delay_seconds": 0,
            **overrides,
        }
        return await client.post("/jobs", json=body)

    return _submit


@pytest.fixture
async def webhook_receiver():
    received: list[dict] = []
    capture = FastAPI()

    @capture.post("/hook")
    async def hook(request: Request):
        received.append(await request.json())
        return {}

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    server = uvicorn.Server(uvicorn.Config(capture, host="127.0.0.1", port=port, log_level="error"))
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.01)
    yield received, f"http://127.0.0.1:{port}/hook"
    server.should_exit = True
    await task
