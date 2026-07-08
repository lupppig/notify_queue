import asyncio
import contextlib
import logging
import os
import signal
import socket
import uuid

import asyncpg
import redis.asyncio as redis

from notify_queue.config import Settings
from notify_queue.db import create_pool
from notify_queue.redis_client import create_redis
from notify_queue.worker.claim import claim_next_job
from notify_queue.worker.delivery import process_job

logger = logging.getLogger("notify_queue.worker")


async def worker_loop(
    pool: asyncpg.Pool,
    redis_client: redis.Redis,
    worker_id: str,
    settings: Settings,
    stop: asyncio.Event,
) -> None:
    logger.info("%s started", worker_id)
    while not stop.is_set():
        job_id = await claim_next_job(pool, redis_client, worker_id, settings)
        if job_id is None:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=settings.worker_idle_sleep_seconds)
            continue
        logger.info("%s processing %s", worker_id, job_id)
        await process_job(pool, redis_client, job_id, worker_id, settings)
    logger.info("%s stopped", worker_id)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    settings = Settings()
    pool = await create_pool(settings.database_url)
    redis_client = create_redis(settings.redis_url)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    base = f"{socket.gethostname()}-{os.getpid()}"
    workers = [
        asyncio.create_task(
            worker_loop(pool, redis_client, f"{base}-{i}-{uuid.uuid4().hex[:6]}", settings, stop)
        )
        for i in range(settings.worker_count)
    ]
    await asyncio.gather(*workers)
    await redis_client.aclose()
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
