"""Scheduler entry point: single-loop process that promotes, recovers, and requeues."""

import asyncio
import contextlib
import logging
import signal

from notify_queue.config import Settings
from notify_queue.db import create_pool
from notify_queue.log import setup_logging
from notify_queue.redis_client import create_redis
from notify_queue.scheduler.scheduler import (
    promote_due_jobs,
    recover_stale_claimed_jobs,
    requeue_stale_queued_jobs,
)

logger = logging.getLogger("notify_queue.scheduler")


async def main() -> None:
    """Run the scheduler loop: promote → recover → requeue → sleep, repeating until signalled.

    The scheduler is the one process the system cannot lose; a failed tick
    is logged and retried on the next poll interval.
    """
    setup_logging("scheduler")
    settings = Settings()
    pool = await create_pool(settings.database_url)
    redis_client = create_redis(settings.redis_url)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    logger.info("scheduler started, polling every %dms", settings.scheduler_poll_interval_ms)
    try:
        while not stop.is_set():
            try:
                promoted = await promote_due_jobs(pool, redis_client, settings)
                recovered = await recover_stale_claimed_jobs(pool, redis_client, settings)
                requeued = await requeue_stale_queued_jobs(pool, redis_client, settings)
                if promoted or recovered or requeued:
                    logger.info(
                        "promoted=%d recovered=%d requeued=%d",
                        len(promoted),
                        len(recovered),
                        len(requeued),
                    )
            except Exception:
                logger.exception("scheduler tick failed")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    stop.wait(), timeout=settings.scheduler_poll_interval_ms / 1000
                )
    finally:
        await redis_client.aclose()
        await pool.close()
        logger.info("scheduler stopped")


if __name__ == "__main__":
    asyncio.run(main())
