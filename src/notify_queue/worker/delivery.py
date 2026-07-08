"""Job delivery pipeline: fetch, rate-limit check, deliver, and handle outcomes."""

import asyncio
import logging
import random
import uuid
from datetime import UTC, datetime, timedelta

import asyncpg
import redis.asyncio as redis

from notify_queue.config import Settings
from notify_queue.dlq import dead_letter
from notify_queue.ratelimit import check_rate_limit
from notify_queue.webhooks import fire_webhook
from notify_queue.worker.heartbeat import heartbeat_loop

logger = logging.getLogger(__name__)

FETCH_JOB = """
SELECT * FROM jobs WHERE id = $1
"""

# Every terminal transition is fenced on ownership (worker_id + claimed): a
# worker that stalled long enough to be reclaimed by the scheduler must not
# overwrite state that now belongs to another worker.
MARK_SENT = """
UPDATE jobs SET status = 'sent', sent_at = NOW(), updated_at = NOW()
WHERE id = $1 AND worker_id = $2 AND status = 'claimed'
RETURNING id
"""

SCHEDULE_RETRY = """
UPDATE jobs
SET status = 'pending', attempt_count = $1, next_retry_at = $2, send_at = $2,
    error_message = $3, worker_id = NULL, claimed_at = NULL, heartbeat_at = NULL,
    updated_at = NOW()
WHERE id = $4 AND worker_id = $5 AND status = 'claimed'
RETURNING id
"""

DEFER_TO_NEXT_WINDOW = """
UPDATE jobs
SET status = 'pending', send_at = $1, worker_id = NULL, claimed_at = NULL,
    heartbeat_at = NULL, updated_at = NOW()
WHERE id = $2 AND worker_id = $3 AND status = 'claimed'
RETURNING id
"""


async def process_job(
    pool: asyncpg.Pool,
    redis_client: redis.Redis,
    job_id: uuid.UUID,
    worker_id: str,
    settings: Settings,
) -> None:
    """Run delivery for a single job with a concurrent heartbeat.

    A background heartbeat task updates ``heartbeat_at`` every interval while
    the delivery executes.  The heartbeat is cancelled once delivery completes
    (successfully or not).
    """
    stop = asyncio.Event()
    heartbeat = asyncio.create_task(
        heartbeat_loop(pool, job_id, worker_id, stop, settings.heartbeat_interval_seconds)
    )
    try:
        await execute_delivery(pool, redis_client, job_id, settings)
    finally:
        stop.set()
        await heartbeat


async def execute_delivery(
    pool: asyncpg.Pool, redis_client: redis.Redis, job_id: uuid.UUID, settings: Settings
) -> None:
    """Fetch the job, enforce the rate limit, and attempt delivery.

    Rate limiting happens at delivery time: a rate-limited job is deferred
    to the next window, never failed (DESIGN.md §9).
    """
    job = await pool.fetchrow(FETCH_JOB, job_id)

    allowed, retry_after_seconds = await check_rate_limit(
        redis_client, job["recipient"], settings.rate_limit_per_hour
    )
    if not allowed:
        await defer_job(pool, redis_client, job, retry_after_seconds)
        return

    if random.random() < settings.delivery_failure_rate:
        error = f"simulated delivery failure on channel {job['channel']}"
        await handle_failure(pool, redis_client, job, error, settings)
    else:
        await handle_success(pool, redis_client, job, settings)


async def handle_success(
    pool: asyncpg.Pool, redis_client: redis.Redis, job: asyncpg.Record, settings: Settings
) -> None:
    """Mark the job as sent (fenced), release its Redis lock, and fire the webhook."""
    updated = await pool.fetchrow(MARK_SENT, job["id"], job["worker_id"])
    if updated is None:
        logger.warning("job %s no longer owned; skipping sent transition", job["id"])
        return
    await redis_client.delete(f"job:lock:{job['id']}")
    await fire_webhook(
        pool,
        job["id"],
        job["callback_url"],
        "sent",
        timeout=settings.webhook_timeout_seconds,
        max_attempts=settings.webhook_max_attempts,
    )


async def handle_failure(
    pool: asyncpg.Pool,
    redis_client: redis.Redis,
    job: asyncpg.Record,
    error: str,
    settings: Settings,
) -> None:
    """Schedule an exponential-backoff retry, or dead-letter if attempts are exhausted.

    Resetting ``send_at`` routes the retry through the scheduler's normal
    promotion path — no separate retry queue (DESIGN.md §7.6).
    """
    attempt = job["attempt_count"] + 1
    if attempt >= job["max_attempts"]:
        await dead_letter(
            pool,
            redis_client,
            job,
            error,
            attempt,
            settings,
            expected_worker_id=job["worker_id"],
        )
        return

    delay = settings.base_retry_delay_seconds * 2 ** (attempt - 1)
    next_retry_at = datetime.now(UTC) + timedelta(seconds=delay)
    updated = await pool.fetchrow(
        SCHEDULE_RETRY, attempt, next_retry_at, error, job["id"], job["worker_id"]
    )
    if updated is None:
        logger.warning("job %s no longer owned; skipping retry transition", job["id"])
        return
    await redis_client.delete(f"job:lock:{job['id']}")
    await fire_webhook(
        pool,
        job["id"],
        job["callback_url"],
        "failed",
        timeout=settings.webhook_timeout_seconds,
        max_attempts=settings.webhook_max_attempts,
    )


async def defer_job(
    pool: asyncpg.Pool,
    redis_client: redis.Redis,
    job: asyncpg.Record,
    retry_after_seconds: int,
) -> None:
    """Defer a rate-limited job to the next hourly window.

    Deferral is not a failure: ``attempt_count`` is untouched and no webhook
    fires.  The job returns to ``pending`` with ``send_at`` at the top of the
    next window.
    """
    next_window = datetime.now(UTC) + timedelta(seconds=retry_after_seconds)
    updated = await pool.fetchrow(DEFER_TO_NEXT_WINDOW, next_window, job["id"], job["worker_id"])
    if updated is None:
        logger.warning("job %s no longer owned; skipping deferral", job["id"])
        return
    await redis_client.delete(f"job:lock:{job['id']}")
