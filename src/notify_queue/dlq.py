"""Dead-letter queue: terminal handling for jobs that exhaust their retry budget."""

import logging

import asyncpg
import redis.asyncio as redis

from notify_queue.config import Settings
from notify_queue.webhooks import fire_webhook

logger = logging.getLogger(__name__)

# Fenced on the caller's ownership: a worker passes its own worker_id, the
# scheduler's recovery path passes None for the job it has just reset.
MARK_DEAD_LETTERED = """
UPDATE jobs
SET status = 'dead_lettered', error_message = $1, attempt_count = $3, failed_at = NOW(),
    worker_id = NULL, claimed_at = NULL, heartbeat_at = NULL, updated_at = NOW()
WHERE id = $2 AND worker_id IS NOT DISTINCT FROM $4
RETURNING id
"""

INSERT_DEAD_LETTER = """
INSERT INTO dead_letter_queue (job_id, recipient, channel, payload, attempt_count, last_error)
VALUES ($1, $2, $3, $4, $5, $6)
"""


async def dead_letter(
    pool: asyncpg.Pool,
    redis_client: redis.Redis,
    job: asyncpg.Record,
    error: str,
    attempt_count: int,
    settings: Settings,
    *,
    expected_worker_id: str | None,
) -> None:
    """Move a job to the dead-letter queue and fire the ``dead_lettered`` webhook.

    The status update and DLQ insert commit in a single transaction so a job
    can never be marked ``dead_lettered`` without a matching DLQ row
    (DESIGN.md §7.7).
    """
    async with pool.acquire() as conn, conn.transaction():
        marked = await conn.fetchrow(
            MARK_DEAD_LETTERED, error, job["id"], attempt_count, expected_worker_id
        )
        if marked is None:
            logger.warning("job %s no longer owned; skipping dead-letter", job["id"])
            return
        await conn.execute(
            INSERT_DEAD_LETTER,
            job["id"],
            job["recipient"],
            job["channel"],
            job["payload"],
            attempt_count,
            error,
        )
    await redis_client.delete(f"job:lock:{job['id']}")
    await fire_webhook(
        pool,
        job["id"],
        job["callback_url"],
        "dead_lettered",
        timeout=settings.webhook_timeout_seconds,
        max_attempts=settings.webhook_max_attempts,
    )
