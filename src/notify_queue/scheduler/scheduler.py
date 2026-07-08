import uuid
from datetime import datetime

import asyncpg
import redis.asyncio as redis

from notify_queue.config import Settings
from notify_queue.dlq import dead_letter

# Weights must dominate Unix timestamps (~1.7e9) so a high-priority job always
# scores below a lower-priority one regardless of send_at.
PRIORITY_WEIGHTS = {"high": 0, "medium": 10_000_000_000, "low": 20_000_000_000}

PROMOTE_DUE_JOBS = """
UPDATE jobs
SET status = 'queued', updated_at = NOW()
WHERE id IN (
    SELECT id FROM jobs
    WHERE status = 'pending'
      AND send_at <= NOW() + make_interval(secs => $1)
    ORDER BY priority ASC, send_at ASC
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
RETURNING id, priority, send_at
"""

RECOVER_STALE_CLAIMED = """
UPDATE jobs
SET status = 'pending',
    attempt_count = attempt_count + 1,
    error_message = 'worker died mid-processing (heartbeat timeout)',
    worker_id = NULL, claimed_at = NULL, heartbeat_at = NULL, updated_at = NOW()
WHERE status = 'claimed'
  AND heartbeat_at < NOW() - make_interval(secs => $1)
RETURNING id, recipient, channel, payload, attempt_count, max_attempts,
          callback_url, error_message
"""

REQUEUE_STALE_QUEUED = """
SELECT id, priority, send_at FROM jobs
WHERE status = 'queued'
  AND updated_at < NOW() - make_interval(secs => $1)
LIMIT $2
"""


def priority_score(priority: str, send_at: datetime) -> float:
    return PRIORITY_WEIGHTS[priority] + send_at.timestamp()


async def _enqueue(redis_client: redis.Redis, rows: list[asyncpg.Record]) -> None:
    pipe = redis_client.pipeline()
    for row in rows:
        score = priority_score(row["priority"], row["send_at"])
        pipe.zadd(f"queue:{row['priority']}", {str(row["id"]): score})
    await pipe.execute()


async def promote_due_jobs(
    pool: asyncpg.Pool, redis_client: redis.Redis, settings: Settings
) -> list[uuid.UUID]:
    rows = await pool.fetch(
        PROMOTE_DUE_JOBS, settings.scheduler_lookahead_seconds, settings.scheduler_batch_size
    )
    if rows:
        await _enqueue(redis_client, rows)
    return [row["id"] for row in rows]


async def recover_stale_claimed_jobs(
    pool: asyncpg.Pool, redis_client: redis.Redis, settings: Settings
) -> list[uuid.UUID]:
    # The reclaim counts as a failed attempt: a hard-crashed worker never runs
    # handle_failure, so this increment is what dead-letters poison messages.
    rows = await pool.fetch(RECOVER_STALE_CLAIMED, settings.heartbeat_timeout_seconds)
    for job in rows:
        await redis_client.delete(f"job:lock:{job['id']}")
        if job["attempt_count"] >= job["max_attempts"]:
            await dead_letter(
                pool, redis_client, job, job["error_message"], job["attempt_count"], settings
            )
    return [row["id"] for row in rows]


async def requeue_stale_queued_jobs(
    pool: asyncpg.Pool, redis_client: redis.Redis, settings: Settings
) -> list[uuid.UUID]:
    # Rescues jobs marked 'queued' that never reached Redis (scheduler crash
    # between the status update and ZADD, or Redis data loss — DESIGN.md §10.4).
    # ZADD is idempotent per member, so re-adding a job still in the queue is a no-op.
    rows = await pool.fetch(
        REQUEUE_STALE_QUEUED, settings.queued_requeue_seconds, settings.scheduler_batch_size
    )
    if rows:
        await _enqueue(redis_client, rows)
    return [row["id"] for row in rows]
