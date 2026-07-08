import uuid

import asyncpg
import redis.asyncio as redis

from notify_queue.config import Settings

PRIORITY_ORDER = ("high", "medium", "low")

CLAIM_QUEUED_JOB = """
UPDATE jobs
SET status = 'claimed', worker_id = $1, claimed_at = NOW(), heartbeat_at = NOW(),
    updated_at = NOW()
WHERE id = $2 AND status = 'queued'
RETURNING id
"""


async def claim_next_job(
    pool: asyncpg.Pool, redis_client: redis.Redis, worker_id: str, settings: Settings
) -> uuid.UUID | None:
    # Three independent exactly-once gates (DESIGN.md §7.2): atomic ZPOPMIN,
    # a SET NX lock, and the conditional status transition in PostgreSQL.
    for priority in PRIORITY_ORDER:
        popped = await redis_client.zpopmin(f"queue:{priority}", count=1)
        if not popped:
            continue
        job_id = uuid.UUID(popped[0][0])

        lock_key = f"job:lock:{job_id}"
        acquired = await redis_client.set(
            lock_key, worker_id, nx=True, ex=settings.job_lock_ttl_seconds
        )
        if not acquired:
            continue

        claimed = await pool.fetchrow(CLAIM_QUEUED_JOB, worker_id, job_id)
        if claimed is None:
            await redis_client.delete(lock_key)
            continue

        return job_id
    return None
