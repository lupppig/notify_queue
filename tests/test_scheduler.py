import asyncio
import uuid
from datetime import UTC, datetime

from notify_queue.scheduler.scheduler import (
    priority_score,
    promote_due_jobs,
    requeue_stale_queued_jobs,
)


async def test_due_job_is_promoted_with_composite_score(submit, pool, redis, settings):
    job_id = (await submit(priority="high")).json()["job_id"]
    promoted = await promote_due_jobs(pool, redis, settings)
    assert [str(j) for j in promoted] == [job_id]
    row = await pool.fetchrow("SELECT status, send_at FROM jobs WHERE id = $1", uuid.UUID(job_id))
    assert row["status"] == "queued"
    send_at = row["send_at"]
    assert await redis.zscore("queue:high", job_id) == priority_score("high", send_at)


async def test_future_job_is_not_promoted(submit, pool, redis, settings):
    await submit(delay_seconds=3600)
    assert await promote_due_jobs(pool, redis, settings) == []


def test_high_priority_always_scores_below_lower_priorities():
    high_far_future = priority_score("high", datetime(2096, 1, 1, tzinfo=UTC))
    medium_past = priority_score("medium", datetime(2020, 1, 1, tzinfo=UTC))
    low_past = priority_score("low", datetime(2020, 1, 1, tzinfo=UTC))
    assert high_far_future < medium_past < low_past


async def test_concurrent_promoters_never_double_promote(submit, pool, redis, settings):
    for _ in range(10):
        await submit()
    results = await asyncio.gather(
        promote_due_jobs(pool, redis, settings),
        promote_due_jobs(pool, redis, settings),
    )
    assert sum(len(r) for r in results) == 10


async def test_queued_job_missing_from_redis_is_requeued(submit, pool, redis, settings):
    job_id = (await submit()).json()["job_id"]
    await promote_due_jobs(pool, redis, settings)
    await redis.flushdb()
    await asyncio.sleep(1.1)
    requeued = await requeue_stale_queued_jobs(pool, redis, settings)
    assert [str(j) for j in requeued] == [job_id]
    assert await redis.zcard("queue:medium") == 1
