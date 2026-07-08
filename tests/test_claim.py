import asyncio
import uuid

from notify_queue.scheduler.scheduler import promote_due_jobs
from notify_queue.worker.claim import claim_next_job


async def test_one_job_is_claimed_by_exactly_one_worker(submit, pool, redis, settings):
    await submit()
    await promote_due_jobs(pool, redis, settings)
    claims = await asyncio.gather(
        *(claim_next_job(pool, redis, f"worker-{i}", settings) for i in range(5))
    )
    assert len([c for c in claims if c is not None]) == 1


async def test_high_priority_queue_is_drained_first(submit, pool, redis, settings):
    low_id = (await submit(priority="low")).json()["job_id"]
    high_id = (await submit(priority="high")).json()["job_id"]
    await promote_due_jobs(pool, redis, settings)
    assert str(await claim_next_job(pool, redis, "w1", settings)) == high_id
    assert str(await claim_next_job(pool, redis, "w1", settings)) == low_id


async def test_stale_queue_entry_is_rejected_by_status_gate(submit, pool, redis, settings):
    job_id = (await submit()).json()["job_id"]
    await promote_due_jobs(pool, redis, settings)
    assert await claim_next_job(pool, redis, "w1", settings) is not None

    # Simulate a double-enqueue (scheduler crash between ZADD and commit):
    # the same id reappears in Redis while the job is already claimed.
    await redis.delete(f"job:lock:{job_id}")
    await redis.zadd("queue:medium", {job_id: 0})
    assert await claim_next_job(pool, redis, "w2", settings) is None
    row = await pool.fetchrow("SELECT status, worker_id FROM jobs WHERE id = $1", uuid.UUID(job_id))
    assert row["status"] == "claimed"
    assert row["worker_id"] == "w1"


async def test_existing_lock_blocks_second_claim(submit, pool, redis, settings):
    job_id = (await submit()).json()["job_id"]
    await promote_due_jobs(pool, redis, settings)
    await redis.set(f"job:lock:{job_id}", "another-worker", ex=60)
    assert await claim_next_job(pool, redis, "w1", settings) is None
