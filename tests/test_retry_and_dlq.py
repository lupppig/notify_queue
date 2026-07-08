import asyncio
import uuid
from datetime import UTC, datetime

from conftest import make_settings

from notify_queue.scheduler.scheduler import promote_due_jobs
from notify_queue.worker.claim import claim_next_job
from notify_queue.worker.delivery import execute_delivery


# Failed delivery attempts consume rate-limit budget, so retry tests raise the
# limit to keep rate limiting out of the picture (test_ratelimit covers it).
def failing_settings():
    return make_settings(delivery_failure_rate=1.0, rate_limit_per_hour=100)


async def claim_one(pool, redis, settings):
    await promote_due_jobs(pool, redis, settings)
    job_id = await claim_next_job(pool, redis, "w1", settings)
    assert job_id is not None
    return job_id


async def fail_until_dead(pool, redis, settings, max_attempts):
    for _ in range(max_attempts):
        job_id = await claim_one(pool, redis, settings)
        await execute_delivery(pool, redis, job_id, settings)
        row = await pool.fetchrow("SELECT status, send_at FROM jobs WHERE id = $1", job_id)
        if row["status"] == "pending":
            await asyncio.sleep(max(0.0, (row["send_at"] - datetime.now(UTC)).total_seconds()))
    return job_id


async def test_failure_schedules_retry_with_backoff(submit, pool, redis, webhook_receiver):
    received, url = webhook_receiver
    failing = failing_settings()
    await submit(callback_url=url)

    job_id = await claim_one(pool, redis, failing)
    before = datetime.now(UTC)
    await execute_delivery(pool, redis, job_id, failing)

    row = await pool.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)
    assert row["status"] == "pending"
    assert row["attempt_count"] == 1
    assert row["worker_id"] is None
    assert 0.5 <= (row["send_at"] - before).total_seconds() <= 1.5
    assert received[-1]["status"] == "failed"


async def test_backoff_doubles_per_attempt(submit, pool, redis):
    failing = failing_settings()
    await submit()
    delays = []
    for _ in range(2):
        job_id = await claim_one(pool, redis, failing)
        before = datetime.now(UTC)
        await execute_delivery(pool, redis, job_id, failing)
        send_at = await pool.fetchval("SELECT send_at FROM jobs WHERE id = $1", job_id)
        delays.append((send_at - before).total_seconds())
        await asyncio.sleep(max(0.0, (send_at - datetime.now(UTC)).total_seconds()))
    assert 0.5 <= delays[0] <= 1.5
    assert 1.5 <= delays[1] <= 2.5


async def test_exhausted_retries_dead_letter_with_dlq_row(submit, pool, redis, webhook_receiver):
    received, url = webhook_receiver
    failing = failing_settings()
    await submit(callback_url=url)

    job_id = await fail_until_dead(pool, redis, failing, failing.max_attempts)

    row = await pool.fetchrow("SELECT status, attempt_count FROM jobs WHERE id = $1", job_id)
    assert row["status"] == "dead_lettered"
    assert row["attempt_count"] == failing.max_attempts
    dlq = await pool.fetchrow("SELECT * FROM dead_letter_queue WHERE job_id = $1", job_id)
    assert dlq["attempt_count"] == failing.max_attempts
    assert dlq["recipient"] == "user@example.com"
    assert received[-1]["status"] == "dead_lettered"
    assert await redis.exists(f"job:lock:{job_id}") == 0


async def test_dead_lettered_job_can_be_replayed(submit, client, pool, redis):
    failing = failing_settings()
    succeeding = make_settings(rate_limit_per_hour=100)
    job_id = uuid.UUID((await submit()).json()["job_id"])
    await fail_until_dead(pool, redis, failing, failing.max_attempts)

    res = await client.post(f"/jobs/{job_id}/retry")
    assert res.status_code == 200

    replayed = await claim_one(pool, redis, succeeding)
    assert replayed == job_id
    await execute_delivery(pool, redis, replayed, succeeding)
    assert await pool.fetchval("SELECT status FROM jobs WHERE id = $1", job_id) == "sent"
