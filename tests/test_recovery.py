from notify_queue.scheduler.scheduler import recover_stale_claimed_jobs
from notify_queue.worker.delivery import handle_failure, handle_success

INSERT_CLAIMED_JOB = """
INSERT INTO jobs (recipient, channel, payload, send_at, status, attempt_count,
                  max_attempts, worker_id, claimed_at, heartbeat_at)
VALUES ($1, 'email', '{}', NOW(), 'claimed', $2, $3, 'dead-worker',
        NOW(), NOW() - make_interval(secs => $4))
RETURNING id
"""


async def test_stale_claim_is_reclaimed_and_attempt_counted(pool, redis, settings):
    job_id = await pool.fetchval(INSERT_CLAIMED_JOB, "user@example.com", 0, 3, 10)
    recovered = await recover_stale_claimed_jobs(pool, redis, settings)
    assert job_id in recovered
    row = await pool.fetchrow(
        "SELECT status, attempt_count, worker_id, heartbeat_at FROM jobs WHERE id = $1", job_id
    )
    assert row["status"] == "pending"
    assert row["attempt_count"] == 1
    assert row["worker_id"] is None
    assert row["heartbeat_at"] is None


async def test_fresh_heartbeat_is_not_reclaimed(pool, redis, settings):
    await pool.fetchval(INSERT_CLAIMED_JOB, "user@example.com", 0, 3, 0)
    assert await recover_stale_claimed_jobs(pool, redis, settings) == []


async def test_poison_job_dead_letters_at_attempt_cap(pool, redis, settings):
    # A job that crashes its worker never runs handle_failure; the reclaim
    # increment is the only thing that can exhaust its attempts.
    job_id = await pool.fetchval(INSERT_CLAIMED_JOB, "user@example.com", 2, 3, 10)
    await recover_stale_claimed_jobs(pool, redis, settings)
    assert await pool.fetchval("SELECT status FROM jobs WHERE id = $1", job_id) == "dead_lettered"
    dlq = await pool.fetchrow("SELECT * FROM dead_letter_queue WHERE job_id = $1", job_id)
    assert dlq["attempt_count"] == 3
    assert "heartbeat timeout" in dlq["last_error"]


async def test_reclaim_removes_dead_workers_lock(pool, redis, settings):
    job_id = await pool.fetchval(INSERT_CLAIMED_JOB, "user@example.com", 0, 3, 10)
    await redis.set(f"job:lock:{job_id}", "dead-worker", ex=60)
    await recover_stale_claimed_jobs(pool, redis, settings)
    assert await redis.exists(f"job:lock:{job_id}") == 0


RECLAIM_JOB = """
UPDATE jobs
SET status = 'pending', worker_id = NULL, attempt_count = attempt_count + 1,
    heartbeat_at = NULL, updated_at = NOW()
WHERE id = $1
"""


async def stale_claim(pool, attempt_count=0, max_attempts=3):
    # A worker's view of the job before the scheduler reclaims it out from
    # under it — the zombie-worker race (DESIGN.md §10.3).
    job_id = await pool.fetchval(
        INSERT_CLAIMED_JOB, "user@example.com", attempt_count, max_attempts, 10
    )
    stale_view = await pool.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)
    await pool.execute(RECLAIM_JOB, job_id)
    return job_id, stale_view


async def test_zombie_worker_cannot_mark_a_reclaimed_job_sent(pool, redis, settings):
    job_id, stale_view = await stale_claim(pool)
    await handle_success(pool, redis, stale_view, settings)
    row = await pool.fetchrow("SELECT status, sent_at FROM jobs WHERE id = $1", job_id)
    assert row["status"] == "pending"
    assert row["sent_at"] is None


async def test_zombie_worker_cannot_reschedule_a_reclaimed_job(pool, redis, settings):
    job_id, stale_view = await stale_claim(pool)
    await handle_failure(pool, redis, stale_view, "late failure", settings)
    row = await pool.fetchrow(
        "SELECT status, attempt_count, next_retry_at, error_message FROM jobs WHERE id = $1",
        job_id,
    )
    assert row["status"] == "pending"
    assert row["attempt_count"] == 1
    # The reclaim path leaves both NULL; an unfenced SCHEDULE_RETRY would have
    # set them, so NULL is what proves the fence held.
    assert row["next_retry_at"] is None
    assert row["error_message"] is None


async def test_zombie_worker_cannot_dead_letter_a_reclaimed_job(pool, redis, settings):
    job_id, stale_view = await stale_claim(pool, attempt_count=2, max_attempts=3)
    await handle_failure(pool, redis, stale_view, "late failure at cap", settings)
    assert await pool.fetchval("SELECT status FROM jobs WHERE id = $1", job_id) == "pending"
    assert (
        await pool.fetchval("SELECT COUNT(*) FROM dead_letter_queue WHERE job_id = $1", job_id) == 0
    )
