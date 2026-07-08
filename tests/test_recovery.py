from notify_queue.scheduler.scheduler import recover_stale_claimed_jobs

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
