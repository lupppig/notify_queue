import asyncio

from conftest import make_settings

from notify_queue.scheduler.scheduler import promote_due_jobs
from notify_queue.worker.__main__ import worker_loop

JOB_COUNT = 20
WORKER_COUNT = 5


async def test_no_duplicate_delivery_under_concurrent_workers(
    submit, pool, redis, webhook_receiver
):
    # Every successful delivery fires exactly one 'sent' webhook, so a
    # duplicate delivery would show up as a second webhook_log row.
    received, url = webhook_receiver
    settings = make_settings(rate_limit_per_hour=1000)

    job_ids = []
    for i in range(JOB_COUNT):
        res = await submit(recipient=f"user{i}@example.com", callback_url=url)
        job_ids.append(res.json()["job_id"])
    await promote_due_jobs(pool, redis, settings)

    # Adversarial double-enqueue (DESIGN.md §10.1): every job also appears in a
    # second queue, as if a crashed scheduler had re-promoted the whole batch.
    for job_id in job_ids:
        await redis.zadd("queue:high", {job_id: 0})

    stop = asyncio.Event()
    workers = [
        asyncio.create_task(worker_loop(pool, redis, f"worker-{i}", settings, stop))
        for i in range(WORKER_COUNT)
    ]
    try:
        for _ in range(600):
            sent = await pool.fetchval("SELECT COUNT(*) FROM jobs WHERE status = 'sent'")
            queued = sum([await redis.zcard(f"queue:{p}") for p in ("high", "medium", "low")])
            if sent == JOB_COUNT and queued == 0:
                break
            await asyncio.sleep(0.05)
        else:
            raise AssertionError(f"delivered {sent}/{JOB_COUNT}, {queued} queue entries left")
    finally:
        stop.set()
        await asyncio.gather(*workers)

    per_job = await pool.fetch(
        "SELECT job_id, COUNT(*) AS fires FROM webhook_log "
        "WHERE status_change = 'sent' GROUP BY job_id"
    )
    assert len(per_job) == JOB_COUNT
    assert all(row["fires"] == 1 for row in per_job)

    assert len(received) == JOB_COUNT
    assert len({hook["job_id"] for hook in received}) == JOB_COUNT
