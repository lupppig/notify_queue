from datetime import UTC, datetime, timedelta

from notify_queue.ratelimit import check_rate_limit, seconds_until_next_window, window_key
from notify_queue.scheduler.scheduler import promote_due_jobs
from notify_queue.worker.claim import claim_next_job
from notify_queue.worker.delivery import execute_delivery


async def test_excess_delivery_defers_instead_of_failing(
    submit, pool, redis, settings, webhook_receiver
):
    received, url = webhook_receiver
    for _ in range(3):
        await submit(recipient="busy@example.com", callback_url=url)

    for _ in range(3):
        await promote_due_jobs(pool, redis, settings)
        job_id = await claim_next_job(pool, redis, "w1", settings)
        await execute_delivery(pool, redis, job_id, settings)

    rows = await pool.fetch("SELECT status, attempt_count, send_at FROM jobs")
    sent = [r for r in rows if r["status"] == "sent"]
    deferred = [r for r in rows if r["status"] == "pending"]
    assert len(sent) == settings.rate_limit_per_hour == 2
    assert len(deferred) == 1
    assert deferred[0]["attempt_count"] == 0

    next_window = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    assert deferred[0]["send_at"] >= next_window

    assert [w["status"] for w in received] == ["sent", "sent"]


async def test_rate_limit_counter_always_has_a_ttl(redis):
    await check_rate_limit(redis, "someone@example.com", 10)
    key = window_key("someone@example.com", datetime.now(UTC))
    assert 0 < await redis.ttl(key) <= 7200


async def test_rejected_check_does_not_consume_budget(redis):
    for _ in range(2):
        allowed, _ = await check_rate_limit(redis, "r@example.com", 2)
        assert allowed
    for _ in range(3):
        allowed, retry_after = await check_rate_limit(redis, "r@example.com", 2)
        assert not allowed
        assert retry_after > 0
    key = window_key("r@example.com", datetime.now(UTC))
    assert int(await redis.get(key)) == 2


def test_deferral_lands_inside_the_next_window():
    now = datetime(2026, 7, 8, 10, 59, 59, tzinfo=UTC)
    later = now + timedelta(seconds=seconds_until_next_window(now))
    assert later.hour == 11
    assert window_key("r", later) != window_key("r", now)
