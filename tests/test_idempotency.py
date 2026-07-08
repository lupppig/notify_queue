import asyncio


async def test_duplicate_key_returns_409_with_original_job(submit):
    first = await submit(idempotency_key="order-1")
    dup = await submit(idempotency_key="order-1")
    assert first.status_code == 201
    assert dup.status_code == 409
    assert dup.json()["existing_job_id"] == first.json()["job_id"]


async def test_concurrent_duplicates_create_exactly_one_job(submit, pool):
    responses = await asyncio.gather(*(submit(idempotency_key="order-2") for _ in range(5)))
    created = [r for r in responses if r.status_code == 201]
    conflicts = [r for r in responses if r.status_code == 409]
    assert len(created) == 1
    assert len(conflicts) == 4
    assert await pool.fetchval("SELECT COUNT(*) FROM jobs") == 1
    winner = created[0].json()["job_id"]
    assert all(r.json()["existing_job_id"] == winner for r in conflicts)


async def test_different_keys_create_separate_jobs(submit, pool):
    await submit(idempotency_key="a")
    await submit(idempotency_key="b")
    assert await pool.fetchval("SELECT COUNT(*) FROM jobs") == 2
