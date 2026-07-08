import uuid


async def test_send_at_and_delay_seconds_are_mutually_exclusive(submit):
    res = await submit(send_at="2030-01-01T00:00:00Z", delay_seconds=10)
    assert res.status_code == 422


async def test_delay_seconds_resolves_relative_send_at(submit):
    res = await submit(delay_seconds=3600)
    assert res.status_code == 201
    assert res.json()["status"] == "pending"


async def test_absurd_delay_is_rejected_not_500(submit):
    res = await submit(delay_seconds=10**20)
    assert res.status_code == 422


async def test_unknown_job_returns_404(client):
    res = await client.get(f"/jobs/{uuid.uuid4()}/status")
    assert res.status_code == 404


async def test_status_roundtrip(submit, client):
    job_id = (await submit()).json()["job_id"]
    res = await client.get(f"/jobs/{job_id}/status")
    body = res.json()
    assert body == {
        "job_id": job_id,
        "status": "pending",
        "attempt_count": 0,
        "sent_at": None,
        "error_message": None,
    }


async def test_metrics_count_jobs_by_status(submit, client):
    await submit()
    await submit(delay_seconds=3600)
    counts = (await client.get("/metrics")).json()
    assert counts["pending"] == 2
    assert sum(counts.values()) == 2


async def test_retrying_a_non_dead_lettered_job_conflicts(submit, client):
    job_id = (await submit()).json()["job_id"]
    res = await client.post(f"/jobs/{job_id}/retry")
    assert res.status_code == 409
