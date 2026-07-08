import logging
import uuid
from typing import Any

import asyncpg
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from notify_queue.models import JobCreate, JobStatus

logger = logging.getLogger(__name__)
router = APIRouter()

JOB_STATUSES = ("pending", "queued", "claimed", "sent", "failed", "dead_lettered")

INSERT_JOB = """
INSERT INTO jobs (recipient, channel, payload, send_at, priority, max_attempts, callback_url)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING id, status, send_at
"""

INSERT_IDEMPOTENCY_KEY = """
INSERT INTO job_idempotency (idempotency_key, job_id) VALUES ($1, $2)
"""

SELECT_EXISTING_JOB_ID = """
SELECT job_id FROM job_idempotency WHERE idempotency_key = $1
"""

SELECT_JOB_STATUS = """
SELECT id, status, attempt_count, sent_at, error_message FROM jobs WHERE id = $1
"""

SELECT_METRICS = """
SELECT status, COUNT(*) AS count FROM jobs GROUP BY status
"""

SELECT_RECENT_JOBS = """
SELECT id, recipient, channel, priority, status, attempt_count, max_attempts,
       send_at, sent_at, error_message, updated_at
FROM jobs
WHERE $1::job_status IS NULL OR status = $1
ORDER BY updated_at DESC
LIMIT $2
"""

RETRY_DEAD_LETTERED_JOB = """
UPDATE jobs
SET status = 'pending', attempt_count = 0, send_at = NOW(), next_retry_at = NULL,
    error_message = NULL, failed_at = NULL, worker_id = NULL, claimed_at = NULL,
    heartbeat_at = NULL, updated_at = NOW()
WHERE id = $1 AND status = 'dead_lettered'
RETURNING id
"""


@router.post("/jobs", status_code=201)
async def create_job(body: JobCreate, request: Request) -> Any:
    pool = request.app.state.pool
    settings = request.app.state.settings
    send_at = body.resolved_send_at()
    async with pool.acquire() as conn:
        try:
            async with conn.transaction():
                job = await conn.fetchrow(
                    INSERT_JOB,
                    body.recipient,
                    body.channel,
                    body.payload,
                    send_at,
                    body.priority,
                    settings.max_attempts,
                    body.callback_url,
                )
                if body.idempotency_key:
                    await conn.execute(INSERT_IDEMPOTENCY_KEY, body.idempotency_key, job["id"])
        except asyncpg.UniqueViolationError:
            existing = await conn.fetchval(SELECT_EXISTING_JOB_ID, body.idempotency_key)
            return JSONResponse(
                status_code=409,
                content={"error": "duplicate_job", "existing_job_id": str(existing)},
            )
    return {
        "job_id": str(job["id"]),
        "status": job["status"],
        "send_at": job["send_at"].isoformat(),
    }


@router.get("/jobs/{job_id}/status")
async def job_status(job_id: uuid.UUID, request: Request) -> dict[str, Any]:
    row = await request.app.state.pool.fetchrow(SELECT_JOB_STATUS, job_id)
    if row is None:
        raise HTTPException(status_code=404, detail="job not found")
    return {
        "job_id": str(row["id"]),
        "status": row["status"],
        "attempt_count": row["attempt_count"],
        "sent_at": row["sent_at"].isoformat() if row["sent_at"] else None,
        "error_message": row["error_message"],
    }


@router.get("/metrics")
async def metrics(request: Request) -> dict[str, int]:
    rows = await request.app.state.pool.fetch(SELECT_METRICS)
    counts = dict.fromkeys(JOB_STATUSES, 0)
    counts.update({row["status"]: row["count"] for row in rows})
    return counts


@router.get("/jobs")
async def list_jobs(
    request: Request,
    status: JobStatus | None = None,
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, list[dict[str, Any]]]:
    rows = await request.app.state.pool.fetch(SELECT_RECENT_JOBS, status, limit)
    return {"jobs": [_serialize_job(row) for row in rows]}


@router.post("/jobs/{job_id}/retry")
async def retry_dead_lettered_job(job_id: uuid.UUID, request: Request) -> dict[str, str]:
    pool = request.app.state.pool
    row = await pool.fetchrow(RETRY_DEAD_LETTERED_JOB, job_id)
    if row is None:
        exists = await pool.fetchval("SELECT 1 FROM jobs WHERE id = $1", job_id)
        if exists is None:
            raise HTTPException(status_code=404, detail="job not found")
        raise HTTPException(status_code=409, detail="only dead_lettered jobs can be retried")
    return {"job_id": str(job_id), "status": "pending"}


@router.post("/webhook-mock")
async def webhook_mock(request: Request) -> dict[str, bool]:
    body = await request.json()
    logger.info("webhook-mock received %s", body)
    return {"received": True}


def _serialize_job(row: asyncpg.Record) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "recipient": row["recipient"],
        "channel": row["channel"],
        "priority": row["priority"],
        "status": row["status"],
        "attempt_count": row["attempt_count"],
        "max_attempts": row["max_attempts"],
        "send_at": row["send_at"].isoformat(),
        "sent_at": row["sent_at"].isoformat() if row["sent_at"] else None,
        "error_message": row["error_message"],
        "updated_at": row["updated_at"].isoformat(),
    }
