import asyncio
import logging
import uuid
from datetime import UTC, datetime

import asyncpg
import httpx

logger = logging.getLogger(__name__)

INSERT_WEBHOOK_LOG = """
INSERT INTO webhook_log (job_id, callback_url, status_change, payload, http_status, attempt)
VALUES ($1, $2, $3, $4, $5, $6)
"""


async def fire_webhook(
    pool: asyncpg.Pool,
    job_id: uuid.UUID,
    callback_url: str | None,
    status: str,
    *,
    timeout: float,
    max_attempts: int,
) -> None:
    # Best-effort: webhook failure never affects the job's delivery status (DESIGN.md §8).
    if not callback_url:
        return
    payload = {
        "job_id": str(job_id),
        "status": status,
        "timestamp": datetime.now(UTC).isoformat(),
    }
    for attempt in range(1, max_attempts + 1):
        http_status = None
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(callback_url, json=payload)
            http_status = response.status_code
        except httpx.HTTPError as exc:
            logger.warning("webhook attempt %d to %s failed: %s", attempt, callback_url, exc)
        await pool.execute(
            INSERT_WEBHOOK_LOG, job_id, callback_url, status, payload, http_status, attempt
        )
        if http_status is not None and http_status < 500:
            return
        if attempt < max_attempts:
            await asyncio.sleep(2**attempt)
