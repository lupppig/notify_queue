import asyncio
import contextlib
import uuid

import asyncpg

UPDATE_HEARTBEAT = """
UPDATE jobs SET heartbeat_at = NOW(), updated_at = NOW()
WHERE id = $1 AND worker_id = $2 AND status = 'claimed'
"""


async def heartbeat_loop(
    pool: asyncpg.Pool,
    job_id: uuid.UUID,
    worker_id: str,
    stop: asyncio.Event,
    interval_seconds: float,
) -> None:
    while not stop.is_set():
        await pool.execute(UPDATE_HEARTBEAT, job_id, worker_id)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
