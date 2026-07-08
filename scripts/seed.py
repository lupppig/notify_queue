import argparse
import asyncio
import random
import uuid
from datetime import UTC, datetime, timedelta

import asyncpg

from notify_queue.config import Settings
from notify_queue.db import create_pool

CHANNELS = ("email", "sms", "push")
PRIORITIES = ("high", "medium", "low")

RECIPIENTS = tuple(
    f"{name}@example.com"
    for name in ("ada", "grace", "linus", "margaret", "alan", "barbara", "dennis", "katherine")
) + ("+2348012345670", "+2348012345671")

ERRORS = (
    "simulated delivery failure on channel email",
    "provider timeout after 5s",
    "recipient mailbox unavailable",
    "connection reset by provider",
)

INSERT_JOB = """
INSERT INTO jobs (recipient, channel, payload, send_at, priority, status, attempt_count,
                  max_attempts, next_retry_at, worker_id, claimed_at, heartbeat_at,
                  sent_at, failed_at, error_message, callback_url, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $17)
RETURNING id
"""

INSERT_DEAD_LETTER = """
INSERT INTO dead_letter_queue (job_id, recipient, channel, payload, attempt_count, last_error)
VALUES ($1, $2, $3, $4, $5, $6)
"""

INSERT_WEBHOOK_LOG = """
INSERT INTO webhook_log (job_id, callback_url, status_change, payload, http_status, fired_at)
VALUES ($1, $2, $3, $4, $5, $6)
"""

INSERT_IDEMPOTENCY = """
INSERT INTO job_idempotency (idempotency_key, job_id) VALUES ($1, $2)
"""


class Seeder:
    def __init__(self, pool: asyncpg.Pool, rng: random.Random) -> None:
        self.pool = pool
        self.rng = rng
        self.now = datetime.now(UTC)
        self.sequence = 0

    async def run(self, total: int) -> None:
        shares = {
            "sent": 0.50,
            "pending_future": 0.15,
            "retrying": 0.10,
            "queued": 0.05,
            "claimed": 0.05,
            "dead_lettered": 0.15,
        }
        counts = {kind: max(1, round(total * share)) for kind, share in shares.items()}
        for kind, count in counts.items():
            seed = getattr(self, f"seed_{kind}")
            for _ in range(count):
                await seed()
        print("seeded: " + " ".join(f"{kind}={count}" for kind, count in counts.items()))

    def base_job(self) -> dict:
        self.sequence += 1
        channel = self.rng.choice(CHANNELS)
        return {
            "recipient": self.rng.choice(RECIPIENTS),
            "channel": channel,
            "payload": self.payload_for(channel),
            "priority": self.rng.choice(PRIORITIES),
            "callback_url": (
                "https://example.com/hooks/notify" if self.rng.random() < 0.5 else None
            ),
            "created_at": self.now - timedelta(minutes=self.rng.randint(5, 72 * 60)),
        }

    def payload_for(self, channel: str) -> dict:
        n = self.sequence
        if channel == "email":
            return {"subject": f"Order #{1000 + n} confirmed", "body": "Thanks for your order."}
        if channel == "sms":
            return {"message": f"Your verification code is {100000 + n}"}
        return {"title": "Package update", "body": f"Shipment #{1000 + n} is out for delivery"}

    async def insert(self, job: dict, **overrides) -> uuid.UUID:
        row = {
            "status": "pending",
            "send_at": job["created_at"],
            "attempt_count": 0,
            "max_attempts": 5,
            "next_retry_at": None,
            "worker_id": None,
            "claimed_at": None,
            "heartbeat_at": None,
            "sent_at": None,
            "failed_at": None,
            "error_message": None,
            **overrides,
        }
        job_id = await self.pool.fetchval(
            INSERT_JOB,
            job["recipient"],
            job["channel"],
            job["payload"],
            row["send_at"],
            job["priority"],
            row["status"],
            row["attempt_count"],
            row["max_attempts"],
            row["next_retry_at"],
            row["worker_id"],
            row["claimed_at"],
            row["heartbeat_at"],
            row["sent_at"],
            row["failed_at"],
            row["error_message"],
            job["callback_url"],
            job["created_at"],
        )
        if self.rng.random() < 0.2:
            await self.pool.execute(INSERT_IDEMPOTENCY, f"seed-{job_id}", job_id)
        return job_id

    async def log_webhook(self, job_id: uuid.UUID, job: dict, status: str, at: datetime) -> None:
        if not job["callback_url"]:
            return
        payload = {"job_id": str(job_id), "status": status, "timestamp": at.isoformat()}
        await self.pool.execute(
            INSERT_WEBHOOK_LOG, job_id, job["callback_url"], status, payload, 200, at
        )

    async def seed_sent(self) -> None:
        job = self.base_job()
        sent_at = job["created_at"] + timedelta(seconds=self.rng.randint(1, 300))
        job_id = await self.insert(
            job,
            status="sent",
            attempt_count=self.rng.choices((0, 1, 2), weights=(8, 2, 1))[0],
            sent_at=sent_at,
        )
        await self.log_webhook(job_id, job, "sent", sent_at)

    async def seed_pending_future(self) -> None:
        job = self.base_job()
        await self.insert(job, send_at=self.now + timedelta(minutes=self.rng.randint(10, 48 * 60)))

    async def seed_retrying(self) -> None:
        job = self.base_job()
        attempt = self.rng.randint(1, 3)
        next_retry = self.now + timedelta(seconds=30 * 2 ** (attempt - 1))
        await self.insert(
            job,
            attempt_count=attempt,
            send_at=next_retry,
            next_retry_at=next_retry,
            error_message=self.rng.choice(ERRORS),
        )

    async def seed_queued(self) -> None:
        job = self.base_job()
        await self.insert(job, status="queued", send_at=self.now)

    async def seed_claimed(self) -> None:
        job = self.base_job()
        await self.insert(
            job,
            status="claimed",
            send_at=self.now - timedelta(seconds=5),
            worker_id=f"seed-host-{self.rng.randint(1000, 9999)}-0-{uuid.uuid4().hex[:6]}",
            claimed_at=self.now,
            heartbeat_at=self.now,
        )

    async def seed_dead_lettered(self) -> None:
        job = self.base_job()
        error = self.rng.choice(ERRORS)
        failed_at = job["created_at"] + timedelta(minutes=self.rng.randint(10, 60))
        job_id = await self.insert(
            job,
            status="dead_lettered",
            attempt_count=5,
            failed_at=failed_at,
            error_message=error,
        )
        await self.pool.execute(
            INSERT_DEAD_LETTER, job_id, job["recipient"], job["channel"], job["payload"], 5, error
        )
        await self.log_webhook(job_id, job, "dead_lettered", failed_at)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed the database with realistic job data")
    parser.add_argument("--total", type=int, default=100)
    parser.add_argument("--wipe", action="store_true", help="truncate all tables first")
    parser.add_argument("--seed", type=int, default=None, help="rng seed for reproducible data")
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    pool = await create_pool(Settings().database_url)
    try:
        if args.wipe:
            await pool.execute("TRUNCATE webhook_log, dead_letter_queue, job_idempotency, jobs")
            print("wiped existing data")
        await Seeder(pool, random.Random(args.seed)).run(args.total)
        rows = await pool.fetch("SELECT status, COUNT(*) AS count FROM jobs GROUP BY status")
        print("database now: " + " ".join(f"{r['status']}={r['count']}" for r in rows))
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
