# FINN Distributed Delayed Job & Notification Delivery System

**Author:** Darasimi Kelani  
**Version:** 1.0  
**Status:** Design Proposal

---

## 1. Problem Statement

Build a distributed notification delivery system that:

- Schedules jobs for future delivery (time-based or delay-based)
- Guarantees exactly-once delivery across concurrent workers
- Supports priority ordering (high jobs always before low)
- Retries failed jobs with exponential backoff
- Dead-letters jobs that exhaust retries
- Rate-limits notifications per recipient per hour
- Fires webhook callbacks on every status change
- Exposes job status and system metrics endpoints
- Scales horizontally without coordination overhead

---

## 2. Why Task Scheduler and Job Executor Are Separate

This is the most important architectural decision in this system.

A single process that both schedules and executes jobs creates a fatal coupling: if the process is busy executing a slow job, it cannot check for newly due jobs. At scale, a backlog of slow jobs would cause time-sensitive notifications to miss their delivery windows silently.

The separation works as follows:

**Task Scheduler** — a lightweight process that runs on a tight polling loop (every 500ms). Its only job is to scan PostgreSQL for jobs whose `send_at <= NOW()` and whose status is `pending`, and promote them into the Redis priority queues. It does no I/O beyond database reads and Redis writes. It is fast, predictable, and stateless.

**Job Executor (Workers)** — a pool of processes that consume from Redis queues, execute delivery, handle retries, fire webhooks, and update job status. Workers do the slow, failure-prone work. They are horizontally scalable and crash-safe.

Because they are separate, you can scale workers independently of the scheduler. You can have one scheduler and fifty workers. If workers back up, the scheduler continues running without being affected. If the scheduler restarts, jobs already in Redis queues continue processing uninterrupted.

The scheduler is the clock. The workers are the hands.

---

## 3. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Client                               │
└─────────────────────────┬───────────────────────────────────┘
                          │ HTTP
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                     FastAPI Layer                           │
│  POST /jobs            GET /jobs/{id}/status               │
│  GET /metrics          POST /webhook-mock (stub receiver)  │
└───────────┬─────────────────────────────────────────────────┘
            │
     ┌──────▼──────┐        ┌─────────────────┐
     │  Idempotency │        │   Rate Limiter  │
     │  Check (PG)  │        │  (Redis INCR)   │
     └──────┬──────┘        └────────┬────────┘
            │                        │
            └───────────┬────────────┘
                        │ write job → PostgreSQL
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                    PostgreSQL (job store)                   │
│  jobs · job_idempotency · dead_letter_queue · webhook_log  │
└───────────────────────┬─────────────────────────────────────┘
                        │ poll send_at <= NOW()
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                   Task Scheduler                            │
│  Runs every 500ms · promotes due jobs to Redis queues      │
│  Updates job status: pending → queued                      │
└───────────────────────┬─────────────────────────────────────┘
                        │ ZADD with score = priority_ts
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              Redis Priority Queues                          │
│  queue:high    queue:medium    queue:low                   │
│  Score = composite: priority_weight + send_at timestamp    │
└───────────────────────┬─────────────────────────────────────┘
                        │ ZPOPMIN (atomic claim)
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              Worker Pool (N instances)                      │
│  Heartbeat · Distributed lock · Delivery stub · Retry DLQ  │
└───────────┬─────────────────────────────────────────────────┘
            │ status change
            ▼
┌─────────────────────────────────────────────────────────────┐
│              Webhook Dispatcher                             │
│  POST to callback_url · log to webhook_log · retry on 5xx  │
└─────────────────────────────────────────────────────────────┘
```

---

## 4. Data Model

### 4.1 jobs

```sql
CREATE TYPE job_status AS ENUM (
    'pending',
    'queued',
    'claimed',
    'sent',
    'failed',
    'dead_lettered'
);

CREATE TYPE channel_type AS ENUM ('email', 'sms', 'push');

CREATE TYPE priority_level AS ENUM ('high', 'medium', 'low');

CREATE TABLE jobs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    recipient           TEXT NOT NULL,
    channel             channel_type NOT NULL,
    payload             JSONB NOT NULL,
    send_at             TIMESTAMPTZ NOT NULL,
    priority            priority_level NOT NULL DEFAULT 'medium',
    status              job_status NOT NULL DEFAULT 'pending',
    attempt_count       INTEGER NOT NULL DEFAULT 0,
    max_attempts        INTEGER NOT NULL DEFAULT 5,
    next_retry_at       TIMESTAMPTZ,
    worker_id           TEXT,
    claimed_at          TIMESTAMPTZ,
    heartbeat_at        TIMESTAMPTZ,
    sent_at             TIMESTAMPTZ,
    failed_at           TIMESTAMPTZ,
    error_message       TEXT,
    callback_url        TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_jobs_scheduler ON jobs (send_at, status)
    WHERE status IN ('pending', 'failed');

CREATE INDEX idx_jobs_heartbeat ON jobs (heartbeat_at, status)
    WHERE status = 'claimed';

CREATE INDEX idx_jobs_recipient ON jobs (recipient, status);
```

**Why `heartbeat_at`?** Workers update this column every 10 seconds while processing a job. If a worker crashes, the scheduler detects that `heartbeat_at < NOW() - 30s` for a claimed job and reclaims it. Without this, crashed jobs stay claimed forever and are never delivered. This is the heartbeat mechanism.

**Why `worker_id`?** Each worker instance has a unique ID (hostname + PID + UUID). The scheduler uses this for crash detection. It also makes debugging trivial: you can query which worker claimed any job.

**Why `next_retry_at` on the jobs table?** Retry scheduling is part of the job record, not a separate table. This avoids a join on every retry check and makes the retry state visible in a single row.

### 4.2 job_idempotency

```sql
CREATE TABLE job_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    job_id          UUID NOT NULL REFERENCES jobs(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**Why a separate table?** The idempotency key is provided by the client and may be a natural key (e.g. an order ID). It does not belong on the jobs table because the same job_id should never have two different idempotency keys, and the same idempotency key should never map to two job_ids. A separate table with a primary key constraint enforces this at the database level without application-level coordination.

### 4.3 dead_letter_queue

```sql
CREATE TABLE dead_letter_queue (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          UUID NOT NULL REFERENCES jobs(id),
    recipient       TEXT NOT NULL,
    channel         channel_type NOT NULL,
    payload         JSONB NOT NULL,
    attempt_count   INTEGER NOT NULL,
    last_error      TEXT,
    dead_lettered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**Why copy fields from jobs?** The DLQ is a terminal state. A job that reaches the DLQ may be retried manually or inspected by support teams. Copying key fields means the DLQ is self-contained: you can read it without joining back to the jobs table, and you can archive or purge the jobs table later without losing DLQ data.

### 4.4 webhook_log

```sql
CREATE TABLE webhook_log (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          UUID NOT NULL REFERENCES jobs(id),
    callback_url    TEXT NOT NULL,
    status_change   TEXT NOT NULL,
    payload         JSONB NOT NULL,
    http_status     INTEGER,
    attempt         INTEGER NOT NULL DEFAULT 1,
    fired_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### 4.5 rate_limit_buckets (Redis, not PostgreSQL)

Rate limiting state lives in Redis only, not PostgreSQL. A rate limit bucket is ephemeral: if Redis restarts, rate limit counts reset. This is acceptable because the consequence is a brief window of over-delivery, not data loss. Using PostgreSQL for rate limit state would add a write on every job submission and every delivery, which is the hottest path in the system.

```
Key pattern: ratelimit:{recipient}:{YYYYMMDDHH}
Type:        String (integer counter)
TTL:         2 hours (one hour of slack after the window closes)
Command:     INCR ratelimit:{recipient}:{window} / EXPIRE on first set
```

---

## 5. API Specification

### POST /jobs

Schedule a notification job.

**Request:**
```json
{
    "recipient": "user@example.com",
    "channel": "email",
    "payload": { "subject": "Hello", "body": "World" },
    "send_at": "2025-08-01T10:00:00Z",
    "delay_seconds": null,
    "priority": "high",
    "callback_url": "https://example.com/webhook",
    "idempotency_key": "order-123-confirmation"
}
```

`send_at` and `delay_seconds` are mutually exclusive. If `delay_seconds` is provided, `send_at = NOW() + delay_seconds`.

**Response 201:**
```json
{
    "job_id": "uuid",
    "status": "pending",
    "send_at": "2025-08-01T10:00:00Z"
}
```

**Response 409 (duplicate idempotency key):**
```json
{
    "error": "duplicate_job",
    "existing_job_id": "uuid"
}
```

**Response 429 (rate limit exceeded):**
```json
{
    "error": "rate_limit_exceeded",
    "retry_after_seconds": 1800
}
```

### GET /jobs/{job_id}/status

```json
{
    "job_id": "uuid",
    "status": "sent",
    "attempt_count": 2,
    "sent_at": "2025-08-01T10:00:04Z",
    "error_message": null
}
```

### GET /metrics

```json
{
    "pending": 142,
    "queued": 18,
    "claimed": 5,
    "sent": 9821,
    "failed": 34,
    "dead_lettered": 7
}
```

**Implementation note:** This is a `SELECT status, COUNT(*) FROM jobs GROUP BY status` query. At scale, this query can be expensive on a large jobs table. The right solution is to maintain a Redis counter per status and increment/decrement atomically on every status transition. The database query becomes a fallback for reconciliation. For the initial implementation, the database query is fine.

### POST /webhook-mock

A stub receiver that logs incoming webhook payloads and returns 200. Used for local testing.

```json
{
    "job_id": "uuid",
    "status": "sent",
    "timestamp": "2025-08-01T10:00:04Z"
}
```

---

## 6. Task Scheduler

The scheduler is a single async Python process running in a tight loop.

```python
async def scheduler_loop():
    while True:
        await promote_due_jobs()
        await recover_stale_claimed_jobs()
        await asyncio.sleep(0.5)
```

### 6.1 Promote Due Jobs

```python
async def promote_due_jobs():
    # Fetch jobs due within the next 5 seconds
    # The 5-second lookahead prevents clock skew between the scheduler
    # and workers from causing missed deliveries.
    jobs = await db.fetch("""
        UPDATE jobs
        SET status = 'queued', updated_at = NOW()
        WHERE id IN (
            SELECT id FROM jobs
            WHERE status = 'pending'
              AND send_at <= NOW() + INTERVAL '5 seconds'
            ORDER BY priority DESC, send_at ASC
            LIMIT 500
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id, priority, send_at
    """)
    
    for job in jobs:
        score = priority_score(job.priority, job.send_at)
        queue_name = f"queue:{job.priority}"
        await redis.zadd(queue_name, {str(job.id): score})
```

**Why `FOR UPDATE SKIP LOCKED`?** In a distributed setup you may run multiple scheduler instances for redundancy. `SKIP LOCKED` ensures two schedulers never promote the same job. Each scheduler grabs a non-overlapping batch. Without this, the same job would be enqueued twice and delivered twice.

**Why a 5-second lookahead?** Clock drift between database and scheduler processes can be 1-2 seconds in cloud environments. Jobs scheduled for exactly `NOW()` might be missed if the scheduler's clock is slightly behind. The lookahead absorbs this drift at the cost of slightly early delivery for some jobs, which is acceptable.

### 6.2 Priority Score

```python
def priority_score(priority: str, send_at: datetime) -> float:
    weights = {"high": 0, "medium": 1_000_000, "low": 2_000_000}
    return weights[priority] + send_at.timestamp()
```

The score is a composite: priority weight (an offset) plus the Unix timestamp. A high-priority job always has a lower score than a medium-priority job regardless of their `send_at` times. Within the same priority level, earlier `send_at` has a lower score and is processed first. Workers use `ZPOPMIN` which returns the lowest score first.

**Example:** A high-priority job due at T=100 has score 100. A medium-priority job due at T=50 has score 1,000,050. The high-priority job is always processed first even though it is due later.

### 6.3 Stale Job Recovery (Heartbeat Check)

```python
async def recover_stale_claimed_jobs():
    # A claimed job with no heartbeat for 30 seconds means the worker died
    stale_jobs = await db.fetch("""
        UPDATE jobs
        SET status = 'pending',
            worker_id = NULL,
            claimed_at = NULL,
            heartbeat_at = NULL,
            updated_at = NOW()
        WHERE status = 'claimed'
          AND heartbeat_at < NOW() - INTERVAL '30 seconds'
        RETURNING id
    """)
    # These jobs will be re-promoted on the next scheduler loop iteration
```

**Why reset to `pending` instead of `queued`?** Resetting to `pending` lets the scheduler re-evaluate whether the job is still due and re-enqueue it cleanly. Jumping directly to `queued` and re-adding to Redis risks double-enqueue if the job is somehow still in the Redis queue from the dead worker's earlier claim.

---

## 7. Worker Pool

Each worker is an async Python process. Multiple workers run concurrently. Workers are stateless: they hold no in-memory job state beyond the current job being processed.

### 7.1 Worker Loop

```python
async def worker_loop(worker_id: str):
    while True:
        job_id = await claim_next_job(worker_id)
        if job_id is None:
            await asyncio.sleep(0.1)
            continue
        await process_job(job_id, worker_id)
```

### 7.2 Claiming a Job (Exactly-Once)

```python
async def claim_next_job(worker_id: str) -> Optional[str]:
    # Poll queues in priority order: high first, then medium, then low
    for priority in ("high", "medium", "low"):
        result = await redis.zpopmin(f"queue:{priority}", count=1)
        if not result:
            continue
        
        job_id = result[0][0].decode()
        
        # Distributed lock: only the worker that wins this SET NX proceeds.
        # If the job was already claimed by another worker (race on zpopmin
        # edge case), this lock fails and we move on.
        lock_key = f"job:lock:{job_id}"
        acquired = await redis.set(
            lock_key, worker_id, nx=True, ex=60
        )
        
        if not acquired:
            # Another worker claimed this job. Extremely rare due to
            # zpopmin being atomic, but possible if the lock already exists
            # from a previous attempt that did not clean up.
            continue
        
        # Update job record: mark as claimed, record worker
        updated = await db.fetchrow("""
            UPDATE jobs
            SET status = 'claimed',
                worker_id = $1,
                claimed_at = NOW(),
                heartbeat_at = NOW(),
                updated_at = NOW()
            WHERE id = $2 AND status = 'queued'
            RETURNING id
        """, worker_id, job_id)
        
        if updated is None:
            # Job was claimed by another worker between zpopmin and
            # our DB update. Release lock and move on.
            await redis.delete(lock_key)
            continue
        
        return job_id
    
    return None
```

**Why both `ZPOPMIN` and a Redis lock?** `ZPOPMIN` is atomic: only one worker gets each job from the queue. The Redis lock is a belt-and-suspenders safety net for the extremely rare case where the same job_id appears in the queue twice (e.g. a scheduler bug during a deploy). The database `WHERE status = 'queued'` check is the third and final gate: even if both Redis operations somehow succeed, only one worker can transition the job from `queued` to `claimed` because PostgreSQL row-level locking ensures this update is atomic.

Three independent exactly-once guarantees. Any one of them is sufficient. All three together make duplicate delivery essentially impossible.

### 7.3 Heartbeat

```python
async def heartbeat_loop(job_id: str, worker_id: str, stop_event: asyncio.Event):
    while not stop_event.is_set():
        await db.execute("""
            UPDATE jobs SET heartbeat_at = NOW(), updated_at = NOW()
            WHERE id = $1 AND worker_id = $2 AND status = 'claimed'
        """, job_id, worker_id)
        await asyncio.sleep(10)

async def process_job(job_id: str, worker_id: str):
    stop_event = asyncio.Event()
    heartbeat_task = asyncio.create_task(
        heartbeat_loop(job_id, worker_id, stop_event)
    )
    try:
        await execute_delivery(job_id, worker_id)
    finally:
        stop_event.set()
        await heartbeat_task
```

The heartbeat runs concurrently with delivery. It updates `heartbeat_at` every 10 seconds. If the worker process dies, the heartbeat stops. The scheduler detects `heartbeat_at < NOW() - 30s` within one scheduler cycle and reclaims the job.

**Why 10s heartbeat and 30s threshold?** Three missed heartbeats before reclaim. This absorbs a brief network hiccup or a slow database write without false positives. The recovery window is at most 30s + 0.5s (scheduler poll interval), which is acceptable for a notification system.

### 7.4 Delivery (Stub)

```python
import random

async def execute_delivery(job_id: str, worker_id: str):
    job = await db.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)
    
    # Simulate configurable failure rate
    failure_rate = float(os.environ.get("DELIVERY_FAILURE_RATE", "0.1"))
    delivery_failed = random.random() < failure_rate
    
    if delivery_failed:
        error = f"Simulated delivery failure on channel {job['channel']}"
        await handle_failure(job, error, worker_id)
    else:
        await handle_success(job, worker_id)
```

### 7.5 Success

```python
async def handle_success(job: dict, worker_id: str):
    await db.execute("""
        UPDATE jobs
        SET status = 'sent', sent_at = NOW(), updated_at = NOW()
        WHERE id = $1
    """, job['id'])
    
    await redis.delete(f"job:lock:{job['id']}")
    await fire_webhook(job['id'], job['callback_url'], 'sent')
```

### 7.6 Retry with Exponential Backoff

```python
BASE_DELAY_SECONDS = 30
MAX_ATTEMPTS = 5

async def handle_failure(job: dict, error: str, worker_id: str):
    attempt = job['attempt_count'] + 1
    
    if attempt >= MAX_ATTEMPTS:
        await dead_letter(job, error)
        return
    
    # Exponential backoff: 30s, 60s, 120s, 240s, 480s
    delay = BASE_DELAY_SECONDS * (2 ** attempt)
    next_retry_at = datetime.utcnow() + timedelta(seconds=delay)
    
    await db.execute("""
        UPDATE jobs
        SET status = 'pending',
            attempt_count = $1,
            next_retry_at = $2,
            send_at = $2,
            error_message = $3,
            worker_id = NULL,
            updated_at = NOW()
        WHERE id = $4
    """, attempt, next_retry_at, error, job['id'])
    
    await redis.delete(f"job:lock:{job['id']}")
    await fire_webhook(job['id'], job['callback_url'], 'failed')
```

**Why reset `send_at` to `next_retry_at`?** The scheduler promotes jobs where `send_at <= NOW()`. By setting `send_at` to the retry time, the retry is handled by exactly the same promotion path as a new job. No special retry queue, no separate retry worker. The scheduler is the only thing that decides when a job enters the Redis queue.

### 7.7 Dead-Letter

```python
async def dead_letter(job: dict, error: str):
    async with db.transaction():
        await db.execute("""
            UPDATE jobs
            SET status = 'dead_lettered',
                error_message = $1,
                failed_at = NOW(),
                worker_id = NULL,
                updated_at = NOW()
            WHERE id = $2
        """, error, job['id'])
        
        await db.execute("""
            INSERT INTO dead_letter_queue
                (job_id, recipient, channel, payload, attempt_count, last_error)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, job['id'], job['recipient'], job['channel'],
             job['payload'], job['attempt_count'], error)
    
    await redis.delete(f"job:lock:{job['id']}")
    await fire_webhook(job['id'], job['callback_url'], 'dead_lettered')
```

The status update and DLQ insert are in a single transaction. If the DLQ insert fails, the job stays in its previous state and will be retried. This prevents a job from being marked `dead_lettered` in the jobs table but missing from the DLQ.

---

## 8. Webhook Dispatcher

```python
import httpx

async def fire_webhook(job_id: str, callback_url: Optional[str], status: str):
    if not callback_url:
        return
    
    payload = {
        "job_id": str(job_id),
        "status": status,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    for attempt in range(1, 4):
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.post(callback_url, json=payload)
            
            await db.execute("""
                INSERT INTO webhook_log
                    (job_id, callback_url, status_change, payload, http_status, attempt)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, job_id, callback_url, status, payload, response.status_code, attempt)
            
            if response.status_code < 500:
                return
            
        except Exception as e:
            await db.execute("""
                INSERT INTO webhook_log
                    (job_id, callback_url, status_change, payload, http_status, attempt)
                VALUES ($1, $2, $3, $4, NULL, $5)
            """, job_id, callback_url, status, payload, attempt)
        
        await asyncio.sleep(2 ** attempt)
```

Webhook delivery is best-effort with 3 retries and exponential backoff. A failed webhook does not affect the job's delivery status. The webhook is a notification to the caller, not a required step in the delivery pipeline.

---

## 9. Rate Limiting

```python
MAX_NOTIFICATIONS_PER_HOUR = int(os.environ.get("RATE_LIMIT_PER_HOUR", "10"))

async def check_rate_limit(recipient: str) -> tuple[bool, int]:
    window = datetime.utcnow().strftime("%Y%m%d%H")
    key = f"ratelimit:{recipient}:{window}"
    
    # Lua script ensures INCR and EXPIRE are atomic
    lua_script = """
    local current = redis.call('INCR', KEYS[1])
    if current == 1 then
        redis.call('EXPIRE', KEYS[1], 7200)
    end
    return current
    """
    
    count = await redis.eval(lua_script, 1, key)
    
    if count > MAX_NOTIFICATIONS_PER_HOUR:
        # Decrement: we incremented before checking
        await redis.decr(key)
        seconds_remaining = (60 - datetime.utcnow().minute) * 60
        return False, seconds_remaining
    
    return True, 0
```

**Why a Lua script?** The `INCR` and `EXPIRE` must be atomic. If the process dies between `INCR` and `EXPIRE`, the key has no TTL and persists forever, permanently blocking the recipient. The Lua script executes both commands in a single atomic Redis operation.

**Why decrement on limit exceeded?** We increment first to check atomically. If the limit is exceeded, we decrement to undo the increment. This is simpler than a check-then-increment pattern and avoids a race where two concurrent requests both read below the limit and both proceed.

---

## 10. Edge Cases and How We Handle Each

### 10.1 Scheduler restarts mid-promotion
The `FOR UPDATE SKIP LOCKED` pattern means a job is only promoted once. If the scheduler crashes after writing to Redis but before updating the job status to `queued`, the job remains `pending` and is re-promoted on the next scheduler run. This causes the job to be in Redis twice. The worker's `WHERE status = 'queued'` gate rejects the second pop because the first worker already transitioned the job to `claimed`. The Redis lock on the second pop also fails. No duplicate delivery.

### 10.2 Worker crashes after claiming but before delivering
The heartbeat stops. The scheduler detects `heartbeat_at < NOW() - 30s` and resets the job to `pending`. The scheduler re-promotes it on the next cycle. The Redis lock expires (60s TTL) before or around the same time. Clean recovery with at most 30-60s delay.

### 10.3 Worker crashes after delivering but before updating status
The job was delivered but still shows `claimed` in the database. The scheduler will reclaim it and a worker will attempt re-delivery. The delivery stub will fire again. This is the only scenario where at-least-once delivery is possible rather than exactly-once. To make this truly exactly-once, the delivery and status update must be in the same atomic operation, which is not possible with an external HTTP call. The mitigation is to make the delivery idempotent at the receiver (not in scope for this system).

### 10.4 Redis queue loss on restart
Redis is configured with AOF persistence. On restart, the queues are rebuilt from the AOF log. If Redis loses data despite persistence, the scheduler re-promotes all `queued` status jobs on its next run because their status never advanced to `claimed`. No jobs are permanently lost.

### 10.5 Clock skew between services
The scheduler uses a 5-second lookahead. Jobs scheduled for exactly `NOW()` are still promoted even with 1-2 second clock drift between the scheduler host and the database host.

### 10.6 Same idempotency key submitted concurrently
Two requests arrive with the same idempotency key at the same millisecond. Both attempt to insert into `job_idempotency`. PostgreSQL's primary key constraint rejects the second insert with a unique violation. The API returns 409 for the second request. The first request proceeds normally. No race, no duplicate job.

### 10.7 Rate limit window boundary
At 10:59:59 a recipient has 9/10 notifications used. At 11:00:00 the window rolls over and the counter resets. They can send 10 more. The sliding window key `ratelimit:{recipient}:{YYYYMMDDHH}` changes at the hour boundary. The previous key expires after 2 hours and is cleaned up by Redis TTL.

### 10.8 Webhook receiver is down
The webhook dispatcher retries 3 times with backoff. If all 3 attempts fail, the failure is logged to `webhook_log` and the dispatcher moves on. The job's delivery status is not affected. The caller can query `GET /jobs/{id}/status` to see the current state.

### 10.9 All workers are busy when a high-priority job becomes due
The scheduler enqueues the job to `queue:high`. Workers poll `queue:high` first on every loop iteration. As soon as any worker finishes its current job, it picks up the high-priority job next. The job waits in the queue until a worker is free. This is the correct behavior: priority determines queue order, not execution latency.

### 10.10 Poison message (job that always crashes the worker process)
The heartbeat stops on worker crash. The scheduler reclaims the job. Another worker claims it and crashes. This repeats until `attempt_count >= MAX_ATTEMPTS`, at which point the job is dead-lettered. The `max_attempts` cap is the poison message circuit breaker. After dead-lettering, no worker ever processes the job again.

---

## 11. Scale Considerations

### What holds at current scale
- One scheduler instance handles tens of thousands of promotions per minute
- Redis sorted sets handle millions of entries with O(log N) ZPOPMIN
- PostgreSQL with proper indexes handles hundreds of writes per second

### What breaks first and why
**PostgreSQL write throughput** is the first bottleneck. Every job lifecycle event (status update, heartbeat, webhook log) is a write. At 10,000 jobs per second, this is 40,000-60,000 writes per second including heartbeats, which exceeds a single PostgreSQL instance.

**Fix when needed:** Partition the jobs table by `created_at` month. Archive completed jobs to a cold store. Use a read replica for `GET /metrics` and status queries. Move heartbeat writes to a separate lightweight table with a short retention.

**Redis memory** is the second bottleneck. At 1 million queued jobs, each entry is ~100 bytes, so 100MB of queue data. Redis can hold this comfortably. At 100 million queued jobs this becomes 10GB, which is manageable with Redis cluster mode.

**Multiple scheduler instances:** Run two schedulers in active-passive mode. Both run, but `FOR UPDATE SKIP LOCKED` ensures they do not promote the same job. If one scheduler is slow or restarting, the other continues without interruption.

## 13. Environment Variables

```env
DATABASE_URL=postgresql://user:pass@localhost:5432/notifications
REDIS_URL=redis://localhost:6379
RATE_LIMIT_PER_HOUR=10
DELIVERY_FAILURE_RATE=0.1
MAX_ATTEMPTS=5
BASE_RETRY_DELAY_SECONDS=30
SCHEDULER_POLL_INTERVAL_MS=500
HEARTBEAT_INTERVAL_SECONDS=10
HEARTBEAT_TIMEOUT_SECONDS=30
WORKER_COUNT=4
```

---

## 14. Running the System

```bash
# Start infrastructure
docker-compose up -d postgres redis

# Run migrations
psql $DATABASE_URL -f migrations/001_initial.sql

# Start API
uvicorn main:app --host 0.0.0.0 --port 8000

# Start scheduler (separate process)
python -m scheduler.scheduler

# Start workers (separate process, N instances)
python -m worker.worker --worker-count 4
```