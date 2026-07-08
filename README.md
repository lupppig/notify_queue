# notify_queue

A distributed delayed job and notification delivery system. Jobs are scheduled for future delivery (time-based or delay-based), processed by horizontally scalable workers with an exactly-once guarantee, prioritized, retried with exponential backoff, dead-lettered when retries are exhausted, and rate-limited per recipient — with webhook callbacks on every status change.

The full architecture and its rationale live in [DESIGN.md](DESIGN.md).

## Architecture

```
Client ──HTTP──▶ FastAPI ──▶ PostgreSQL (source of truth: jobs, idempotency, DLQ, webhook log)
                                  │  poll send_at <= NOW() every 500ms
                                  ▼
                            Task Scheduler ──ZADD──▶ Redis priority queues (high / medium / low)
                                  ▲                        │  ZPOPMIN
                 retries/deferrals loop back               ▼
                 via send_at reset                   Worker pool (N processes)
                                                     claim → deliver → webhook
```

Three separate processes, each independently scalable:

- **API** — accepts jobs, enforces idempotency, serves status/metrics and the dashboard
- **Scheduler** — promotes due jobs into Redis, recovers jobs from crashed workers (heartbeat timeout), rescues queued jobs lost from Redis
- **Workers** — claim jobs exactly-once, deliver (stubbed sender with a configurable failure rate), retry with backoff, dead-letter at the attempt cap, fire webhooks

## Guarantees

| Guarantee | Mechanism |
|---|---|
| Exactly-once delivery | Three independent gates: atomic `ZPOPMIN` → Redis `SET NX` lock → conditional `UPDATE ... WHERE status = 'queued'` in PostgreSQL |
| Priority ordering | Separate queue per priority, polled high-first; composite score (priority weight + `send_at`) orders within a queue |
| Crash recovery | Workers heartbeat every 10s; the scheduler reclaims claimed jobs with no heartbeat for 30s and counts the reclaim as a failed attempt, so poison messages dead-letter instead of looping forever |
| Bounded retries | Exponential backoff (30s, 60s, 120s, 240s by default), then a transactional move to the dead-letter queue |
| Idempotency | `job_idempotency` primary-key constraint; duplicates get 409 with the original job id |
| Rate limiting | Atomic Redis `INCR`+`EXPIRE` (Lua) per recipient per hour, checked at delivery time — excess jobs are deferred to the next window, never rejected |

## Setup

Requires Docker, [uv](https://docs.astral.sh/uv/) and Python 3.12+.

```bash
docker compose up -d --wait
uv sync
docker compose exec -T postgres psql -U notify -d notifications < migrations/001_initial.sql
```

Default ports: PostgreSQL on **5433**, Redis on **6380** (chosen to avoid clashing with locally running instances). Copy `.env.example` to `.env` to override anything.

## Running

Each process runs separately (three terminals):

```bash
uv run uvicorn notify_queue.api.app:app --host 127.0.0.1 --port 8080
uv run python -m notify_queue.scheduler
uv run python -m notify_queue.worker
```

Open **http://127.0.0.1:8080** for the dashboard: live metrics, a job submission form, real-time job statuses, and one-click replay of dead-lettered jobs.

### API

| Endpoint | Purpose |
|---|---|
| `POST /jobs` | Schedule a job (`send_at` or `delay_seconds`, priority, optional `idempotency_key` and `callback_url`) |
| `GET /jobs/{id}/status` | Job status, attempts, error |
| `GET /jobs?status=&limit=` | Recent jobs |
| `POST /jobs/{id}/retry` | Replay a dead-lettered job |
| `GET /metrics` | Job counts by status |
| `POST /webhook-mock` | Stub webhook receiver for local testing |

## Simulation

With all three processes running:

```bash
uv run python scripts/simulate.py
```

Submits 50 mixed-priority delayed jobs, a duplicate idempotency pair (asserts 201 then 409), and a 15-job burst to one recipient to trip the rate limit — then watches metrics until the queue drains and verifies the books balance (`sent + dead_lettered + deferred == submitted`). Vary the failure rate on the workers to see the retry and dead-letter paths:

```bash
DELIVERY_FAILURE_RATE=0.5 uv run python -m notify_queue.worker
```

A worthwhile live demo: `kill -9` a worker mid-run and watch its claimed jobs get reclaimed by the scheduler and finished by the surviving workers.

## Tests

```bash
uv run pytest
```

31 integration tests run against the real PostgreSQL and Redis from compose (no mocks — the guarantees under test *are* database semantics), using a dedicated `notifications_test` database and Redis db 1. Coverage includes: concurrent duplicate submissions, concurrent claim races, no duplicate delivery under concurrent workers with deliberately double-enqueued jobs, backoff timing, transactional dead-lettering, heartbeat-based crash recovery, poison-message dead-lettering, and rate-limit deferral semantics.

## Configuration

All timings and limits come from the environment (see `.env.example`):

| Variable | Default | |
|---|---|---|
| `DATABASE_URL` | `postgresql://notify:notify@localhost:5433/notifications` | |
| `REDIS_URL` | `redis://localhost:6380/0` | |
| `RATE_LIMIT_PER_HOUR` | `10` | per recipient per hour |
| `DELIVERY_FAILURE_RATE` | `0.1` | stub sender failure probability |
| `MAX_ATTEMPTS` | `5` | retries before dead-lettering |
| `BASE_RETRY_DELAY_SECONDS` | `30` | doubles per attempt |
| `SCHEDULER_POLL_INTERVAL_MS` | `500` | |
| `HEARTBEAT_INTERVAL_SECONDS` | `10` | worker heartbeat cadence |
| `HEARTBEAT_TIMEOUT_SECONDS` | `30` | stale-claim reclaim threshold |
| `WORKER_COUNT` | `4` | worker loops per worker process |

## Layout

```
migrations/          schema (enums, jobs, job_idempotency, dead_letter_queue, webhook_log)
src/notify_queue/
  api/               FastAPI app and routes
  scheduler/         promotion, stale-claim recovery, queued-job rescue
  worker/            claim, heartbeat, delivery, retry, deferral
  ratelimit.py       atomic INCR+EXPIRE fixed-window limiter
  webhooks.py        best-effort dispatcher with retries
  dlq.py             transactional dead-lettering
web/                 dashboard (vanilla HTML/CSS/JS, no build step)
scripts/simulate.py  end-to-end load simulation
tests/               integration tests against real Postgres/Redis
```
