# notify_queue

A distributed delayed job and notification delivery system: schedule notifications for future delivery, process them with concurrent workers under an exactly-once guarantee, with priority queues, exponential-backoff retries, a dead-letter queue, per-recipient rate limiting and webhook callbacks.

Architecture and design rationale: [DESIGN.md](DESIGN.md).

## Requirements

Docker, [uv](https://docs.astral.sh/uv/), Python 3.12+.

## Quick start

```bash
make setup    # start postgres + redis, install deps, apply schema
make dev      # run api, scheduler and workers (ctrl-c stops all)
```

Dashboard: **http://127.0.0.1:8080** — submit jobs, watch statuses live, view metrics, replay dead-lettered jobs.

## Commands

```bash
make test       # run the test suite
make seed       # wipe and fill the database with realistic data
make simulate   # drive the running system end to end
make lint       # style checks
make reset      # empty tables and queues
make help       # everything else
```

To run the processes individually: `make api`, `make scheduler`, `make worker`.

## Configuration

Everything is set via environment variables — see [.env.example](.env.example). Postgres runs on port **5433**, Redis on **6380**, the API on **8080**.
