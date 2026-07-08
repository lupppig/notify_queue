COMPOSE := docker compose
PSQL := $(COMPOSE) exec -T postgres psql -U notify -d notifications
REDIS := $(COMPOSE) exec -T redis redis-cli
API_HOST ?= 127.0.0.1
API_PORT ?= 8080

.DEFAULT_GOAL := help

.PHONY: help setup up down nuke install migrate api scheduler worker dev \
        test lint fmt seed simulate reset psql redis

help: ## list available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

setup: up install migrate ## start infra, install deps, apply schema

up: ## start postgres and redis
	$(COMPOSE) up -d --wait

down: ## stop postgres and redis
	$(COMPOSE) down

nuke: ## stop infra and delete all data volumes
	$(COMPOSE) down -v

install: ## install python dependencies
	uv sync

migrate: ## apply the schema migration
	$(PSQL) -v ON_ERROR_STOP=1 < migrations/001_initial.sql

api: ## run the api + dashboard
	uv run uvicorn notify_queue.api.app:app --host $(API_HOST) --port $(API_PORT)

scheduler: ## run the task scheduler
	uv run python -m notify_queue.scheduler

worker: ## run the worker pool
	uv run python -m notify_queue.worker

dev: ## run api, scheduler and workers together (ctrl-c stops all)
	@trap 'kill 0' EXIT INT TERM; \
	uv run uvicorn notify_queue.api.app:app --host $(API_HOST) --port $(API_PORT) & \
	uv run python -m notify_queue.scheduler & \
	uv run python -m notify_queue.worker & \
	wait

test: ## run the test suite
	uv run pytest

lint: ## check style and formatting
	uv run ruff check src/ tests/ scripts/
	uv run ruff format --check src/ tests/ scripts/

fmt: ## fix style and formatting
	uv run ruff check --fix src/ tests/ scripts/
	uv run ruff format src/ tests/ scripts/

seed: ## wipe and seed the database with realistic data
	uv run python scripts/seed.py --wipe

simulate: ## drive the running system end to end
	uv run python scripts/simulate.py --api http://$(API_HOST):$(API_PORT)

reset: ## empty all tables and redis queues
	$(PSQL) -c "TRUNCATE webhook_log, dead_letter_queue, job_idempotency, jobs"
	$(REDIS) -n 0 flushdb

psql: ## open a psql shell
	$(COMPOSE) exec postgres psql -U notify -d notifications

redis: ## open a redis-cli shell
	$(COMPOSE) exec redis redis-cli
