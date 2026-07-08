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

CREATE INDEX idx_jobs_queued ON jobs (updated_at)
    WHERE status = 'queued';

CREATE INDEX idx_jobs_recipient ON jobs (recipient, status);

CREATE TABLE job_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    job_id          UUID NOT NULL REFERENCES jobs(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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
