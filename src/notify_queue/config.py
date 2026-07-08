"""Application settings loaded from environment variables and ``.env``."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Central configuration for the notification queue system.

    Every timing constant and limit is an environment variable so the system
    can be tuned without code changes.  See ``.env.example`` for reference.
    """

    model_config = SettingsConfigDict(env_file=".env")

    database_url: str = "postgresql://notify:notify@localhost:5433/notifications"
    redis_url: str = "redis://localhost:6380/0"
    rate_limit_per_hour: int = 10
    delivery_failure_rate: float = 0.1
    max_attempts: int = 5
    base_retry_delay_seconds: int = 30
    scheduler_poll_interval_ms: int = 500
    scheduler_lookahead_seconds: int = 5
    scheduler_batch_size: int = 500
    queued_requeue_seconds: int = 30
    heartbeat_interval_seconds: float = 10
    heartbeat_timeout_seconds: float = 30
    worker_count: int = 4
    worker_idle_sleep_seconds: float = 0.1
    error_backoff_seconds: float = 1.0
    job_lock_ttl_seconds: int = 60
    webhook_timeout_seconds: float = 5.0
    webhook_max_attempts: int = 3
