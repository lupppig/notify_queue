from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

Channel = Literal["email", "sms", "push"]
Priority = Literal["high", "medium", "low"]
JobStatus = Literal["pending", "queued", "claimed", "sent", "failed", "dead_lettered"]


class JobCreate(BaseModel):
    recipient: str = Field(min_length=1)
    channel: Channel
    payload: dict[str, Any]
    send_at: datetime | None = None
    delay_seconds: int | None = Field(default=None, ge=0)
    priority: Priority = "medium"
    callback_url: str | None = None
    idempotency_key: str | None = Field(default=None, min_length=1)

    @field_validator("send_at")
    @classmethod
    def _assume_utc(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value

    @model_validator(mode="after")
    def _mutually_exclusive_schedule(self) -> "JobCreate":
        if self.send_at is not None and self.delay_seconds is not None:
            raise ValueError("send_at and delay_seconds are mutually exclusive")
        return self

    def resolved_send_at(self) -> datetime:
        if self.send_at is not None:
            return self.send_at
        if self.delay_seconds is not None:
            return datetime.now(UTC) + timedelta(seconds=self.delay_seconds)
        return datetime.now(UTC)
