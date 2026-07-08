"""Per-recipient, per-hour rate limiting backed by Redis counters."""

from datetime import UTC, datetime, timedelta

import redis.asyncio as redis

# INCR and EXPIRE must be one atomic operation: a crash between them would
# leave a counter with no TTL, permanently blocking the recipient (DESIGN.md §9).
INCR_WITH_TTL = """
local current = redis.call('INCR', KEYS[1])
if current == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return current
"""

WINDOW_TTL_SECONDS = 7200


def window_key(recipient: str, now: datetime) -> str:
    """Return the Redis key for the recipient's current hourly rate-limit window."""
    return f"ratelimit:{recipient}:{now:%Y%m%d%H}"


def seconds_until_next_window(now: datetime) -> int:
    """Calculate the number of seconds until the next hourly window opens."""
    next_window = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return int((next_window - now).total_seconds()) + 1


async def check_rate_limit(
    redis_client: redis.Redis, recipient: str, limit: int
) -> tuple[bool, int]:
    """Check whether the recipient is within the hourly delivery limit.

    Returns ``(True, 0)`` if delivery is allowed, or ``(False, retry_after)``
    with the number of seconds until the next window if the limit is exceeded.
    Uses increment-then-check for atomic concurrency safety; the overshoot is
    undone with a DECR when the limit is breached.
    """
    now = datetime.now(UTC)
    key = window_key(recipient, now)
    count = await redis_client.eval(INCR_WITH_TTL, 1, key, WINDOW_TTL_SECONDS)
    if count > limit:
        await redis_client.decr(key)
        return False, seconds_until_next_window(now)
    return True, 0
