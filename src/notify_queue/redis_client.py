"""Redis async client factory."""

import redis.asyncio as redis


def create_redis(redis_url: str) -> redis.Redis:
    """Create and return an async Redis client with string-decoded responses."""
    return redis.from_url(redis_url, decode_responses=True)
