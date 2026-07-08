import redis.asyncio as redis


def create_redis(redis_url: str) -> redis.Redis:
    return redis.from_url(redis_url, decode_responses=True)
