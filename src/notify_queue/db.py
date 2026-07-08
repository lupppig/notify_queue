"""PostgreSQL connection pool factory with JSONB codec registration."""

import json

import asyncpg


async def _init_connection(conn: asyncpg.Connection) -> None:
    """Register a JSONB codec so Python dicts round-trip transparently."""
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")


async def create_pool(database_url: str) -> asyncpg.Pool:
    """Create and return an asyncpg connection pool with JSONB support."""
    return await asyncpg.create_pool(database_url, min_size=1, max_size=10, init=_init_connection)
