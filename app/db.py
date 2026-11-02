"""asyncpg pool and queries. Parameterized SQL only, no ORM."""

from __future__ import annotations

import asyncpg


async def create_pool(dsn: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn, min_size=1, max_size=20)


async def ping(pool: asyncpg.Pool) -> bool:
    return await pool.fetchval("SELECT 1") == 1
