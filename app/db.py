"""asyncpg pool and queries. Parameterized SQL only, no ORM."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

import asyncpg

CAMPAIGN_COLS = (
    "id, name, platform, external_id, daily_budget_cents, status, "
    "breach_at, paused_at, pause_lag_ms, created_at"
)


async def create_pool(dsn: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn, min_size=1, max_size=20)


async def ping(pool: asyncpg.Pool) -> bool:
    return await pool.fetchval("SELECT 1") == 1


async def all_budgets(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    return await pool.fetch("SELECT id, daily_budget_cents FROM campaigns")


async def insert_campaign(
    pool: asyncpg.Pool, name: str, platform: str, external_id: str, budget_cents: int
) -> asyncpg.Record:
    return await pool.fetchrow(
        f"""INSERT INTO campaigns (name, platform, external_id, daily_budget_cents)
            VALUES ($1, $2, $3, $4) RETURNING {CAMPAIGN_COLS}""",
        name,
        platform,
        external_id,
        budget_cents,
    )


async def get_campaign(pool: asyncpg.Pool, cid: int) -> asyncpg.Record | None:
    return await pool.fetchrow(f"SELECT {CAMPAIGN_COLS} FROM campaigns WHERE id = $1", cid)


async def update_budget(pool: asyncpg.Pool, cid: int, budget_cents: int) -> asyncpg.Record | None:
    return await pool.fetchrow(
        f"UPDATE campaigns SET daily_budget_cents = $2 WHERE id = $1 RETURNING {CAMPAIGN_COLS}",
        cid,
        budget_cents,
    )


async def ledger_since(pool: asyncpg.Pool, cid: int, since: datetime) -> int:
    return await pool.fetchval(
        """SELECT COALESCE(SUM(amount_cents), 0)::bigint FROM spend_events
           WHERE campaign_id = $1 AND ts >= $2""",
        cid,
        since,
    )


async def hourly_spend(pool: asyncpg.Pool, cid: int, since: datetime) -> list[asyncpg.Record]:
    return await pool.fetch(
        """SELECT bucket, spend_cents::bigint AS spend_cents, events
           FROM spend_hourly WHERE campaign_id = $1 AND bucket >= $2 ORDER BY bucket""",
        cid,
        since,
    )


async def insert_spend_events(
    pool: asyncpg.Pool, rows: Sequence[tuple[datetime, int, str, int]]
) -> None:
    """rows: (ts, campaign_id, event_id, amount_cents). Retried event_ids are no-ops."""
    await pool.executemany(
        """INSERT INTO spend_events (ts, campaign_id, event_id, amount_cents)
           VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING""",
        rows,
    )


async def pause_stats(pool: asyncpg.Pool, since: datetime) -> dict[str, Any]:
    row = await pool.fetchrow(
        """SELECT count(*) AS campaigns,
                  count(*) FILTER (WHERE status = 'paused') AS paused,
                  percentile_cont(0.5)  WITHIN GROUP (ORDER BY pause_lag_ms) AS lag_p50_ms,
                  percentile_cont(0.95) WITHIN GROUP (ORDER BY pause_lag_ms) AS lag_p95_ms,
                  max(pause_lag_ms) AS lag_max_ms
           FROM campaigns WHERE created_at >= $1""",
        since,
    )
    return dict(row)


async def mark_paused(pool: asyncpg.Pool, cid: int, breach_ms: int, lag_ms: int) -> None:
    await pool.execute(
        """UPDATE campaigns
           SET status = 'paused', breach_at = to_timestamp($2 / 1000.0),
               paused_at = now(), pause_lag_ms = $3
           WHERE id = $1""",
        cid,
        breach_ms,
        lag_ms,
    )


async def pacing_candidates(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    return await pool.fetch(
        """SELECT id, daily_budget_cents, status FROM campaigns
           WHERE status IN ('active', 'overpacing')"""
    )


async def set_status_if(pool: asyncpg.Pool, cid: int, expected: str, new: str) -> bool:
    """Compare-and-set so the pacing loop never overwrites a pause."""
    result = await pool.execute(
        "UPDATE campaigns SET status = $3 WHERE id = $1 AND status = $2", cid, expected, new
    )
    return result == "UPDATE 1"
