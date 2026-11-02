"""FastAPI app: routes, lifespan and metrics middleware.

Consistency model: Redis is the source of truth for admission and Postgres is the ledger.
If the ledger insert fails after Redis approved a batch, the Redis counter is ahead of the
ledger. We log that and return 500 for the batch; a reconciliation job (stretch) would
compare the two. The load test reports any mismatch.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from uuid import uuid4

import asyncpg
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel, Field

from app import db
from app.config import PAUSE_STREAM, get_settings, setup_logging
from app.redis_budget import BudgetGuard, Decision

log = logging.getLogger("pacepilot.api")

EVENTS_TOTAL = Counter("pp_events_total", "Spend events by admission result", ["result"])
HTTP_SECONDS = Histogram("pp_http_request_seconds", "HTTP request latency", ["route", "method"])
PAUSES_TRIGGERED = Counter("pp_pauses_triggered_total", "Pause signals sent to the stream")

REDIS_MAX_CONNECTIONS = 200  # waits for a free connection instead of erroring


# ---------- models ----------


class CampaignIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    platform: Literal["meta", "google", "tiktok"] = "meta"
    daily_budget_cents: int = Field(gt=0)


class BudgetIn(BaseModel):
    daily_budget_cents: int = Field(gt=0)


class SpendEvent(BaseModel):
    event_id: str = Field(min_length=1, max_length=64)
    campaign_id: int
    amount_cents: int = Field(gt=0)
    ts: datetime | None = None  # defaults to now (UTC)


class SpendBatch(BaseModel):
    events: list[SpendEvent] = Field(min_length=1, max_length=500)


class CampaignOut(BaseModel):
    id: int
    name: str
    platform: str
    external_id: str
    daily_budget_cents: int
    status: str
    breach_at: datetime | None
    paused_at: datetime | None
    pause_lag_ms: int | None
    created_at: datetime


class CampaignDetail(CampaignOut):
    spent_today_cents: int
    ledger_today_cents: int
    remaining_cents: int


class EventResult(BaseModel):
    event_id: str
    status: str
    spent_cents: int


class BatchResult(BaseModel):
    approved: int
    rejected: int
    duplicate: int
    unknown: int
    results: list[EventResult]


class HourlyPoint(BaseModel):
    bucket: datetime
    spend_cents: int
    events: int


class Stats(BaseModel):
    campaigns: int
    paused: int
    pause_lag_p50_ms: float | None
    pause_lag_p95_ms: float | None
    pause_lag_max_ms: int | None


# ---------- lifespan ----------


def make_redis(url: str) -> aioredis.Redis:
    pool = aioredis.BlockingConnectionPool.from_url(
        url, max_connections=REDIS_MAX_CONNECTIONS, decode_responses=True
    )
    return aioredis.Redis(connection_pool=pool)


async def sync_budgets(pool: asyncpg.Pool, guard: BudgetGuard) -> int:
    """Rebuild Redis budgets from Postgres, so Redis can be recreated from the DB."""
    rows = await db.all_budgets(pool)
    for row in rows:
        await guard.set_budget(row["id"], row["daily_budget_cents"])
    return len(rows)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    setup_logging()
    settings = get_settings()
    app.state.settings = settings
    app.state.pool = await db.create_pool(settings.database_url)
    app.state.redis = make_redis(settings.redis_url)
    app.state.guard = BudgetGuard(
        app.state.redis,
        mode=settings.counter_mode,
        pause_threshold=settings.pause_threshold,
        dedupe_ttl_s=settings.dedupe_ttl_s,
    )
    synced = await sync_budgets(app.state.pool, app.state.guard)
    log.info("started counter_mode=%s budgets_synced=%d", settings.counter_mode, synced)
    try:
        yield
    finally:
        await app.state.redis.aclose()
        await app.state.pool.close()


app = FastAPI(title="PacePilot", version="0.1.0", lifespan=lifespan)


@app.middleware("http")
async def metrics_middleware(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    start = time.perf_counter()
    response = await call_next(request)
    route = request.scope.get("route")
    template = getattr(route, "path", "unmatched")  # template, not the raw path
    HTTP_SECONDS.labels(route=template, method=request.method).observe(time.perf_counter() - start)
    return response


# ---------- helpers ----------


def utc_midnight(now: datetime | None = None) -> datetime:
    now = now or datetime.now(UTC)
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


def epoch_ms() -> int:
    return int(time.time() * 1000)


async def require_campaign(pool: asyncpg.Pool, cid: int) -> asyncpg.Record:
    row = await db.get_campaign(pool, cid)
    if row is None:
        raise HTTPException(status_code=404, detail="campaign not found")
    return row


async def signal_pauses(r: aioredis.Redis, campaign_ids: list[int]) -> None:
    breach_ms = epoch_ms()
    for cid in campaign_ids:
        await r.xadd(PAUSE_STREAM, {"campaign_id": cid, "breach_ms": breach_ms})
        PAUSES_TRIGGERED.inc()


def summarize(events: list[SpendEvent], decisions: list[Decision]) -> BatchResult:
    counts = {"approved": 0, "rejected": 0, "duplicate": 0, "unknown_campaign": 0}
    results = []
    for ev, d in zip(events, decisions, strict=True):
        counts[d.status] += 1
        EVENTS_TOTAL.labels(result=d.status).inc()
        results.append(
            EventResult(event_id=ev.event_id, status=d.status, spent_cents=d.spent_cents)
        )
    return BatchResult(
        approved=counts["approved"],
        rejected=counts["rejected"],
        duplicate=counts["duplicate"],
        unknown=counts["unknown_campaign"],
        results=results,
    )


# ---------- routes ----------


@app.post("/campaigns", status_code=201, response_model=CampaignOut)
async def create_campaign(body: CampaignIn, request: Request) -> dict[str, Any]:
    state = request.app.state
    external_id = f"{body.platform}_{uuid4().hex[:12]}"
    row = await db.insert_campaign(
        state.pool, body.name, body.platform, external_id, body.daily_budget_cents
    )
    await state.guard.set_budget(row["id"], row["daily_budget_cents"])
    return dict(row)


@app.get("/campaigns/{cid}", response_model=CampaignDetail)
async def get_campaign(cid: int, request: Request) -> dict[str, Any]:
    state = request.app.state
    row = await require_campaign(state.pool, cid)
    spent, ledger = await asyncio.gather(
        state.guard.spent(cid), db.ledger_since(state.pool, cid, utc_midnight())
    )
    return {
        **dict(row),
        "spent_today_cents": spent,
        "ledger_today_cents": ledger,
        "remaining_cents": max(row["daily_budget_cents"] - spent, 0),
    }


@app.patch("/campaigns/{cid}/budget", response_model=CampaignOut)
async def patch_budget(cid: int, body: BudgetIn, request: Request) -> dict[str, Any]:
    state = request.app.state
    row = await db.update_budget(state.pool, cid, body.daily_budget_cents)
    if row is None:
        raise HTTPException(status_code=404, detail="campaign not found")
    await state.guard.set_budget(cid, body.daily_budget_cents)
    return dict(row)


@app.get("/campaigns/{cid}/spend", response_model=list[HourlyPoint])
async def campaign_spend(
    cid: int, request: Request, hours: int = Query(24, ge=1, le=24 * 90)
) -> list[dict[str, Any]]:
    pool = request.app.state.pool
    await require_campaign(pool, cid)
    since = datetime.now(UTC) - timedelta(hours=hours)
    return [dict(r) for r in await db.hourly_spend(pool, cid, since)]


@app.post("/events/spend", response_model=BatchResult)
async def ingest_spend(batch: SpendBatch, request: Request) -> BatchResult:
    state = request.app.state
    guard: BudgetGuard = state.guard
    decisions = await asyncio.gather(
        *(guard.admit(ev.campaign_id, ev.event_id, ev.amount_cents) for ev in batch.events)
    )
    result = summarize(batch.events, decisions)

    # Pause signals go out before the ledger write: Redis has already set today's
    # "paused" flag, so if we skipped XADD on a DB error the pause would never fire.
    paused_ids = [
        ev.campaign_id for ev, d in zip(batch.events, decisions, strict=True) if d.pause_triggered
    ]
    await signal_pauses(state.redis, paused_ids)

    now = datetime.now(UTC)
    rows = [
        (ev.ts or now, ev.campaign_id, ev.event_id, ev.amount_cents)
        for ev, d in zip(batch.events, decisions, strict=True)
        if d.status == "approved"
    ]
    if rows:
        try:
            await db.insert_spend_events(state.pool, rows)
        except (OSError, asyncpg.PostgresError):
            log.exception("ledger insert failed after Redis approval; counter ahead of ledger")
            raise HTTPException(status_code=500, detail="ledger write failed") from None
    return result


@app.get("/stats", response_model=Stats)
async def stats(request: Request, since: datetime | None = None) -> Stats:
    since = since or datetime(1970, 1, 1, tzinfo=UTC)
    row = await db.pause_stats(request.app.state.pool, since)
    return Stats(
        campaigns=row["campaigns"],
        paused=row["paused"],
        pause_lag_p50_ms=row["lag_p50_ms"],
        pause_lag_p95_ms=row["lag_p95_ms"],
        pause_lag_max_ms=row["lag_max_ms"],
    )


@app.get("/health")
async def health(request: Request) -> JSONResponse:
    state = request.app.state
    body: dict[str, str] = {
        "db": "error",
        "redis": "error",
        "counter_mode": state.settings.counter_mode,
    }
    try:
        if await db.ping(state.pool):
            body["db"] = "ok"
    except (OSError, asyncpg.PostgresError) as exc:
        log.warning("health db check failed: %s", exc)
    try:
        if await state.redis.ping():
            body["redis"] = "ok"
    except (OSError, aioredis.RedisError) as exc:
        log.warning("health redis check failed: %s", exc)
    healthy = body["db"] == "ok" and body["redis"] == "ok"
    return JSONResponse(body, status_code=200 if healthy else 503)


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
