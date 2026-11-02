"""FastAPI app: routes, lifespan and metrics middleware."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg
import redis.asyncio as aioredis
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app import db
from app.config import get_settings

log = logging.getLogger("pacepilot.api")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    app.state.settings = settings
    app.state.pool = await db.create_pool(settings.database_url)
    app.state.redis = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        yield
    finally:
        await app.state.redis.aclose()
        await app.state.pool.close()


app = FastAPI(title="PacePilot", version="0.1.0", lifespan=lifespan)


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
