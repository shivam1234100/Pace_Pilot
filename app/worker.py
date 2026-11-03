"""Background worker: pause consumer, stuck-message reclaimer and pacing loop.

Run with `python -m app.worker`. Delivery is at-least-once: a message is acked only after
the platform call and the DB update succeed; anything else stays pending and the reclaimer
retries it. Pausing is idempotent, so a repeat is harmless.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import socket
import time
from datetime import UTC, datetime

import asyncpg
import redis.asyncio as aioredis
from prometheus_client import Counter, Histogram, start_http_server

from app import db
from app.adapters import ResilientAdapter, build_adapters
from app.config import PAUSE_GROUP, PAUSE_STREAM, Settings, get_settings, setup_logging
from app.redis_budget import day_key, spent_key
from app.resilience import CircuitOpenError, TransientError

log = logging.getLogger("pacepilot.worker")

METRICS_PORT = 9101
READ_COUNT = 50
READ_BLOCK_MS = 1000
RECLAIM_EVERY_S = 15
RECLAIM_MIN_IDLE_MS = 10_000
MIN_DAY_FRACTION = 0.05  # don't judge pacing in the first ~72 minutes of the UTC day

PAUSE_LAG = Histogram(
    "pp_pause_lag_seconds",
    "Budget breach to campaign paused",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
)
PAUSE_FAILURES = Counter("pp_pause_failures_total", "Pause attempts left pending for retry")

Message = tuple[str, dict[str, str]]


class Worker:
    def __init__(
        self,
        settings: Settings,
        pool: asyncpg.Pool,
        r: aioredis.Redis,
        adapters: dict[str, ResilientAdapter],
    ) -> None:
        self.settings = settings
        self.pool = pool
        self.r = r
        self.adapters = adapters
        self.consumer = socket.gethostname()

    # ----- pause consumer -----

    async def ensure_group(self) -> None:
        try:
            await self.r.xgroup_create(PAUSE_STREAM, PAUSE_GROUP, id="$", mkstream=True)
        except aioredis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def handle(self, msg_id: str, fields: dict[str, str]) -> None:
        cid = int(fields["campaign_id"])
        breach_ms = int(fields["breach_ms"])
        extra = {"campaign_id": cid}
        row = await db.get_campaign(self.pool, cid)
        if row is None:
            log.warning("pause for unknown campaign, dropping", extra=extra)
            await self.r.xack(PAUSE_STREAM, PAUSE_GROUP, msg_id)
            return
        try:
            await self.adapters[row["platform"]].pause_campaign(row["external_id"])
        except (TransientError, CircuitOpenError) as exc:
            PAUSE_FAILURES.inc()
            log.warning("pause failed, left pending: %r", exc, extra=extra)
            return
        lag_ms = int(time.time() * 1000) - breach_ms
        await db.mark_paused(self.pool, cid, breach_ms, lag_ms)
        await self.r.xack(PAUSE_STREAM, PAUSE_GROUP, msg_id)
        PAUSE_LAG.observe(lag_ms / 1000)
        log.info("campaign paused lag_ms=%d", lag_ms, extra=extra)

    async def handle_many(self, messages: list[Message]) -> None:
        results = await asyncio.gather(
            *(self.handle(mid, fields) for mid, fields in messages), return_exceptions=True
        )
        for (mid, _), res in zip(messages, results, strict=True):
            if isinstance(res, Exception):
                log.error("message %s failed: %r", mid, res)

    async def consume_loop(self) -> None:
        await self.ensure_group()
        log.info("pause consumer started as %s", self.consumer)
        while True:
            try:
                resp = await self.r.xreadgroup(
                    PAUSE_GROUP,
                    self.consumer,
                    {PAUSE_STREAM: ">"},
                    count=READ_COUNT,
                    block=READ_BLOCK_MS,
                )
            except (OSError, aioredis.RedisError) as exc:
                log.error("xreadgroup failed: %r", exc)
                await asyncio.sleep(1)
                continue
            for _stream, messages in resp or []:
                await self.handle_many(messages)

    # ----- reclaimer -----

    async def reclaim_once(self) -> int:
        """Take over messages pending > RECLAIM_MIN_IDLE_MS (failed or from a dead consumer)."""
        start, total = "0-0", 0
        while True:
            next_start, messages, *_ = await self.r.xautoclaim(
                PAUSE_STREAM,
                PAUSE_GROUP,
                self.consumer,
                RECLAIM_MIN_IDLE_MS,
                start_id=start,
                count=READ_COUNT,
            )
            if messages:
                total += len(messages)
                await self.handle_many(messages)
            if next_start == "0-0":
                return total
            start = next_start

    async def reclaim_loop(self) -> None:
        while True:
            await asyncio.sleep(RECLAIM_EVERY_S)
            try:
                n = await self.reclaim_once()
                if n:
                    log.info("reclaimed %d pending pause messages", n)
            except (OSError, aioredis.RedisError) as exc:
                log.error("reclaim failed: %r", exc)

    # ----- pacing -----

    async def pace_once(self, now: datetime | None = None) -> None:
        now = now or datetime.now(UTC)
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        fraction = (now - midnight).total_seconds() / 86400
        if fraction <= MIN_DAY_FRACTION:
            return
        rows = await db.pacing_candidates(self.pool)
        if not rows:
            return
        day = day_key(now)
        spent_raw = await self.r.mget([spent_key(row["id"], day) for row in rows])
        for row, raw in zip(rows, spent_raw, strict=True):
            ideal = row["daily_budget_cents"] * fraction
            over = int(raw or 0) > ideal * self.settings.overpace_factor
            new = "overpacing" if over else "active"
            if new != row["status"] and await db.set_status_if(
                self.pool, row["id"], row["status"], new
            ):
                log.info(
                    "pacing %s -> %s spent=%s ideal=%.0f",
                    row["status"],
                    new,
                    raw or 0,
                    ideal,
                    extra={"campaign_id": row["id"]},
                )

    async def pacing_loop(self) -> None:
        while True:
            try:
                await self.pace_once()
            except (OSError, asyncpg.PostgresError, aioredis.RedisError) as exc:
                log.error("pacing pass failed: %r", exc)
            await asyncio.sleep(self.settings.pacing_interval_s)


async def run() -> None:
    setup_logging()
    settings = get_settings()
    start_http_server(METRICS_PORT)
    pool = await db.create_pool(settings.database_url)
    r = aioredis.from_url(settings.redis_url, decode_responses=True)
    worker = Worker(settings, pool, r, build_adapters(settings.mock_failure_rate))

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    tasks = [
        asyncio.create_task(worker.consume_loop(), name="consumer"),
        asyncio.create_task(worker.reclaim_loop(), name="reclaimer"),
        asyncio.create_task(worker.pacing_loop(), name="pacing"),
    ]
    log.info("worker started, metrics on :%d", METRICS_PORT)
    await stop.wait()
    log.info("shutting down")
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await r.aclose()
    await pool.close()


if __name__ == "__main__":
    asyncio.run(run())
