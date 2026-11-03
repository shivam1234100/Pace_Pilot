"""Load test: concurrency, overspend, idempotency and latency against a running stack.

Creates campaigns, fires spend events whose total demand exceeds total budget (so budgets
run out under contention), resends a fraction of events to test idempotency, then reads back
every campaign and /stats. Run once with COUNTER_MODE=atomic and once with naive.

    python loadtest/run.py --out results/atomic.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import platform
import random
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

PLATFORMS = ("meta", "google", "tiktok")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--base-url", default="http://localhost:8000")
    p.add_argument("--campaigns", type=int, default=200)
    p.add_argument("--budget-cents", type=int, default=50_000)
    p.add_argument("--events", type=int, default=60_000)
    p.add_argument("--batch-size", type=int, default=10)
    p.add_argument("--concurrency", type=int, default=100)
    p.add_argument("--dup-rate", type=float, default=0.05)
    p.add_argument("--amount-min", type=int, default=50)
    p.add_argument("--amount-max", type=int, default=500)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--worker-wait-s", type=float, default=5.0)
    p.add_argument("--out", default=None, help="default: results/<counter_mode>.json")
    return p.parse_args()


def percentile(values: list[float], pct: float) -> float | None:
    """Nearest-rank percentile."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(pct / 100 * len(ordered)))
    return ordered[rank - 1]


def round_or_none(value: float | None, digits: int = 1) -> float | None:
    return None if value is None else round(value, digits)


async def wait_healthy(client: httpx.AsyncClient, timeout_s: float = 60) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            resp = await client.get("/health")
            if resp.status_code == 200:
                return resp.json()
        except httpx.TransportError:
            pass
        if time.monotonic() > deadline:
            raise SystemExit("API not healthy; is the stack up? (make up)")
        await asyncio.sleep(1)


async def create_campaigns(
    client: httpx.AsyncClient, n: int, budget: int, rng: random.Random, sem: asyncio.Semaphore
) -> list[int]:
    async def one(i: int, plat: str) -> int:
        async with sem:
            body = {"name": f"load-{i}", "platform": plat, "daily_budget_cents": budget}
            resp = await client.post("/campaigns", json=body)
            resp.raise_for_status()
            return resp.json()["id"]

    plats = [rng.choice(PLATFORMS) for _ in range(n)]
    return list(await asyncio.gather(*(one(i, p) for i, p in enumerate(plats))))


def generate_events(args: argparse.Namespace, ids: list[int], rng: random.Random) -> list[dict]:
    events: list[dict] = []
    for _ in range(args.events):
        if events and rng.random() < args.dup_rate:
            events.append(dict(rng.choice(events)))  # resend: same id, amount and ts
            continue
        events.append(
            {
                "event_id": uuid.UUID(int=rng.getrandbits(128)).hex,
                "campaign_id": rng.choice(ids),
                "amount_cents": rng.randint(args.amount_min, args.amount_max),
                "ts": datetime.now(UTC).isoformat(),
            }
        )
    return events


async def send_batches(
    client: httpx.AsyncClient, batches: list[list[dict]], sem: asyncio.Semaphore
) -> tuple[list[float], dict[str, int], int]:
    latencies_ms: list[float] = []
    counts = {"approved": 0, "rejected": 0, "duplicate": 0, "unknown": 0}
    errors = 0

    async def one(batch: list[dict]) -> None:
        nonlocal errors
        async with sem:
            start = time.perf_counter()
            try:
                resp = await client.post("/events/spend", json={"events": batch})
            except httpx.TransportError:
                errors += 1
                return
            latencies_ms.append((time.perf_counter() - start) * 1000)
            if resp.status_code != 200:
                errors += 1
                return
            body = resp.json()
            for key in counts:
                counts[key] += body[key]

    await asyncio.gather(*(one(b) for b in batches))
    return latencies_ms, counts, errors


async def read_back(
    client: httpx.AsyncClient, ids: list[int], sem: asyncio.Semaphore
) -> list[dict[str, Any]]:
    async def one(cid: int) -> dict[str, Any]:
        async with sem:
            resp = await client.get(f"/campaigns/{cid}")
            resp.raise_for_status()
            return resp.json()

    return list(await asyncio.gather(*(one(c) for c in ids)))


def summarize(
    args: argparse.Namespace,
    health: dict[str, Any],
    events: list[dict],
    send_s: float,
    latencies_ms: list[float],
    counts: dict[str, int],
    errors: int,
    campaigns: list[dict[str, Any]],
    stats: dict[str, Any],
) -> dict[str, Any]:
    total_budget = sum(c["daily_budget_cents"] for c in campaigns)
    demand = sum(e["amount_cents"] for e in events)
    over = [c for c in campaigns if c["spent_today_cents"] > c["daily_budget_cents"]]
    overspend = sum(c["spent_today_cents"] - c["daily_budget_cents"] for c in over)
    mismatched = [c for c in campaigns if c["spent_today_cents"] != c["ledger_today_cents"]]
    return {
        "counter_mode": health["counter_mode"],
        "settings": {k: v for k, v in vars(args).items() if k != "out"},
        "events_sent": len(events),
        "requests": math.ceil(len(events) / args.batch_size),
        "request_errors": errors,
        "send_seconds": round(send_s, 2),
        "throughput_events_per_s": round(len(events) / send_s, 1),
        "latency_ms": {
            "p50": round_or_none(percentile(latencies_ms, 50)),
            "p95": round_or_none(percentile(latencies_ms, 95)),
            "p99": round_or_none(percentile(latencies_ms, 99)),
        },
        "results": counts,
        "demand_to_budget_ratio": round(demand / total_budget, 2),
        "total_budget_cents": total_budget,
        "total_spent_cents": sum(c["spent_today_cents"] for c in campaigns),
        "overspend": {
            "campaigns_over_budget": len(over),
            "total_overspend_cents": overspend,
            "overspend_pct_of_total_budget": round(100 * overspend / total_budget, 3),
        },
        "ledger_mismatch_campaigns": len(mismatched),
        "pauses": {
            "campaigns_paused": stats["paused"],
            "pause_lag_p50_ms": round_or_none(stats["pause_lag_p50_ms"]),
            "pause_lag_p95_ms": round_or_none(stats["pause_lag_p95_ms"]),
            "pause_lag_max_ms": stats["pause_lag_max_ms"],
        },
        "client_host": f"{platform.system()} {platform.machine()}",
    }


def print_report(r: dict[str, Any]) -> None:
    o, p, lat = r["overspend"], r["pauses"], r["latency_ms"]
    print(f"\n=== PacePilot load test: counter_mode={r['counter_mode']} ===")
    print(f"events sent           {r['events_sent']} in {r['send_seconds']}s")
    print(f"throughput            {r['throughput_events_per_s']} events/s")
    print(f"request latency (ms)  p50={lat['p50']} p95={lat['p95']} p99={lat['p99']}")
    print(f"request errors        {r['request_errors']}")
    print(f"results               {r['results']}")
    print(f"demand / budget       {r['demand_to_budget_ratio']}x")
    print(
        f"overspend             {o['campaigns_over_budget']} campaigns, "
        f"{o['total_overspend_cents']} cents ({o['overspend_pct_of_total_budget']}% of budget)"
    )
    print(f"ledger mismatches     {r['ledger_mismatch_campaigns']} campaigns")
    print(
        f"paused                {p['campaigns_paused']} campaigns, "
        f"lag p50={p['pause_lag_p50_ms']}ms p95={p['pause_lag_p95_ms']}ms"
    )


async def run(args: argparse.Namespace) -> dict[str, Any]:
    rng = random.Random(args.seed)
    sem = asyncio.Semaphore(args.concurrency)
    limits = httpx.Limits(
        max_connections=args.concurrency, max_keepalive_connections=args.concurrency
    )
    async with httpx.AsyncClient(base_url=args.base_url, timeout=60, limits=limits) as client:
        health = await wait_healthy(client)
        started_at = datetime.now(UTC).isoformat()
        ids = await create_campaigns(client, args.campaigns, args.budget_cents, rng, sem)
        events = generate_events(args, ids, rng)
        batches = [events[i : i + args.batch_size] for i in range(0, len(events), args.batch_size)]

        t0 = time.perf_counter()
        latencies, counts, errors = await send_batches(client, batches, sem)
        send_s = time.perf_counter() - t0

        await asyncio.sleep(args.worker_wait_s)
        campaigns = await read_back(client, ids, sem)
        stats = (await client.get("/stats", params={"since": started_at})).json()

    report = summarize(args, health, events, send_s, latencies, counts, errors, campaigns, stats)
    report["started_at"] = started_at
    return report


def main() -> None:
    args = parse_args()
    report = asyncio.run(run(args))
    print_report(report)
    out = Path(args.out or f"results/{report['counter_mode']}.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2) + "\n")
    print(f"saved {out}")


if __name__ == "__main__":
    main()
