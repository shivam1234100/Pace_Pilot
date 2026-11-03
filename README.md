# PacePilot

Real-time ad budget pacing service that makes campaign overspend impossible under concurrent spend events, and auto-pauses campaigns on flaky ad platform APIs.

> README in progress: architecture, design decisions, SQL benchmark, quickstart and API reference come in the next phase.

## Results

Every number below comes from a real run on this repo. The raw output is in [`results/`](results/).

### Load test: atomic Lua admission vs naive check-then-write

Same settings for both runs (the `loadtest/run.py` defaults): 200 campaigns with a $500.00 daily budget each, 60,000 spend events of 50–500 cents in batches of 10, 100 concurrent requests, 5% of events resent to test idempotency, seed 42. Total demand is 1.65× total budget, so budgets run out under contention.

| Metric | Atomic (Lua) | Naive (check-then-write) |
|---|---:|---:|
| Throughput | 2,397.7 events/s | 2,193.7 events/s |
| Request latency p50 / p95 / p99 | 235.4 / 1,371.0 / 2,674.5 ms | 307.9 / 1,463.2 / 2,979.5 ms |
| Request errors | 0 | 0 |
| Approved / rejected / duplicate | 36,496 / 20,387 / 3,117 | 36,527 / 20,367 / 3,106 |
| **Campaigns over budget** | **0 of 200** | **34 of 200** |
| **Total overspend** | **0 cents (0%)** | **5,969 cents (0.06% of total budget)** |
| Redis counter ≠ ledger | 0 campaigns | 10 campaigns |
| Campaigns auto-paused | 200 of 200 | 200 of 200 |
| Pause lag p50 / p95 (breach → paused) | 291.0 / 609.6 ms | 403.5 / 1,253.6 ms |

What the naive run shows:

- **Overspend.** Coroutines read the same `spent` value before any of them increments it, so several events pass the budget check together. The Lua script does the read, the check and the increment in one atomic step, so this can't happen.
- **Counter ahead of ledger.** In all 10 mismatched campaigns, Redis is ahead of the ledger by 93–423 cents, each within the 50–500 cent range of a single event. That fits the naive idempotency check racing: two copies of a resent event both pass `EXISTS` before either writes its dedupe key, so Redis counts it twice. The ledger's `UNIQUE (event_id, ts)` stores it once.

Setup and caveats:
- **Machine.** API, worker, Redis and TimescaleDB ran in Docker Desktop (8 CPUs, 4 GB) on an Apple M1 MacBook Air with 8 GB. The load generator ran on the same machine, so throughput and latency are local single-box figures, not production numbers.
- **One API process.** The API is a single async uvicorn worker process, which keeps the Prometheus counters correct without multiprocess mode. The naive race still appears, because coroutines interleave at every `await`.
- **Mock failures included in pause lag.** The worker's mock ad platform fails 10% of calls, so retries with backoff are part of the pause-lag numbers.

Reproduce (with the stack running):

```bash
make up
make loadtest         # writes results/atomic.json
make loadtest-naive   # restarts the API with COUNTER_MODE=naive, writes results/naive.json, restores atomic
```

### Tests

18 unit tests (`make test`: budget admission on fakeredis with Lua, plus resilience primitives) and 13 integration tests against the running stack (`make test-integration`).

## Consistency model

Redis is the source of truth for admission and Postgres is the ledger. If the ledger insert fails after Redis approved a batch, the counter is ahead of the ledger: the API logs it and returns 500 for that batch. A reconciliation job (not built yet) would compare the two. The load test reports any mismatch.
