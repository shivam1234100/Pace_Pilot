"""Ad platform adapters.

The worker only talks to AdPlatformAdapter, so a real integration implements the same two
methods. A real Meta adapter would call the Marketing API (POST /{campaign_id} with
status=PAUSED); a real Google adapter would call Google Ads CampaignService.MutateCampaigns
to set the campaign status to PAUSED. Neither is called here: MockAdapter simulates latency
and transient failures, and ResilientAdapter wraps any adapter with a token-bucket rate
limit, retries with backoff and jitter, and a circuit breaker.
"""

from __future__ import annotations

import asyncio
import random
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable

from prometheus_client import Counter, Gauge

from app.resilience import (
    CircuitBreaker,
    CircuitOpenError,
    TokenBucket,
    TransientError,
    retry_with_backoff,
)

PLATFORMS = ("meta", "google", "tiktok")

ADAPTER_CALLS = Counter(
    "pp_adapter_calls_total", "Ad platform calls by outcome", ["platform", "result"]
)
CIRCUIT_OPEN = Gauge("pp_circuit_open", "1 while the platform's breaker is open", ["platform"])


class AdPlatformAdapter(ABC):
    name: str

    @abstractmethod
    async def pause_campaign(self, external_id: str) -> None: ...

    @abstractmethod
    async def update_budget(self, external_id: str, daily_budget_cents: int) -> None: ...


class MockAdapter(AdPlatformAdapter):
    def __init__(
        self,
        name: str,
        failure_rate: float = 0.1,
        latency_ms: tuple[int, int] = (40, 180),
        seed: int | None = None,
    ) -> None:
        self.name = name
        self.failure_rate = failure_rate
        self.latency_ms = latency_ms
        self._rng = random.Random(seed)
        self.paused: set[str] = set()
        self.budgets: dict[str, int] = {}

    async def _simulate_call(self) -> None:
        await asyncio.sleep(self._rng.uniform(*self.latency_ms) / 1000)
        if self._rng.random() < self.failure_rate:
            raise TransientError(f"{self.name}: simulated 503")

    async def pause_campaign(self, external_id: str) -> None:
        await self._simulate_call()
        self.paused.add(external_id)

    async def update_budget(self, external_id: str, daily_budget_cents: int) -> None:
        await self._simulate_call()
        self.budgets[external_id] = daily_budget_cents


class ResilientAdapter(AdPlatformAdapter):
    def __init__(self, inner: AdPlatformAdapter, rate_per_s: float = 20, burst: float = 20) -> None:
        self.name = inner.name
        self.inner = inner
        self.bucket = TokenBucket(rate_per_s, burst)
        self.breaker = CircuitBreaker()
        CIRCUIT_OPEN.labels(platform=self.name).set(0)

    async def pause_campaign(self, external_id: str) -> None:
        await self._call(lambda: self.inner.pause_campaign(external_id))

    async def update_budget(self, external_id: str, daily_budget_cents: int) -> None:
        await self._call(lambda: self.inner.update_budget(external_id, daily_budget_cents))

    async def _call(self, op: Callable[[], Awaitable[None]]) -> None:
        try:
            self.breaker.before_call()
        except CircuitOpenError:
            ADAPTER_CALLS.labels(platform=self.name, result="circuit_open").inc()
            raise

        async def attempt() -> None:
            await self.bucket.acquire()
            await op()

        try:
            await retry_with_backoff(attempt)
        except TransientError:
            self.breaker.on_failure()
            ADAPTER_CALLS.labels(platform=self.name, result="failed").inc()
            raise
        finally:
            CIRCUIT_OPEN.labels(platform=self.name).set(int(self.breaker.state == "open"))
        self.breaker.on_success()
        CIRCUIT_OPEN.labels(platform=self.name).set(0)
        ADAPTER_CALLS.labels(platform=self.name, result="ok").inc()


def build_adapters(failure_rate: float = 0.1) -> dict[str, ResilientAdapter]:
    return {p: ResilientAdapter(MockAdapter(p, failure_rate=failure_rate)) for p in PLATFORMS}
