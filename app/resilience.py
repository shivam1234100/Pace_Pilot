"""Resilience primitives for calls to ad platform APIs.

- TokenBucket: client-side rate limit so we stay under each platform's quota.
- CircuitBreaker: stop hammering a platform that keeps failing (no retry storms).
- retry_with_backoff: retry transient errors with exponential backoff and full jitter.

Clocks are injectable so tests can drive time without sleeping.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from typing import Literal

Clock = Callable[[], float]
BreakerState = Literal["closed", "open", "half_open"]


class TransientError(Exception):
    """Retryable failure: HTTP 429, 5xx or a timeout."""


class CircuitOpenError(Exception):
    """The breaker is open; the call was not attempted."""


class TokenBucket:
    def __init__(self, rate: float, capacity: float, clock: Clock = time.monotonic) -> None:
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self._clock = clock
        self._updated = clock()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = self._clock()
        self.tokens = min(self.capacity, self.tokens + (now - self._updated) * self.rate)
        self._updated = now

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                self._refill()
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                await asyncio.sleep((1 - self.tokens) / self.rate)


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        reset_timeout_s: float = 10,
        clock: Clock = time.monotonic,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.reset_timeout_s = reset_timeout_s
        self._clock = clock
        self.state: BreakerState = "closed"
        self.failures = 0
        self._opened_at = 0.0

    def before_call(self) -> None:
        if self.state != "open":
            return
        if self._clock() - self._opened_at >= self.reset_timeout_s:
            self.state = "half_open"  # let a trial call through
            return
        raise CircuitOpenError("circuit open")

    def on_success(self) -> None:
        self.state = "closed"
        self.failures = 0

    def on_failure(self) -> None:
        self.failures += 1
        if self.state == "half_open" or self.failures >= self.failure_threshold:
            self.state = "open"
            self._opened_at = self._clock()


async def retry_with_backoff[T](
    fn: Callable[[], Awaitable[T]],
    attempts: int = 4,
    base_delay_s: float = 0.2,
    max_delay_s: float = 2.0,
) -> T:
    """Retry only TransientError, sleeping uniform(0, min(max, base * 2**attempt))."""
    for attempt in range(attempts):
        try:
            return await fn()
        except TransientError:
            if attempt == attempts - 1:
                raise
            await asyncio.sleep(random.uniform(0, min(max_delay_s, base_delay_s * 2**attempt)))
    raise AssertionError("unreachable")  # attempts < 1
