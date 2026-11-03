"""Resilience tests: fake clocks and a no-op sleep, so nothing actually waits."""

from __future__ import annotations

import asyncio

import pytest

from app.resilience import (
    CircuitBreaker,
    CircuitOpenError,
    TokenBucket,
    TransientError,
    retry_with_backoff,
)


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    recorded: list[float] = []

    async def fake_sleep(delay: float) -> None:
        recorded.append(delay)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    return recorded


def test_breaker_opens_half_opens_and_closes() -> None:
    clock = FakeClock()
    cb = CircuitBreaker(failure_threshold=3, reset_timeout_s=10, clock=clock)
    for _ in range(3):
        cb.before_call()
        cb.on_failure()
    assert cb.state == "open"
    with pytest.raises(CircuitOpenError):
        cb.before_call()
    clock.now += 10
    cb.before_call()
    assert cb.state == "half_open"
    cb.on_success()
    assert cb.state == "closed"
    assert cb.failures == 0


def test_breaker_half_open_failure_reopens() -> None:
    clock = FakeClock()
    cb = CircuitBreaker(failure_threshold=2, reset_timeout_s=5, clock=clock)
    cb.on_failure()
    cb.on_failure()
    clock.now += 5
    cb.before_call()
    assert cb.state == "half_open"
    cb.on_failure()
    assert cb.state == "open"
    with pytest.raises(CircuitOpenError):
        cb.before_call()


async def test_retry_succeeds_after_two_transient_failures(sleeps: list[float]) -> None:
    calls = 0

    async def flaky() -> str:
        nonlocal calls
        calls += 1
        if calls <= 2:
            raise TransientError("503")
        return "ok"

    assert await retry_with_backoff(flaky, attempts=4, base_delay_s=0.2) == "ok"
    assert calls == 3
    assert len(sleeps) == 2
    assert 0 <= sleeps[0] <= 0.2 and 0 <= sleeps[1] <= 0.4  # full jitter bounds


async def test_retry_gives_up_and_reraises(sleeps: list[float]) -> None:
    calls = 0

    async def always_fails() -> None:
        nonlocal calls
        calls += 1
        raise TransientError("429")

    with pytest.raises(TransientError):
        await retry_with_backoff(always_fails, attempts=4)
    assert calls == 4
    assert len(sleeps) == 3


async def test_retry_does_not_retry_other_errors(sleeps: list[float]) -> None:
    calls = 0

    async def broken() -> None:
        nonlocal calls
        calls += 1
        raise ValueError("bad request")

    with pytest.raises(ValueError):
        await retry_with_backoff(broken, attempts=4)
    assert calls == 1
    assert sleeps == []


async def test_token_bucket_burst_is_immediate(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = FakeClock()
    slept: list[float] = []

    async def advancing_sleep(delay: float) -> None:
        slept.append(delay)
        clock.now += delay

    monkeypatch.setattr(asyncio, "sleep", advancing_sleep)
    bucket = TokenBucket(rate=10, capacity=2, clock=clock)
    await bucket.acquire()
    await bucket.acquire()
    assert slept == []
    await bucket.acquire()  # bucket empty: waits one token at 10/s
    assert slept == [pytest.approx(0.1)]
