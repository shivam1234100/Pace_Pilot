"""BudgetGuard tests on fakeredis with Lua support. The date is pinned so keys are stable."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import fakeredis
import pytest

from app.redis_budget import BudgetGuard

DAY = "20260101"
CID = 1


@pytest.fixture
async def r() -> AsyncIterator[fakeredis.FakeAsyncRedis]:
    client = fakeredis.FakeAsyncRedis(decode_responses=True)
    yield client
    await client.aclose()


@pytest.fixture(params=["atomic", "naive"])
async def guard(request: pytest.FixtureRequest, r: fakeredis.FakeAsyncRedis) -> BudgetGuard:
    g = BudgetGuard(r, mode=request.param)
    await g.set_budget(CID, 10_000)
    return g


async def test_approves_under_budget_and_counts_sum(guard: BudgetGuard) -> None:
    amounts = [1_000, 2_500, 300]
    for i, amt in enumerate(amounts):
        d = await guard.admit(CID, f"e{i}", amt, day=DAY)
        assert d.status == "approved"
    assert await guard.spent(CID, day=DAY) == sum(amounts)


async def test_rejects_over_budget_and_spent_unchanged(guard: BudgetGuard) -> None:
    await guard.admit(CID, "e1", 9_000, day=DAY)
    d = await guard.admit(CID, "e2", 1_001, day=DAY)
    assert d.status == "rejected"
    assert d.spent_cents == 9_000
    assert await guard.spent(CID, day=DAY) == 9_000


async def test_duplicate_event_counted_once(guard: BudgetGuard) -> None:
    first = await guard.admit(CID, "same", 500, day=DAY)
    second = await guard.admit(CID, "same", 500, day=DAY)
    assert first.status == "approved"
    assert second.status == "duplicate"
    assert await guard.spent(CID, day=DAY) == 500


async def test_pause_fires_exactly_once(guard: BudgetGuard) -> None:
    below = await guard.admit(CID, "e1", 9_700, day=DAY)  # 97%: under the 98% threshold
    crossing = await guard.admit(CID, "e2", 100, day=DAY)  # 98%: crosses it
    after = await guard.admit(CID, "e3", 100, day=DAY)  # 99%: already paused today
    assert below.pause_triggered is False
    assert crossing.pause_triggered is True
    assert after.status == "approved"
    assert after.pause_triggered is False


async def test_unknown_campaign(guard: BudgetGuard) -> None:
    d = await guard.admit(999, "e1", 100, day=DAY)
    assert d.status == "unknown_campaign"


async def test_concurrent_admits_never_overspend(r: fakeredis.FakeAsyncRedis) -> None:
    budget = 10_000
    g = BudgetGuard(r, mode="atomic")
    await g.set_budget(CID, budget)
    decisions = await asyncio.gather(*(g.admit(CID, f"e{i}", 100, day=DAY) for i in range(500)))
    approved = sum(d.status == "approved" for d in decisions)
    spent = await g.spent(CID, day=DAY)
    assert spent <= budget
    assert approved * 100 == spent
    assert approved == 100


async def test_invalid_mode_raises(r: fakeredis.FakeAsyncRedis) -> None:
    with pytest.raises(ValueError):
        BudgetGuard(r, mode="optimistic")
