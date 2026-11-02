"""BudgetGuard: decides whether a spend event fits a campaign's remaining daily budget.

Two modes:
- atomic: one Lua script does dedupe, budget check, increment and pause flag in a single
  Redis call. Redis runs scripts one at a time, so concurrent events cannot overspend.
- naive: the same logic as separate round trips. It is racy on purpose (see _admit_naive)
  so the load test can show the overspend that the Lua script prevents.

Every key for a campaign shares the hash tag {cid}, so all keys the script touches live in
one Redis Cluster slot (multi-key scripts require that).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

import redis.asyncio as aioredis

DAY_KEY_TTL_S = 172800  # 48h: today's counters outlive the UTC day, then expire

Status = Literal["approved", "rejected", "duplicate", "unknown_campaign"]

ADMIT_LUA = """
-- KEYS: 1=dedupe, 2=spent(today), 3=budget, 4=paused flag(today)
-- ARGV: 1=amount_cents, 2=dedupe ttl seconds, 3=pause threshold (0..1)
-- Returns {code, spent_cents, pause_triggered}
--   code: 1 approved, 2 rejected (over budget), 0 duplicate, -1 unknown campaign
local budget = tonumber(redis.call('GET', KEYS[3]))
if not budget then return {-1, 0, 0} end
local spent = tonumber(redis.call('GET', KEYS[2]) or '0')
if redis.call('EXISTS', KEYS[1]) == 1 then return {0, spent, 0} end
local amt = tonumber(ARGV[1])
if spent + amt > budget then
  redis.call('SET', KEYS[1], 'r', 'EX', ARGV[2])
  return {2, spent, 0}
end
spent = redis.call('INCRBY', KEYS[2], amt)
redis.call('EXPIRE', KEYS[2], 172800)
redis.call('SET', KEYS[1], 'a', 'EX', ARGV[2])
local pause = 0
if spent >= budget * tonumber(ARGV[3]) then
  if redis.call('SET', KEYS[4], '1', 'NX', 'EX', 172800) then pause = 1 end
end
return {1, spent, pause}
"""

_CODES: dict[int, Status] = {
    1: "approved",
    2: "rejected",
    0: "duplicate",
    -1: "unknown_campaign",
}


@dataclass(frozen=True)
class Decision:
    status: Status
    spent_cents: int
    pause_triggered: bool


def day_key(now: datetime | None = None) -> str:
    """UTC calendar day as YYYYMMDD."""
    return (now or datetime.now(UTC)).strftime("%Y%m%d")


def budget_key(cid: int) -> str:
    return f"pp:{{{cid}}}:budget"


def spent_key(cid: int, day: str) -> str:
    return f"pp:{{{cid}}}:spent:{day}"


def paused_key(cid: int, day: str) -> str:
    return f"pp:{{{cid}}}:paused:{day}"


def event_key(cid: int, event_id: str) -> str:
    return f"pp:{{{cid}}}:evt:{event_id}"


class BudgetGuard:
    def __init__(
        self,
        r: aioredis.Redis,
        mode: str = "atomic",
        pause_threshold: float = 0.98,
        dedupe_ttl_s: int = 172800,
    ) -> None:
        if mode not in ("atomic", "naive"):
            raise ValueError(f"unknown counter mode {mode!r}; use 'atomic' or 'naive'")
        self.r = r
        self.mode = mode
        self.pause_threshold = pause_threshold
        self.dedupe_ttl_s = dedupe_ttl_s
        self._script = r.register_script(ADMIT_LUA)

    async def set_budget(self, cid: int, cents: int) -> None:
        await self.r.set(budget_key(cid), cents)

    async def spent(self, cid: int, day: str | None = None) -> int:
        value = await self.r.get(spent_key(cid, day or day_key()))
        return int(value or 0)

    async def admit(
        self, cid: int, event_id: str, amount_cents: int, day: str | None = None
    ) -> Decision:
        day = day or day_key()
        if self.mode == "naive":
            return await self._admit_naive(cid, event_id, amount_cents, day)
        keys = [
            event_key(cid, event_id),
            spent_key(cid, day),
            budget_key(cid),
            paused_key(cid, day),
        ]
        args = [amount_cents, self.dedupe_ttl_s, self.pause_threshold]
        code, spent, pause = await self._script(keys=keys, args=args)
        return Decision(_CODES[int(code)], int(spent), bool(int(pause)))

    async def _admit_naive(self, cid: int, event_id: str, amount_cents: int, day: str) -> Decision:
        # INTENTIONALLY RACY. Each await is a separate round trip, so other coroutines can
        # read the same "spent" value between our check and our INCRBY. Under concurrency
        # several events all pass the check and the campaign overspends. Exists only so the
        # load test can compare it with the atomic Lua path. Never use it for real money.
        budget_raw = await self.r.get(budget_key(cid))
        if budget_raw is None:
            return Decision("unknown_campaign", 0, False)
        budget = int(budget_raw)
        spent = int(await self.r.get(spent_key(cid, day)) or 0)
        if await self.r.exists(event_key(cid, event_id)):
            return Decision("duplicate", spent, False)
        if spent + amount_cents > budget:
            await self.r.set(event_key(cid, event_id), "r", ex=self.dedupe_ttl_s)
            return Decision("rejected", spent, False)
        spent = int(await self.r.incrby(spent_key(cid, day), amount_cents))
        await self.r.expire(spent_key(cid, day), DAY_KEY_TTL_S)
        await self.r.set(event_key(cid, event_id), "a", ex=self.dedupe_ttl_s)
        pause = False
        if spent >= budget * self.pause_threshold:
            pause = bool(await self.r.set(paused_key(cid, day), "1", nx=True, ex=DAY_KEY_TTL_S))
        return Decision("approved", spent, pause)
