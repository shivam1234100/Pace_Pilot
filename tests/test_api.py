"""API tests against the running stack (make up). Run with: pytest -m integration"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

import httpx
import pytest

pytestmark = pytest.mark.integration

BASE_URL = os.environ.get("PP_BASE_URL", "http://localhost:8000")


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10) as c:
        yield c


async def make_campaign(client: httpx.AsyncClient, budget: int = 10_000) -> dict[str, Any]:
    resp = await client.post(
        "/campaigns", json={"name": "it-test", "platform": "google", "daily_budget_cents": budget}
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def event(cid: int, amount: int, event_id: str | None = None) -> dict[str, Any]:
    return {"event_id": event_id or uuid4().hex, "campaign_id": cid, "amount_cents": amount}


async def test_create_and_get_campaign(client: httpx.AsyncClient) -> None:
    c = await make_campaign(client)
    assert c["external_id"].startswith("google_")
    assert c["status"] == "active"
    got = (await client.get(f"/campaigns/{c['id']}")).json()
    assert got["spent_today_cents"] == 0
    assert got["remaining_cents"] == 10_000


async def test_spend_counts_and_ledger_matches_redis(client: httpx.AsyncClient) -> None:
    cid = (await make_campaign(client))["id"]
    events = [event(cid, 3_000), event(cid, 3_000), event(cid, 3_000), event(cid, 3_000)]
    body = (await client.post("/events/spend", json={"events": events})).json()
    assert (body["approved"], body["rejected"]) == (3, 1)
    got = (await client.get(f"/campaigns/{cid}")).json()
    assert got["spent_today_cents"] == 9_000
    assert got["ledger_today_cents"] == got["spent_today_cents"]


async def test_duplicate_counted_once(client: httpx.AsyncClient) -> None:
    cid = (await make_campaign(client))["id"]
    ev = event(cid, 500)
    first = (await client.post("/events/spend", json={"events": [ev]})).json()
    retry = (await client.post("/events/spend", json={"events": [ev, ev]})).json()
    assert first["approved"] == 1
    assert retry["duplicate"] == 2
    got = (await client.get(f"/campaigns/{cid}")).json()
    assert got["spent_today_cents"] == got["ledger_today_cents"] == 500


async def test_unknown_campaign_event(client: httpx.AsyncClient) -> None:
    body = (await client.post("/events/spend", json={"events": [event(10**12, 100)]})).json()
    assert body["unknown"] == 1


async def test_budget_patch(client: httpx.AsyncClient) -> None:
    cid = (await make_campaign(client, budget=1_000))["id"]
    resp = await client.patch(f"/campaigns/{cid}/budget", json={"daily_budget_cents": 5_000})
    assert resp.status_code == 200
    body = (await client.post("/events/spend", json={"events": [event(cid, 2_000)]})).json()
    assert body["approved"] == 1


async def test_missing_campaign_404(client: httpx.AsyncClient) -> None:
    assert (await client.get("/campaigns/999999999")).status_code == 404
    assert (await client.get("/campaigns/999999999/spend")).status_code == 404
    resp = await client.patch("/campaigns/999999999/budget", json={"daily_budget_cents": 1})
    assert resp.status_code == 404


@pytest.mark.parametrize(
    "path,payload",
    [
        ("/campaigns", {"name": "", "daily_budget_cents": 100}),
        ("/campaigns", {"name": "x", "daily_budget_cents": 0}),
        ("/campaigns", {"name": "x", "platform": "bing", "daily_budget_cents": 100}),
        ("/events/spend", {"events": []}),
        ("/events/spend", {"events": [{"event_id": "e", "campaign_id": 1, "amount_cents": -5}]}),
    ],
)
async def test_validation_422(client: httpx.AsyncClient, path: str, payload: dict) -> None:
    assert (await client.post(path, json=payload)).status_code == 422


async def test_hourly_spend_series(client: httpx.AsyncClient) -> None:
    cid = (await make_campaign(client))["id"]
    await client.post("/events/spend", json={"events": [event(cid, 700), event(cid, 300)]})
    series = (await client.get(f"/campaigns/{cid}/spend", params={"hours": 24})).json()
    assert sum(p["spend_cents"] for p in series) == 1_000
    assert sum(p["events"] for p in series) == 2


async def test_metrics_exposed(client: httpx.AsyncClient) -> None:
    text = (await client.get("/metrics")).text
    assert "pp_events_total" in text
    assert 'route="/campaigns/{cid}"' in text
