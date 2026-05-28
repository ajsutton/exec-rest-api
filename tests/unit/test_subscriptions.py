"""Tests for SubscriptionManager — multiplexing, reference counting, gap on reconnect."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from exec_rest_api.subscriptions import (
    GAP,
    StreamEvent,
    SubscriptionManager,
    SubscriptionUnavailable,
)


class FakeWebSocket:
    """In-memory stand-in for UpstreamWebSocket."""

    def __init__(self) -> None:
        self.request_log: list[tuple[str, list[Any]]] = []
        self.next_subscription_id = 0
        self.on_notification: Callable[[dict[str, Any]], None] | None = None
        self.on_reconnect: Callable[[], Awaitable[None]] | None = None
        self.connected = True
        # The fake_subscribe handler returns a synthetic subscription id.
        self.replies: dict[str, Any] = {}

    async def request(self, method: str, params: list[Any] | None = None) -> Any:
        self.request_log.append((method, list(params or [])))
        if method == "eth_subscribe":
            self.next_subscription_id += 1
            return f"0x{self.next_subscription_id:x}"
        if method == "eth_unsubscribe":
            return True
        return self.replies.get(method)

    def emit(self, subscription_id: str, payload: Any) -> None:
        assert self.on_notification is not None
        self.on_notification(
            {
                "jsonrpc": "2.0",
                "method": "eth_subscription",
                "params": {"subscription": subscription_id, "result": payload},
            }
        )

    async def trigger_reconnect(self) -> None:
        assert self.on_reconnect is not None
        await self.on_reconnect()


@pytest.fixture
def fake_ws() -> FakeWebSocket:
    return FakeWebSocket()


async def _drive_with_subscription(mgr: SubscriptionManager, fake_ws: FakeWebSocket) -> None:
    """Wire fake_ws's notification/reconnect callbacks to the manager."""
    fake_ws.on_notification = mgr.on_notification
    fake_ws.on_reconnect = mgr.on_reconnect


async def test_subscribe_calls_upstream_once_per_kind(fake_ws):
    mgr = SubscriptionManager(ws=fake_ws)
    await _drive_with_subscription(mgr, fake_ws)
    sub_a = await mgr.subscribe(kind="newHeads", params=None)
    sub_b = await mgr.subscribe(kind="newHeads", params=None)
    try:
        # Only one upstream subscribe call, despite two consumers
        assert [m for m, _ in fake_ws.request_log if m == "eth_subscribe"] == [
            "eth_subscribe"
        ]
        # Fan out: both consumers see the event
        fake_ws.emit("0x1", {"number": "0x10"})
        a_event = await asyncio.wait_for(sub_a.__anext__(), 1.0)
        b_event = await asyncio.wait_for(sub_b.__anext__(), 1.0)
        assert a_event == StreamEvent(kind="event", payload={"number": "0x10"})
        assert b_event == StreamEvent(kind="event", payload={"number": "0x10"})
    finally:
        await sub_a.aclose()
        await sub_b.aclose()


async def test_unsubscribe_when_last_consumer_leaves(fake_ws):
    mgr = SubscriptionManager(ws=fake_ws)
    await _drive_with_subscription(mgr, fake_ws)
    sub = await mgr.subscribe(kind="newHeads", params=None)
    await sub.aclose()
    # Allow background cleanup to run
    await asyncio.sleep(0)
    methods = [m for m, _ in fake_ws.request_log]
    assert "eth_subscribe" in methods
    assert "eth_unsubscribe" in methods


async def test_distinct_params_get_distinct_subscriptions(fake_ws):
    mgr = SubscriptionManager(ws=fake_ws)
    await _drive_with_subscription(mgr, fake_ws)
    sub_a = await mgr.subscribe(kind="logs", params={"address": "0xa"})
    sub_b = await mgr.subscribe(kind="logs", params={"address": "0xb"})
    try:
        subs = [m for m, _ in fake_ws.request_log if m == "eth_subscribe"]
        assert len(subs) == 2
    finally:
        await sub_a.aclose()
        await sub_b.aclose()


async def test_gap_emitted_on_reconnect(fake_ws):
    mgr = SubscriptionManager(ws=fake_ws)
    await _drive_with_subscription(mgr, fake_ws)
    sub = await mgr.subscribe(kind="newHeads", params=None)
    try:
        # Pre-reconnect: subscription id is "0x1"
        fake_ws.emit("0x1", {"number": "0x10"})
        assert (await asyncio.wait_for(sub.__anext__(), 1.0)).kind == "event"
        await fake_ws.trigger_reconnect()
        # First post-reconnect event must be a gap
        gap_event = await asyncio.wait_for(sub.__anext__(), 1.0)
        assert gap_event == GAP
        # And the manager re-issued eth_subscribe (new subscription id)
        fake_ws.emit("0x2", {"number": "0x11"})
        live_event = await asyncio.wait_for(sub.__anext__(), 1.0)
        assert live_event == StreamEvent(kind="event", payload={"number": "0x11"})
    finally:
        await sub.aclose()


async def test_subscribe_raises_when_ws_unavailable(fake_ws):
    fake_ws.connected = False
    mgr = SubscriptionManager(ws=fake_ws)
    await _drive_with_subscription(mgr, fake_ws)
    with pytest.raises(SubscriptionUnavailable):
        await mgr.subscribe(kind="newHeads", params=None)
