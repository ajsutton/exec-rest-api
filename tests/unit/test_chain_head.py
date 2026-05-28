"""Tests for the chain-head tracker (subscribe-with-poll-fallback)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

from exec_rest_api.chain_head import ChainHeadTracker
from exec_rest_api.metrics import Metrics
from exec_rest_api.subscriptions import GAP, StreamEvent, SubscriptionUnavailable


class _FakeStream:
    """Minimal async iterator matching SubscriptionManager.subscribe()'s return type."""

    def __init__(self, events: list[StreamEvent]) -> None:
        self._queue: asyncio.Queue[StreamEvent] = asyncio.Queue()
        for e in events:
            self._queue.put_nowait(e)
        self._closed = False

    def __aiter__(self) -> _FakeStream:
        return self

    async def __anext__(self) -> StreamEvent:
        if self._closed:
            raise StopAsyncIteration
        return await self._queue.get()

    async def aclose(self) -> None:
        self._closed = True

    def push(self, event: StreamEvent) -> None:
        self._queue.put_nowait(event)


async def test_starts_with_no_current_value():
    """Before start(), and before any event arrives, current is None."""
    metrics = Metrics()
    upstream = AsyncMock()
    subs = AsyncMock()
    tracker = ChainHeadTracker(upstream=upstream, subscriptions=subs, metrics=metrics)
    assert tracker.current is None


async def test_subscription_path_updates_current_and_gauge():
    """When subscribe() succeeds, newHeads events update the current value."""
    metrics = Metrics()
    upstream = AsyncMock()
    subs = AsyncMock()
    stream = _FakeStream(
        [
            StreamEvent(kind="event", payload={"number": "0x10", "hash": "0xabc"}),
        ]
    )
    subs.subscribe.return_value = stream

    tracker = ChainHeadTracker(upstream=upstream, subscriptions=subs, metrics=metrics)
    await tracker.start()
    for _ in range(50):
        if tracker.current is not None:
            break
        await asyncio.sleep(0.01)
    assert tracker.current == 16
    assert "exec_rest_api_chain_head_block 16" in metrics.render()
    await tracker.stop()


async def test_subscription_path_ignores_gap_events():
    """A gap event must not crash the consumer; current keeps its prior value."""
    metrics = Metrics()
    upstream = AsyncMock()
    subs = AsyncMock()
    stream = _FakeStream(
        [
            StreamEvent(kind="event", payload={"number": "0x1"}),
            GAP,
            StreamEvent(kind="event", payload={"number": "0x2"}),
        ]
    )
    subs.subscribe.return_value = stream

    tracker = ChainHeadTracker(upstream=upstream, subscriptions=subs, metrics=metrics)
    await tracker.start()
    for _ in range(50):
        if tracker.current == 2:
            break
        await asyncio.sleep(0.01)
    assert tracker.current == 2
    await tracker.stop()


async def test_polling_path_when_ws_unavailable():
    """If subscribe raises SubscriptionUnavailable, falls back to polling."""
    metrics = Metrics()
    upstream = AsyncMock()
    upstream.call.side_effect = ["0x5", "0x6", "0x7"]
    subs = AsyncMock()
    subs.subscribe.side_effect = SubscriptionUnavailable("ws not connected")

    tracker = ChainHeadTracker(
        upstream=upstream,
        subscriptions=subs,
        metrics=metrics,
        poll_interval_seconds=0.05,
    )
    await tracker.start()
    for _ in range(100):
        if tracker.current == 5:
            break
        await asyncio.sleep(0.01)
    assert tracker.current == 5
    upstream.call.assert_any_call("eth_blockNumber")
    await tracker.stop()


async def test_polling_continues_through_transient_errors():
    """A transient upstream error during polling must not kill the tracker."""
    from exec_rest_api.upstream import UpstreamError

    metrics = Metrics()
    upstream = AsyncMock()
    upstream.call.side_effect = [UpstreamError("down"), "0x9"]
    subs = AsyncMock()
    subs.subscribe.side_effect = SubscriptionUnavailable("ws not connected")

    tracker = ChainHeadTracker(
        upstream=upstream,
        subscriptions=subs,
        metrics=metrics,
        poll_interval_seconds=0.05,
    )
    await tracker.start()
    for _ in range(100):
        if tracker.current == 9:
            break
        await asyncio.sleep(0.02)
    assert tracker.current == 9
    await tracker.stop()


async def test_stop_idempotent():
    metrics = Metrics()
    subs = AsyncMock()
    subs.subscribe.side_effect = SubscriptionUnavailable("ws not connected")
    upstream = AsyncMock()
    upstream.call.return_value = "0x1"
    tracker = ChainHeadTracker(
        upstream=upstream,
        subscriptions=subs,
        metrics=metrics,
        poll_interval_seconds=0.05,
    )
    await tracker.start()
    await tracker.stop()
    await tracker.stop()  # second call must not raise


async def test_works_without_subscription_manager():
    """If subscriptions is None, tracker polls."""
    metrics = Metrics()
    upstream = AsyncMock()
    upstream.call.return_value = "0x4"
    tracker = ChainHeadTracker(
        upstream=upstream,
        subscriptions=None,
        metrics=metrics,
        poll_interval_seconds=0.05,
    )
    await tracker.start()
    for _ in range(100):
        if tracker.current == 4:
            break
        await asyncio.sleep(0.02)
    assert tracker.current == 4
    await tracker.stop()
