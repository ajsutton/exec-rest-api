"""Subscription multiplexer for the upstream WebSocket.

One upstream `eth_subscribe` per unique (kind, params) tuple, fanned out to N
consumer queues. On WS reconnect, all active upstream subscriptions are
re-issued and each consumer queue receives a synthetic `GAP` event so the SSE
handler can emit `event: gap` to its client.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Callable, Coroutine
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from exec_rest_api.upstream_ws import UpstreamWsClosed

logger = logging.getLogger("exec_rest_api.subscriptions")

StreamKind = Literal["newHeads", "logs", "newPendingTransactions", "syncing"]


class SubscriptionUnavailable(Exception):
    """Raised when the upstream WS isn't connected and a subscribe is attempted."""


@dataclass(frozen=True)
class StreamEvent:
    """One event delivered to a consumer.

    `kind == "event"` carries the JSON-RPC `result` payload verbatim.
    `kind == "gap"` is a synthetic sentinel (see GAP).
    """

    kind: Literal["event", "gap"]
    payload: Any = None


GAP = StreamEvent(kind="gap")


class _WebSocketLike(Protocol):
    @property
    def connected(self) -> bool: ...

    async def request(self, method: str, params: list[Any] | None = None) -> Any: ...


def _canonicalize(params: Any) -> str:
    """Stable canonical form of subscribe params for use as a dict key."""
    return json.dumps(params, sort_keys=True, separators=(",", ":"))


@dataclass
class _Slot:
    """One upstream subscription and its consumer queues.

    `subscription_id` is only `None` transiently during the reconnect window.
    """

    kind: StreamKind
    params: Any
    subscription_id: str | None
    consumers: list[asyncio.Queue[StreamEvent]]


class _ConsumerStream:
    """AsyncIterator wrapper around a queue with guaranteed aclose cleanup.

    Using a class rather than an async generator ensures that `aclose()` always
    runs the cleanup coroutine — even if the iterator was never entered. An async
    generator's `finally` block only executes if the generator has been started.
    """

    def __init__(
        self,
        *,
        queue: asyncio.Queue[StreamEvent],
        on_close: Callable[[], Coroutine[Any, Any, None]],
    ) -> None:
        self._queue = queue
        self._on_close = on_close
        self._closed = False

    def __aiter__(self) -> AsyncIterator[StreamEvent]:
        return self

    async def __anext__(self) -> StreamEvent:
        if self._closed:
            raise StopAsyncIteration
        return await self._queue.get()

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._on_close()


class SubscriptionManager:
    """Multiplex upstream eth_subscribe calls across N client SSE streams."""

    def __init__(self, *, ws: _WebSocketLike) -> None:
        self._ws = ws
        self._slots: dict[tuple[StreamKind, str], _Slot] = {}
        self._slot_by_subscription_id: dict[str, _Slot] = {}
        self._lock = asyncio.Lock()

    # ── internal helpers ──────────────────────────────────────────────────

    def _enqueue(self, q: asyncio.Queue[StreamEvent], event: StreamEvent, kind: StreamKind) -> None:
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning("dropping event for slow consumer on %s", kind)

    # ── public callbacks for UpstreamWebSocket ────────────────────────────

    def on_notification(self, payload: dict[str, Any]) -> None:
        params = payload.get("params") or {}
        sub_id = params.get("subscription")
        if not isinstance(sub_id, str):
            return
        slot = self._slot_by_subscription_id.get(sub_id)
        if slot is None:
            return
        event = StreamEvent(kind="event", payload=params.get("result"))
        for q in slot.consumers:
            self._enqueue(q, event, slot.kind)

    async def on_reconnect(self) -> None:
        """Re-issue all active subscriptions and notify all consumers of the gap."""
        async with self._lock:
            self._slot_by_subscription_id.clear()
            for slot in list(self._slots.values()):
                # Re-subscribe upstream first
                params_list = _params_to_subscribe_args(slot.kind, slot.params)
                try:
                    new_id = await self._ws.request("eth_subscribe", params_list)
                except Exception as exc:
                    logger.warning("re-subscribe failed for %s: %r", slot.kind, exc)
                    for q in slot.consumers:
                        self._enqueue(q, GAP, slot.kind)
                    continue
                slot.subscription_id = new_id
                self._slot_by_subscription_id[new_id] = slot
                for q in slot.consumers:
                    self._enqueue(q, GAP, slot.kind)

    # ── public subscribe API ──────────────────────────────────────────────

    async def subscribe(
        self,
        *,
        kind: StreamKind,
        params: Any,
    ) -> AsyncIterator[StreamEvent]:
        """Open a new consumer queue against (kind, params)."""
        if not self._ws.connected:
            raise SubscriptionUnavailable("upstream WS not connected")

        key = (kind, _canonicalize(params))
        queue: asyncio.Queue[StreamEvent] = asyncio.Queue(maxsize=1024)

        async with self._lock:
            slot = self._slots.get(key)
            if slot is None:
                params_list = _params_to_subscribe_args(kind, params)
                try:
                    sub_id = await self._ws.request("eth_subscribe", params_list)
                except UpstreamWsClosed as exc:
                    raise SubscriptionUnavailable(str(exc)) from exc
                slot = _Slot(
                    kind=kind, params=params, subscription_id=sub_id, consumers=[queue]
                )
                self._slots[key] = slot
                self._slot_by_subscription_id[sub_id] = slot
            else:
                slot.consumers.append(queue)

        return _ConsumerStream(
            queue=queue,
            on_close=lambda: self._remove_consumer(key, slot, queue),
        )

    async def _remove_consumer(
        self,
        key: tuple[StreamKind, str],
        slot: _Slot,
        queue: asyncio.Queue[StreamEvent],
    ) -> None:
        async with self._lock:
            try:
                slot.consumers.remove(queue)
            except ValueError:
                return
            if slot.consumers:
                return
            sub_id = slot.subscription_id
            self._slots.pop(key, None)
            if sub_id is not None:
                self._slot_by_subscription_id.pop(sub_id, None)
        if sub_id is not None and self._ws.connected:
            try:
                await self._ws.request("eth_unsubscribe", [sub_id])
            except Exception as exc:
                logger.debug("eth_unsubscribe failed (cleaning up anyway): %r", exc)


def _params_to_subscribe_args(kind: StreamKind, params: Any) -> list[Any]:
    """Translate (kind, params) into the JSON-RPC eth_subscribe argument list."""
    if params is None:
        return [kind]
    return [kind, params]
