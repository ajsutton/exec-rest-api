"""Chain-head tracker.

Owns a single background task that keeps `current` populated with the latest
block number known to the upstream. Source order:

1. Subscribe to ``newHeads`` via the SubscriptionManager. The manager handles
   WS reconnect transparently, so we stay subscribed for the process lifetime.
2. If subscribing fails (no WS at all, or it dropped before subscribe time),
   fall back to polling ``eth_blockNumber`` at a fixed interval.

On every update, push the value to the ``chain_head_block`` Prometheus gauge.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncGenerator

from exec_rest_api.encoding import EncodingError, hex_to_int
from exec_rest_api.metrics import Metrics
from exec_rest_api.subscriptions import StreamEvent, SubscriptionManager, SubscriptionUnavailable
from exec_rest_api.upstream import UpstreamClient, UpstreamError, UpstreamJsonRpcError

logger = logging.getLogger("exec_rest_api.chain_head")


class ChainHeadTracker:
    """Maintains the latest known chain head."""

    def __init__(
        self,
        *,
        upstream: UpstreamClient,
        subscriptions: SubscriptionManager | None,
        metrics: Metrics,
        poll_interval_seconds: float = 12.0,
    ) -> None:
        self._upstream = upstream
        self._subscriptions = subscriptions
        self._metrics = metrics
        self._poll_interval = poll_interval_seconds
        self._current: int | None = None
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    @property
    def current(self) -> int | None:
        return self._current

    async def start(self) -> None:
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._run(), name="chain-head-tracker")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._stop_event.set()
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await self._task
        self._task = None

    # ── internals ────────────────────────────────────────────────────────

    def _update(self, block_number: int) -> None:
        self._current = block_number
        self._metrics.set_chain_head_block(block_number)

    async def _run(self) -> None:
        # Try the subscription path first; on failure, fall back to polling.
        stream: AsyncGenerator[StreamEvent, None] | None = None
        if self._subscriptions is not None:
            try:
                raw = await self._subscriptions.subscribe(kind="newHeads", params=None)
                # The concrete return type (_ConsumerStream) supports aclose(); cast so
                # mypy knows the variable has the full AsyncGenerator interface.
                stream = raw  # type: ignore[assignment]
            except SubscriptionUnavailable as exc:
                logger.info("newHeads subscribe unavailable (%s); polling instead", exc)
            except Exception as exc:
                logger.warning("newHeads subscribe failed (%r); polling instead", exc)

        if stream is not None:
            try:
                await self._consume_subscription(stream)
            finally:
                await stream.aclose()
        else:
            await self._poll_loop()

    async def _consume_subscription(self, stream: AsyncGenerator[StreamEvent, None]) -> None:
        async for event in stream:
            if self._stop_event.is_set():
                return
            if event.kind != "event":
                # GAP — SubscriptionManager has re-subscribed; events resume shortly.
                continue
            payload = event.payload or {}
            number_hex = payload.get("number") if isinstance(payload, dict) else None
            if not isinstance(number_hex, str):
                continue
            try:
                self._update(hex_to_int(number_hex))
            except EncodingError:
                logger.debug("ignoring malformed newHeads number: %r", number_hex)

    async def _poll_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                hex_value = await self._upstream.call("eth_blockNumber")
                self._update(hex_to_int(hex_value))
            except (UpstreamError, UpstreamJsonRpcError, EncodingError) as exc:
                logger.debug("chain head poll failed: %r", exc)
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._poll_interval
                )
            except asyncio.TimeoutError:
                continue
