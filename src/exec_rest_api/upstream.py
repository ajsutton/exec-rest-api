"""JSON-RPC HTTP client.

One `UpstreamClient` per process. Owns no session — the caller passes in an
`aiohttp.ClientSession` so connection pool configuration lives in the server
bootstrap. No retries: JSON-RPC isn't universally idempotent, and the proxy
prefers to surface failure to the caller rather than risk double-submits.

The optional `on_call` observer fires once per call with
``(method, status, duration_seconds)`` where status is ``"ok"``,
``"error"`` (JSON-RPC error returned), or ``"transport-error"``. Observer
exceptions are swallowed.
"""

from __future__ import annotations

import itertools
import logging
import time
from collections.abc import Callable
from typing import Any

import aiohttp
from aiohttp import ClientSession

logger = logging.getLogger(__name__)


class UpstreamError(Exception):
    """Transport-level failure talking to the upstream (HTTP status, garbled body, timeout)."""


class UpstreamJsonRpcError(Exception):
    """JSON-RPC error object returned by the upstream.

    Carries the raw `code`, `message`, and `data` so the error mapper can
    translate it into a Problem.
    """

    def __init__(self, *, code: int, message: str, data: Any = None) -> None:
        super().__init__(f"jsonrpc error {code}: {message}")
        self.code = code
        self.message = message
        self.data = data


OnCall = Callable[[str, str, float], None]


class UpstreamClient:
    """Async JSON-RPC client over HTTP."""

    def __init__(
        self,
        *,
        session: ClientSession,
        http_url: str,
        default_timeout_seconds: float = 30.0,
        on_call: OnCall | None = None,
    ) -> None:
        self._session = session
        self._url = http_url
        self._timeout = aiohttp.ClientTimeout(total=default_timeout_seconds)
        self._id_counter = itertools.count(1)
        self._on_call = on_call

    def _notify(self, method: str, status: str, duration_seconds: float) -> None:
        if self._on_call is None:
            return
        try:
            self._on_call(method, status, duration_seconds)
        except Exception:
            logger.exception("UpstreamClient on_call observer raised; ignoring")

    async def call(
        self,
        method: str,
        params: list[Any] | None = None,
        *,
        timeout_seconds: float | None = None,
    ) -> Any:
        """Issue one JSON-RPC request. Returns the `result` field on success.

        Raises:
            UpstreamError: transport failure (timeout, HTTP non-2xx, malformed response).
            UpstreamJsonRpcError: upstream returned a JSON-RPC `error` object.
        """
        body = {
            "jsonrpc": "2.0",
            "id": next(self._id_counter),
            "method": method,
            "params": params or [],
        }
        timeout = (
            aiohttp.ClientTimeout(total=timeout_seconds)
            if timeout_seconds is not None
            else self._timeout
        )
        start = time.monotonic()
        status = "ok"
        try:
            try:
                async with self._session.post(self._url, json=body, timeout=timeout) as resp:
                    if resp.status != 200:
                        raise UpstreamError(f"upstream HTTP {resp.status}")
                    try:
                        payload = await resp.json(content_type=None)
                    except (aiohttp.ContentTypeError, ValueError) as e:
                        raise UpstreamError(f"upstream returned non-JSON body: {e}") from e
            except aiohttp.ClientError as e:
                raise UpstreamError(f"upstream transport error: {e}") from e
            if not isinstance(payload, dict):
                raise UpstreamError(f"upstream returned non-object: {payload!r}")
            if "error" in payload:
                err = payload["error"]
                if not isinstance(err, dict):
                    raise UpstreamError(f"upstream error object malformed: {err!r}")
                status = "error"
                raise UpstreamJsonRpcError(
                    code=int(err.get("code", -32603)),
                    message=str(err.get("message", "")),
                    data=err.get("data"),
                )
            if "result" not in payload:
                raise UpstreamError(f"upstream response has neither result nor error: {payload!r}")
            return payload["result"]
        except UpstreamJsonRpcError:
            raise
        except UpstreamError:
            status = "transport-error"
            raise
        finally:
            self._notify(method, status, time.monotonic() - start)
