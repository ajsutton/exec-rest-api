"""Top-level test fixtures.

The `anvil_url` and `proxy_client` fixtures live here so they can be shared
between integration and conformance suites without `pytest_plugins` declarations
inside sub-directory conftests (pytest 7+ rejects those).
"""

from __future__ import annotations

import asyncio
import contextlib
import shutil
import socket
import subprocess
import time
from collections.abc import Iterator

import aiohttp
import pytest
import pytest_asyncio

from exec_rest_api.config import Config
from exec_rest_api.handlers import (
    accounts,
    blocks,
    chain,
    computed,
    gas,
    health,
    logs,
    traces,
    transactions,
    utils_keccak,
)
from exec_rest_api.server import create_app
from exec_rest_api.upstream import UpstreamClient


def _find_free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def anvil_url() -> Iterator[str]:
    anvil = shutil.which("anvil")
    if anvil is None:
        pytest.skip(
            "anvil not found on PATH. Install foundry "
            "(https://book.getfoundry.sh/getting-started/installation) and retry."
        )
    port = _find_free_port()
    proc = subprocess.Popen(  # noqa: S603
        [
            anvil,
            "--port", str(port),
            "--silent",
            "--block-time", "1",  # auto-mine every second so syncing/timestamps look realistic
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"http://127.0.0.1:{port}"
    # Poll for liveness
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                break
        except OSError:
            time.sleep(0.1)
    else:
        proc.kill()
        pytest.fail("anvil failed to start within 10s")
    try:
        yield url
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def _build_config(upstream_http: str) -> Config:
    return Config(
        upstream_http=upstream_http,
        upstream_ws=upstream_http.replace("http://", "ws://"),
        listen="127.0.0.1:0",
        upstream_timeout_seconds=10.0,
        default_page_size=1000,
        max_page_size=10000,
        sse_buffer_bytes=65536,
        sse_replay_window=1024,
        sse_heartbeat_seconds=30,
        ready_sync_lag=10,
        log_level="info",
        log_format="json",
        metrics_enabled=True,
    )


@pytest_asyncio.fixture
async def proxy_client(anvil_url, aiohttp_client):
    """Build the proxy app talking to anvil and return an aiohttp test client."""
    from exec_rest_api.handlers import streams as streams_handler
    from exec_rest_api.subscriptions import SubscriptionManager
    from exec_rest_api.upstream_ws import UpstreamWebSocket

    ws_url = anvil_url.replace("http://", "ws://")
    async with aiohttp.ClientSession() as session:
        upstream = UpstreamClient(session=session, http_url=anvil_url)
        ws_client = UpstreamWebSocket(
            session=session,
            url=ws_url,
            on_notification=lambda _: None,
            backoff_schedule=(0.1,),
        )
        manager = SubscriptionManager(ws=ws_client)
        ws_client.on_notification = manager.on_notification
        ws_client.on_reconnect = manager.on_reconnect
        with contextlib.suppress(asyncio.TimeoutError, Exception):
            await asyncio.wait_for(ws_client.start(), timeout=5.0)

        app = create_app(config=_build_config(anvil_url), upstream=upstream)
        app["subscriptions"] = manager
        health.register_routes(app)
        chain.register_routes(app)
        gas.register_routes(app)
        transactions.register_routes(app)
        blocks.register_routes(app)
        accounts.register_routes(app)
        logs.register_routes(app)
        traces.register_routes(app)
        computed.register_routes(app)
        utils_keccak.register_routes(app)
        streams_handler.register_routes(app)
        try:
            client = await aiohttp_client(app)
            yield client
        finally:
            await ws_client.stop()
