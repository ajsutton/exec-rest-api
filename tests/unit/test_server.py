"""Tests for server scaffolding: middleware behaviour, error mapping."""

import logging
from unittest.mock import AsyncMock

import pytest
from aiohttp import web

from exec_rest_api.config import Config
from exec_rest_api.server import create_app
from exec_rest_api.upstream import UpstreamClient, UpstreamJsonRpcError


def _make_config(**overrides) -> Config:
    base = dict(
        upstream_http="http://localhost:8545",
        upstream_ws="ws://localhost:8545",
        listen="127.0.0.1:8080",
        upstream_timeout_seconds=30.0,
        default_page_size=1000,
        max_page_size=10000,
        sse_buffer_bytes=65536,
        sse_replay_window=1024,
        sse_heartbeat_seconds=30,
        ready_sync_lag=10,
        log_level="info",
        log_format=None,
        metrics_enabled=True,
    )
    base.update(overrides)
    return Config(**base)


@pytest.fixture
def app_with_test_route(aiohttp_client):
    """Build an app with a route that lets us trigger each error path."""

    async def factory(mock_upstream: UpstreamClient) -> web.Application:
        config = _make_config()
        app = create_app(config=config, upstream=mock_upstream)

        async def trigger_jsonrpc_error(request: web.Request) -> web.Response:
            raise UpstreamJsonRpcError(code=-32601, message="method not found")

        async def trigger_unexpected(request: web.Request) -> web.Response:
            raise RuntimeError("boom")

        app.router.add_get("/_test/jsonrpc-error", trigger_jsonrpc_error)
        app.router.add_get("/_test/unexpected", trigger_unexpected)
        return app

    return factory


async def test_request_id_generated_when_absent(aiohttp_client, app_with_test_route):
    mock = AsyncMock(spec=UpstreamClient)
    app = await app_with_test_route(mock)

    async def echo_request_id(request: web.Request) -> web.Response:
        return web.Response(text=request["request_id"])

    app.router.add_get("/_test/echo-id", echo_request_id)
    client = await aiohttp_client(app)
    resp = await client.get("/_test/echo-id")
    text = await resp.text()
    assert text  # non-empty UUID
    assert resp.headers["X-Request-ID"] == text


async def test_request_id_honored_when_provided(aiohttp_client, app_with_test_route):
    mock = AsyncMock(spec=UpstreamClient)
    app = await app_with_test_route(mock)

    async def echo_request_id(request: web.Request) -> web.Response:
        return web.Response(text=request["request_id"])

    app.router.add_get("/_test/echo-id", echo_request_id)
    client = await aiohttp_client(app)
    resp = await client.get("/_test/echo-id", headers={"X-Request-ID": "fixed-id-123"})
    assert (await resp.text()) == "fixed-id-123"
    assert resp.headers["X-Request-ID"] == "fixed-id-123"


async def test_jsonrpc_error_translated_to_problem(aiohttp_client, app_with_test_route):
    mock = AsyncMock(spec=UpstreamClient)
    app = await app_with_test_route(mock)
    client = await aiohttp_client(app)

    resp = await client.get("/_test/jsonrpc-error")
    assert resp.status == 501
    assert resp.content_type == "application/problem+json"
    body = await resp.json()
    assert body["type"].endswith("/method-not-supported-by-upstream")
    assert body["status"] == 501
    assert body["code"] == -32601


async def test_unexpected_exception_returns_500_problem(
    aiohttp_client, app_with_test_route, caplog
):
    mock = AsyncMock(spec=UpstreamClient)
    app = await app_with_test_route(mock)
    client = await aiohttp_client(app)

    with caplog.at_level(logging.ERROR):
        resp = await client.get("/_test/unexpected")
    assert resp.status == 500
    assert resp.content_type == "application/problem+json"
    body = await resp.json()
    assert body["type"].endswith("/internal-error")
    # Internal error: detail does NOT leak the exception message
    assert "boom" not in body.get("detail", "")
    # But the log output does (via the captured exception traceback), so operators can debug.
    # caplog.text includes formatted exception info from logger.exception().
    assert "boom" in caplog.text
