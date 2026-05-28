"""Tests for server scaffolding: middleware behaviour, error mapping."""

import logging
from unittest.mock import AsyncMock

import pytest
from aiohttp import web

from exec_rest_api.config import Config
from exec_rest_api.server import add_get, create_app
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


async def test_add_get_matches_with_and_without_trailing_slash(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    app = create_app(config=_make_config(), upstream=mock)

    async def hello(request: web.Request) -> web.Response:
        return web.Response(text="hello")

    add_get(app, "/foo", hello)

    client = await aiohttp_client(app)
    assert (await (await client.get("/foo")).text()) == "hello"
    assert (await (await client.get("/foo/")).text()) == "hello"


async def test_add_get_root_path_unchanged(aiohttp_client):
    """For path '/', no second route should be added (it would collide)."""
    mock = AsyncMock(spec=UpstreamClient)
    app = create_app(config=_make_config(), upstream=mock)

    async def hello(request: web.Request) -> web.Response:
        return web.Response(text="root")

    add_get(app, "/", hello)

    client = await aiohttp_client(app)
    assert (await (await client.get("/")).text()) == "root"


# ── Plan 5: metrics middleware, X-Upstream-Method, X-Block-Height ──────────


class _StubChainHead:
    def __init__(self, value: int | None) -> None:
        self.current = value


async def test_metrics_middleware_counts_and_times(aiohttp_client):
    from exec_rest_api.metrics import Metrics
    metrics = Metrics()
    mock = AsyncMock(spec=UpstreamClient)
    app = create_app(config=_make_config(), upstream=mock)
    app["metrics"] = metrics

    async def ok(request: web.Request) -> web.Response:
        return web.Response(text="ok")

    app.router.add_get("/_test/ok", ok)
    client = await aiohttp_client(app)
    await client.get("/_test/ok")
    out = metrics.render()
    assert (
        'exec_rest_api_requests_total{method="GET",path_template="/_test/ok",status="200"} 1'
        in out
    )
    assert "exec_rest_api_request_duration_seconds_count 1" in out


async def test_metrics_middleware_path_template_for_dynamic_routes(aiohttp_client):
    from exec_rest_api.metrics import Metrics
    metrics = Metrics()
    mock = AsyncMock(spec=UpstreamClient)
    app = create_app(config=_make_config(), upstream=mock)
    app["metrics"] = metrics

    async def ok(request: web.Request) -> web.Response:
        return web.Response(text="ok")

    app.router.add_get("/_test/{id}", ok)
    client = await aiohttp_client(app)
    await client.get("/_test/123")
    out = metrics.render()
    assert 'path_template="/_test/{id}"' in out


async def test_metrics_middleware_path_template_for_unmatched(aiohttp_client):
    from exec_rest_api.metrics import Metrics
    metrics = Metrics()
    mock = AsyncMock(spec=UpstreamClient)
    app = create_app(config=_make_config(), upstream=mock)
    app["metrics"] = metrics
    client = await aiohttp_client(app)
    await client.get("/this-does-not-exist")
    out = metrics.render()
    assert 'path_template="__not_found__"' in out
    assert 'status="404"' in out


async def test_x_upstream_method_header_from_contextvar(aiohttp_client):
    from exec_rest_api.metrics import (
        Metrics,
        current_request_upstream_methods,
    )
    metrics = Metrics()
    mock = AsyncMock(spec=UpstreamClient)
    app = create_app(config=_make_config(), upstream=mock)
    app["metrics"] = metrics

    async def calls_upstream(request: web.Request) -> web.Response:
        methods = current_request_upstream_methods.get()
        # Simulate UpstreamClient appending two methods
        if methods is not None:
            methods.append("eth_chainId")
            methods.append("net_version")
        return web.Response(text="ok")

    app.router.add_get("/_test/multi", calls_upstream)
    client = await aiohttp_client(app)
    resp = await client.get("/_test/multi")
    assert resp.headers["X-Upstream-Method"] == "eth_chainId,net_version"


async def test_x_upstream_method_header_absent_when_no_upstream_calls(aiohttp_client):
    from exec_rest_api.metrics import Metrics
    metrics = Metrics()
    mock = AsyncMock(spec=UpstreamClient)
    app = create_app(config=_make_config(), upstream=mock)
    app["metrics"] = metrics

    async def no_upstream(request: web.Request) -> web.Response:
        return web.Response(text="ok")

    app.router.add_get("/_test/no-upstream", no_upstream)
    client = await aiohttp_client(app)
    resp = await client.get("/_test/no-upstream")
    assert "X-Upstream-Method" not in resp.headers


async def test_x_block_height_header_from_tracker(aiohttp_client):
    from exec_rest_api.metrics import Metrics
    metrics = Metrics()
    mock = AsyncMock(spec=UpstreamClient)
    app = create_app(config=_make_config(), upstream=mock)
    app["metrics"] = metrics
    app["chain_head"] = _StubChainHead(value=18234567)

    async def ok(request: web.Request) -> web.Response:
        return web.Response(text="ok")

    app.router.add_get("/_test/height", ok)
    client = await aiohttp_client(app)
    resp = await client.get("/_test/height")
    assert resp.headers["X-Block-Height"] == "18234567"


async def test_x_block_height_header_absent_when_unknown(aiohttp_client):
    from exec_rest_api.metrics import Metrics
    metrics = Metrics()
    mock = AsyncMock(spec=UpstreamClient)
    app = create_app(config=_make_config(), upstream=mock)
    app["metrics"] = metrics
    app["chain_head"] = _StubChainHead(value=None)

    async def ok(request: web.Request) -> web.Response:
        return web.Response(text="ok")

    app.router.add_get("/_test/no-height", ok)
    client = await aiohttp_client(app)
    resp = await client.get("/_test/no-height")
    assert "X-Block-Height" not in resp.headers


async def test_metrics_middleware_records_500_status_on_exception(
    aiohttp_client, app_with_test_route
):
    from exec_rest_api.metrics import Metrics
    metrics = Metrics()
    mock = AsyncMock(spec=UpstreamClient)
    app = await app_with_test_route(mock)
    app["metrics"] = metrics
    client = await aiohttp_client(app)
    await client.get("/_test/unexpected")
    out = metrics.render()
    assert 'status="500"' in out
