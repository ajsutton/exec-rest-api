"""Tests for the JSON-RPC HTTP client."""

from typing import Any

import pytest
from aiohttp import ClientSession, web

from exec_rest_api.upstream import UpstreamClient, UpstreamError, UpstreamJsonRpcError


@pytest.fixture
async def stub_upstream(aiohttp_server):
    """A minimal aiohttp app simulating an upstream JSON-RPC server."""
    captured: list[dict[str, Any]] = []

    async def handler(request: web.Request) -> web.Response:
        body = await request.json()
        captured.append(body)
        # Test-controlled reply: read the method name and dispatch
        method = body["method"]
        rpc_id = body["id"]
        if method == "rpc_ok":
            return web.json_response({"jsonrpc": "2.0", "id": rpc_id, "result": "hello"})
        if method == "rpc_error":
            return web.json_response(
                {
                    "jsonrpc": "2.0",
                    "id": rpc_id,
                    "error": {"code": -32000, "message": "oh no", "data": {"hint": 1}},
                }
            )
        if method == "rpc_http_500":
            return web.Response(status=500, text="boom")
        if method == "rpc_garbled":
            return web.Response(status=200, text="not json")
        return web.json_response(
            {"jsonrpc": "2.0", "id": rpc_id, "error": {"code": -32601, "message": "no"}}
        )

    app = web.Application()
    app.router.add_post("/", handler)
    server = await aiohttp_server(app)
    return server, captured


async def test_call_success(stub_upstream):
    server, captured = stub_upstream
    async with ClientSession() as session:
        client = UpstreamClient(session=session, http_url=str(server.make_url("/")))
        result = await client.call("rpc_ok", ["param1", 42])
        assert result == "hello"
    assert captured == [
        {"jsonrpc": "2.0", "id": 1, "method": "rpc_ok", "params": ["param1", 42]}
    ]


async def test_call_jsonrpc_error_raises(stub_upstream):
    server, _ = stub_upstream
    async with ClientSession() as session:
        client = UpstreamClient(session=session, http_url=str(server.make_url("/")))
        with pytest.raises(UpstreamJsonRpcError) as exc_info:
            await client.call("rpc_error", [])
        assert exc_info.value.code == -32000
        assert exc_info.value.message == "oh no"
        assert exc_info.value.data == {"hint": 1}


async def test_call_http_500_raises(stub_upstream):
    server, _ = stub_upstream
    async with ClientSession() as session:
        client = UpstreamClient(session=session, http_url=str(server.make_url("/")))
        with pytest.raises(UpstreamError):
            await client.call("rpc_http_500", [])


async def test_call_garbled_response_raises(stub_upstream):
    server, _ = stub_upstream
    async with ClientSession() as session:
        client = UpstreamClient(session=session, http_url=str(server.make_url("/")))
        with pytest.raises(UpstreamError):
            await client.call("rpc_garbled", [])


async def test_call_id_increments(stub_upstream):
    server, captured = stub_upstream
    async with ClientSession() as session:
        client = UpstreamClient(session=session, http_url=str(server.make_url("/")))
        await client.call("rpc_ok", [])
        await client.call("rpc_ok", [])
        await client.call("rpc_ok", [])
    assert [c["id"] for c in captured] == [1, 2, 3]


async def test_call_many_parallel(stub_upstream):
    """Many requests in parallel get unique IDs and correct responses."""
    import asyncio
    server, _ = stub_upstream
    async with ClientSession() as session:
        client = UpstreamClient(session=session, http_url=str(server.make_url("/")))
        results = await asyncio.gather(*(client.call("rpc_ok", []) for _ in range(20)))
    assert results == ["hello"] * 20
