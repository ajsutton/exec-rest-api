"""/chain/* handlers."""

from __future__ import annotations

import asyncio

from aiohttp import web

from exec_rest_api.encoding import hex_to_int
from exec_rest_api.upstream import UpstreamClient


async def chain(request: web.Request) -> web.Response:
    """Composite: chainId + networkId + client + blockNumber + syncing in one round trip."""
    upstream: UpstreamClient = request.app["upstream"]
    chain_id_hex, network_id, client_ver, sync, block_hex = await asyncio.gather(
        upstream.call("eth_chainId"),
        upstream.call("net_version"),
        upstream.call("web3_clientVersion"),
        upstream.call("eth_syncing"),
        upstream.call("eth_blockNumber"),
    )
    return web.json_response(
        {
            "chainId": hex_to_int(chain_id_hex),
            "networkId": network_id,
            "client": client_ver,
            "blockNumber": hex_to_int(block_hex),
            "syncing": _sync_to_rest(sync),
        }
    )


async def chain_id(request: web.Request) -> web.Response:
    upstream: UpstreamClient = request.app["upstream"]
    chain_id_hex = await upstream.call("eth_chainId")
    return web.json_response({"chainId": hex_to_int(chain_id_hex)})


async def chain_sync_status(request: web.Request) -> web.Response:
    upstream: UpstreamClient = request.app["upstream"]
    sync = await upstream.call("eth_syncing")
    return web.json_response(_sync_to_rest(sync))


async def chain_client(request: web.Request) -> web.Response:
    upstream: UpstreamClient = request.app["upstream"]
    client_ver = await upstream.call("web3_clientVersion")
    return web.json_response({"client": client_ver})


async def chain_peers(request: web.Request) -> web.Response:
    upstream: UpstreamClient = request.app["upstream"]
    peer_hex, listening = await asyncio.gather(
        upstream.call("net_peerCount"),
        upstream.call("net_listening"),
    )
    return web.json_response({"peerCount": hex_to_int(peer_hex), "listening": bool(listening)})


def _sync_to_rest(rpc_value: object) -> dict[str, object]:
    """Convert eth_syncing response (False or dict) to REST shape."""
    if rpc_value is False:
        return {"syncing": False}
    if isinstance(rpc_value, dict):
        return {
            "syncing": True,
            "startingBlock": hex_to_int(rpc_value["startingBlock"]),
            "currentBlock": hex_to_int(rpc_value["currentBlock"]),
            "highestBlock": hex_to_int(rpc_value["highestBlock"]),
        }
    raise ValueError(f"unexpected eth_syncing response: {rpc_value!r}")


def register_routes(app: web.Application) -> None:
    app.router.add_get("/chain", chain)
    app.router.add_get("/chain/id", chain_id)
    app.router.add_get("/chain/sync-status", chain_sync_status)
    app.router.add_get("/chain/client", chain_client)
    app.router.add_get("/chain/peers", chain_peers)
