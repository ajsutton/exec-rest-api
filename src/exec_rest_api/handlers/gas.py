"""/gas/* handlers — gas prices and EIP-1559 fee history."""

from __future__ import annotations

from aiohttp import web

from exec_rest_api.block_id import BlockIdError, parse_block_id
from exec_rest_api.encoding import decimal_to_hex, hex_to_int, wei_from_rpc
from exec_rest_api.errors import Problem, problem_response
from exec_rest_api.server import add_get
from exec_rest_api.upstream import UpstreamClient


async def gas_price(request: web.Request) -> web.Response:
    upstream: UpstreamClient = request.app["upstream"]
    rpc = await upstream.call("eth_gasPrice")
    return web.json_response({"wei": wei_from_rpc(rpc)})


async def priority_fee(request: web.Request) -> web.Response:
    upstream: UpstreamClient = request.app["upstream"]
    rpc = await upstream.call("eth_maxPriorityFeePerGas")
    return web.json_response({"wei": wei_from_rpc(rpc)})


async def blob_base_fee(request: web.Request) -> web.Response:
    upstream: UpstreamClient = request.app["upstream"]
    rpc = await upstream.call("eth_blobBaseFee")
    return web.json_response({"wei": wei_from_rpc(rpc)})


async def fee_history(request: web.Request) -> web.Response:
    block_count_raw = request.query.get("blockCount")
    newest_raw = request.query.get("newest")
    percentiles_raw = request.query.get("rewardPercentiles")

    if block_count_raw is None:
        return _bad_request(request.path, "missing required query parameter: blockCount")
    if newest_raw is None:
        return _bad_request(request.path, "missing required query parameter: newest")
    try:
        block_count = int(block_count_raw)
        if block_count < 1:
            raise ValueError
    except ValueError:
        return _bad_request(
            request.path,
            f"blockCount must be a positive integer, got {block_count_raw!r}",
        )
    try:
        newest = parse_block_id(newest_raw)
    except BlockIdError as e:
        return _bad_request(request.path, str(e))
    percentiles: list[float] = []
    if percentiles_raw is not None and percentiles_raw != "":
        try:
            percentiles = [float(p) for p in percentiles_raw.split(",")]
        except ValueError:
            return _bad_request(
                request.path,
                "rewardPercentiles must be a comma-separated list of numbers, "
                f"got {percentiles_raw!r}",
            )

    upstream: UpstreamClient = request.app["upstream"]
    rpc = await upstream.call(
        "eth_feeHistory",
        [decimal_to_hex(block_count), newest.to_rpc_param(), percentiles],
    )

    out: dict[str, object] = {
        "oldestBlock": hex_to_int(rpc["oldestBlock"]),
        "baseFeePerGas": [wei_from_rpc(x) for x in rpc["baseFeePerGas"]],
        "gasUsedRatio": list(rpc.get("gasUsedRatio") or []),
    }
    if "reward" in rpc and rpc["reward"] is not None:
        out["reward"] = [[wei_from_rpc(x) for x in row] for row in rpc["reward"]]
    if "baseFeePerBlobGas" in rpc and rpc["baseFeePerBlobGas"] is not None:
        out["baseFeePerBlobGas"] = [wei_from_rpc(x) for x in rpc["baseFeePerBlobGas"]]
    if "blobGasUsedRatio" in rpc and rpc["blobGasUsedRatio"] is not None:
        out["blobGasUsedRatio"] = list(rpc["blobGasUsedRatio"])
    return web.json_response(out)


def _bad_request(path: str, detail: str) -> web.Response:
    return problem_response(
        Problem(
            status=400,
            type_slug="invalid-request",
            title="Invalid request",
            detail=detail,
            instance=path,
        )
    )


def register_routes(app: web.Application) -> None:
    add_get(app, "/gas/price", gas_price)
    add_get(app, "/gas/priority-fee", priority_fee)
    add_get(app, "/gas/blob-base-fee", blob_base_fee)
    add_get(app, "/gas/fee-history", fee_history)
