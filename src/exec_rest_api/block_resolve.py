"""Resolve a parsed BlockId to a concrete integer block number via the upstream.

Pure resolution: parsing happens upstream (caller uses `parse_block_id`).
Returns the integer block number, or None when the block does not exist
(e.g., an unknown hash, or a tag like `safe` that the chain has no value for yet).
"""

from __future__ import annotations

from exec_rest_api.block_id import BlockId
from exec_rest_api.encoding import hex_to_int
from exec_rest_api.upstream import UpstreamClient


async def resolve_block_id(
    upstream: UpstreamClient, bid: BlockId
) -> int | None:
    """Resolve a parsed BlockId to an integer block number, or None if not found."""
    if bid.is_number():
        assert bid.number is not None
        return bid.number
    if bid.is_tag():
        if bid.tag == "earliest":
            return 0
        if bid.tag == "latest":
            head_hex = await upstream.call("eth_blockNumber")
            return hex_to_int(head_hex)
        # safe / finalized / pending — fetch the block summary
        rpc = await upstream.call("eth_getBlockByNumber", [bid.tag, False])
        if rpc is None:
            return None
        return hex_to_int(rpc["number"])
    assert bid.hash is not None
    rpc = await upstream.call("eth_getBlockByHash", [bid.hash, False])
    if rpc is None:
        return None
    return hex_to_int(rpc["number"])
