"""Tests for the /logs pagination/chunking helper."""

from typing import Any
from unittest.mock import AsyncMock

import pytest

from exec_rest_api.pagination import (
    DEFAULT_CHUNK_SIZE,
    PaginationResult,
    fetch_logs_paginated,
)
from exec_rest_api.upstream import UpstreamClient, UpstreamJsonRpcError


def _log(block: int, log_index: int) -> dict[str, Any]:
    return {
        "address": "0x" + "aa" * 20,
        "topics": [],
        "data": "0x",
        "blockHash": f"0x{block:064x}",
        "blockNumber": f"0x{block:x}",
        "transactionHash": "0x" + "ee" * 32,
        "transactionIndex": "0x0",
        "logIndex": f"0x{log_index:x}",
        "removed": False,
    }


async def test_single_chunk_complete():
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = [_log(10, 0), _log(11, 0)]
    result = await fetch_logs_paginated(
        upstream=mock,
        filter_={"address": "0x" + "aa" * 20},
        from_block=10,
        to_block=11,
        limit=100,
    )
    assert isinstance(result, PaginationResult)
    assert len(result.items) == 2
    assert result.next_from_block is None  # exhausted


async def test_limit_stops_mid_range():
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = [_log(10, 0), _log(10, 1), _log(11, 0)]
    result = await fetch_logs_paginated(
        upstream=mock,
        filter_={},
        from_block=10,
        to_block=20,
        limit=2,
    )
    assert len(result.items) == 2
    # We stopped after consuming both logs from block 10
    assert result.next_from_block == 10
    assert result.last_log_index == 1


async def test_resume_skips_already_emitted():
    """When resuming with a last_log_index, logs at or below that index on
    the resume block are skipped."""
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = [_log(10, 0), _log(10, 1), _log(10, 2), _log(11, 0)]
    result = await fetch_logs_paginated(
        upstream=mock,
        filter_={},
        from_block=10,
        to_block=20,
        limit=100,
        skip_until_log_index=1,
    )
    # Logs at block 10 with logIndex <= 1 skipped: drops indices 0 and 1
    block_log_pairs = [
        (int(log["blockNumber"], 16), int(log["logIndex"], 16)) for log in result.items
    ]
    assert (10, 0) not in block_log_pairs
    assert (10, 1) not in block_log_pairs
    assert (10, 2) in block_log_pairs
    assert (11, 0) in block_log_pairs


async def test_halves_on_too_large_then_succeeds():
    """Upstream rejects with 'query returned more than X', we halve and retry."""
    mock = AsyncMock(spec=UpstreamClient)
    call_count = 0

    async def call(method: str, params: list[Any] | None = None) -> Any:
        nonlocal call_count
        call_count += 1
        assert method == "eth_getLogs"
        filt = params[0]
        from_hex = int(filt["fromBlock"], 16)
        to_hex = int(filt["toBlock"], 16)
        span = to_hex - from_hex + 1
        # Reject any span larger than 1000 blocks
        if span > 1000:
            raise UpstreamJsonRpcError(
                code=-32000, message="query returned more than 10000 results"
            )
        # Otherwise return one log per requested range
        return [_log(from_hex, 0)]

    mock.call.side_effect = call
    result = await fetch_logs_paginated(
        upstream=mock,
        filter_={},
        from_block=0,
        to_block=DEFAULT_CHUNK_SIZE - 1,  # one default-sized chunk
        limit=10000,
    )
    # We expect: first call rejected, second halved, etc., until success
    assert len(result.items) > 0
    assert call_count > 1


async def test_too_large_unhalvable_propagates():
    """If even a single block fails, the error propagates (becomes 413)."""
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.side_effect = UpstreamJsonRpcError(
        code=-32000, message="query returned more than 10000 results"
    )
    with pytest.raises(UpstreamJsonRpcError):
        await fetch_logs_paginated(
            upstream=mock,
            filter_={},
            from_block=0,
            to_block=0,  # 1-block range can't be halved further
            limit=100,
        )


async def test_non_payload_error_propagates():
    """Non-too-large errors propagate immediately."""
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.side_effect = UpstreamJsonRpcError(code=-32601, message="method not found")
    with pytest.raises(UpstreamJsonRpcError):
        await fetch_logs_paginated(
            upstream=mock, filter_={}, from_block=0, to_block=100, limit=100
        )


async def test_filter_passed_through_with_block_range_overridden():
    captured: list[dict[str, Any]] = []

    mock = AsyncMock(spec=UpstreamClient)

    async def call(method: str, params: list[Any] | None = None) -> Any:
        captured.append(params[0])
        return []

    mock.call.side_effect = call
    await fetch_logs_paginated(
        upstream=mock,
        filter_={
            "address": "0x" + "aa" * 20,
            "topics": ["0x" + "11" * 32],
            "fromBlock": "ignored",
            "toBlock": "ignored",
        },
        from_block=100,
        to_block=200,
        limit=10,
    )
    assert captured[0]["address"] == "0x" + "aa" * 20
    assert captured[0]["topics"] == ["0x" + "11" * 32]
    assert captured[0]["fromBlock"] == "0x64"
    assert captured[0]["toBlock"] == "0xc8"
