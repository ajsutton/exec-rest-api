"""Tests for block_resolve.resolve_block_id."""

from unittest.mock import AsyncMock

from exec_rest_api.block_id import BlockId
from exec_rest_api.block_resolve import resolve_block_id
from exec_rest_api.upstream import UpstreamClient


async def test_resolve_number_no_rpc():
    mock = AsyncMock(spec=UpstreamClient)
    assert await resolve_block_id(mock, BlockId(number=42)) == 42
    mock.call.assert_not_awaited()


async def test_resolve_earliest_no_rpc():
    mock = AsyncMock(spec=UpstreamClient)
    assert await resolve_block_id(mock, BlockId(tag="earliest")) == 0
    mock.call.assert_not_awaited()


async def test_resolve_latest_calls_eth_blockNumber():
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0x100"
    assert await resolve_block_id(mock, BlockId(tag="latest")) == 256
    mock.call.assert_awaited_once_with("eth_blockNumber")


async def test_resolve_safe_returns_none_when_missing():
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = None
    assert await resolve_block_id(mock, BlockId(tag="safe")) is None
    mock.call.assert_awaited_once_with("eth_getBlockByNumber", ["safe", False])


async def test_resolve_safe_returns_number_when_present():
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = {"number": "0x1000"}
    assert await resolve_block_id(mock, BlockId(tag="safe")) == 4096


async def test_resolve_hash_returns_number():
    mock = AsyncMock(spec=UpstreamClient)
    h = "0x" + "ab" * 32
    mock.call.return_value = {"number": "0xff"}
    assert await resolve_block_id(mock, BlockId(hash=h)) == 255
    mock.call.assert_awaited_once_with("eth_getBlockByHash", [h, False])


async def test_resolve_hash_returns_none_when_missing():
    mock = AsyncMock(spec=UpstreamClient)
    h = "0x" + "ab" * 32
    mock.call.return_value = None
    assert await resolve_block_id(mock, BlockId(hash=h)) is None
