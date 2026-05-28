"""Tests for SSE framing helpers."""

from __future__ import annotations

import asyncio
import json
import time

import pytest

from exec_rest_api.sse import (
    format_comment,
    format_event,
    format_retry,
    stream_with_heartbeat,
)


def test_format_event_minimal():
    out = format_event(event="block", id_="42", data={"number": 42})
    # SSE field order does not matter, but the framing terminator must
    assert out.endswith(b"\n\n")
    text = out.decode("utf-8")
    assert "event: block" in text
    assert "id: 42" in text
    assert 'data: {"number":42}' in text or 'data: {"number": 42}' in text


def test_format_event_data_is_single_line():
    """Multi-line data would break SSE; the framing must compact JSON to one line."""
    out = format_event(event="x", id_="1", data={"k": "a\nb"})
    body = out.decode("utf-8")
    data_lines = [line for line in body.splitlines() if line.startswith("data:")]
    assert len(data_lines) == 1


def test_format_event_no_id():
    out = format_event(event="sync-status", id_=None, data={"syncing": False})
    assert b"id:" not in out
    assert b"event: sync-status" in out


def test_format_comment_strips_newlines():
    out = format_comment("ping 1700000000")
    assert out == b": ping 1700000000\n\n"


def test_format_retry():
    assert format_retry(5000) == b"retry: 5000\n\n"


async def test_stream_with_heartbeat_emits_pings_when_quiet():
    """If the source is idle for `interval`, a heartbeat is yielded."""

    async def source():
        await asyncio.sleep(0.2)
        yield b"event: x\ndata: 1\n\n"
        # Never produce again

    pings: list[bytes] = []
    started = time.monotonic()
    async for chunk in stream_with_heartbeat(source(), interval_seconds=0.05):
        if chunk.startswith(b":"):
            pings.append(chunk)
            if len(pings) >= 2:
                break
        if time.monotonic() - started > 2.0:
            pytest.fail("stream_with_heartbeat never yielded heartbeats")
    assert all(p.startswith(b": ping ") for p in pings)


async def test_stream_with_heartbeat_passes_source_through():
    async def source():
        yield b"event: x\ndata: 1\n\n"
        yield b"event: y\ndata: 2\n\n"

    seen: list[bytes] = []
    async for chunk in stream_with_heartbeat(source(), interval_seconds=1.0):
        if chunk.startswith(b":"):
            continue
        seen.append(chunk)
    assert seen == [b"event: x\ndata: 1\n\n", b"event: y\ndata: 2\n\n"]
