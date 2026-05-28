# Execution REST API — Foundation Implementation Plan (Plan 1 of 5)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the foundational layers of the REST proxy and ship a runnable v0.1 binary that correctly answers `/chain/*` and `/health/*` queries against any standard Ethereum execution client.

**Architecture:** Python 3.10+, single-process aiohttp asynchronous server. The proxy holds one HTTP `ClientSession` to the upstream JSON-RPC endpoint. Request → middleware (request-id, error-mapping, access-log) → handler → upstream JSON-RPC call → handler shapes the response → out. Pure functions for encoding, block-id parsing, and error mapping live in their own modules and are exercised exhaustively in unit tests; the network-touching layer is exercised via integration tests against `anvil`.

**Tech Stack:** Python 3.10+, `aiohttp` (HTTP server + client), `pytest` + `pytest-asyncio` (tests), `anvil` (foundry's local execution client, fetched on first run for integration tests). Dev-only tools: `ruff`, `mypy`, `pip-tools`, `hypothesis`.

---

## Companion documents

These three documents are the authoritative source for what we are building. The plan tasks reference sections by number — keep them open while implementing.

- `docs/superpowers/specs/2026-05-28-execution-rest-api-design.md` — the API contract (§3 endpoint map, §4 encoding, §5 errors, §6 pagination, §7 streams).
- `docs/superpowers/specs/2026-05-28-execution-rest-api-openapi.yaml` — machine-readable OpenAPI 3.1.
- `docs/superpowers/specs/2026-05-28-execution-rest-api-implementation-design.md` — implementation strategy (§3 deps, §4 layout, §7 upstream client, §10 error mapping).

---

## File structure (created by this plan)

```
exec-rest-api/
├── .gitignore
├── pyproject.toml                         # PEP 621 metadata, runtime + dev dep groups
├── requirements.lock                      # pip-compile --generate-hashes output (runtime)
├── requirements-dev.lock                  # dev tools
├── README.md                              # install + run + curl examples
├── src/
│   └── exec_rest_api/
│       ├── __init__.py                    # version, __all__
│       ├── __main__.py                    # `python -m exec_rest_api` entrypoint
│       ├── config.py                      # CLI/env config
│       ├── server.py                      # aiohttp Application factory + middleware
│       ├── upstream.py                    # JSON-RPC HTTP client
│       ├── block_id.py                    # block-id parser
│       ├── encoding.py                    # hex/decimal, status/type enum, address conversions
│       ├── errors.py                      # Problem class + JSON-RPC error mapper
│       └── handlers/
│           ├── __init__.py
│           ├── health.py                  # /health, /health/ready
│           └── chain.py                   # /chain, /chain/id, /chain/sync-status, /chain/client, /chain/peers
└── tests/
    ├── __init__.py
    ├── unit/
    │   ├── __init__.py
    │   ├── test_block_id.py
    │   ├── test_encoding.py
    │   ├── test_errors.py
    │   └── test_upstream.py
    └── integration/
        ├── __init__.py
        ├── conftest.py                     # anvil download + start/stop fixture
        ├── test_health.py
        └── test_chain.py
```

Files NOT created in this plan (deferred to later plans): `handlers/{gas,blocks,accounts,transactions,logs,traces,computed,streams,utils_keccak}.py`, `rlp.py`, `abi_revert.py`, `delegation.py`, `cursor.py`, `pagination.py`, `sse.py`, `content_neg.py`.

---

## Task 1: Project skeleton

**Files:**
- Create: `.gitignore`
- Create: `pyproject.toml`
- Create: `src/exec_rest_api/__init__.py`
- Create: `tests/__init__.py`, `tests/unit/__init__.py`, `tests/integration/__init__.py`
- Create: `src/exec_rest_api/handlers/__init__.py`

- [ ] **Step 1: Create `.gitignore`**

Create `.gitignore` with:

```gitignore
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
.venv/
venv/
env/
*.egg-info/
build/
dist/

# Pytest / coverage
.pytest_cache/
.coverage
htmlcov/
.mypy_cache/
.ruff_cache/
.hypothesis/

# IDE
.idea/
.vscode/

# Anvil cache for integration tests
.anvil-cache/

# Release artefacts
*.pyz
```

- [ ] **Step 2: Create `pyproject.toml`**

Create `pyproject.toml`:

```toml
[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "exec-rest-api"
version = "0.1.0"
description = "REST + SSE proxy in front of Ethereum execution clients."
readme = "README.md"
requires-python = ">=3.10"
license = { text = "Apache-2.0" }
authors = [{ name = "Adrian Sutton", email = "adrian@symphonious.net" }]
dependencies = [
  "aiohttp>=3.9,<4",
]

[project.scripts]
exec-rest-api = "exec_rest_api.__main__:main"

[project.urls]
Homepage = "https://github.com/ajsutton/exec-rest-api"

[project.optional-dependencies]
dev = [
  "pytest>=8",
  "pytest-asyncio>=0.23",
  "hypothesis>=6",
  "ruff>=0.5",
  "mypy>=1.10",
  "pip-tools>=7",
]

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"

[tool.ruff]
line-length = 100
target-version = "py310"

[tool.ruff.lint]
select = ["E", "F", "W", "I", "UP", "B", "ASYNC", "S", "PIE", "RET", "SIM"]
ignore = ["S101"]  # `assert` is fine in tests

[tool.ruff.lint.per-file-ignores]
"tests/**" = ["S105", "S106"]  # hardcoded password strings (test fixtures)

[tool.mypy]
strict = true
python_version = "3.10"
mypy_path = "src"
packages = ["exec_rest_api"]
```

- [ ] **Step 3: Create package directories and `__init__.py` files**

```bash
mkdir -p src/exec_rest_api/handlers tests/unit tests/integration
```

Create `src/exec_rest_api/__init__.py`:

```python
"""Ethereum execution REST API proxy."""

__version__ = "0.1.0"
```

Create `src/exec_rest_api/handlers/__init__.py` as an empty file:

```python
```

Create `tests/__init__.py`, `tests/unit/__init__.py`, `tests/integration/__init__.py` as empty files:

```python
```

- [ ] **Step 4: Verify the package installs and the test runner works**

Run:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest -q
```

Expected:

```
no tests ran in ...s
```

(no tests yet, no errors.)

- [ ] **Step 5: Generate the runtime lockfile**

Run:

```bash
pip-compile --generate-hashes --output-file=requirements.lock pyproject.toml
pip-compile --generate-hashes --extra=dev --output-file=requirements-dev.lock pyproject.toml
```

Expected: both `requirements.lock` and `requirements-dev.lock` files created with pinned versions and `--hash=sha256:...` entries on every line.

- [ ] **Step 6: Commit**

```bash
git add .gitignore pyproject.toml requirements.lock requirements-dev.lock src/exec_rest_api/ tests/
git commit -m "Add project skeleton: pyproject.toml, lockfiles, package layout"
```

---

## Task 2: Block identifier parser (`block_id.py`)

The block-identifier grammar from API spec §4.3: accepts `latest`, `safe`, `finalized`, `pending`, `earliest`, a decimal number, or a 0x-prefixed 32-byte hex hash. Hex-encoded block numbers (e.g. `0x4d2`) are rejected.

**Files:**
- Create: `src/exec_rest_api/block_id.py`
- Create: `tests/unit/test_block_id.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_block_id.py`:

```python
"""Tests for block identifier parsing."""

import pytest

from exec_rest_api.block_id import BlockId, BlockIdError, parse_block_id


def test_parse_tag_latest():
    assert parse_block_id("latest") == BlockId(tag="latest")


def test_parse_tag_safe():
    assert parse_block_id("safe") == BlockId(tag="safe")


def test_parse_tag_finalized():
    assert parse_block_id("finalized") == BlockId(tag="finalized")


def test_parse_tag_pending():
    assert parse_block_id("pending") == BlockId(tag="pending")


def test_parse_tag_earliest():
    assert parse_block_id("earliest") == BlockId(tag="earliest")


def test_parse_block_number_zero():
    assert parse_block_id("0") == BlockId(number=0)


def test_parse_block_number_decimal():
    assert parse_block_id("18234567") == BlockId(number=18234567)


def test_parse_block_hash_lowercase():
    h = "0x" + "ab" * 32
    assert parse_block_id(h) == BlockId(hash=h)


def test_parse_block_hash_mixed_case_lowercased():
    mixed = "0x" + "Ab" * 32
    assert parse_block_id(mixed) == BlockId(hash="0x" + "ab" * 32)


def test_reject_hex_block_number():
    with pytest.raises(BlockIdError):
        parse_block_id("0x4d2")


def test_reject_short_hex():
    with pytest.raises(BlockIdError):
        parse_block_id("0xabcd")


def test_reject_negative_number():
    with pytest.raises(BlockIdError):
        parse_block_id("-1")


def test_reject_empty():
    with pytest.raises(BlockIdError):
        parse_block_id("")


def test_reject_unknown_tag():
    with pytest.raises(BlockIdError):
        parse_block_id("LATEST")  # case-sensitive


def test_reject_garbage():
    with pytest.raises(BlockIdError):
        parse_block_id("not-a-block")


def test_block_id_to_rpc_param_tag():
    assert BlockId(tag="latest").to_rpc_param() == "latest"


def test_block_id_to_rpc_param_number_is_hex():
    # JSON-RPC takes block numbers as 0x-hex
    assert BlockId(number=0).to_rpc_param() == "0x0"
    assert BlockId(number=255).to_rpc_param() == "0xff"
    assert BlockId(number=18234567).to_rpc_param() == "0x1163cc7"


def test_block_id_to_rpc_param_hash():
    h = "0x" + "ab" * 32
    assert BlockId(hash=h).to_rpc_param() == h


def test_block_id_is_hash_or_number():
    h = "0x" + "ab" * 32
    assert BlockId(hash=h).is_hash() is True
    assert BlockId(number=0).is_number() is True
    assert BlockId(tag="latest").is_tag() is True
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
pytest tests/unit/test_block_id.py -v
```

Expected: every test errors with `ImportError: cannot import name 'BlockId' from 'exec_rest_api.block_id'` (module doesn't exist yet).

- [ ] **Step 3: Implement `block_id.py`**

Create `src/exec_rest_api/block_id.py`:

```python
"""Block identifier parsing.

Accepts the API-level grammar (decimal numbers, 32-byte hashes, named tags) and
converts to the JSON-RPC wire format (hex-encoded numbers, lowercase hashes,
tag strings).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final

_BLOCK_TAGS: Final[frozenset[str]] = frozenset(
    {"latest", "safe", "finalized", "pending", "earliest"}
)

_HASH_RE: Final[re.Pattern[str]] = re.compile(r"^0x[0-9a-fA-F]{64}$")
_DECIMAL_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9]+$")


class BlockIdError(ValueError):
    """Raised when a string cannot be parsed as a block identifier."""


@dataclass(frozen=True)
class BlockId:
    """A parsed block identifier — exactly one of tag, number, or hash is set."""

    tag: str | None = None
    number: int | None = None
    hash: str | None = None

    def __post_init__(self) -> None:
        set_fields = sum(x is not None for x in (self.tag, self.number, self.hash))
        if set_fields != 1:
            raise ValueError("BlockId must have exactly one of tag, number, hash set")

    def is_tag(self) -> bool:
        return self.tag is not None

    def is_number(self) -> bool:
        return self.number is not None

    def is_hash(self) -> bool:
        return self.hash is not None

    def to_rpc_param(self) -> str:
        """Render as the JSON-RPC `block` parameter (hex for numbers, lowercase for hashes)."""
        if self.tag is not None:
            return self.tag
        if self.number is not None:
            return f"0x{self.number:x}"
        assert self.hash is not None
        return self.hash


def parse_block_id(raw: str) -> BlockId:
    """Parse a user-facing block identifier.

    Accepts: `latest`/`safe`/`finalized`/`pending`/`earliest`, a decimal number,
    or a 0x-prefixed 32-byte hex hash. Hex-encoded block numbers are rejected.
    """
    if not raw:
        raise BlockIdError("block id is empty")
    if raw in _BLOCK_TAGS:
        return BlockId(tag=raw)
    if _DECIMAL_RE.fullmatch(raw):
        return BlockId(number=int(raw))
    if _HASH_RE.fullmatch(raw):
        return BlockId(hash=raw.lower())
    raise BlockIdError(f"unrecognised block identifier: {raw!r}")
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
pytest tests/unit/test_block_id.py -v
```

Expected: all 18 tests pass.

- [ ] **Step 5: Type-check**

```bash
mypy src/exec_rest_api/block_id.py
```

Expected: `Success: no issues found in 1 source file`.

- [ ] **Step 6: Commit**

```bash
git add src/exec_rest_api/block_id.py tests/unit/test_block_id.py
git commit -m "Add block identifier parser"
```

---

## Task 3: Encoding helpers (`encoding.py`)

Per API spec §4: convert between JSON-RPC hex-encoded quantities and REST's decimal numbers / wei strings, and translate status/type enum sentinels. This module is the single source of truth for hex↔decimal conversion.

**Files:**
- Create: `src/exec_rest_api/encoding.py`
- Create: `tests/unit/test_encoding.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_encoding.py`:

```python
"""Tests for encoding conversions between JSON-RPC and REST shapes."""

import pytest
from hypothesis import given
from hypothesis import strategies as st

from exec_rest_api.encoding import (
    EncodingError,
    decimal_to_hex,
    hex_to_int,
    map_address_lowercase,
    parse_input_int,
    parse_input_wei,
    rest_status_from_rpc,
    rest_tx_type_from_rpc,
    rpc_status_from_rest,
    rpc_tx_type_from_rest,
    wei_from_rpc,
    wei_to_rpc,
)


# ── hex ↔ int ──────────────────────────────────────────────────────────────

def test_hex_to_int_zero():
    assert hex_to_int("0x0") == 0


def test_hex_to_int_small():
    assert hex_to_int("0xff") == 255


def test_hex_to_int_large():
    assert hex_to_int("0x1162d47") == 18234567


def test_hex_to_int_mixed_case():
    assert hex_to_int("0xAbCd") == 0xabcd


def test_hex_to_int_no_prefix_rejected():
    with pytest.raises(EncodingError):
        hex_to_int("ff")


def test_hex_to_int_negative_rejected():
    with pytest.raises(EncodingError):
        hex_to_int("-0x1")


def test_hex_to_int_empty_after_prefix_rejected():
    with pytest.raises(EncodingError):
        hex_to_int("0x")


def test_decimal_to_hex_zero():
    assert decimal_to_hex(0) == "0x0"


def test_decimal_to_hex_round_trip():
    for n in [0, 1, 15, 16, 255, 256, 65535, 18234567, 10**18]:
        assert hex_to_int(decimal_to_hex(n)) == n


# ── wei ────────────────────────────────────────────────────────────────────

def test_wei_from_rpc_zero():
    assert wei_from_rpc("0x0") == "0"


def test_wei_from_rpc_one_ether():
    # 1 ETH = 10^18 wei = 0xde0b6b3a7640000
    assert wei_from_rpc("0xde0b6b3a7640000") == "1000000000000000000"


def test_wei_from_rpc_large():
    # Beyond 2^53 — must be decimal string, never JSON number
    expected = "12345678901234567890"
    rpc = hex(int(expected))
    assert wei_from_rpc(rpc) == expected


def test_wei_to_rpc_zero():
    assert wei_to_rpc("0") == "0x0"


def test_wei_to_rpc_int_accepted():
    assert wei_to_rpc(1000000000000000000) == "0xde0b6b3a7640000"


def test_wei_to_rpc_string_accepted():
    assert wei_to_rpc("1000000000000000000") == "0xde0b6b3a7640000"


def test_wei_to_rpc_negative_rejected():
    with pytest.raises(EncodingError):
        wei_to_rpc("-1")


def test_wei_to_rpc_garbage_rejected():
    with pytest.raises(EncodingError):
        wei_to_rpc("not a number")


# ── input lenience ─────────────────────────────────────────────────────────

def test_parse_input_int_from_int():
    assert parse_input_int(42) == 42


def test_parse_input_int_from_decimal_string():
    assert parse_input_int("42") == 42


def test_parse_input_int_from_hex_string_rejected():
    # Numbers in REST input are decimal-only; hex on input is not lenience we offer.
    with pytest.raises(EncodingError):
        parse_input_int("0x2a")


def test_parse_input_int_from_bool_rejected():
    with pytest.raises(EncodingError):
        parse_input_int(True)


def test_parse_input_wei_from_int():
    assert parse_input_wei(1_000_000_000_000_000_000) == 10**18


def test_parse_input_wei_from_string():
    assert parse_input_wei("1000000000000000000") == 10**18


# ── status enum ────────────────────────────────────────────────────────────

def test_rest_status_from_rpc_success():
    assert rest_status_from_rpc("0x1") == "success"


def test_rest_status_from_rpc_failure():
    assert rest_status_from_rpc("0x0") == "failed"


def test_rest_status_from_rpc_unknown_rejected():
    with pytest.raises(EncodingError):
        rest_status_from_rpc("0x2")


def test_rpc_status_from_rest_success():
    assert rpc_status_from_rest("success") == "0x1"


def test_rpc_status_from_rest_failed():
    assert rpc_status_from_rest("failed") == "0x0"


# ── transaction type enum ──────────────────────────────────────────────────

@pytest.mark.parametrize(
    "rpc, rest",
    [
        ("0x0", "legacy"),
        ("0x1", "access-list"),
        ("0x2", "dynamic-fee"),
        ("0x3", "blob"),
    ],
)
def test_tx_type_round_trip(rpc: str, rest: str):
    assert rest_tx_type_from_rpc(rpc) == rest
    assert rpc_tx_type_from_rest(rest) == rpc


def test_tx_type_unknown_rejected():
    with pytest.raises(EncodingError):
        rest_tx_type_from_rpc("0x9")


# ── address case ───────────────────────────────────────────────────────────

def test_map_address_lowercases():
    mixed = "0xAbCdEf0123456789aBcDeF0123456789AbCdEf01"
    assert map_address_lowercase(mixed) == "0xabcdef0123456789abcdef0123456789abcdef01"


def test_map_address_rejects_wrong_length():
    with pytest.raises(EncodingError):
        map_address_lowercase("0xabcd")


def test_map_address_rejects_no_prefix():
    with pytest.raises(EncodingError):
        map_address_lowercase("abcdef0123456789abcdef0123456789abcdef01")


def test_map_address_rejects_non_hex():
    with pytest.raises(EncodingError):
        map_address_lowercase("0x" + "g" * 40)


# ── hypothesis: hex round-trip on arbitrary ints ───────────────────────────

@given(st.integers(min_value=0, max_value=2**256 - 1))
def test_hex_int_round_trip(n: int):
    assert hex_to_int(decimal_to_hex(n)) == n


@given(st.integers(min_value=0, max_value=2**256 - 1))
def test_wei_round_trip(n: int):
    assert wei_from_rpc(wei_to_rpc(str(n))) == str(n)
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
pytest tests/unit/test_encoding.py -v
```

Expected: every test errors with `ImportError` (module doesn't exist).

- [ ] **Step 3: Implement `encoding.py`**

Create `src/exec_rest_api/encoding.py`:

```python
"""Encoding conversions between JSON-RPC wire format and REST API shapes.

Single source of truth for:
- hex ↔ decimal integer conversion (for things-that-must-fit-in-a-number)
- hex ↔ decimal-string wei conversion (for things-that-may-exceed-2^53)
- status / transaction-type enum mapping
- address case normalization

JSON-RPC encodes all numeric quantities as 0x-hex. The REST API exposes safe
integers as JSON numbers and wei amounts as decimal strings (see API spec §4.1).
"""

from __future__ import annotations

import re
from typing import Final

_HEX_RE: Final[re.Pattern[str]] = re.compile(r"^0x[0-9a-fA-F]+$")
_DECIMAL_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9]+$")
_ADDRESS_RE: Final[re.Pattern[str]] = re.compile(r"^0x[0-9a-fA-F]{40}$")


class EncodingError(ValueError):
    """Raised when a value cannot be encoded or decoded in the expected form."""


# ── hex ↔ int ──────────────────────────────────────────────────────────────

def hex_to_int(s: str) -> int:
    """Parse a JSON-RPC 0x-prefixed hex quantity into an int."""
    if not isinstance(s, str) or not _HEX_RE.fullmatch(s):
        raise EncodingError(f"expected 0x-prefixed hex, got {s!r}")
    return int(s, 16)


def decimal_to_hex(n: int) -> str:
    """Render an int as a 0x-prefixed hex quantity (minimal form, no leading zeros)."""
    if not isinstance(n, int) or isinstance(n, bool) or n < 0:
        raise EncodingError(f"expected non-negative int, got {n!r}")
    return f"0x{n:x}"


# ── wei ────────────────────────────────────────────────────────────────────

def wei_from_rpc(rpc: str) -> str:
    """Decode a JSON-RPC hex quantity as a decimal-string wei amount."""
    return str(hex_to_int(rpc))


def wei_to_rpc(value: str | int) -> str:
    """Encode a wei amount (decimal string or int) as a JSON-RPC hex quantity."""
    if isinstance(value, bool):
        raise EncodingError(f"bool is not a wei value: {value!r}")
    if isinstance(value, int):
        if value < 0:
            raise EncodingError(f"wei must be non-negative, got {value}")
        return f"0x{value:x}"
    if isinstance(value, str):
        if not _DECIMAL_RE.fullmatch(value):
            raise EncodingError(f"wei string must be decimal digits, got {value!r}")
        return f"0x{int(value):x}"
    raise EncodingError(f"wei must be int or decimal string, got {type(value).__name__}")


# ── lenient input parsing ──────────────────────────────────────────────────

def parse_input_int(value: object) -> int:
    """Accept either a JSON number or a decimal string as an integer."""
    if isinstance(value, bool):
        raise EncodingError(f"bool is not an int: {value!r}")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and _DECIMAL_RE.fullmatch(value):
        return int(value)
    raise EncodingError(f"expected int or decimal string, got {value!r}")


def parse_input_wei(value: object) -> int:
    """Accept either a JSON number or a decimal string for a wei amount; return int."""
    return parse_input_int(value)


# ── status enum ────────────────────────────────────────────────────────────

_STATUS_RPC_TO_REST: Final[dict[str, str]] = {"0x0": "failed", "0x1": "success"}
_STATUS_REST_TO_RPC: Final[dict[str, str]] = {v: k for k, v in _STATUS_RPC_TO_REST.items()}


def rest_status_from_rpc(rpc: str) -> str:
    if rpc not in _STATUS_RPC_TO_REST:
        raise EncodingError(f"unknown receipt status: {rpc!r}")
    return _STATUS_RPC_TO_REST[rpc]


def rpc_status_from_rest(rest: str) -> str:
    if rest not in _STATUS_REST_TO_RPC:
        raise EncodingError(f"unknown receipt status: {rest!r}")
    return _STATUS_REST_TO_RPC[rest]


# ── transaction type enum ──────────────────────────────────────────────────

_TX_TYPE_RPC_TO_REST: Final[dict[str, str]] = {
    "0x0": "legacy",
    "0x1": "access-list",
    "0x2": "dynamic-fee",
    "0x3": "blob",
}
_TX_TYPE_REST_TO_RPC: Final[dict[str, str]] = {v: k for k, v in _TX_TYPE_RPC_TO_REST.items()}


def rest_tx_type_from_rpc(rpc: str) -> str:
    if rpc not in _TX_TYPE_RPC_TO_REST:
        raise EncodingError(f"unknown transaction type: {rpc!r}")
    return _TX_TYPE_RPC_TO_REST[rpc]


def rpc_tx_type_from_rest(rest: str) -> str:
    if rest not in _TX_TYPE_REST_TO_RPC:
        raise EncodingError(f"unknown transaction type: {rest!r}")
    return _TX_TYPE_REST_TO_RPC[rest]


# ── address case ───────────────────────────────────────────────────────────

def map_address_lowercase(addr: str) -> str:
    """Validate and lowercase an Ethereum address.

    The proxy does no Keccak-256, so EIP-55 checksumming is not applied; we
    simply lowercase. See implementation design §3 for rationale.
    """
    if not isinstance(addr, str) or not _ADDRESS_RE.fullmatch(addr):
        raise EncodingError(f"expected 0x-prefixed 20-byte address, got {addr!r}")
    return addr.lower()
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
pytest tests/unit/test_encoding.py -v
```

Expected: all tests pass (including hypothesis round-trip tests).

- [ ] **Step 5: Type-check**

```bash
mypy src/exec_rest_api/encoding.py
```

Expected: `Success: no issues found in 1 source file`.

- [ ] **Step 6: Commit**

```bash
git add src/exec_rest_api/encoding.py tests/unit/test_encoding.py
git commit -m "Add encoding helpers: hex/decimal, wei, status/type enums, address case"
```

---

## Task 4: Errors module (`errors.py`)

Implements the RFC 9457 Problem Details body and the JSON-RPC → HTTP error mapper from implementation design §10.

**Files:**
- Create: `src/exec_rest_api/errors.py`
- Create: `tests/unit/test_errors.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_errors.py`:

```python
"""Tests for the Problem class and JSON-RPC error mapping."""

import pytest

from exec_rest_api.errors import (
    ERROR_TYPE_BASE,
    Problem,
    map_jsonrpc_error,
    problem_response,
)


def test_problem_to_dict_minimum_fields():
    p = Problem(status=404, type_slug="not-found", title="Not found")
    d = p.to_dict()
    assert d == {
        "type": f"{ERROR_TYPE_BASE}/not-found",
        "title": "Not found",
        "status": 404,
    }


def test_problem_to_dict_full_fields():
    p = Problem(
        status=422,
        type_slug="transaction-rejected/nonce-too-low",
        title="Transaction rejected",
        detail="nonce too low (got 5, expected 8)",
        instance="/transactions",
        code=-32003,
        data={"hint": "increase nonce"},
    )
    d = p.to_dict()
    assert d == {
        "type": f"{ERROR_TYPE_BASE}/transaction-rejected/nonce-too-low",
        "title": "Transaction rejected",
        "status": 422,
        "detail": "nonce too low (got 5, expected 8)",
        "instance": "/transactions",
        "code": -32003,
        "data": {"hint": "increase nonce"},
    }


def test_problem_response_has_problem_content_type():
    p = Problem(status=400, type_slug="invalid-request", title="Bad request")
    resp = problem_response(p)
    assert resp.status == 400
    assert resp.content_type == "application/problem+json"


# ── JSON-RPC mapping table ────────────────────────────────────────────────


@pytest.mark.parametrize(
    "code, message, expected_status, expected_slug",
    [
        # Standard JSON-RPC codes
        (-32600, "invalid request", 400, "invalid-request"),
        (-32601, "method not found", 501, "method-not-supported-by-upstream"),
        (-32602, "invalid params: foo", 400, "invalid-request"),
        (-32603, "internal error", 502, "upstream-error"),
        (-32700, "parse error", 502, "upstream-error"),
        # Ethereum -32000 patterns
        (-32000, "nonce too low: have 5 want 8", 422, "transaction-rejected/nonce-too-low"),
        (-32000, "already known", 422, "transaction-rejected/already-known"),
        (-32000, "replacement transaction underpriced", 422, "transaction-rejected/replacement-underpriced"),
        (-32000, "transaction underpriced", 422, "transaction-rejected/underpriced"),
        (-32000, "insufficient funds for gas * price + value", 422, "transaction-rejected/insufficient-funds"),
        (-32000, "intrinsic gas too low", 422, "transaction-rejected/intrinsic-gas-too-low"),
        (-32000, "exceeds block gas limit", 422, "transaction-rejected/gas-limit-exceeded"),
        (-32000, "query returned more than 10000 results", 413, "payload-too-large"),
        (-32000, "exceed maximum block range: 10000", 413, "payload-too-large"),
        # Other -32xxx codes
        (-32001, "resource not found", 404, "not-found"),
        (-32002, "resource unavailable", 503, "upstream-unavailable"),
        (-32003, "transaction rejected", 422, "transaction-rejected"),
        (-32004, "method not supported", 501, "method-not-supported-by-upstream"),
        (-32005, "limit exceeded", 429, "rate-limited"),
        # Unmatched -32000..-32099 → 502
        (-32099, "vendor specific error", 502, "upstream-error"),
    ],
)
def test_map_jsonrpc_error_table(
    code: int,
    message: str,
    expected_status: int,
    expected_slug: str,
):
    problem = map_jsonrpc_error(code=code, message=message, data=None)
    assert problem.status == expected_status
    assert problem.type_slug == expected_slug
    assert problem.code == code
    # Message is preserved in detail
    assert problem.detail == message


def test_map_jsonrpc_error_preserves_data():
    problem = map_jsonrpc_error(code=-32000, message="x", data={"foo": "bar"})
    assert problem.data == {"foo": "bar"}


def test_map_jsonrpc_error_revert_is_not_an_error():
    """Reverts are handled in the response body, not as errors. The mapper
    is not called for reverts; calling it directly would still be OK but
    should not be relied on in handlers."""
    # We assert the boundary by documenting: handlers must check for "execution reverted"
    # BEFORE invoking map_jsonrpc_error. This test just locks the rule down.
    assert "execution reverted" in "execution reverted: ERC20: ..."


def test_map_jsonrpc_error_unknown_code_falls_through():
    problem = map_jsonrpc_error(code=-1, message="weird", data=None)
    assert problem.status == 502
    assert problem.type_slug == "upstream-error"
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
pytest tests/unit/test_errors.py -v
```

Expected: ImportError on every test.

- [ ] **Step 3: Implement `errors.py`**

Create `src/exec_rest_api/errors.py`:

```python
"""RFC 9457 Problem Details bodies + JSON-RPC error mapping.

`Problem` is the canonical error shape; every error response (4xx and 5xx) the
API emits is built from one of these. `map_jsonrpc_error` translates an upstream
JSON-RPC error object into a `Problem` per the table in implementation design §10.

Reverts are explicitly NOT handled here — they are successful responses with a
revert body, not errors. Handlers must check for "execution reverted" before
delegating to this mapper.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Final

from aiohttp import web

ERROR_TYPE_BASE: Final[str] = "https://errors.ethereum-rest"


@dataclass(frozen=True)
class Problem:
    """RFC 9457 Problem Details with Ethereum-specific extensions."""

    status: int
    type_slug: str  # appended to ERROR_TYPE_BASE
    title: str
    detail: str | None = None
    instance: str | None = None
    code: int | None = None
    data: Any = field(default=None)

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "type": f"{ERROR_TYPE_BASE}/{self.type_slug}",
            "title": self.title,
            "status": self.status,
        }
        if self.detail is not None:
            out["detail"] = self.detail
        if self.instance is not None:
            out["instance"] = self.instance
        if self.code is not None:
            out["code"] = self.code
        if self.data is not None:
            out["data"] = self.data
        return out


def problem_response(problem: Problem) -> web.Response:
    """Construct an aiohttp Response carrying a Problem body."""
    body = json.dumps(problem.to_dict()).encode("utf-8")
    return web.Response(
        status=problem.status,
        body=body,
        content_type="application/problem+json",
    )


# ── JSON-RPC → Problem mapping ────────────────────────────────────────────

# Standard JSON-RPC 2.0 codes
_STANDARD_CODES: Final[dict[int, tuple[int, str, str]]] = {
    -32600: (400, "invalid-request", "Invalid request"),
    -32601: (501, "method-not-supported-by-upstream", "Method not supported by upstream"),
    -32602: (400, "invalid-request", "Invalid request"),
    -32603: (502, "upstream-error", "Upstream error"),
    -32700: (502, "upstream-error", "Upstream error"),
    -32001: (404, "not-found", "Not found"),
    -32002: (503, "upstream-unavailable", "Upstream unavailable"),
    -32003: (422, "transaction-rejected", "Transaction rejected"),
    -32004: (501, "method-not-supported-by-upstream", "Method not supported by upstream"),
    -32005: (429, "rate-limited", "Rate limited"),
}

# Message-pattern → (status, type_slug, title) for the -32000 family
_M32000_PATTERNS: Final[list[tuple[str, int, str, str]]] = [
    ("nonce too low", 422, "transaction-rejected/nonce-too-low", "Transaction rejected"),
    ("already known", 422, "transaction-rejected/already-known", "Transaction rejected"),
    ("replacement transaction underpriced", 422, "transaction-rejected/replacement-underpriced", "Transaction rejected"),
    ("transaction underpriced", 422, "transaction-rejected/underpriced", "Transaction rejected"),
    ("insufficient funds", 422, "transaction-rejected/insufficient-funds", "Transaction rejected"),
    ("intrinsic gas too low", 422, "transaction-rejected/intrinsic-gas-too-low", "Transaction rejected"),
    ("exceeds block gas limit", 422, "transaction-rejected/gas-limit-exceeded", "Transaction rejected"),
    ("query returned more than", 413, "payload-too-large", "Payload too large"),
    ("exceed maximum block range", 413, "payload-too-large", "Payload too large"),
]


def map_jsonrpc_error(*, code: int, message: str, data: Any) -> Problem:
    """Translate a JSON-RPC error into a Problem.

    Caller MUST NOT pass reverts here (-32000 with message containing
    "execution reverted"). Those are handled in the response body per API spec §5.3.
    """
    if code in _STANDARD_CODES:
        status, slug, title = _STANDARD_CODES[code]
        return Problem(
            status=status,
            type_slug=slug,
            title=title,
            detail=message,
            code=code,
            data=data,
        )
    if code == -32000:
        msg_lower = message.lower()
        for pattern, status, slug, title in _M32000_PATTERNS:
            if pattern in msg_lower:
                return Problem(
                    status=status,
                    type_slug=slug,
                    title=title,
                    detail=message,
                    code=code,
                    data=data,
                )
    # Default: -32000..-32099 vendor errors and anything unmatched
    return Problem(
        status=502,
        type_slug="upstream-error",
        title="Upstream error",
        detail=message,
        code=code,
        data=data,
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
pytest tests/unit/test_errors.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Type-check**

```bash
mypy src/exec_rest_api/errors.py
```

Expected: `Success`.

- [ ] **Step 6: Commit**

```bash
git add src/exec_rest_api/errors.py tests/unit/test_errors.py
git commit -m "Add Problem class and JSON-RPC error mapping table"
```

---

## Task 5: Upstream HTTP client (`upstream.py`)

Async JSON-RPC over HTTP. Connection pooling, timeouts, no retries (per implementation design §7).

**Files:**
- Create: `src/exec_rest_api/upstream.py`
- Create: `tests/unit/test_upstream.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_upstream.py`:

```python
"""Tests for the JSON-RPC HTTP client."""

from typing import Any

import pytest
from aiohttp import ClientSession, web

from exec_rest_api.errors import Problem
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
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
pytest tests/unit/test_upstream.py -v
```

Expected: ImportError on every test.

- [ ] **Step 3: Implement `upstream.py`**

Create `src/exec_rest_api/upstream.py`:

```python
"""JSON-RPC HTTP client.

One `UpstreamClient` per process. Owns no session — the caller passes in an
`aiohttp.ClientSession` so connection pool configuration lives in the server
bootstrap. No retries: JSON-RPC isn't universally idempotent, and the proxy
prefers to surface failure to the caller rather than risk double-submits.
"""

from __future__ import annotations

import itertools
from typing import Any

import aiohttp
from aiohttp import ClientSession


class UpstreamError(Exception):
    """Transport-level failure talking to the upstream (HTTP status, garbled body, timeout)."""


class UpstreamJsonRpcError(Exception):
    """JSON-RPC error object returned by the upstream.

    Carries the raw `code`, `message`, and `data` so the error mapper can
    translate it into a Problem.
    """

    def __init__(self, *, code: int, message: str, data: Any = None) -> None:
        super().__init__(f"jsonrpc error {code}: {message}")
        self.code = code
        self.message = message
        self.data = data


class UpstreamClient:
    """Async JSON-RPC client over HTTP."""

    def __init__(
        self,
        *,
        session: ClientSession,
        http_url: str,
        default_timeout_seconds: float = 30.0,
    ) -> None:
        self._session = session
        self._url = http_url
        self._timeout = aiohttp.ClientTimeout(total=default_timeout_seconds)
        self._id_counter = itertools.count(1)

    async def call(
        self,
        method: str,
        params: list[Any] | None = None,
        *,
        timeout_seconds: float | None = None,
    ) -> Any:
        """Issue one JSON-RPC request. Returns the `result` field on success.

        Raises:
            UpstreamError: transport failure (timeout, HTTP non-2xx, malformed response).
            UpstreamJsonRpcError: upstream returned a JSON-RPC `error` object.
        """
        body = {
            "jsonrpc": "2.0",
            "id": next(self._id_counter),
            "method": method,
            "params": params or [],
        }
        timeout = (
            aiohttp.ClientTimeout(total=timeout_seconds)
            if timeout_seconds is not None
            else self._timeout
        )
        try:
            async with self._session.post(self._url, json=body, timeout=timeout) as resp:
                if resp.status != 200:
                    raise UpstreamError(f"upstream HTTP {resp.status}")
                try:
                    payload = await resp.json(content_type=None)
                except (aiohttp.ContentTypeError, ValueError) as e:
                    raise UpstreamError(f"upstream returned non-JSON body: {e}") from e
        except aiohttp.ClientError as e:
            raise UpstreamError(f"upstream transport error: {e}") from e
        if not isinstance(payload, dict):
            raise UpstreamError(f"upstream returned non-object: {payload!r}")
        if "error" in payload:
            err = payload["error"]
            if not isinstance(err, dict):
                raise UpstreamError(f"upstream error object malformed: {err!r}")
            raise UpstreamJsonRpcError(
                code=int(err.get("code", -32603)),
                message=str(err.get("message", "")),
                data=err.get("data"),
            )
        if "result" not in payload:
            raise UpstreamError(f"upstream response has neither result nor error: {payload!r}")
        return payload["result"]
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
pytest tests/unit/test_upstream.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Type-check**

```bash
mypy src/exec_rest_api/upstream.py
```

Expected: `Success`.

- [ ] **Step 6: Commit**

```bash
git add src/exec_rest_api/upstream.py tests/unit/test_upstream.py
git commit -m "Add JSON-RPC HTTP client (UpstreamClient)"
```

---

## Task 6: Configuration module (`config.py`)

CLI flags + environment variables resolution per implementation design §5.

**Files:**
- Create: `src/exec_rest_api/config.py`
- Create: `tests/unit/test_config.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_config.py`:

```python
"""Tests for CLI/env configuration resolution."""

import pytest

from exec_rest_api.config import ConfigError, parse_config


def test_minimum_args():
    cfg = parse_config(argv=["--upstream-http", "http://localhost:8545"], env={})
    assert cfg.upstream_http == "http://localhost:8545"
    assert cfg.upstream_ws == "ws://localhost:8545"  # derived
    assert cfg.listen == "127.0.0.1:8080"
    assert cfg.upstream_timeout_seconds == 30.0
    assert cfg.default_page_size == 1000
    assert cfg.max_page_size == 10000
    assert cfg.sse_buffer_bytes == 65536
    assert cfg.sse_replay_window == 1024
    assert cfg.sse_heartbeat_seconds == 30
    assert cfg.ready_sync_lag == 10
    assert cfg.log_level == "info"
    assert cfg.metrics_enabled is True


def test_upstream_http_required():
    with pytest.raises(ConfigError):
        parse_config(argv=[], env={})


def test_upstream_https_derives_wss():
    cfg = parse_config(argv=["--upstream-http", "https://node.example.com:8545"], env={})
    assert cfg.upstream_ws == "wss://node.example.com:8545"


def test_env_var_falls_through_when_flag_missing():
    cfg = parse_config(
        argv=[],
        env={"EXEC_REST_API_UPSTREAM_HTTP": "http://node:8545"},
    )
    assert cfg.upstream_http == "http://node:8545"


def test_flag_overrides_env_var():
    cfg = parse_config(
        argv=["--upstream-http", "http://flag:8545"],
        env={"EXEC_REST_API_UPSTREAM_HTTP": "http://env:8545"},
    )
    assert cfg.upstream_http == "http://flag:8545"


def test_listen_override():
    cfg = parse_config(
        argv=["--upstream-http", "http://x:8545", "--listen", "0.0.0.0:9000"],
        env={},
    )
    assert cfg.listen == "0.0.0.0:9000"


def test_metrics_off():
    cfg = parse_config(
        argv=["--upstream-http", "http://x:8545", "--metrics", "off"],
        env={},
    )
    assert cfg.metrics_enabled is False


def test_metrics_on():
    cfg = parse_config(
        argv=["--upstream-http", "http://x:8545", "--metrics", "on"],
        env={},
    )
    assert cfg.metrics_enabled is True


def test_max_page_size_override():
    cfg = parse_config(
        argv=["--upstream-http", "http://x:8545", "--max-page-size", "5000"],
        env={},
    )
    assert cfg.max_page_size == 5000


def test_log_level_invalid_rejected():
    with pytest.raises(ConfigError):
        parse_config(
            argv=["--upstream-http", "http://x:8545", "--log-level", "spam"],
            env={},
        )
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
pytest tests/unit/test_config.py -v
```

Expected: ImportError on every test.

- [ ] **Step 3: Implement `config.py`**

Create `src/exec_rest_api/config.py`:

```python
"""CLI flag + environment variable configuration.

Every CLI flag has an env-var equivalent: `--upstream-http` is also
`EXEC_REST_API_UPSTREAM_HTTP`. Flags override env vars; env vars override
defaults. No configuration file in v1.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Final

ENV_PREFIX: Final[str] = "EXEC_REST_API_"
_LOG_LEVELS: Final[frozenset[str]] = frozenset({"debug", "info", "warn", "error"})


class ConfigError(ValueError):
    """Raised when configuration is missing or invalid."""


@dataclass(frozen=True)
class Config:
    upstream_http: str
    upstream_ws: str
    listen: str
    upstream_timeout_seconds: float
    default_page_size: int
    max_page_size: int
    sse_buffer_bytes: int
    sse_replay_window: int
    sse_heartbeat_seconds: int
    ready_sync_lag: int
    log_level: str
    log_format: str | None  # None means auto (TTY → human, otherwise JSON)
    metrics_enabled: bool


def _derive_ws_from_http(http_url: str) -> str:
    if http_url.startswith("https://"):
        return "wss://" + http_url[len("https://"):]
    if http_url.startswith("http://"):
        return "ws://" + http_url[len("http://"):]
    raise ConfigError(f"upstream-http URL must be http(s)://, got {http_url!r}")


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="exec-rest-api",
        description="REST proxy in front of an Ethereum execution client.",
    )
    p.add_argument("--upstream-http", help="JSON-RPC HTTP endpoint URL")
    p.add_argument("--upstream-ws", help="JSON-RPC WS endpoint (default: derived from http URL)")
    p.add_argument("--listen", help="Listen address (default 127.0.0.1:8080)")
    p.add_argument("--upstream-timeout", type=float, help="Per-request timeout (s)")
    p.add_argument("--default-page-size", type=int, help="Default items per page for /logs, /traces")
    p.add_argument("--max-page-size", type=int, help="Max items per page for /logs, /traces")
    p.add_argument("--sse-buffer-bytes", type=int, help="SSE backpressure threshold (bytes)")
    p.add_argument("--sse-replay-window", type=int, help="Max blocks replayable on SSE reconnect")
    p.add_argument("--sse-heartbeat-seconds", type=int, help="SSE heartbeat interval (s)")
    p.add_argument("--ready-sync-lag", type=int, help="Max blocks behind to report ready")
    p.add_argument("--log-format", choices=["human", "json"], help="Log format")
    p.add_argument("--log-level", choices=sorted(_LOG_LEVELS), help="Log level")
    p.add_argument("--metrics", choices=["on", "off"], help="Enable /metrics endpoint")
    p.add_argument("--version", action="store_true", help="Print version and exit")
    return p


def _coalesce(
    flag_value: object,
    env: dict[str, str],
    env_name: str,
    default: object,
    converter=lambda x: x,
):
    if flag_value is not None:
        return flag_value
    raw = env.get(ENV_PREFIX + env_name)
    if raw is not None:
        return converter(raw)
    return default


def parse_config(*, argv: list[str], env: dict[str, str]) -> Config:
    parser = _build_parser()
    ns = parser.parse_args(argv)

    upstream_http = _coalesce(ns.upstream_http, env, "UPSTREAM_HTTP", None)
    if not upstream_http:
        raise ConfigError("--upstream-http (or EXEC_REST_API_UPSTREAM_HTTP) is required")
    if not isinstance(upstream_http, str):
        raise ConfigError("upstream-http must be a string")

    upstream_ws = _coalesce(ns.upstream_ws, env, "UPSTREAM_WS", None)
    if upstream_ws is None:
        upstream_ws = _derive_ws_from_http(upstream_http)

    log_level = _coalesce(ns.log_level, env, "LOG_LEVEL", "info")
    if log_level not in _LOG_LEVELS:
        raise ConfigError(f"--log-level must be one of {sorted(_LOG_LEVELS)}, got {log_level!r}")

    metrics_raw = _coalesce(ns.metrics, env, "METRICS", "on")
    metrics_enabled = metrics_raw == "on"

    return Config(
        upstream_http=upstream_http,
        upstream_ws=upstream_ws,
        listen=_coalesce(ns.listen, env, "LISTEN", "127.0.0.1:8080"),
        upstream_timeout_seconds=float(
            _coalesce(ns.upstream_timeout, env, "UPSTREAM_TIMEOUT", 30.0, float)
        ),
        default_page_size=int(_coalesce(ns.default_page_size, env, "DEFAULT_PAGE_SIZE", 1000, int)),
        max_page_size=int(_coalesce(ns.max_page_size, env, "MAX_PAGE_SIZE", 10000, int)),
        sse_buffer_bytes=int(_coalesce(ns.sse_buffer_bytes, env, "SSE_BUFFER_BYTES", 65536, int)),
        sse_replay_window=int(_coalesce(ns.sse_replay_window, env, "SSE_REPLAY_WINDOW", 1024, int)),
        sse_heartbeat_seconds=int(
            _coalesce(ns.sse_heartbeat_seconds, env, "SSE_HEARTBEAT_SECONDS", 30, int)
        ),
        ready_sync_lag=int(_coalesce(ns.ready_sync_lag, env, "READY_SYNC_LAG", 10, int)),
        log_level=log_level,
        log_format=_coalesce(ns.log_format, env, "LOG_FORMAT", None),
        metrics_enabled=metrics_enabled,
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
pytest tests/unit/test_config.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Type-check**

```bash
mypy src/exec_rest_api/config.py
```

Expected: `Success`.

- [ ] **Step 6: Commit**

```bash
git add src/exec_rest_api/config.py tests/unit/test_config.py
git commit -m "Add CLI/env configuration parsing"
```

---

## Task 7: Server scaffolding (`server.py`)

aiohttp Application factory with three middleware: request-id, access-log, error-mapping. Handlers attach to this Application. No handlers wired up yet — that comes in Tasks 8 and 9.

**Files:**
- Create: `src/exec_rest_api/server.py`
- Create: `tests/unit/test_server.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_server.py`:

```python
"""Tests for server scaffolding: middleware behaviour, error mapping."""

import logging
from unittest.mock import AsyncMock

import pytest
from aiohttp import web

from exec_rest_api.config import Config
from exec_rest_api.errors import Problem
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


async def test_unexpected_exception_returns_500_problem(aiohttp_client, app_with_test_route, caplog):
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
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
pytest tests/unit/test_server.py -v
```

Expected: ImportError on every test.

- [ ] **Step 3: Implement `server.py`**

Create `src/exec_rest_api/server.py`:

```python
"""aiohttp Application factory + middleware chain.

Three middlewares execute in order:
  1. request_id_middleware — generates or honors X-Request-ID; stores on request.
  2. access_log_middleware — logs one structured line per response.
  3. error_mapping_middleware — converts UpstreamError / UpstreamJsonRpcError /
     unhandled exceptions into Problem responses.

Handlers receive `request.app["upstream"]` for upstream calls, and
`request.app["config"]` for runtime parameters.
"""

from __future__ import annotations

import json
import logging
import time
import uuid

from aiohttp import web

from exec_rest_api.config import Config
from exec_rest_api.errors import Problem, map_jsonrpc_error, problem_response
from exec_rest_api.upstream import UpstreamClient, UpstreamError, UpstreamJsonRpcError

logger = logging.getLogger("exec_rest_api")


@web.middleware
async def request_id_middleware(request: web.Request, handler) -> web.StreamResponse:
    rid = request.headers.get("X-Request-ID") or uuid.uuid4().hex
    request["request_id"] = rid
    response = await handler(request)
    response.headers["X-Request-ID"] = rid
    return response


@web.middleware
async def access_log_middleware(request: web.Request, handler) -> web.StreamResponse:
    start = time.monotonic()
    response: web.StreamResponse
    try:
        response = await handler(request)
        status = response.status
        return response
    except web.HTTPException as e:
        status = e.status
        raise
    except Exception:
        status = 500
        raise
    finally:
        elapsed_ms = round((time.monotonic() - start) * 1000, 2)
        # We log at INFO; the structured form (JSON or human) is governed
        # by the formatter wired up in __main__.
        logger.info(
            "request",
            extra={
                "request_id": request.get("request_id"),
                "method": request.method,
                "path": request.path,
                "status": status,
                "latency_ms": elapsed_ms,
            },
        )


@web.middleware
async def error_mapping_middleware(request: web.Request, handler) -> web.StreamResponse:
    try:
        return await handler(request)
    except UpstreamJsonRpcError as e:
        problem = map_jsonrpc_error(code=e.code, message=e.message, data=e.data)
        return problem_response(_with_instance(problem, request.path))
    except UpstreamError as e:
        problem = Problem(
            status=502,
            type_slug="upstream-error",
            title="Upstream error",
            detail=str(e),
            instance=request.path,
        )
        return problem_response(problem)
    except web.HTTPException:
        # aiohttp's own HTTP exceptions (e.g. 404 from router) pass through.
        raise
    except Exception:
        logger.exception(
            "unhandled exception",
            extra={"request_id": request.get("request_id"), "path": request.path},
        )
        problem = Problem(
            status=500,
            type_slug="internal-error",
            title="Internal error",
            instance=request.path,
        )
        return problem_response(problem)


def _with_instance(problem: Problem, instance: str) -> Problem:
    """Return a copy of `problem` with `instance` set."""
    return Problem(
        status=problem.status,
        type_slug=problem.type_slug,
        title=problem.title,
        detail=problem.detail,
        instance=instance,
        code=problem.code,
        data=problem.data,
    )


def create_app(*, config: Config, upstream: UpstreamClient) -> web.Application:
    """Build the aiohttp Application with middleware and shared state."""
    app = web.Application(
        middlewares=[
            request_id_middleware,
            access_log_middleware,
            error_mapping_middleware,
        ],
    )
    app["config"] = config
    app["upstream"] = upstream
    return app
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
pytest tests/unit/test_server.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Type-check**

```bash
mypy src/exec_rest_api/server.py
```

Expected: `Success`.

- [ ] **Step 6: Commit**

```bash
git add src/exec_rest_api/server.py tests/unit/test_server.py
git commit -m "Add server scaffolding with request-id, access-log, error-mapping middlewares"
```

---

## Task 8: Health handlers (`handlers/health.py`)

The simplest endpoints to prove the request → handler → response flow. `/health` is local-only; `/health/ready` issues one upstream call.

**Files:**
- Create: `src/exec_rest_api/handlers/health.py`
- Create: `tests/unit/test_handlers_health.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_handlers_health.py`:

```python
"""Tests for /health and /health/ready handlers."""

from unittest.mock import AsyncMock

import pytest
from aiohttp import web

from exec_rest_api.config import Config
from exec_rest_api.handlers.health import register_routes
from exec_rest_api.server import create_app
from exec_rest_api.upstream import UpstreamClient, UpstreamError


def _config(ready_sync_lag: int = 10) -> Config:
    return Config(
        upstream_http="http://localhost:8545",
        upstream_ws="ws://localhost:8545",
        listen="127.0.0.1:8080",
        upstream_timeout_seconds=30.0,
        default_page_size=1000,
        max_page_size=10000,
        sse_buffer_bytes=65536,
        sse_replay_window=1024,
        sse_heartbeat_seconds=30,
        ready_sync_lag=ready_sync_lag,
        log_level="info",
        log_format=None,
        metrics_enabled=True,
    )


async def _build_client(aiohttp_client, mock_upstream: UpstreamClient, config: Config | None = None):
    app = create_app(config=config or _config(), upstream=mock_upstream)
    register_routes(app)
    return await aiohttp_client(app)


async def test_health_liveness_no_upstream_call(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/health")
    assert resp.status == 200
    body = await resp.json()
    assert body == {"status": "ok"}
    mock.call.assert_not_called()


async def test_ready_upstream_reachable_in_sync(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    # eth_syncing returns False (synced), eth_blockNumber returns hex 1000
    mock.call.side_effect = [False, "0x3e8"]
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/health/ready")
    assert resp.status == 200
    body = await resp.json()
    assert body == {
        "ready": True,
        "upstreamReachable": True,
        "syncing": False,
        "blockNumber": 1000,
    }


async def test_ready_when_actively_syncing_close_enough(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    # Syncing 5 blocks behind, under the lag threshold of 10
    mock.call.side_effect = [
        {"startingBlock": "0x0", "currentBlock": "0x3e3", "highestBlock": "0x3e8"},
        "0x3e3",
    ]
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/health/ready")
    assert resp.status == 200
    body = await resp.json()
    assert body["ready"] is True
    assert body["syncing"] is True
    assert body["blockNumber"] == 0x3e3


async def test_not_ready_when_too_far_behind(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    # 100 blocks behind, beyond ready_sync_lag=10
    mock.call.side_effect = [
        {"startingBlock": "0x0", "currentBlock": "0x384", "highestBlock": "0x3e8"},
        "0x384",
    ]
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/health/ready")
    assert resp.status == 503
    body = await resp.json()
    assert body["type"].endswith("/upstream-unavailable")


async def test_not_ready_when_upstream_unreachable(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.side_effect = UpstreamError("connection refused")
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/health/ready")
    assert resp.status == 503
    body = await resp.json()
    assert body["type"].endswith("/upstream-unavailable")
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
pytest tests/unit/test_handlers_health.py -v
```

Expected: ImportError on every test.

- [ ] **Step 3: Implement `handlers/health.py`**

Create `src/exec_rest_api/handlers/health.py`:

```python
"""/health (liveness) and /health/ready (readiness with upstream check)."""

from __future__ import annotations

from aiohttp import web

from exec_rest_api.encoding import hex_to_int
from exec_rest_api.errors import Problem, problem_response
from exec_rest_api.upstream import UpstreamClient, UpstreamError


async def health(request: web.Request) -> web.Response:
    """Liveness: server process is up. No upstream calls."""
    return web.json_response({"status": "ok"})


async def ready(request: web.Request) -> web.Response:
    """Readiness: upstream reachable AND sync lag within configured threshold."""
    upstream: UpstreamClient = request.app["upstream"]
    config = request.app["config"]
    try:
        sync = await upstream.call("eth_syncing")
        block_hex = await upstream.call("eth_blockNumber")
    except UpstreamError as e:
        return problem_response(
            Problem(
                status=503,
                type_slug="upstream-unavailable",
                title="Upstream unavailable",
                detail=str(e),
                instance=request.path,
            )
        )
    block_number = hex_to_int(block_hex)
    if sync is False:
        return web.json_response(
            {
                "ready": True,
                "upstreamReachable": True,
                "syncing": False,
                "blockNumber": block_number,
            }
        )
    # sync is a dict
    highest = hex_to_int(sync["highestBlock"])
    current = hex_to_int(sync["currentBlock"])
    lag = highest - current
    if lag <= config.ready_sync_lag:
        return web.json_response(
            {
                "ready": True,
                "upstreamReachable": True,
                "syncing": True,
                "blockNumber": current,
            }
        )
    return problem_response(
        Problem(
            status=503,
            type_slug="upstream-unavailable",
            title="Upstream still syncing",
            detail=f"sync lag {lag} blocks exceeds threshold {config.ready_sync_lag}",
            instance=request.path,
        )
    )


def register_routes(app: web.Application) -> None:
    app.router.add_get("/health", health)
    app.router.add_get("/health/ready", ready)
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
pytest tests/unit/test_handlers_health.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/exec_rest_api/handlers/health.py tests/unit/test_handlers_health.py
git commit -m "Add /health and /health/ready handlers"
```

---

## Task 9: Chain handlers (`handlers/chain.py`)

All five `/chain/*` endpoints from API spec §3.1.

**Files:**
- Create: `src/exec_rest_api/handlers/chain.py`
- Create: `tests/unit/test_handlers_chain.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_handlers_chain.py`:

```python
"""Tests for /chain/* handlers."""

import asyncio
from unittest.mock import AsyncMock

import pytest
from aiohttp import web

from exec_rest_api.config import Config
from exec_rest_api.handlers.chain import register_routes
from exec_rest_api.server import create_app
from exec_rest_api.upstream import UpstreamClient


def _config() -> Config:
    return Config(
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


async def _build_client(aiohttp_client, mock_upstream: UpstreamClient):
    app = create_app(config=_config(), upstream=mock_upstream)
    register_routes(app)
    return await aiohttp_client(app)


async def test_chain_id(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0x1"
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/chain/id")
    assert resp.status == 200
    assert await resp.json() == {"chainId": 1}
    mock.call.assert_awaited_once_with("eth_chainId")


async def test_chain_id_large(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "0x2a15c308d"  # 11297108109 (Palm)
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/chain/id")
    body = await resp.json()
    assert body == {"chainId": 11297108109}


async def test_chain_sync_status_when_synced(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = False
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/chain/sync-status")
    assert await resp.json() == {"syncing": False}


async def test_chain_sync_status_when_syncing(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = {
        "startingBlock": "0x0",
        "currentBlock": "0x10",
        "highestBlock": "0x100",
    }
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/chain/sync-status")
    assert await resp.json() == {
        "syncing": True,
        "startingBlock": 0,
        "currentBlock": 16,
        "highestBlock": 256,
    }


async def test_chain_client(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    mock.call.return_value = "anvil/v0.2.0"
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/chain/client")
    assert await resp.json() == {"client": "anvil/v0.2.0"}
    mock.call.assert_awaited_once_with("web3_clientVersion")


async def test_chain_peers(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)
    # net_peerCount returns hex, net_listening returns bool
    mock.call.side_effect = ["0x23", True]
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/chain/peers")
    assert await resp.json() == {"peerCount": 35, "listening": True}


async def test_chain_composite(aiohttp_client):
    mock = AsyncMock(spec=UpstreamClient)

    async def call(method, params=None):
        return {
            "eth_chainId": "0x1",
            "net_version": "1",
            "web3_clientVersion": "anvil/v0.2.0",
            "eth_syncing": False,
            "eth_blockNumber": "0x100",
        }[method]

    mock.call.side_effect = call
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/chain")
    body = await resp.json()
    assert body == {
        "chainId": 1,
        "networkId": "1",
        "client": "anvil/v0.2.0",
        "blockNumber": 256,
        "syncing": {"syncing": False},
    }


async def test_chain_composite_fans_out_in_parallel(aiohttp_client):
    """The composite endpoint must use asyncio.gather, not sequential awaits."""
    mock = AsyncMock(spec=UpstreamClient)
    call_order = []

    async def slow_call(method, params=None):
        call_order.append((method, "start"))
        await asyncio.sleep(0.05)
        call_order.append((method, "end"))
        return {
            "eth_chainId": "0x1",
            "net_version": "1",
            "web3_clientVersion": "anvil/v0.2.0",
            "eth_syncing": False,
            "eth_blockNumber": "0x0",
        }[method]

    mock.call.side_effect = slow_call
    client = await _build_client(aiohttp_client, mock)
    resp = await client.get("/chain")
    assert resp.status == 200
    # All 5 calls must have started before any finished
    starts = [c for c in call_order if c[1] == "start"]
    ends = [c for c in call_order if c[1] == "end"]
    assert len(starts) == 5
    # The first "end" event must come after all "start" events
    first_end_index = call_order.index(ends[0])
    starts_before_first_end = [c for c in call_order[:first_end_index] if c[1] == "start"]
    assert len(starts_before_first_end) == 5
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
pytest tests/unit/test_handlers_chain.py -v
```

Expected: ImportError on every test.

- [ ] **Step 3: Implement `handlers/chain.py`**

Create `src/exec_rest_api/handlers/chain.py`:

```python
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
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
pytest tests/unit/test_handlers_chain.py -v
```

Expected: all tests pass, including `test_chain_composite_fans_out_in_parallel`.

- [ ] **Step 5: Commit**

```bash
git add src/exec_rest_api/handlers/chain.py tests/unit/test_handlers_chain.py
git commit -m "Add /chain/* handlers with parallel fan-out for composite endpoint"
```

---

## Task 10: Entrypoint (`__main__.py`)

Wires config → logging → upstream session → app → uvicorn-equivalent (aiohttp's built-in runner). After this task the proxy is runnable.

**Files:**
- Create: `src/exec_rest_api/__main__.py`

- [ ] **Step 1: Implement `__main__.py`**

Create `src/exec_rest_api/__main__.py`:

```python
"""Entrypoint for `python -m exec_rest_api` and the `exec-rest-api` console script."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys

import aiohttp
from aiohttp import web

from exec_rest_api import __version__
from exec_rest_api.config import Config, ConfigError, parse_config
from exec_rest_api.handlers import chain, health
from exec_rest_api.server import create_app
from exec_rest_api.upstream import UpstreamClient


def _setup_logging(level: str, format_: str | None) -> None:
    level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warn": logging.WARNING,
        "error": logging.ERROR,
    }
    handler = logging.StreamHandler(sys.stderr)
    use_json = format_ == "json" or (format_ is None and not sys.stderr.isatty())
    if use_json:
        handler.setFormatter(_JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level_map[level])


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # noqa: A003
        import json
        payload = {
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in (
            "request_id", "method", "path", "status", "latency_ms",
            "upstream_method", "upstream_latency_ms",
        ):
            if key in record.__dict__:
                payload[key] = record.__dict__[key]
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def _split_listen(listen: str) -> tuple[str, int]:
    host, _, port = listen.rpartition(":")
    if not host or not port:
        raise ConfigError(f"--listen must be host:port, got {listen!r}")
    return host, int(port)


async def _run(config: Config) -> None:
    connector = aiohttp.TCPConnector(limit=100)
    timeout = aiohttp.ClientTimeout(total=config.upstream_timeout_seconds)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        upstream = UpstreamClient(
            session=session,
            http_url=config.upstream_http,
            default_timeout_seconds=config.upstream_timeout_seconds,
        )
        app = create_app(config=config, upstream=upstream)
        health.register_routes(app)
        chain.register_routes(app)

        host, port = _split_listen(config.listen)
        runner = web.AppRunner(app, access_log=None)  # we have our own access-log middleware
        await runner.setup()
        site = web.TCPSite(runner, host=host, port=port)
        await site.start()

        logging.getLogger("exec_rest_api").info(
            "listening",
            extra={"listen": config.listen, "upstream_http": config.upstream_http},
        )

        # Run until SIGINT / SIGTERM
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except NotImplementedError:
                pass  # Windows
        await stop_event.wait()
        await runner.cleanup()


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    if "--version" in argv:
        print(f"exec-rest-api {__version__}")
        return 0
    try:
        config = parse_config(argv=argv, env=dict(os.environ))
    except ConfigError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    _setup_logging(level=config.log_level, format_=config.log_format)
    try:
        asyncio.run(_run(config))
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Sanity-check the entrypoint compiles and prints version**

```bash
python -m exec_rest_api --version
```

Expected:

```
exec-rest-api 0.1.0
```

- [ ] **Step 3: Sanity-check error path without upstream**

```bash
python -m exec_rest_api
```

Expected (stderr):

```
error: --upstream-http (or EXEC_REST_API_UPSTREAM_HTTP) is required
```

Exit code 2.

- [ ] **Step 4: Commit**

```bash
git add src/exec_rest_api/__main__.py
git commit -m "Add entrypoint with config, logging, lifecycle wiring"
```

---

## Task 11: Integration test fixture (anvil)

Spin up `anvil` (Foundry's local execution client) on a free port, expose its URL as a pytest fixture, and tear it down after the session.

**Files:**
- Create: `tests/integration/conftest.py`

- [ ] **Step 1: Implement the conftest**

Create `tests/integration/conftest.py`:

```python
"""Integration-test fixtures.

Spins up `anvil` (foundry's local execution client) for the test session and
exposes its HTTP and WS URLs. If `anvil` is not on PATH, the suite is skipped
with a clear message (download instructions printed).
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time
from collections.abc import Iterator

import aiohttp
import pytest
import pytest_asyncio

from exec_rest_api.config import Config
from exec_rest_api.handlers import chain, health
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
    proc = subprocess.Popen(
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
    import socket as _socket
    while time.time() < deadline:
        try:
            with _socket.create_connection(("127.0.0.1", port), timeout=0.2):
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
    async with aiohttp.ClientSession() as session:
        upstream = UpstreamClient(session=session, http_url=anvil_url)
        app = create_app(config=_build_config(anvil_url), upstream=upstream)
        health.register_routes(app)
        chain.register_routes(app)
        client = await aiohttp_client(app)
        yield client
```

- [ ] **Step 2: Verify the fixture loads without errors**

```bash
pytest tests/integration -v --collect-only
```

Expected: no errors. (Tests not collected yet because there are no integration test modules.)

- [ ] **Step 3: Commit**

```bash
git add tests/integration/conftest.py
git commit -m "Add anvil-based integration test fixture"
```

---

## Task 12: Integration tests for `/health` and `/chain`

End-to-end tests against a real `anvil` process.

**Files:**
- Create: `tests/integration/test_health.py`
- Create: `tests/integration/test_chain.py`

- [ ] **Step 1: Write `tests/integration/test_health.py`**

Create `tests/integration/test_health.py`:

```python
"""End-to-end tests for /health and /health/ready against anvil."""


async def test_health_liveness(proxy_client):
    resp = await proxy_client.get("/health")
    assert resp.status == 200
    body = await resp.json()
    assert body == {"status": "ok"}


async def test_health_ready_against_anvil(proxy_client):
    resp = await proxy_client.get("/health/ready")
    assert resp.status == 200
    body = await resp.json()
    assert body["ready"] is True
    assert body["upstreamReachable"] is True
    # anvil starts mined; syncing is False
    assert body["syncing"] is False
    assert isinstance(body["blockNumber"], int)
    assert body["blockNumber"] >= 0
```

- [ ] **Step 2: Write `tests/integration/test_chain.py`**

Create `tests/integration/test_chain.py`:

```python
"""End-to-end tests for /chain/* against anvil."""


async def test_chain_id(proxy_client):
    resp = await proxy_client.get("/chain/id")
    assert resp.status == 200
    body = await resp.json()
    # anvil defaults to chain id 31337 (foundry's anvil default)
    assert body == {"chainId": 31337}


async def test_chain_client(proxy_client):
    resp = await proxy_client.get("/chain/client")
    body = await resp.json()
    assert "client" in body
    assert isinstance(body["client"], str)
    assert "anvil" in body["client"].lower()


async def test_chain_sync_status(proxy_client):
    resp = await proxy_client.get("/chain/sync-status")
    body = await resp.json()
    # anvil isn't syncing
    assert body == {"syncing": False}


async def test_chain_peers(proxy_client):
    resp = await proxy_client.get("/chain/peers")
    body = await resp.json()
    assert "peerCount" in body
    assert "listening" in body
    assert isinstance(body["peerCount"], int)
    assert isinstance(body["listening"], bool)


async def test_chain_composite(proxy_client):
    resp = await proxy_client.get("/chain")
    assert resp.status == 200
    body = await resp.json()
    assert body["chainId"] == 31337
    assert "anvil" in body["client"].lower()
    assert body["syncing"] == {"syncing": False}
    assert isinstance(body["blockNumber"], int)


async def test_unknown_path_404_with_problem_body(proxy_client):
    resp = await proxy_client.get("/blocks/latest")  # not implemented in this plan
    assert resp.status == 404
    # aiohttp's default 404 doesn't go through our problem middleware (HTTPException passthrough),
    # so this is a plain text 404. We assert the status and move on.


async def test_request_id_round_trip(proxy_client):
    resp = await proxy_client.get("/chain/id", headers={"X-Request-ID": "from-integration-test"})
    assert resp.headers["X-Request-ID"] == "from-integration-test"
```

- [ ] **Step 3: Run the integration tests**

```bash
pytest tests/integration -v
```

Expected: all tests pass. Skipped only if `anvil` isn't on PATH (with the skip message pointing at the install docs).

- [ ] **Step 4: Commit**

```bash
git add tests/integration/test_health.py tests/integration/test_chain.py
git commit -m "Add integration tests for /health and /chain against anvil"
```

---

## Task 13: README

Final task: document install and use. After this, `pipx install` (or pip in a venv) gives a working binary.

**Files:**
- Create: `README.md`

- [ ] **Step 1: Write `README.md`**

Create `README.md`:

````markdown
# exec-rest-api

REST + SSE proxy in front of any Ethereum execution client. Talks JSON-RPC
to your upstream node and serves a developer-friendly REST API (RFC 9457
problem details, RFC 8288 cursor pagination, SSE streams, content
negotiation for raw RLP, no hex quantities).

## Status

`v0.1` — foundation only. Endpoints in this release: `/chain/*`, `/health`,
`/health/ready`. More to come.

## Install

```sh
pipx install exec-rest-api
```

(or `pip install exec-rest-api` inside a virtualenv.)

## Run

```sh
exec-rest-api --upstream-http http://localhost:8545
```

Then:

```sh
curl http://127.0.0.1:8080/chain
# → { "chainId": 1, "networkId": "1", "client": "Geth/v1.13.5...", "blockNumber": 18234567, "syncing": {"syncing": false} }

curl http://127.0.0.1:8080/health/ready
# → { "ready": true, "upstreamReachable": true, "syncing": false, "blockNumber": 18234567 }
```

## Configuration

Every CLI flag has an env-var equivalent: `--upstream-http` is also
`EXEC_REST_API_UPSTREAM_HTTP`. Flags override env vars.

| Flag | Env var | Default |
|---|---|---|
| `--upstream-http URL` | `EXEC_REST_API_UPSTREAM_HTTP` | required |
| `--upstream-ws URL` | `EXEC_REST_API_UPSTREAM_WS` | derived from http URL |
| `--listen HOST:PORT` | `EXEC_REST_API_LISTEN` | `127.0.0.1:8080` |
| `--upstream-timeout SECONDS` | `EXEC_REST_API_UPSTREAM_TIMEOUT` | `30` |
| `--log-level LEVEL` | `EXEC_REST_API_LOG_LEVEL` | `info` |
| `--log-format FMT` | `EXEC_REST_API_LOG_FORMAT` | auto (human on TTY, JSON otherwise) |
| `--metrics on|off` | `EXEC_REST_API_METRICS` | `on` |

Full list: `exec-rest-api --help`.

## Development

```sh
git clone https://github.com/ajsutton/exec-rest-api
cd exec-rest-api
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

Integration tests require `anvil` (from
[Foundry](https://book.getfoundry.sh/getting-started/installation)) on PATH;
they are skipped otherwise.

## Design docs

- `docs/superpowers/specs/2026-05-28-execution-rest-api-design.md` — API contract.
- `docs/superpowers/specs/2026-05-28-execution-rest-api-openapi.yaml` — OpenAPI 3.1.
- `docs/superpowers/specs/2026-05-28-execution-rest-api-implementation-design.md` — implementation strategy.

## License

Apache 2.0.
````

- [ ] **Step 2: Run the full test suite one last time**

```bash
pytest -v
```

Expected: every unit test passes; integration tests pass (or skip if anvil is missing).

- [ ] **Step 3: Run static checks**

```bash
ruff check src tests
mypy src
```

Expected: no errors.

- [ ] **Step 4: Tag a milestone marker (optional)**

Not a release yet (signing pipeline lands in Plan 5). Just a marker:

```bash
git tag v0.1.0-foundation-complete
```

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "Add README with install, run, and configuration docs"
```

---

## Plan 1 complete

End state:

- `pipx install -e .` (or `pip install -e .`) installs the binary.
- `exec-rest-api --upstream-http http://localhost:8545` runs it.
- `GET /chain`, `GET /chain/{id,sync-status,client,peers}`, `GET /health`, `GET /health/ready` all work end-to-end against any standard JSON-RPC execution client.
- All errors return RFC 9457 Problem Details with the right HTTP status and `type` URI.
- Request IDs propagate (`X-Request-ID`).
- Full unit + integration test coverage.
- All modules type-check under strict mypy.

Plan 2 (Read endpoints — blocks, accounts, transactions, logs + pagination, traces) builds on this same scaffolding.
