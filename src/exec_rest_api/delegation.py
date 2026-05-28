"""EIP-7702 delegation detection.

An EOA that has delegated execution to a contract has its `eth_getCode` return
exactly 23 bytes starting with the magic prefix `0xef0100`. The remaining 20
bytes are the delegate's address. Any other code (empty, longer, different
prefix) means the account is not delegating.
"""

from __future__ import annotations

import re
from typing import Final

_DELEGATION_PREFIX: Final[str] = "ef0100"
_DELEGATION_CODE_LEN_HEX: Final[int] = 46  # 23 bytes = 46 hex chars after 0x
_HEX_RE: Final[re.Pattern[str]] = re.compile(r"^0x([0-9a-fA-F]{2})*$")


class DelegationError(ValueError):
    """Raised when the code string is not a valid 0x-prefixed hex bytestring."""


def detect_delegate(code_hex: str) -> str | None:
    """Return the delegate address (0x-prefixed, lowercase) if `code_hex` is
    an EIP-7702 delegation indicator, else `None`.

    Raises:
        DelegationError: `code_hex` is not a valid 0x-prefixed even-length hex string.
    """
    if not isinstance(code_hex, str) or not _HEX_RE.fullmatch(code_hex):
        raise DelegationError(f"expected 0x-prefixed even-length hex, got {code_hex!r}")
    payload = code_hex[2:].lower()
    if len(payload) != _DELEGATION_CODE_LEN_HEX:
        return None
    if not payload.startswith(_DELEGATION_PREFIX):
        return None
    return "0x" + payload[len(_DELEGATION_PREFIX):]
