"""HMAC-SHA256 signed keyset pagination cursors for Seeknal Serving."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
from collections.abc import Mapping, Sequence
from typing import Any

from seeknal.serving.enforce import EnforcementError
from seeknal.serving.parameter_binding import (
    MAX_PARAMETERS,
    MAX_SAFE_INTEGER,
    MAX_STRING_UTF8_BYTES,
    MIN_SAFE_INTEGER,
)

CURSOR_VERSION = 1
_B64URL_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_VALID_DIRECTIONS = frozenset({"asc", "desc"})
_MAX_CURSOR_TOKEN_CHARS = 65_536


class CursorError(EnforcementError):
    """Raised when a keyset pagination cursor is malformed, tampered, or mismatched."""

    code: str = "cursor_invalid"

    def __init__(
        self,
        message: str = "Invalid pagination cursor",
        *,
        code: str | None = None,
    ) -> None:
        super().__init__(message, code=code or "cursor_invalid")


def _b64url_encode(data: bytes) -> str:
    """Encode bytes as unpadded base64url ASCII."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode_strict(segment: str) -> bytes:
    """Decode an unpadded base64url segment and verify canonical round-trip encoding."""
    if not isinstance(segment, str) or not segment or not _B64URL_RE.fullmatch(segment):
        raise CursorError("Invalid pagination cursor")
    rem = len(segment) % 4
    if rem == 1:
        raise CursorError("Invalid pagination cursor")
    padded = segment + ("=" * ((4 - rem) % 4))
    try:
        decoded = base64.urlsafe_b64decode(padded.encode("ascii"))
    except Exception as exc:
        raise CursorError("Invalid pagination cursor") from exc
    # Reject non-canonical trailing bits so any bit-flip in the token fails closed.
    if _b64url_encode(decoded) != segment:
        raise CursorError("Invalid pagination cursor")
    return decoded


def _normalize_secret_key(secret_key: bytes | str | None) -> bytes:
    """Normalize HMAC secret key to non-empty bytes."""
    if secret_key is None or isinstance(secret_key, bool):
        raise CursorError("Invalid pagination cursor")
    if isinstance(secret_key, (bytes, bytearray)):
        key_bytes = bytes(secret_key)
    elif isinstance(secret_key, str):
        try:
            key_bytes = secret_key.encode("utf-8", errors="strict")
        except UnicodeEncodeError as exc:
            raise CursorError("Invalid pagination cursor") from exc
    else:
        raise CursorError("Invalid pagination cursor")
    if not key_bytes:
        raise CursorError("Invalid pagination cursor")
    return key_bytes


def _normalize_order_spec(order_spec: Sequence[tuple[str, str]]) -> list[list[str]]:
    """Validate and normalize [(col, dir), ...] to [[col_lower, dir_lower], ...]."""
    if isinstance(order_spec, (str, bytes, bytearray, Mapping)) or not isinstance(
        order_spec, Sequence
    ):
        raise CursorError("Invalid pagination cursor")
    if len(order_spec) == 0 or len(order_spec) > MAX_PARAMETERS:
        raise CursorError("Invalid pagination cursor")

    out: list[list[str]] = []
    for item in order_spec:
        if (
            isinstance(item, (str, bytes, bytearray, Mapping))
            or not isinstance(item, Sequence)
            or len(item) != 2
        ):
            raise CursorError("Invalid pagination cursor")
        col, direction = item[0], item[1]
        if not isinstance(col, str) or not isinstance(direction, str):
            raise CursorError("Invalid pagination cursor")
        col_norm = col.strip().lower()
        dir_norm = direction.strip().lower()
        if not col_norm or dir_norm not in _VALID_DIRECTIONS:
            raise CursorError("Invalid pagination cursor")
        out.append([col_norm, dir_norm])
    return out


def _validate_cursor_scalar(val: Any) -> bool | int | str:
    """Validate a single keyset cursor scalar under non-null seeknal-params-v1 rules."""
    if val is None:
        raise CursorError("Invalid pagination cursor")
    if isinstance(val, bool):
        return val
    if isinstance(val, float):
        raise CursorError("Invalid pagination cursor")
    if type(val) is int:
        if val < MIN_SAFE_INTEGER or val > MAX_SAFE_INTEGER:
            raise CursorError("Invalid pagination cursor")
        return 0 if val == 0 else int(val)
    if type(val) is str:
        try:
            utf8_bytes = val.encode("utf-8", errors="strict")
        except UnicodeEncodeError as exc:
            raise CursorError("Invalid pagination cursor") from exc
        if len(utf8_bytes) > MAX_STRING_UTF8_BYTES:
            raise CursorError("Invalid pagination cursor")
        return val
    raise CursorError("Invalid pagination cursor")


def _reject_json_float(_raw: str) -> Any:
    raise CursorError("Invalid pagination cursor")


def _reject_json_constant(_raw: str) -> Any:
    raise CursorError("Invalid pagination cursor")


def encode_cursor(
    table: str,
    order_spec: Sequence[tuple[str, str]],
    key_values: Sequence[Any],
    secret_key: bytes | str,
) -> str:
    """Encode and HMAC-SHA256 sign a keyset pagination cursor token."""
    key_bytes = _normalize_secret_key(secret_key)

    if not isinstance(table, str) or not table.strip():
        raise CursorError("Invalid pagination cursor")
    table_norm = table.strip().lower()

    norm_order = _normalize_order_spec(order_spec)

    if isinstance(key_values, (str, bytes, bytearray, Mapping)) or not isinstance(
        key_values, Sequence
    ):
        raise CursorError("Invalid pagination cursor")
    if len(key_values) != len(norm_order):
        raise CursorError("Invalid pagination cursor")

    norm_keys = [_validate_cursor_scalar(val) for val in key_values]

    payload = {
        "v": CURSOR_VERSION,
        "t": table_norm,
        "o": norm_order,
        "k": norm_keys,
    }
    payload_bytes = json.dumps(
        payload,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")

    sig_bytes = hmac.new(key_bytes, payload_bytes, hashlib.sha256).digest()
    return f"{_b64url_encode(payload_bytes)}.{_b64url_encode(sig_bytes)}"


def decode_cursor(
    token: str,
    *,
    expected_table: str,
    expected_order_spec: Sequence[tuple[str, str]],
    secret_key: bytes | str | None,
) -> tuple[Any, ...]:
    """Verify HMAC-SHA256 signature and query binding of a keyset cursor token."""
    if not isinstance(token, str) or not token or len(token) > _MAX_CURSOR_TOKEN_CHARS:
        raise CursorError("Invalid pagination cursor")

    key_bytes = _normalize_secret_key(secret_key)

    if not isinstance(expected_table, str) or not expected_table.strip():
        raise CursorError("Invalid pagination cursor")
    expected_t = expected_table.strip().lower()
    expected_o = _normalize_order_spec(expected_order_spec)

    parts = token.split(".")
    if len(parts) != 2:
        raise CursorError("Invalid pagination cursor")

    payload_b64, sig_b64 = parts
    payload_bytes = _b64url_decode_strict(payload_b64)
    sig_bytes = _b64url_decode_strict(sig_b64)
    if len(sig_bytes) != 32:
        raise CursorError("Invalid pagination cursor")

    expected_sig = hmac.new(key_bytes, payload_bytes, hashlib.sha256).digest()
    if not hmac.compare_digest(sig_bytes, expected_sig):
        raise CursorError("Invalid pagination cursor")

    try:
        payload_text = payload_bytes.decode("utf-8", errors="strict")
        payload = json.loads(
            payload_text,
            parse_float=_reject_json_float,
            parse_constant=_reject_json_constant,
        )
    except CursorError:
        raise
    except Exception as exc:
        raise CursorError("Invalid pagination cursor") from exc

    if not isinstance(payload, dict) or set(payload.keys()) != {"v", "t", "o", "k"}:
        raise CursorError("Invalid pagination cursor")

    version = payload.get("v")
    if type(version) is not int or version != CURSOR_VERSION:
        raise CursorError("Invalid pagination cursor")

    tbl = payload.get("t")
    if type(tbl) is not str or tbl != expected_t:
        raise CursorError("Invalid pagination cursor")

    order = payload.get("o")
    if not isinstance(order, list) or order != expected_o:
        raise CursorError("Invalid pagination cursor")

    keys = payload.get("k")
    if not isinstance(keys, list) or len(keys) != len(expected_o):
        raise CursorError("Invalid pagination cursor")

    validated_keys = tuple(_validate_cursor_scalar(val) for val in keys)
    return validated_keys


__all__ = [
    "CURSOR_VERSION",
    "CursorError",
    "decode_cursor",
    "encode_cursor",
]
