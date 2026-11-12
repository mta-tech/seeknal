"""Cross-language parameter digest v1 and safe scalar validation for Seeknal Serving.

Wire contract (`seeknal-params-v1`):
- Prefix: ASCII `seeknal-params-v1\\n`, then ASCII decimal parameter count, then `\\n`.
- Each parameter scalar is concatenated in array order:
  - `None` (null): ASCII `n;`
  - `bool`: ASCII `t;` or `f;` (detected before `int`)
  - safe `int`: ASCII `i`, canonical base-10 signed integer (`-0` normalizes to `0`), then `;`;
    range `[-9007199254740991, 9007199254740991]`
  - UTF-8 `str`: ASCII `s`, decimal UTF-8 byte length, `:`, exact UTF-8 bytes, then `;`
- SHA-256 digest is lower-case 64-character hexadecimal.
- No JSON canonicalization or Unicode normalization is performed.
- Rejects floats, unsafe integers, objects/nested arrays/binary, unpaired surrogates,
  >32 parameters, >16 KiB UTF-8 per string, and >64 KiB total encoded frame (including prefix).
- Never includes parameter values in exception messages.
"""

from __future__ import annotations

import hashlib
import hmac
import re
from collections.abc import Mapping, Sequence
from typing import Any

from seeknal.serving.enforce import EnforcementError

PARAMS_DIGEST_PREFIX = b"seeknal-params-v1\n"
MAX_PARAMETERS = 32
MAX_STRING_UTF8_BYTES = 16_384
MAX_FRAME_BYTES = 65_536
MIN_SAFE_INTEGER = -9_007_199_254_740_991
MAX_SAFE_INTEGER = 9_007_199_254_740_991

SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")

TEST_VECTORS: Mapping[str, Mapping[str, Any]] = {
    "empty": {
        "params": (),
        "frame": b"seeknal-params-v1\n0\n",
        "sha256": "93359dd2643ee5e2add07a3878feced492c95fe8b6fd67213552a98cb3468483",
    },
    "null_only": {
        "params": (None,),
        "frame": b"seeknal-params-v1\n1\nn;",
        "sha256": "75c8c1639cb338e84b11ac1998fec7654dd3caba8c375f2353a2ebc8935300f2",
    },
    "booleans": {
        "params": (True, False),
        "frame": b"seeknal-params-v1\n2\nt;f;",
        "sha256": "34745469e4d589cf08aab1f3ef0ccd17920f75c335291d4ac6acb6b158af6eac",
    },
    "zero_and_neg_zero": {
        "params": (0, -0),
        "frame": b"seeknal-params-v1\n2\ni0;i0;",
        "sha256": "b32ee4bb52cd9af3fd3fb58138300b0f7c1932efe37918f49039186bead696ff",
    },
    "int_boundaries": {
        "params": (-9_007_199_254_740_991, -1, 0, 1, 9_007_199_254_740_991),
        "frame": b"seeknal-params-v1\n5\ni-9007199254740991;i-1;i0;i1;i9007199254740991;",
        "sha256": "1ebcdf7c6c30045c245633e934759aca7aed6256319c745f5ae954116e8f69a6",
    },
    "empty_string": {
        "params": ("",),
        "frame": b"seeknal-params-v1\n1\ns0:;",
        "sha256": "665a8a0a084d9e9e3629aa06caa7cd6e05b8e764d3db3af9dfe2fab526d34e92",
    },
    "non_ascii_and_combining": {
        "params": ("caf\u00e9", "cafe\u0301", "\u65e5\u672c\u8a9e", "\U0001f680"),
        "frame": (
            b"seeknal-params-v1\n4\n"
            b"s5:caf\xc3\xa9;s6:cafe\xcc\x81;s9:\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e;s4:\xf0\x9f\x9a\x80;"
        ),
        "sha256": "74a7890f8fbe4323c8133b514ff654499e10f1a99b666b530f66cd04d4bf3a0d",
    },
    "embedded_delimiters": {
        "params": ("n;t;f;i0;s0:;\nseeknal-params-v1\n",),
        "frame": b"seeknal-params-v1\n1\ns32:n;t;f;i0;s0:;\nseeknal-params-v1\n;",
        "sha256": "53686edef0a148a7e00f47116e67d692599cf69e77c4eaa2e9846f3961a81ced",
    },
    "pilot_order_by_id": {
        "params": (42,),
        "frame": b"seeknal-params-v1\n1\ni42;",
        "sha256": "da8b1ac96f96fef78702f927864e1c653f14c9694f349cbea49b6fa3f0238b5b",
    },
    "pilot_orders_by_month": {
        "params": (11,),
        "frame": b"seeknal-params-v1\n1\ni11;",
        "sha256": "7a052edbff033a0c2d2d6c53f28be3deaa23616ab0b5f3e74ced9b9fa91fddb1",
    },
}


class ParameterBindingError(EnforcementError):
    """Raised when parameter encoding or digest verification fails."""


def encode_parameter_frame(params: Sequence[Any]) -> bytes:
    """Encode a positional parameter sequence into exact `seeknal-params-v1` frame bytes."""
    if isinstance(params, (str, bytes, bytearray, memoryview, Mapping)) or not isinstance(
        params, Sequence
    ):
        raise ParameterBindingError("parameters must be a sequence")

    count = len(params)
    if count > MAX_PARAMETERS:
        raise ParameterBindingError(
            f"parameter count {count} exceeds maximum of {MAX_PARAMETERS}"
        )

    buf = bytearray(f"seeknal-params-v1\n{count}\n".encode("ascii"))

    for idx, val in enumerate(params):
        if val is None:
            chunk = b"n;"
        elif isinstance(val, bool):
            chunk = b"t;" if val else b"f;"
        elif type(val) is int:
            if val < MIN_SAFE_INTEGER or val > MAX_SAFE_INTEGER:
                raise ParameterBindingError(
                    f"integer parameter at index {idx} is outside safe integer range"
                )
            norm = 0 if val == 0 else int(val)
            chunk = f"i{norm};".encode("ascii")
        elif type(val) is str:
            try:
                utf8_bytes = val.encode("utf-8", errors="strict")
            except UnicodeEncodeError:
                raise ParameterBindingError(
                    f"string parameter at index {idx} contains invalid UTF-8 or unpaired surrogate"
                ) from None

            if len(utf8_bytes) > MAX_STRING_UTF8_BYTES:
                raise ParameterBindingError(
                    f"string parameter at index {idx} exceeds maximum UTF-8 length of {MAX_STRING_UTF8_BYTES} bytes"
                )
            chunk = f"s{len(utf8_bytes)}:".encode("ascii") + utf8_bytes + b";"
        else:
            raise ParameterBindingError(
                f"unsupported parameter type '{type(val).__name__}' at index {idx}"
            )

        buf.extend(chunk)
        if len(buf) > MAX_FRAME_BYTES:
            raise ParameterBindingError(
                f"encoded parameter frame exceeds maximum size of {MAX_FRAME_BYTES} bytes"
            )

    return bytes(buf)


def parameter_digest(params: Sequence[Any]) -> str:
    """Compute the lower-case 64-hex SHA-256 digest of `params` under `seeknal-params-v1`."""
    frame = encode_parameter_frame(params)
    return hashlib.sha256(frame).hexdigest()


def verify_parameter_digest(params: Sequence[Any], expected_sha256: str) -> str:
    """Verify that `params` matches `expected_sha256` (lower-case 64-hex SHA-256)."""
    if (
        isinstance(expected_sha256, bool)
        or not isinstance(expected_sha256, str)
        or not SHA256_HEX_RE.fullmatch(expected_sha256)
    ):
        raise ParameterBindingError(
            "expected parameter digest must be a lower-case 64-character hex SHA-256 string"
        )
    actual_sha256 = parameter_digest(params)
    if not hmac.compare_digest(actual_sha256, expected_sha256):
        raise ParameterBindingError(
            f"parameter SHA256 mismatch: expected {expected_sha256}, got {actual_sha256}"
        )
    return actual_sha256


__all__ = [
    "MAX_FRAME_BYTES",
    "MAX_PARAMETERS",
    "MAX_SAFE_INTEGER",
    "MAX_STRING_UTF8_BYTES",
    "MIN_SAFE_INTEGER",
    "PARAMS_DIGEST_PREFIX",
    "ParameterBindingError",
    "SHA256_HEX_RE",
    "TEST_VECTORS",
    "encode_parameter_frame",
    "parameter_digest",
    "verify_parameter_digest",
]
