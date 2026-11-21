"""Tests for HMAC-SHA256 keyset pagination cursors in Seeknal Serving."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json

import pytest

from seeknal.serving.keyset import (
    CURSOR_VERSION,
    CursorError,
    decode_cursor,
    encode_cursor,
)
from seeknal.serving.parameter_binding import (
    MAX_SAFE_INTEGER,
    MAX_STRING_UTF8_BYTES,
    MIN_SAFE_INTEGER,
)

SECRET = b"cursor-hmac-test-key-32-bytes-xx"


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _forge_signed_payload(payload_obj: object, secret: bytes = SECRET) -> str:
    raw = json.dumps(payload_obj, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )
    sig = hmac.new(secret, raw, hashlib.sha256).digest()
    return f"{_b64url(raw)}.{_b64url(sig)}"


def test_cursor_round_trip_scalars_and_wire_shape():
    order_spec = [("As_Of_Date", "DESC"), ("Txn_ID", "DESC"), ("Flag", "DESC")]
    values = ["2026-10-31", 9001, True]

    token = encode_cursor(
        "CBN.Rpt.Rpt_Txn_Detail",
        order_spec,
        values,
        SECRET,
    )
    parts = token.split(".")
    assert len(parts) == 2
    assert "=" not in token

    # Verify wire JSON format matches §5 {v, t, o, k}
    padded = parts[0] + ("=" * ((4 - len(parts[0]) % 4) % 4))
    decoded_json = json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))
    assert decoded_json == {
        "v": CURSOR_VERSION,
        "t": "cbn.rpt.rpt_txn_detail",
        "o": [["as_of_date", "desc"], ["txn_id", "desc"], ["flag", "desc"]],
        "k": ["2026-10-31", 9001, True],
    }

    out = decode_cursor(
        token,
        expected_table="cbn.rpt.rpt_txn_detail",
        expected_order_spec=[
            ("as_of_date", "desc"),
            ("txn_id", "desc"),
            ("flag", "desc"),
        ],
        secret_key=SECRET.decode("ascii"),
    )
    assert out == ("2026-10-31", 9001, True)
    assert type(out[1]) is int
    assert type(out[2]) is bool


def test_cursor_safe_int_boundaries_and_unicode_strings():
    order_spec = [
        ("c_min", "asc"),
        ("c_zero", "asc"),
        ("c_max", "asc"),
        ("c_str", "asc"),
    ]
    values = [
        MIN_SAFE_INTEGER,
        -0,
        MAX_SAFE_INTEGER,
        "caf\u00e9 \u65e5\u672c\u8a9e \U0001f680 .;_-",
    ]
    token = encode_cursor("t", order_spec, values, "str-secret")
    decoded = decode_cursor(
        token,
        expected_table="T",
        expected_order_spec=order_spec,
        secret_key=b"str-secret",
    )
    assert decoded == (
        MIN_SAFE_INTEGER,
        0,
        MAX_SAFE_INTEGER,
        "caf\u00e9 \u65e5\u672c\u8a9e \U0001f680 .;_-",
    )


def test_cursor_every_character_flip_rejected():
    token = encode_cursor(
        "cbn.rpt.rpt_txn_detail",
        [("as_of_date", "desc"), ("txn_id", "desc")],
        ["2026-10-31", 42],
        SECRET,
    )
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

    for idx, ch in enumerate(token):
        if ch == ".":
            continue
        replacement = "A" if ch != "A" else "B"
        assert replacement in alphabet
        flipped = token[:idx] + replacement + token[idx + 1 :]
        with pytest.raises(CursorError) as exc_info:
            decode_cursor(
                flipped,
                expected_table="cbn.rpt.rpt_txn_detail",
                expected_order_spec=[("as_of_date", "desc"), ("txn_id", "desc")],
                secret_key=SECRET,
            )
        assert exc_info.value.code == "cursor_invalid"


def test_cursor_foreign_table_and_mismatched_order_rejected():
    token = encode_cursor(
        "cbn.rpt.table_a",
        [("as_of_date", "desc"), ("txn_id", "desc")],
        ["2026-10-31", 100],
        SECRET,
    )

    # Foreign table
    with pytest.raises(CursorError) as exc_tbl:
        decode_cursor(
            token,
            expected_table="cbn.rpt.table_b",
            expected_order_spec=[("as_of_date", "desc"), ("txn_id", "desc")],
            secret_key=SECRET,
        )
    assert exc_tbl.value.code == "cursor_invalid"

    # Direction mismatch
    with pytest.raises(CursorError) as exc_dir:
        decode_cursor(
            token,
            expected_table="cbn.rpt.table_a",
            expected_order_spec=[("as_of_date", "asc"), ("txn_id", "asc")],
            secret_key=SECRET,
        )
    assert exc_dir.value.code == "cursor_invalid"

    # Column order / name mismatch
    with pytest.raises(CursorError):
        decode_cursor(
            token,
            expected_table="cbn.rpt.table_a",
            expected_order_spec=[("txn_id", "desc"), ("as_of_date", "desc")],
            secret_key=SECRET,
        )

    # Arity mismatch
    with pytest.raises(CursorError):
        decode_cursor(
            token,
            expected_table="cbn.rpt.table_a",
            expected_order_spec=[("as_of_date", "desc")],
            secret_key=SECRET,
        )


def test_cursor_wrong_or_missing_secret_rejected():
    planted_secret = "super-secret-cursor-hmac-key"
    token = encode_cursor("users", [("id", "asc")], [10], planted_secret)

    for bad_key in (None, "", b"", "wrong-secret", 123, True):
        with pytest.raises(CursorError) as exc_info:
            decode_cursor(
                token,
                expected_table="users",
                expected_order_spec=[("id", "asc")],
                secret_key=bad_key,  # type: ignore[arg-type]
            )
        assert exc_info.value.code == "cursor_invalid"
        assert planted_secret not in str(exc_info.value)


@pytest.mark.parametrize(
    "bad_val",
    [
        None,
        1.5,
        0.0,
        float("nan"),
        float("inf"),
        MAX_SAFE_INTEGER + 1,
        MIN_SAFE_INTEGER - 1,
        b"bytes",
        ["nested"],
        {"nested": 1},
        "a" * (MAX_STRING_UTF8_BYTES + 1),
        "bad\ud800surrogate",
    ],
)
def test_encode_cursor_rejects_invalid_scalars(bad_val):
    with pytest.raises(CursorError) as exc_info:
        encode_cursor("users", [("id", "asc")], [bad_val], SECRET)
    assert exc_info.value.code == "cursor_invalid"


def test_decode_cursor_rejects_forged_payload_variants():
    bad_payloads = [
        {"v": 2, "t": "users", "o": [["id", "asc"]], "k": [1]},
        {"v": True, "t": "users", "o": [["id", "asc"]], "k": [1]},
        {"v": 1, "t": "users", "o": [["id", "asc"]], "k": [None]},
        {"v": 1, "t": "users", "o": [["id", "asc"]], "k": [1.25]},
        {"v": 1, "t": "users", "o": [["id", "asc"]], "k": [MAX_SAFE_INTEGER + 1]},
        {"v": 1, "t": "users", "o": [["id", "asc"]], "k": [1, 2]},
        {"v": 1, "t": "users", "o": [["id", "asc"]], "k": [1], "extra": "x"},
        ["not", "a", "dict"],
    ]
    for obj in bad_payloads:
        forged = _forge_signed_payload(obj)
        with pytest.raises(CursorError) as exc_info:
            decode_cursor(
                forged,
                expected_table="users",
                expected_order_spec=[("id", "asc")],
                secret_key=SECRET,
            )
        assert exc_info.value.code == "cursor_invalid"


@pytest.mark.parametrize(
    "bad_token",
    [
        "",
        "no-dot-separator",
        "a.b.c",
        ".sig",
        "payload.",
        "not!base64.not!base64",
        f"{_b64url(b'{}')}.{_b64url(b'short-sig')}",
    ],
)
def test_decode_cursor_rejects_malformed_tokens(bad_token):
    with pytest.raises(CursorError) as exc_info:
        decode_cursor(
            bad_token,
            expected_table="users",
            expected_order_spec=[("id", "asc")],
            secret_key=SECRET,
        )
    assert exc_info.value.code == "cursor_invalid"
