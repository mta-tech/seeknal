"""Tests for cross-language parameter digest v1 (`seeknal.serving.parameter_binding`)."""

from __future__ import annotations

import hashlib
from decimal import Decimal

import pytest

from seeknal.serving.enforce import EnforcementError
from seeknal.serving.parameter_binding import (
    MAX_FRAME_BYTES,
    MAX_PARAMETERS,
    MAX_SAFE_INTEGER,
    MAX_STRING_UTF8_BYTES,
    MIN_SAFE_INTEGER,
    ParameterBindingError,
    TEST_VECTORS,
    encode_parameter_frame,
    parameter_digest,
    verify_parameter_digest,
)


def test_independent_cross_language_vectors():
    """Verify exact wire bytes and SHA-256 hex digests for all canonical v1 vectors."""
    expected_vectors = {
        "empty": (
            [],
            b"seeknal-params-v1\n0\n",
            "93359dd2643ee5e2add07a3878feced492c95fe8b6fd67213552a98cb3468483",
        ),
        "null_only": (
            [None],
            b"seeknal-params-v1\n1\nn;",
            "75c8c1639cb338e84b11ac1998fec7654dd3caba8c375f2353a2ebc8935300f2",
        ),
        "booleans": (
            [True, False],
            b"seeknal-params-v1\n2\nt;f;",
            "34745469e4d589cf08aab1f3ef0ccd17920f75c335291d4ac6acb6b158af6eac",
        ),
        "zero_and_neg_zero": (
            [0, -0],
            b"seeknal-params-v1\n2\ni0;i0;",
            "b32ee4bb52cd9af3fd3fb58138300b0f7c1932efe37918f49039186bead696ff",
        ),
        "int_boundaries": (
            [-9007199254740991, -1, 0, 1, 9007199254740991],
            b"seeknal-params-v1\n5\ni-9007199254740991;i-1;i0;i1;i9007199254740991;",
            "1ebcdf7c6c30045c245633e934759aca7aed6256319c745f5ae954116e8f69a6",
        ),
        "empty_string": (
            [""],
            b"seeknal-params-v1\n1\ns0:;",
            "665a8a0a084d9e9e3629aa06caa7cd6e05b8e764d3db3af9dfe2fab526d34e92",
        ),
        "non_ascii_and_combining": (
            ["caf\u00e9", "cafe\u0301", "\u65e5\u672c\u8a9e", "\U0001f680"],
            (
                b"seeknal-params-v1\n4\n"
                b"s5:caf\xc3\xa9;s6:cafe\xcc\x81;s9:\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e;s4:\xf0\x9f\x9a\x80;"
            ),
            "74a7890f8fbe4323c8133b514ff654499e10f1a99b666b530f66cd04d4bf3a0d",
        ),
        "embedded_delimiters": (
            ["n;t;f;i0;s0:;\nseeknal-params-v1\n"],
            b"seeknal-params-v1\n1\ns32:n;t;f;i0;s0:;\nseeknal-params-v1\n;",
            "53686edef0a148a7e00f47116e67d692599cf69e77c4eaa2e9846f3961a81ced",
        ),
        "pilot_order_by_id": (
            [42],
            b"seeknal-params-v1\n1\ni42;",
            "da8b1ac96f96fef78702f927864e1c653f14c9694f349cbea49b6fa3f0238b5b",
        ),
        "pilot_orders_by_month": (
            [11],
            b"seeknal-params-v1\n1\ni11;",
            "7a052edbff033a0c2d2d6c53f28be3deaa23616ab0b5f3e74ced9b9fa91fddb1",
        ),
    }

    for name, (params, expected_frame, expected_sha256) in expected_vectors.items():
        frame = encode_parameter_frame(params)
        assert frame == expected_frame, f"frame mismatch for {name}"
        digest = parameter_digest(params)
        assert digest == expected_sha256, f"digest mismatch for {name}"
        assert verify_parameter_digest(params, expected_sha256) == expected_sha256
        assert TEST_VECTORS[name]["frame"] == expected_frame
        assert TEST_VECTORS[name]["sha256"] == expected_sha256


def test_boolean_vs_integer_and_null_vs_string_distinction():
    """Booleans, integers, nulls, and strings with delimiter characters never collide."""
    d_true = parameter_digest([True])
    d_one = parameter_digest([1])
    d_false = parameter_digest([False])
    d_zero = parameter_digest([0])
    d_null = parameter_digest([None])
    d_str_null = parameter_digest(["n;"])
    d_str_empty = parameter_digest([""])

    assert len({d_true, d_one, d_false, d_zero, d_null, d_str_null, d_str_empty}) == 7
    assert encode_parameter_frame([True]) == b"seeknal-params-v1\n1\nt;"
    assert encode_parameter_frame([1]) == b"seeknal-params-v1\n1\ni1;"
    assert encode_parameter_frame([False]) == b"seeknal-params-v1\n1\nf;"
    assert encode_parameter_frame([0]) == b"seeknal-params-v1\n1\ni0;"


def test_no_unicode_normalization():
    """Precomposed and decomposed Unicode sequences produce distinct digests."""
    nfc = "caf\u00e9"
    nfd = "cafe\u0301"
    assert encode_parameter_frame([nfc]) == b"seeknal-params-v1\n1\ns5:caf\xc3\xa9;"
    assert encode_parameter_frame([nfd]) == b"seeknal-params-v1\n1\ns6:cafe\xcc\x81;"
    assert parameter_digest([nfc]) != parameter_digest([nfd])


def test_safe_integer_bounds():
    """Safe integer endpoints are accepted; outside endpoints are rejected without leaking value."""
    assert encode_parameter_frame([MIN_SAFE_INTEGER]) == b"seeknal-params-v1\n1\ni-9007199254740991;"
    assert encode_parameter_frame([MAX_SAFE_INTEGER]) == b"seeknal-params-v1\n1\ni9007199254740991;"

    too_low = MIN_SAFE_INTEGER - 1
    too_high = MAX_SAFE_INTEGER + 1
    secret_big = 999999999999999999999999

    for bad_int in (too_low, too_high, secret_big, -secret_big):
        with pytest.raises(ParameterBindingError) as exc_info:
            parameter_digest([bad_int])
        assert isinstance(exc_info.value, EnforcementError)
        assert str(bad_int) not in str(exc_info.value)


def test_reject_unsupported_scalar_and_container_types():
    """Reject floats, decimals, bytes, nested lists, dicts, sets, and custom objects without printing values."""
    secret_sentinel = "SENSITIVE_PARAM_VALUE_DO_NOT_LEAK"

    class CustomObj:
        def __repr__(self) -> str:
            return secret_sentinel

        def __str__(self) -> str:
            return secret_sentinel

    bad_values = [
        0.0,
        1.5,
        -3.14,
        float("nan"),
        float("inf"),
        float("-inf"),
        Decimal("10.5"),
        b"binary-secret",
        bytearray(b"binary-secret"),
        memoryview(b"binary-secret"),
        [secret_sentinel],
        (secret_sentinel,),
        {"k": secret_sentinel},
        {secret_sentinel},
        CustomObj(),
    ]

    for val in bad_values:
        with pytest.raises(ParameterBindingError) as exc_info:
            parameter_digest([val])
        msg = str(exc_info.value)
        assert secret_sentinel not in msg
        assert "binary-secret" not in msg
        assert "10.5" not in msg
        assert "-3.14" not in msg


def test_reject_non_sequence_params_container():
    """Reject strings, bytes, mappings, or scalars passed as the top-level params argument."""
    for non_seq in ("SELECT 1", b"bytes", {"a": 1}, 42, None):
        with pytest.raises(ParameterBindingError) as exc_info:
            parameter_digest(non_seq)  # type: ignore[arg-type]
        assert "SELECT 1" not in str(exc_info.value)


def test_reject_unpaired_surrogates_without_leaking_value():
    """Strings containing unpaired UTF-16 surrogates are rejected without leaking string contents."""
    for s in ("\ud800", "\udfff", "SECRET_PREFIX_\ud83d_SECRET_SUFFIX"):
        with pytest.raises(ParameterBindingError) as exc_info:
            parameter_digest([s])
        assert "SECRET_PREFIX" not in str(exc_info.value)
        assert "SECRET_SUFFIX" not in str(exc_info.value)
        assert exc_info.value.__cause__ is None


def test_parameter_count_limit():
    """Up to 32 parameters are accepted; 33 or more are rejected."""
    params_32 = list(range(MAX_PARAMETERS))
    digest_32 = parameter_digest(params_32)
    assert len(digest_32) == 64

    params_33 = list(range(MAX_PARAMETERS + 1))
    with pytest.raises(ParameterBindingError, match="exceeds maximum of 32"):
        parameter_digest(params_33)


def test_per_string_utf8_byte_limit():
    """Strings up to 16,384 UTF-8 bytes are accepted; 16,385 UTF-8 bytes are rejected."""
    exact_16k = "a" * MAX_STRING_UTF8_BYTES
    assert len(parameter_digest([exact_16k])) == 64

    over_16k_ascii = "a" * (MAX_STRING_UTF8_BYTES + 1)
    with pytest.raises(ParameterBindingError, match="exceeds maximum UTF-8 length"):
        parameter_digest([over_16k_ascii])

    # Multi-byte UTF-8: 5,462 characters * 3 bytes/char = 16,386 UTF-8 bytes (> 16,384)
    over_16k_utf8 = "\u65e5" * 5462
    assert len(over_16k_utf8) < MAX_STRING_UTF8_BYTES
    assert len(over_16k_utf8.encode("utf-8")) > MAX_STRING_UTF8_BYTES
    with pytest.raises(ParameterBindingError, match="exceeds maximum UTF-8 length"):
        parameter_digest([over_16k_utf8])


def test_total_frame_byte_limit_includes_prefix_and_framing():
    """Total frame limit of 65,536 bytes includes header prefix, count, and scalar framing."""
    # Prefix for 4 params: b"seeknal-params-v1\n4\n" -> 20 bytes.
    # Three 16,384-byte strings: each is b"s16384:" (7 bytes) + 16,384 + b";" (1 byte) = 16,392 bytes.
    # 3 * 16,392 = 49,176 bytes.
    # Remaining budget in 65,536 bytes: 65,536 - 20 - 49,176 = 16,340 bytes.
    # 4th string of length 16,332 has framing b"s16332:" (7 bytes) + 16,332 + b";" (1 byte) = 16,340 bytes.
    s_16k = "x" * MAX_STRING_UTF8_BYTES
    s_fit = "y" * 16_332
    frame_exact = encode_parameter_frame([s_16k, s_16k, s_16k, s_fit])
    assert len(frame_exact) == MAX_FRAME_BYTES
    assert parameter_digest([s_16k, s_16k, s_16k, s_fit]) == hashlib.sha256(frame_exact).hexdigest()

    # 1 byte larger (16,333-byte 4th string -> total frame 65,537 bytes) must be rejected
    s_over = "y" * 16_333
    with pytest.raises(ParameterBindingError, match="exceeds maximum size of 65536 bytes"):
        encode_parameter_frame([s_16k, s_16k, s_16k, s_over])


def test_verify_parameter_digest_validation_and_mismatch():
    """verify_parameter_digest rejects malformed expected hex and mismatches without hash fingerprint or values."""
    digest = parameter_digest([42])
    assert verify_parameter_digest([42], digest) == digest

    # Mismatch with different parameter value must not print the parameter value or hash fingerprints
    secret_val = "TOP_SECRET_STRING_98765"
    secret_digest = parameter_digest([secret_val])
    with pytest.raises(ParameterBindingError, match="parameter SHA256 mismatch") as exc_info:
        verify_parameter_digest([secret_val], digest)
    err_msg = str(exc_info.value)
    assert secret_val not in err_msg
    assert digest not in err_msg
    assert secret_digest not in err_msg

    # Uppercase hex is rejected (contract requires lower-case 64-hex)
    with pytest.raises(ParameterBindingError, match="lower-case 64-character hex"):
        verify_parameter_digest([42], digest.upper())

    # Short/non-hex/non-string rejected
    for bad_expected in ("abc", "g" * 64, 123, True, None):
        with pytest.raises(ParameterBindingError):
            verify_parameter_digest([42], bad_expected)  # type: ignore[arg-type]
