"""Tests for Seeknal Serving grant token signing and verification."""

from __future__ import annotations

import base64
import json
import time

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.serving.grant_token import (
    GrantTokenError,
    _b64url_encode,
    sign_grant_token,
    verify_grant_token,
)


@pytest.fixture
def ec_key_pair():
    private_key = ec.generate_private_key(ec.SECP256R1())
    public_key = private_key.public_key()
    return private_key, public_key


@pytest.fixture
def valid_claims():
    now = time.time()
    return {
        "sub": "analyst@example.com",
        "ws": "analytics",
        "jti": "grant-abc-123",
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": now,
        "exp": now + 60,
        "sql_sha256": "dummy_sha256",
        "max_rows": 1000,
        "tables": {
            "claims.claim": {
                "columns": ["claim_id", "amount"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    }


def test_valid_token(ec_key_pair, valid_claims):
    private_key, public_key = ec_key_pair
    token = sign_grant_token(valid_claims, private_key, kid="key-1")

    # Verify with EC public key object
    claims = verify_grant_token(token, {"key-1": public_key})
    assert claims["sub"] == valid_claims["sub"]
    assert claims["ws"] == valid_claims["ws"]

    # Verify with PEM bytes
    pem_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    claims_pem = verify_grant_token(token, {"key-1": pem_bytes})
    assert claims_pem["sub"] == valid_claims["sub"]

    # Verify with PEM string
    pem_str = pem_bytes.decode("utf-8")
    claims_str = verify_grant_token(token, {"key-1": pem_str})
    assert claims_str["sub"] == valid_claims["sub"]

    # Verify with audience list containing expected audience
    valid_claims_aud_list = dict(valid_claims)
    valid_claims_aud_list["aud"] = ["other-service", "seeknal-serving"]
    token_aud_list = sign_grant_token(valid_claims_aud_list, private_key, kid="key-1")
    verified = verify_grant_token(token_aud_list, {"key-1": public_key})
    assert verified["sub"] == valid_claims["sub"]


def test_tampered_payload(ec_key_pair, valid_claims):
    private_key, public_key = ec_key_pair
    token = sign_grant_token(valid_claims, private_key, kid="key-1")
    header_b64, payload_b64, sig_b64 = token.split(".")

    # Decode and tamper payload
    payload = json.loads(base64.urlsafe_b64decode(payload_b64 + "==").decode())
    payload["sub"] = "hacker@evil.com"
    tampered_b64 = _b64url_encode(json.dumps(payload).encode())
    tampered_token = f"{header_b64}.{tampered_b64}.{sig_b64}"

    with pytest.raises(GrantTokenError) as exc_info:
        verify_grant_token(tampered_token, {"key-1": public_key})
    assert "signature" in str(exc_info.value).lower()
    assert token not in str(exc_info.value)
    assert tampered_token not in str(exc_info.value)


def test_tampered_signature(ec_key_pair, valid_claims):
    private_key, public_key = ec_key_pair
    token = sign_grant_token(valid_claims, private_key, kid="key-1")
    header_b64, payload_b64, sig_b64 = token.split(".")

    # Tamper with signature bytes
    sig_bytes = bytearray(base64.urlsafe_b64decode(sig_b64 + "=="))
    sig_bytes[0] ^= 0xFF
    tampered_sig_b64 = _b64url_encode(bytes(sig_bytes))
    tampered_token = f"{header_b64}.{payload_b64}.{tampered_sig_b64}"

    with pytest.raises(GrantTokenError) as exc_info:
        verify_grant_token(tampered_token, {"key-1": public_key})
    assert "signature" in str(exc_info.value).lower()
    assert token not in str(exc_info.value)


def test_alg_none(ec_key_pair, valid_claims):
    private_key, public_key = ec_key_pair
    header = {"alg": "none", "typ": "JWT", "kid": "key-1"}
    header_b64 = _b64url_encode(json.dumps(header).encode())
    payload_b64 = _b64url_encode(json.dumps(valid_claims).encode())
    sig_b64 = _b64url_encode(b"0" * 64)
    token = f"{header_b64}.{payload_b64}.{sig_b64}"

    with pytest.raises(GrantTokenError) as exc_info:
        verify_grant_token(token, {"key-1": public_key})
    assert "ES256" in str(exc_info.value)
    assert token not in str(exc_info.value)


def test_alg_hs256(ec_key_pair, valid_claims):
    private_key, public_key = ec_key_pair
    header = {"alg": "HS256", "typ": "JWT", "kid": "key-1"}
    header_b64 = _b64url_encode(json.dumps(header).encode())
    payload_b64 = _b64url_encode(json.dumps(valid_claims).encode())
    sig_b64 = _b64url_encode(b"0" * 64)
    token = f"{header_b64}.{payload_b64}.{sig_b64}"

    with pytest.raises(GrantTokenError) as exc_info:
        verify_grant_token(token, {"key-1": public_key})
    assert "ES256" in str(exc_info.value)


def test_alg_missing(ec_key_pair, valid_claims):
    private_key, public_key = ec_key_pair
    header = {"typ": "JWT", "kid": "key-1"}
    header_b64 = _b64url_encode(json.dumps(header).encode())
    payload_b64 = _b64url_encode(json.dumps(valid_claims).encode())
    sig_b64 = _b64url_encode(b"0" * 64)
    token = f"{header_b64}.{payload_b64}.{sig_b64}"

    with pytest.raises(GrantTokenError) as exc_info:
        verify_grant_token(token, {"key-1": public_key})
    assert "alg" in str(exc_info.value).lower()


def test_unknown_kid(ec_key_pair, valid_claims):
    private_key, public_key = ec_key_pair
    token = sign_grant_token(valid_claims, private_key, kid="unknown-kid")

    with pytest.raises(GrantTokenError) as exc_info:
        verify_grant_token(token, {"known-kid": public_key})
    assert "unknown-kid" in str(exc_info.value)
    assert token not in str(exc_info.value)


def test_wrong_iss_and_aud(ec_key_pair, valid_claims):
    private_key, public_key = ec_key_pair

    # Wrong issuer
    bad_iss = dict(valid_claims, iss="untrusted-catalog")
    token_bad_iss = sign_grant_token(bad_iss, private_key, kid="key-1")
    with pytest.raises(GrantTokenError) as exc_info:
        verify_grant_token(token_bad_iss, {"key-1": public_key})
    assert "issuer" in str(exc_info.value).lower()

    # Wrong audience
    bad_aud = dict(valid_claims, aud="other-service")
    token_bad_aud = sign_grant_token(bad_aud, private_key, kid="key-1")
    with pytest.raises(GrantTokenError) as exc_info:
        verify_grant_token(token_bad_aud, {"key-1": public_key})
    assert "audience" in str(exc_info.value).lower()


def test_expired(ec_key_pair, valid_claims):
    private_key, public_key = ec_key_pair
    now = time.time()
    claims = dict(valid_claims, iat=now - 200, exp=now - 20)
    token = sign_grant_token(claims, private_key, kid="key-1")

    with pytest.raises(GrantTokenError) as exc_info:
        verify_grant_token(token, {"key-1": public_key}, now=now, leeway_seconds=5)
    assert "expired" in str(exc_info.value).lower()


def test_nbf_in_future(ec_key_pair, valid_claims):
    private_key, public_key = ec_key_pair
    now = time.time()
    claims = dict(valid_claims, iat=now, nbf=now + 50, exp=now + 100)
    token = sign_grant_token(claims, private_key, kid="key-1")

    with pytest.raises(GrantTokenError) as exc_info:
        verify_grant_token(token, {"key-1": public_key}, now=now, leeway_seconds=5)
    assert "nbf" in str(exc_info.value).lower() or "not yet valid" in str(exc_info.value).lower()


def test_iat_in_future(ec_key_pair, valid_claims):
    private_key, public_key = ec_key_pair
    now = time.time()
    claims = dict(valid_claims, iat=now + 20, exp=now + 60)
    token = sign_grant_token(claims, private_key, kid="key-1")

    with pytest.raises(GrantTokenError) as exc_info:
        verify_grant_token(token, {"key-1": public_key}, now=now, leeway_seconds=5)
    assert "iat" in str(exc_info.value).lower()


def test_lifetime_over_300s(ec_key_pair, valid_claims):
    private_key, public_key = ec_key_pair
    now = time.time()
    claims = dict(valid_claims, iat=now, exp=now + 301)
    token = sign_grant_token(claims, private_key, kid="key-1")

    with pytest.raises(GrantTokenError) as exc_info:
        verify_grant_token(token, {"key-1": public_key}, now=now)
    assert "lifetime" in str(exc_info.value).lower()


def test_malformed_token(ec_key_pair):
    _, public_key = ec_key_pair

    # Wrong segment count
    with pytest.raises(GrantTokenError) as exc_info:
        verify_grant_token("seg1.seg2", {"key-1": public_key})
    assert "3 segments" in str(exc_info.value)

    with pytest.raises(GrantTokenError) as exc_info:
        verify_grant_token("seg1.seg2.seg3.seg4", {"key-1": public_key})
    assert "3 segments" in str(exc_info.value)

    # Empty token
    with pytest.raises(GrantTokenError) as exc_info:
        verify_grant_token("", {"key-1": public_key})
    assert "3 segments" in str(exc_info.value)

    # Bad base64 characters
    with pytest.raises(GrantTokenError) as exc_info:
        verify_grant_token("bad base64!.payload.signature", {"key-1": public_key})
    assert "base64" in str(exc_info.value).lower()


@pytest.mark.parametrize("claim_name", ["iat", "exp", "nbf", "max_rows"])
@pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), float("-inf"), True, False])
def test_non_finite_and_boolean_claims(ec_key_pair, valid_claims, claim_name, bad_value):
    private_key, public_key = ec_key_pair
    claims = dict(valid_claims)
    claims[claim_name] = bad_value
    token = sign_grant_token(claims, private_key, kid="key-1")

    with pytest.raises(GrantTokenError) as exc_info:
        verify_grant_token(token, {"key-1": public_key})
    assert claim_name in str(exc_info.value).lower()

