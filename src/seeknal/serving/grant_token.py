"""Grant token signing and verification using ES256 compact JWS."""

from __future__ import annotations

import base64
import json
import math
import re
import time
from collections.abc import Mapping
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import (
    decode_dss_signature,
    encode_dss_signature,
)

from seeknal.serving.enforce import EnforcementError

B64URL_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


class GrantTokenError(EnforcementError):
    """Raised when grant token verification or decoding fails."""


def _b64url_encode(data: bytes) -> str:
    """Encode bytes to unpadded base64url string."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(s: str) -> bytes:
    """Decode unpadded base64url string to bytes."""
    if not isinstance(s, str) or not s or not B64URL_PATTERN.match(s):
        raise GrantTokenError("Invalid base64url segment")
    rem = len(s) % 4
    padded = s + ("=" * ((4 - rem) % 4))
    try:
        return base64.urlsafe_b64decode(padded.encode("ascii"))
    except Exception as exc:
        raise GrantTokenError("Failed to decode base64url segment") from exc


def _parse_numeric_claim(val: Any, name: str) -> float:
    """Parse and validate numeric claim, rejecting booleans, non-numbers, NaN and infinity."""
    if isinstance(val, bool):
        raise GrantTokenError(f"'{name}' claim must be a number, not boolean")
    try:
        f = float(val)
    except (ValueError, TypeError):
        raise GrantTokenError(f"'{name}' claim must be a number")
    if math.isnan(f) or math.isinf(f):
        raise GrantTokenError(f"'{name}' claim must be finite")
    return f


def _load_private_key(key: Any) -> ec.EllipticCurvePrivateKey:
    """Load an EC private key from instance, bytes, or PEM string."""
    if isinstance(key, ec.EllipticCurvePrivateKey):
        return key
    if isinstance(key, str):
        key = key.encode("utf-8")
    if isinstance(key, bytes):
        try:
            priv = serialization.load_pem_private_key(key, password=None)
            if isinstance(priv, ec.EllipticCurvePrivateKey):
                return priv
            raise GrantTokenError("Private key is not an EC key")
        except Exception as exc:
            raise GrantTokenError(f"Failed to load PEM private key: {exc}") from exc
    raise GrantTokenError("Unsupported private key type")


def _load_public_key(key: Any, kid: str) -> ec.EllipticCurvePublicKey:
    """Load an EC public key from instance, bytes, or PEM string."""
    if isinstance(key, ec.EllipticCurvePublicKey):
        pub = key
    elif isinstance(key, (str, bytes)):
        key_bytes = key.encode("utf-8") if isinstance(key, str) else key
        try:
            loaded = serialization.load_pem_public_key(key_bytes)
            if isinstance(loaded, ec.EllipticCurvePublicKey):
                pub = loaded
            else:
                raise GrantTokenError(f"Key '{kid}' is not an EC public key")
        except Exception as exc:
            raise GrantTokenError(f"Failed to load PEM public key for kid '{kid}': {exc}") from exc
    else:
        raise GrantTokenError(f"Unsupported key type for kid '{kid}'")

    if not isinstance(pub.curve, ec.SECP256R1):
        raise GrantTokenError(f"Key '{kid}' does not use SECP256R1 (P-256) curve")
    return pub


def sign_grant_token(claims: Mapping[str, Any], private_key: Any, *, kid: str) -> str:
    """Sign a grant claims dictionary as an ES256 compact JWS (JWT)."""
    priv_key = _load_private_key(private_key)
    header = {"alg": "ES256", "typ": "JWT", "kid": kid}
    header_json = json.dumps(header, separators=(",", ":")).encode("utf-8")
    payload_json = json.dumps(claims, separators=(",", ":")).encode("utf-8")

    header_b64 = _b64url_encode(header_json)
    payload_b64 = _b64url_encode(payload_json)
    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")

    dss_sig = priv_key.sign(signing_input, ec.ECDSA(hashes.SHA256()))
    r, s = decode_dss_signature(dss_sig)
    raw_sig = r.to_bytes(32, "big") + s.to_bytes(32, "big")
    sig_b64 = _b64url_encode(raw_sig)

    return f"{header_b64}.{payload_b64}.{sig_b64}"


def verify_grant_token(
    token: str,
    public_keys: Mapping[str, Any],
    *,
    audience: str = "seeknal-serving",
    issuer: str = "atlas-catalog",
    now: float | None = None,
    leeway_seconds: float = 5,
    max_lifetime_seconds: float = 300,
) -> dict[str, Any]:
    """Verify an ES256 compact JWS grant token and return its claims dictionary."""
    if not isinstance(token, str):
        raise GrantTokenError("Token must be a string")

    parts = token.split(".")
    if len(parts) != 3:
        raise GrantTokenError(f"Expected 3 segments in token, got {len(parts)}")

    header_b64, payload_b64, sig_b64 = parts

    # 1. Decode and validate header
    header_bytes = _b64url_decode(header_b64)
    try:
        header = json.loads(header_bytes.decode("utf-8"))
    except Exception as exc:
        raise GrantTokenError("Invalid token header JSON") from exc

    if not isinstance(header, dict):
        raise GrantTokenError("Token header must be a JSON object")

    if "alg" not in header:
        raise GrantTokenError("Missing 'alg' in token header")
    if header["alg"] != "ES256":
        raise GrantTokenError(f"Unsupported algorithm '{header['alg']}', only ES256 permitted")

    typ = header.get("typ")
    if typ is not None and typ != "JWT":
        raise GrantTokenError(f"Unsupported token type '{typ}', expected JWT")

    kid = header.get("kid")
    if not kid or not isinstance(kid, str):
        raise GrantTokenError("Missing or invalid 'kid' in token header")

    if kid not in public_keys:
        raise GrantTokenError(f"Unknown key ID '{kid}'")

    pub_key = _load_public_key(public_keys[kid], kid)

    # 2. Decode signature and verify
    raw_sig = _b64url_decode(sig_b64)
    if len(raw_sig) != 64:
        raise GrantTokenError(f"Signature must be exactly 64 bytes, got {len(raw_sig)}")

    r = int.from_bytes(raw_sig[:32], "big")
    s = int.from_bytes(raw_sig[32:], "big")
    try:
        dss_sig = encode_dss_signature(r, s)
    except Exception as exc:
        raise GrantTokenError("Invalid signature format") from exc

    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
    try:
        pub_key.verify(dss_sig, signing_input, ec.ECDSA(hashes.SHA256()))
    except InvalidSignature:
        raise GrantTokenError("Invalid token signature")
    except Exception as exc:
        raise GrantTokenError(f"Signature verification failed: {exc}")

    # 3. Decode and validate payload
    payload_bytes = _b64url_decode(payload_b64)
    try:
        claims = json.loads(payload_bytes.decode("utf-8"))
    except Exception as exc:
        raise GrantTokenError("Invalid token payload JSON") from exc

    if not isinstance(claims, dict):
        raise GrantTokenError("Token claims must be a JSON object")

    # 4. Validate claims
    current_time = time.time() if now is None else float(now)

    # iss
    iss = claims.get("iss")
    if iss != issuer:
        raise GrantTokenError(f"Invalid issuer '{iss}', expected '{issuer}'")

    # aud
    aud = claims.get("aud")
    aud_match = False
    if isinstance(aud, str):
        aud_match = (aud == audience)
    elif isinstance(aud, (list, tuple)):
        aud_match = (audience in aud)
    if not aud_match:
        raise GrantTokenError(f"Invalid audience '{aud}', expected '{audience}'")

    # iat
    if "iat" not in claims:
        raise GrantTokenError("Missing required 'iat' claim")
    iat = _parse_numeric_claim(claims["iat"], "iat")
    if iat > current_time + leeway_seconds:
        raise GrantTokenError("Token 'iat' is in the future beyond leeway")

    # exp
    if "exp" not in claims:
        raise GrantTokenError("Missing required 'exp' claim")
    exp = _parse_numeric_claim(claims["exp"], "exp")
    if exp < current_time - leeway_seconds:
        raise GrantTokenError("Token has expired")

    if exp - iat > max_lifetime_seconds:
        raise GrantTokenError(
            f"Token lifetime of {exp - iat:.1f}s exceeds maximum {max_lifetime_seconds}s"
        )
    if exp < iat:
        raise GrantTokenError("Token 'exp' must not be before 'iat'")

    # nbf
    if "nbf" in claims:
        nbf = _parse_numeric_claim(claims["nbf"], "nbf")
        if nbf > current_time + leeway_seconds:
            raise GrantTokenError("Token is not yet valid ('nbf' is in the future)")

    # max_rows
    if "max_rows" in claims:
        _parse_numeric_claim(claims["max_rows"], "max_rows")

    return claims
