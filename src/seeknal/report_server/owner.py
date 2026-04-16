from __future__ import annotations

import hashlib
import hmac
import os
import secrets


def mint() -> tuple[str, str, str]:
    """Mint a new owner secret.

    Returns (plain_hex_64chars, hash_hex, salt_hex).
    plain_hex is shown once to the publisher; hash+salt are stored in DB.
    """
    plain = secrets.token_hex(32)  # 64 hex chars
    salt = os.urandom(16)
    digest = hashlib.scrypt(
        plain.encode(),
        salt=salt,
        n=16384,
        r=8,
        p=1,
    )
    return plain, digest.hex(), salt.hex()


def verify(plain_hex: str, stored_hash_hex: str, stored_salt_hex: str) -> bool:
    """Verify a plain owner secret against stored scrypt hash."""
    try:
        salt = bytes.fromhex(stored_salt_hex)
        expected = bytes.fromhex(stored_hash_hex)
        actual = hashlib.scrypt(
            plain_hex.encode(),
            salt=salt,
            n=16384,
            r=8,
            p=1,
        )
        return hmac.compare_digest(actual, expected)
    except (ValueError, OverflowError):
        return False
