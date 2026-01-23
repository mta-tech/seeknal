"""Fernet encryption for sensitive data.

Uses SEEKNAL_ENCRYPT_KEY from environment. If not set, operations
that require encryption will fail with a clear error message.
"""
from __future__ import annotations

import os

from cryptography.fernet import Fernet, InvalidToken


class EncryptionError(Exception):
    """Raised when encryption/decryption fails."""

    pass


def _get_fernet() -> Fernet:
    """Get Fernet instance from environment key."""
    key = os.environ.get("SEEKNAL_ENCRYPT_KEY")
    if not key:
        raise EncryptionError(
            "SEEKNAL_ENCRYPT_KEY not set. Generate one with: "
            "python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'"
        )
    try:
        return Fernet(key.encode() if isinstance(key, str) else key)
    except Exception as e:
        raise EncryptionError(f"Invalid SEEKNAL_ENCRYPT_KEY: {e}")


def encrypt_value(plaintext: str) -> str:
    """Encrypt a string value.

    Args:
        plaintext: The value to encrypt.

    Returns:
        Base64-encoded encrypted value.

    Raises:
        EncryptionError: If SEEKNAL_ENCRYPT_KEY is not set or invalid.
    """
    if not plaintext:
        return ""
    fernet = _get_fernet()
    return fernet.encrypt(plaintext.encode()).decode("utf-8")


def decrypt_value(ciphertext: str) -> str:
    """Decrypt a string value.

    Args:
        ciphertext: Base64-encoded encrypted value.

    Returns:
        Decrypted plaintext.

    Raises:
        EncryptionError: If SEEKNAL_ENCRYPT_KEY is not set, invalid, or
                        ciphertext is corrupted.
    """
    if not ciphertext:
        return ""
    fernet = _get_fernet()
    try:
        return fernet.decrypt(ciphertext.encode()).decode("utf-8")
    except InvalidToken:
        raise EncryptionError("Failed to decrypt: invalid key or corrupted data")


def is_encryption_configured() -> bool:
    """Check if encryption is properly configured."""
    return bool(os.environ.get("SEEKNAL_ENCRYPT_KEY"))
