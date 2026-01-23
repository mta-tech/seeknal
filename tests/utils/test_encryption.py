"""Tests for seeknal.utils.encryption module."""
import pytest
import os
from unittest.mock import patch
from cryptography.fernet import Fernet

from seeknal.utils.encryption import (
    encrypt_value,
    decrypt_value,
    is_encryption_configured,
    EncryptionError,
)


@pytest.fixture
def valid_key():
    """Generate a valid Fernet key for testing."""
    return Fernet.generate_key().decode()


def test_encrypt_decrypt_roundtrip(valid_key, monkeypatch):
    """Values can be encrypted and decrypted."""
    monkeypatch.setenv("SEEKNAL_ENCRYPT_KEY", valid_key)

    plaintext = "my-secret-password"
    ciphertext = encrypt_value(plaintext)

    assert ciphertext != plaintext
    assert decrypt_value(ciphertext) == plaintext


def test_encrypt_empty_string_returns_empty(valid_key, monkeypatch):
    """Empty strings return empty without encryption."""
    monkeypatch.setenv("SEEKNAL_ENCRYPT_KEY", valid_key)
    assert encrypt_value("") == ""
    assert decrypt_value("") == ""


def test_encrypt_without_key_raises():
    """Encryption fails if SEEKNAL_ENCRYPT_KEY not set."""
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(EncryptionError, match="SEEKNAL_ENCRYPT_KEY not set"):
            encrypt_value("secret")


def test_decrypt_with_wrong_key_raises(valid_key, monkeypatch):
    """Decryption fails with wrong key."""
    monkeypatch.setenv("SEEKNAL_ENCRYPT_KEY", valid_key)
    ciphertext = encrypt_value("secret")

    # Change to different key
    new_key = Fernet.generate_key().decode()
    monkeypatch.setenv("SEEKNAL_ENCRYPT_KEY", new_key)

    with pytest.raises(EncryptionError, match="invalid key or corrupted"):
        decrypt_value(ciphertext)


def test_is_encryption_configured(valid_key, monkeypatch):
    """is_encryption_configured returns correct status."""
    monkeypatch.delenv("SEEKNAL_ENCRYPT_KEY", raising=False)
    assert not is_encryption_configured()

    monkeypatch.setenv("SEEKNAL_ENCRYPT_KEY", valid_key)
    assert is_encryption_configured()


def test_invalid_key_raises():
    """Invalid key format raises EncryptionError."""
    with patch.dict(os.environ, {"SEEKNAL_ENCRYPT_KEY": "not-a-valid-key"}):
        with pytest.raises(EncryptionError, match="Invalid SEEKNAL_ENCRYPT_KEY"):
            encrypt_value("secret")
