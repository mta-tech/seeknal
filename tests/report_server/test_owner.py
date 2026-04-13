from __future__ import annotations

import hmac as hmac_stdlib

from seeknal.report_server import owner


def test_mint_verify_roundtrip() -> None:
    plain, hash_hex, salt_hex = owner.mint()
    assert owner.verify(plain, hash_hex, salt_hex) is True


def test_rejects_wrong_secret() -> None:
    _, hash_hex, salt_hex = owner.mint()
    assert owner.verify("wrong_secret_value", hash_hex, salt_hex) is False


def test_mint_produces_64_char_hex() -> None:
    plain, _, _ = owner.mint()
    assert len(plain) == 64
    int(plain, 16)  # raises ValueError if not valid hex


def test_constant_time_comparison(monkeypatch) -> None:
    called_with = []

    original = hmac_stdlib.compare_digest

    def tracking_compare_digest(a, b):
        called_with.append((a, b))
        return original(a, b)

    monkeypatch.setattr(hmac_stdlib, "compare_digest", tracking_compare_digest)

    plain, hash_hex, salt_hex = owner.mint()
    owner.verify(plain, hash_hex, salt_hex)

    assert len(called_with) == 1, "hmac.compare_digest must be called exactly once per verify"
