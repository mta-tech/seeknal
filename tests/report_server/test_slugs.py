from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import IntegrityError

from seeknal.report_server.slugs import SlugExhaustedError, generate_unique


def _make_session(side_effects=None) -> MagicMock:
    session = MagicMock()
    session.get.return_value = None
    return session


def test_generates_6_char_base32() -> None:
    session = _make_session()
    slug = generate_unique(session)
    assert len(slug) == 6, f"Expected 6 chars, got {len(slug)!r}"
    assert slug.isalnum(), f"Expected alphanumeric slug, got {slug!r}"
    assert slug == slug.lower(), f"Expected lowercase slug, got {slug!r}"


def test_unique_retry_on_collision() -> None:
    session = MagicMock()
    existing = MagicMock()
    # First 4 probes return a collision (existing row), 5th returns None (free slot)
    session.get.side_effect = [existing, existing, existing, existing, None]

    slug = generate_unique(session)

    assert len(slug) == 6
    assert session.get.call_count == 5


def test_raises_after_5_collisions() -> None:
    session = MagicMock()
    existing = MagicMock()
    # All 5 probes find a collision
    session.get.side_effect = [existing] * 5

    with pytest.raises(SlugExhaustedError):
        generate_unique(session)
