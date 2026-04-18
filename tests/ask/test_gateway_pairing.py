from __future__ import annotations

import asyncio
import re
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

from starlette.testclient import TestClient

from seeknal.ask.gateway.pairing import (
    FilePairingStore,
    PairCodeExpiredError,
    PairCodeUsedError,
    PublicSessionStore,
    TelegramLinkStore,
    normalize_pair_code,
)
from seeknal.ask.gateway.server import create_gateway_app


def test_normalize_pair_code_accepts_spaces_and_case():
    assert normalize_pair_code(" Calm River 315 ") == "calm-river-315"
    assert normalize_pair_code("CALM_river_315") == "calm-river-315"


def test_file_pairing_store_creates_human_readable_code(tmp_path):
    store = FilePairingStore(tmp_path)

    record = asyncio.run(store.create_pair_code("telegram-123"))

    assert re.fullmatch(r"[a-z]+-[a-z]+-\d{3}", record.code)
    assert record.session_id == "telegram-123"
    redeemed = asyncio.run(store.redeem_pair_code(record.code))
    assert redeemed.session_id == "telegram-123"
    assert redeemed.used_at is not None


def test_file_pairing_store_rejects_expired_and_used_codes(tmp_path):
    store = FilePairingStore(tmp_path)
    record = asyncio.run(store.create_pair_code("telegram-123"))

    asyncio.run(store.redeem_pair_code(record.code))
    try:
        asyncio.run(store.redeem_pair_code(record.code))
    except PairCodeUsedError:
        pass
    else:
        raise AssertionError("Expected used code redemption to fail")

    fresh = asyncio.run(store.create_pair_code("telegram-456"))
    path = store._record_path(fresh.tenant_id, fresh.code)  # noqa: SLF001
    path.write_text(
        store._serialize(replace(  # noqa: SLF001
            fresh,
            expires_at=fresh.created_at - timedelta(seconds=1),
        )),
        encoding="utf-8",
    )
    try:
        asyncio.run(store.redeem_pair_code(fresh.code))
    except PairCodeExpiredError:
        pass
    else:
        raise AssertionError("Expected expired code redemption to fail")


def test_telegram_link_store_persists_mapping(tmp_path):
    store = TelegramLinkStore(tmp_path)
    assert store.get_session_id("123") is None

    store.link_chat("123", "existing-session")

    assert store.get_session_id("123") == "existing-session"


def test_telegram_link_store_lists_links(tmp_path):
    store = TelegramLinkStore(tmp_path)
    store.link_chat("123", "session-a")
    store.link_chat("456", "session-b")

    all_links = store.list_links()
    assert len(all_links) == 2
    assert {item["chat_id"] for item in all_links} == {"123", "456"}

    session_a_links = store.list_links(session_id="session-a")
    assert len(session_a_links) == 1
    assert session_a_links[0]["chat_id"] == "123"


def test_public_session_store_round_trip(tmp_path):
    store = PublicSessionStore(tmp_path)
    assert store.get_session_id() is None

    store.set_session("public-session")
    assert store.get_session_id() == "public-session"
    assert store.clear() is True
    assert store.get_session_id() is None


def test_gateway_app_initializes_telegram_link_store(tmp_path):
    app = create_gateway_app(tmp_path)
    with TestClient(app) as client:
        assert client.app.state.telegram_link_store is not None
        assert client.app.state.public_session_store is not None


def test_chat_ui_no_longer_contains_browser_pairing_controls():
    html = Path("src/seeknal/ask/gateway/static/chat.html").read_text(encoding="utf-8")

    assert 'id="btn-pair-session"' not in html
    assert 'id="pair-code"' not in html
    assert "/pair/redeem" not in html
