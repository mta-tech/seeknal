from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from seeknal.ask.gateway.pairing import PublicSessionStore, TelegramLinkStore
from seeknal.ask.gateway.channels.telegram import TelegramChannel
from seeknal.ask.gateway.tenant import DEFAULT_TENANT


@pytest.mark.asyncio
async def test_handle_pair_redeems_code_and_links_chat(tmp_path):
    channel = TelegramChannel(Path(tmp_path), token="token")
    pairing_store = AsyncMock()
    pairing_store.redeem_pair_code.return_value = SimpleNamespace(
        code="calm-river-315",
        session_id="existing-session",
    )
    channel.set_pairing_store(pairing_store)
    link_store = TelegramLinkStore(tmp_path)
    channel.set_link_store(link_store)

    update = SimpleNamespace(
        effective_chat=SimpleNamespace(id=12345),
        message=SimpleNamespace(reply_text=AsyncMock()),
    )
    context = SimpleNamespace(args=["calm-river-315"])

    await channel._handle_pair(update, context)

    pairing_store.redeem_pair_code.assert_awaited_once_with(
        "calm-river-315",
        tenant_id=DEFAULT_TENANT,
    )
    assert link_store.get_session_id("12345") == "existing-session"
    update.message.reply_text.assert_awaited_once()
    assert "existing-session" in update.message.reply_text.await_args.args[0]


@pytest.mark.asyncio
async def test_handle_pair_reports_when_pairing_store_missing(tmp_path):
    channel = TelegramChannel(Path(tmp_path), token="token")
    update = SimpleNamespace(
        effective_chat=SimpleNamespace(id=12345),
        message=SimpleNamespace(reply_text=AsyncMock()),
    )

    await channel._handle_pair(update, None)

    update.message.reply_text.assert_awaited_once()
    assert "not available" in update.message.reply_text.await_args.args[0].lower()


@pytest.mark.asyncio
async def test_handle_pair_requires_code_argument(tmp_path):
    channel = TelegramChannel(Path(tmp_path), token="token")
    channel.set_pairing_store(AsyncMock())
    channel.set_link_store(TelegramLinkStore(tmp_path))
    update = SimpleNamespace(
        effective_chat=SimpleNamespace(id=12345),
        message=SimpleNamespace(reply_text=AsyncMock()),
    )

    await channel._handle_pair(update, SimpleNamespace(args=[]))

    update.message.reply_text.assert_awaited_once()
    assert "ask the admin" in update.message.reply_text.await_args.args[0].lower()


@pytest.mark.asyncio
async def test_handle_message_uses_linked_session_id(tmp_path):
    channel = TelegramChannel(Path(tmp_path), token="token")
    link_store = TelegramLinkStore(tmp_path)
    link_store.link_chat("12345", "existing-session")
    channel.set_link_store(link_store)
    channel._run_agent = AsyncMock(return_value="done")

    update = SimpleNamespace(
        effective_chat=SimpleNamespace(id=12345, send_action=AsyncMock()),
        message=SimpleNamespace(
            text="hello",
            reply_text=AsyncMock(return_value=SimpleNamespace(delete=AsyncMock(), edit_text=AsyncMock())),
        ),
    )

    await channel._handle_message(update, None)

    channel._run_agent.assert_awaited()
    assert channel._run_agent.await_args.args[0] == "existing-session"


@pytest.mark.asyncio
async def test_handle_message_requires_pairing_first(tmp_path):
    channel = TelegramChannel(Path(tmp_path), token="token")
    channel.set_link_store(TelegramLinkStore(tmp_path))
    channel._run_agent = AsyncMock(return_value="done")

    update = SimpleNamespace(
        effective_chat=SimpleNamespace(id=12345, send_action=AsyncMock()),
        message=SimpleNamespace(
            text="hello",
            reply_text=AsyncMock(),
        ),
    )

    await channel._handle_message(update, None)

    channel._run_agent.assert_not_awaited()
    update.effective_chat.send_action.assert_not_awaited()
    update.message.reply_text.assert_awaited_once()
    assert "not paired yet" in update.message.reply_text.await_args.args[0].lower()


@pytest.mark.asyncio
async def test_handle_message_uses_public_session_when_configured(tmp_path):
    channel = TelegramChannel(Path(tmp_path), token="token")
    channel.set_link_store(TelegramLinkStore(tmp_path))
    public_store = PublicSessionStore(tmp_path)
    public_store.set_session("public-session")
    channel.set_public_session_store(public_store)
    channel._run_agent = AsyncMock(return_value="done")

    update = SimpleNamespace(
        effective_chat=SimpleNamespace(id=12345, send_action=AsyncMock()),
        message=SimpleNamespace(
            text="hello",
            reply_text=AsyncMock(return_value=SimpleNamespace(delete=AsyncMock(), edit_text=AsyncMock())),
        ),
    )

    await channel._handle_message(update, None)

    channel._run_agent.assert_awaited()
    assert channel._run_agent.await_args.args[0] == "public-session"
