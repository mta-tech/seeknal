from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

# Skip only in unusually minimal editable environments without python-telegram-bot.
# The handler methods under test call `from telegram.constants import ChatAction`
# at runtime, so without the extra the tests fail mid-execution.
pytest.importorskip("telegram")

from seeknal.ask.gateway.pairing import PublicSessionStore, TelegramLinkStore
from seeknal.ask.gateway.channels.telegram import TelegramChannel
from seeknal.ask.gateway.tenant import DEFAULT_TENANT


@pytest.mark.asyncio
async def test_start_uses_polling_error_callback(monkeypatch, tmp_path):
    from telegram.ext import ApplicationBuilder

    captured: dict[str, object] = {}

    class FakeUpdater:
        running = True

        async def start_polling(self, **kwargs):
            captured.update(kwargs)

        async def stop(self):
            self.running = False

    class FakeApp:
        def __init__(self):
            self.updater = FakeUpdater()
            self.handlers = []

        def add_handler(self, handler):
            self.handlers.append(handler)

        async def initialize(self):
            return None

        async def start(self):
            return None

        async def stop(self):
            return None

        async def shutdown(self):
            return None

    class FakeBuilder:
        def token(self, token):
            captured["token"] = token
            return self

        def build(self):
            return FakeApp()

    monkeypatch.setattr(ApplicationBuilder, "__new__", lambda cls: FakeBuilder())

    channel = TelegramChannel(Path(tmp_path), token="token")
    await channel.start()

    assert captured["token"] == "token"
    assert captured["error_callback"] == channel._handle_polling_error
    await channel.stop()


def test_polling_conflict_logs_concise_warning(caplog, tmp_path):
    from telegram.error import Conflict

    channel = TelegramChannel(Path(tmp_path), token="token")

    with caplog.at_level("WARNING"):
        channel._handle_polling_error(Conflict("terminated by other getUpdates request"))

    assert "Telegram polling conflict" in caplog.text
    assert "Traceback" not in caplog.text


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


# ---------------------------------------------------------------------------
# Document upload: suffix categories + ZIP extraction safety
# ---------------------------------------------------------------------------


def test_upload_suffixes_accept_png_pdf_txt_zip():
    """png/pdf/txt/zip must be accepted document types (not rejected)."""
    from seeknal.ask.gateway.channels.telegram import (
        _ARCHIVE_SUFFIXES,
        _TABULAR_SUFFIXES,
        _UPLOAD_ALLOWED_SUFFIXES,
        _VISION_DOC_SUFFIXES,
    )

    for ext in (".png", ".pdf", ".txt", ".zip"):
        assert ext in _UPLOAD_ALLOWED_SUFFIXES, f"{ext} should be accepted"
    # Categories are disjoint — a suffix routes to exactly one path.
    assert not (_TABULAR_SUFFIXES & _VISION_DOC_SUFFIXES)
    assert not (_TABULAR_SUFFIXES & _ARCHIVE_SUFFIXES)
    assert not (_VISION_DOC_SUFFIXES & _ARCHIVE_SUFFIXES)
    # The union is exactly the allow-list.
    assert (
        _TABULAR_SUFFIXES | _VISION_DOC_SUFFIXES | _ARCHIVE_SUFFIXES
    ) == _UPLOAD_ALLOWED_SUFFIXES
    # png/pdf/txt route to the vision path; zip is its own category.
    assert {".png", ".pdf", ".txt"} <= _VISION_DOC_SUFFIXES
    assert ".zip" in _ARCHIVE_SUFFIXES


def test_extract_archive_valid_zip(tmp_path):
    """A normal ZIP extracts to a subdir and lists its members."""
    import zipfile

    channel = TelegramChannel(Path(tmp_path), token="token")
    staging = tmp_path / "staging"
    staging.mkdir()
    zip_path = staging / "batch.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("a.csv", "x,y\n1,2\n")
        zf.writestr("nested/b.txt", "hello")

    extract_dir, members, err = channel._extract_archive(zip_path, staging)

    assert err is None
    assert extract_dir is not None and extract_dir.is_dir()
    assert sorted(members) == ["a.csv", "nested/b.txt"]
    assert (extract_dir / "a.csv").read_text() == "x,y\n1,2\n"
    assert (extract_dir / "nested" / "b.txt").read_text() == "hello"


def test_extract_archive_rejects_zip_slip(tmp_path):
    """A member with a ../ path that escapes the extract dir aborts extraction."""
    import zipfile

    channel = TelegramChannel(Path(tmp_path), token="token")
    staging = tmp_path / "staging"
    staging.mkdir()
    zip_path = staging / "evil.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("../../escape.txt", "pwned")

    extract_dir, members, err = channel._extract_archive(zip_path, staging)

    assert extract_dir is None
    assert members == []
    assert err is not None and "escape" in err.lower()


def test_extract_archive_rejects_too_many_entries(tmp_path):
    """An archive over the entry-count cap is refused."""
    import zipfile

    from seeknal.ask.gateway.channels.telegram import _ARCHIVE_MAX_ENTRIES

    channel = TelegramChannel(Path(tmp_path), token="token")
    staging = tmp_path / "staging"
    staging.mkdir()
    zip_path = staging / "bomb.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        for i in range(_ARCHIVE_MAX_ENTRIES + 1):
            zf.writestr(f"f{i}.csv", "a\n1\n")

    extract_dir, members, err = channel._extract_archive(zip_path, staging)

    assert extract_dir is None
    assert members == []
    assert err is not None and str(_ARCHIVE_MAX_ENTRIES) in err


def test_extract_archive_rejects_bad_zip(tmp_path):
    """A non-ZIP file produces a clean error, not a crash."""
    channel = TelegramChannel(Path(tmp_path), token="token")
    staging = tmp_path / "staging"
    staging.mkdir()
    not_a_zip = staging / "fake.zip"
    not_a_zip.write_text("this is plainly not a zip archive")

    extract_dir, members, err = channel._extract_archive(not_a_zip, staging)

    assert extract_dir is None
    assert members == []
    assert err is not None and "zip" in err.lower()
