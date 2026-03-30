"""Tests for seeknal.ask.gateway.channels.telegram — Telegram channel plugin."""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch


# ---------------------------------------------------------------------------
# Helper to run async tests without pytest-asyncio dependency
# ---------------------------------------------------------------------------


def _run(coro):
    """Run an async coroutine synchronously."""
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Channel protocol conformance
# ---------------------------------------------------------------------------


class TestChannelProtocol:
    """Verify TelegramChannel satisfies the Channel protocol."""

    def test_conforms_to_channel_protocol(self):
        from seeknal.ask.gateway.channels.base import Channel
        from seeknal.ask.gateway.channels.telegram import TelegramChannel

        channel = TelegramChannel(
            bot_token="fake-token",
            project_path=Path("/tmp/test"),
        )
        assert isinstance(channel, Channel)

    def test_has_required_methods(self):
        from seeknal.ask.gateway.channels.telegram import TelegramChannel

        channel = TelegramChannel(
            bot_token="fake-token",
            project_path=Path("/tmp/test"),
        )
        assert hasattr(channel, "start")
        assert hasattr(channel, "stop")
        assert hasattr(channel, "deliver")
        assert callable(channel.start)
        assert callable(channel.stop)
        assert callable(channel.deliver)


# ---------------------------------------------------------------------------
# Message splitting
# ---------------------------------------------------------------------------


class TestStripMarkdown:
    """Test _strip_markdown converts markdown to plain text."""

    def test_removes_bold(self):
        from seeknal.ask.gateway.channels.telegram import _strip_markdown

        assert _strip_markdown("**bold text**") == "bold text"

    def test_removes_headers(self):
        from seeknal.ask.gateway.channels.telegram import _strip_markdown

        assert _strip_markdown("### Header\nContent") == "Header\nContent"

    def test_removes_code_fences(self):
        from seeknal.ask.gateway.channels.telegram import _strip_markdown

        result = _strip_markdown("```sql\nSELECT 1\n```")
        assert "```" not in result
        assert "SELECT 1" in result

    def test_removes_inline_code(self):
        from seeknal.ask.gateway.channels.telegram import _strip_markdown

        assert _strip_markdown("use `SELECT *`") == "use SELECT *"

    def test_converts_bullets(self):
        from seeknal.ask.gateway.channels.telegram import _strip_markdown

        result = _strip_markdown("- item one\n- item two")
        assert "• item one" in result
        assert "• item two" in result

    def test_converts_links(self):
        from seeknal.ask.gateway.channels.telegram import _strip_markdown

        result = _strip_markdown("[click here](https://example.com)")
        assert result == "click here (https://example.com)"

    def test_plain_text_unchanged(self):
        from seeknal.ask.gateway.channels.telegram import _strip_markdown

        text = "This is plain text with no formatting."
        assert _strip_markdown(text) == text

    def test_preserves_underscores_in_identifiers(self):
        from seeknal.ask.gateway.channels.telegram import _strip_markdown

        text = "transform_enriched_orders and source_customers"
        assert _strip_markdown(text) == text


class TestSplitMessage:
    """Test _split_message for Telegram's 4096-char limit."""

    def test_short_message_not_split(self):
        from seeknal.ask.gateway.channels.telegram import _split_message

        result = _split_message("hello world")
        assert result == ["hello world"]

    def test_exact_limit_not_split(self):
        from seeknal.ask.gateway.channels.telegram import _split_message

        text = "a" * 4096
        result = _split_message(text)
        assert result == [text]

    def test_long_message_split_at_newline(self):
        from seeknal.ask.gateway.channels.telegram import _split_message

        # Create text with newlines — should split at the last newline before limit
        line = "x" * 100 + "\n"
        text = line * 50  # 50 * 101 = 5050 chars
        result = _split_message(text, limit=4096)
        assert len(result) >= 2
        for chunk in result:
            assert len(chunk) <= 4096

    def test_long_message_no_newline_hard_split(self):
        from seeknal.ask.gateway.channels.telegram import _split_message

        text = "a" * 5000
        result = _split_message(text, limit=4096)
        assert len(result) == 2
        assert len(result[0]) == 4096
        assert len(result[1]) == 904

    def test_split_preserves_content(self):
        from seeknal.ask.gateway.channels.telegram import _split_message

        text = "a" * 8192
        result = _split_message(text, limit=4096)
        reassembled = "".join(result)
        assert reassembled == text

    def test_empty_message(self):
        from seeknal.ask.gateway.channels.telegram import _split_message

        result = _split_message("")
        assert result == [""]

    def test_custom_limit(self):
        from seeknal.ask.gateway.channels.telegram import _split_message

        text = "hello\nworld\nfoo\nbar"
        result = _split_message(text, limit=12)
        assert len(result) >= 2
        for chunk in result:
            assert len(chunk) <= 12


# ---------------------------------------------------------------------------
# Session ID resolution
# ---------------------------------------------------------------------------


class TestSessionResolution:
    """Test that sessions are resolved as telegram:{user_id}."""

    def test_session_id_format(self):
        """Verify the session ID is constructed as telegram:{user_id}."""
        user_id = 123456
        session_id = f"telegram:{user_id}"
        assert session_id == "telegram:123456"

    def test_handle_message_uses_user_id_for_session(self):
        """When a message arrives, session_id should be telegram:{user_id}."""
        from seeknal.ask.gateway.channels.telegram import TelegramChannel

        channel = TelegramChannel(
            bot_token="fake-token",
            project_path=Path("/tmp/test"),
        )

        # Track what session_id is passed to _run_agent
        captured_session_id = None

        async def mock_run_agent(session_id, question, chat=None):
            nonlocal captured_session_id
            captured_session_id = session_id
            return "test response"

        channel._run_agent = mock_run_agent

        # Build mock update
        update = MagicMock()
        update.message.text = "hello"
        update.effective_user.id = 42
        update.effective_chat.id = 42
        update.message.chat.send_action = AsyncMock()
        update.message.reply_text = AsyncMock()

        ctx = MagicMock()
        _run(channel._handle_message(update, ctx))

        assert captured_session_id == "telegram:42"


# ---------------------------------------------------------------------------
# /start command
# ---------------------------------------------------------------------------


class TestStartCommand:
    """Test the /start command handler."""

    def test_start_command_sends_welcome(self):
        from seeknal.ask.gateway.channels.telegram import TelegramChannel

        channel = TelegramChannel(
            bot_token="fake-token",
            project_path=Path("/tmp/test"),
        )

        update = MagicMock()
        update.message.reply_text = AsyncMock()

        ctx = MagicMock()
        _run(channel._handle_start(update, ctx))

        update.message.reply_text.assert_called_once()
        welcome_text = update.message.reply_text.call_args[0][0]
        assert "Seeknal Ask" in welcome_text
        assert "data" in welcome_text.lower()

    def test_start_command_ignores_none_message(self):
        from seeknal.ask.gateway.channels.telegram import TelegramChannel

        channel = TelegramChannel(
            bot_token="fake-token",
            project_path=Path("/tmp/test"),
        )

        update = MagicMock()
        update.message = None

        ctx = MagicMock()
        _run(channel._handle_start(update, ctx))
        # Should not raise


# ---------------------------------------------------------------------------
# Handle message — typing and reply
# ---------------------------------------------------------------------------


class TestHandleMessage:
    """Test _handle_message sends typing and replies."""

    def test_sends_typing_action(self):
        from seeknal.ask.gateway.channels.telegram import TelegramChannel

        channel = TelegramChannel(
            bot_token="fake-token",
            project_path=Path("/tmp/test"),
        )

        async def mock_run_agent(session_id, question, chat=None):
            return "analysis result"

        channel._run_agent = mock_run_agent

        update = MagicMock()
        update.message.text = "what is the avg order value?"
        update.effective_user.id = 99
        update.effective_chat.id = 99
        update.message.chat.send_action = AsyncMock()
        update.message.reply_text = AsyncMock()

        ctx = MagicMock()
        _run(channel._handle_message(update, ctx))

        update.message.chat.send_action.assert_called_with("typing")

    def test_sends_reply(self):
        from seeknal.ask.gateway.channels.telegram import TelegramChannel

        channel = TelegramChannel(
            bot_token="fake-token",
            project_path=Path("/tmp/test"),
        )

        async def mock_run_agent(session_id, question, chat=None):
            return "The average order value is $42.50"

        channel._run_agent = mock_run_agent

        update = MagicMock()
        update.message.text = "avg order value?"
        update.effective_user.id = 77
        update.effective_chat.id = 77
        update.message.chat.send_action = AsyncMock()
        update.message.reply_text = AsyncMock()

        ctx = MagicMock()
        _run(channel._handle_message(update, ctx))

        update.message.reply_text.assert_called_once_with(
            "The average order value is $42.50"
        )

    def test_splits_long_reply(self):
        from seeknal.ask.gateway.channels.telegram import TelegramChannel

        channel = TelegramChannel(
            bot_token="fake-token",
            project_path=Path("/tmp/test"),
        )

        long_answer = "x" * 5000

        async def mock_run_agent(session_id, question, chat=None):
            return long_answer

        channel._run_agent = mock_run_agent

        update = MagicMock()
        update.message.text = "give me a long answer"
        update.effective_user.id = 55
        update.effective_chat.id = 55
        update.message.chat.send_action = AsyncMock()
        update.message.reply_text = AsyncMock()

        ctx = MagicMock()
        _run(channel._handle_message(update, ctx))

        assert update.message.reply_text.call_count == 2

    def test_handles_agent_error_gracefully(self):
        from seeknal.ask.gateway.channels.telegram import TelegramChannel

        channel = TelegramChannel(
            bot_token="fake-token",
            project_path=Path("/tmp/test"),
        )

        async def mock_run_agent(session_id, question, chat=None):
            raise RuntimeError("agent exploded")

        channel._run_agent = mock_run_agent

        update = MagicMock()
        update.message.text = "break things"
        update.effective_user.id = 11
        update.effective_chat.id = 11
        update.message.chat.send_action = AsyncMock()
        update.message.reply_text = AsyncMock()

        ctx = MagicMock()
        _run(channel._handle_message(update, ctx))

        update.message.reply_text.assert_called_once()
        error_text = update.message.reply_text.call_args[0][0]
        assert "sorry" in error_text.lower()

    def test_ignores_empty_message(self):
        from seeknal.ask.gateway.channels.telegram import TelegramChannel

        channel = TelegramChannel(
            bot_token="fake-token",
            project_path=Path("/tmp/test"),
        )

        update = MagicMock()
        update.message.text = "   "
        update.effective_user.id = 33
        update.effective_chat.id = 33
        update.message.chat.send_action = AsyncMock()
        update.message.reply_text = AsyncMock()

        ctx = MagicMock()
        _run(channel._handle_message(update, ctx))

        update.message.reply_text.assert_not_called()

    def test_ignores_none_message(self):
        from seeknal.ask.gateway.channels.telegram import TelegramChannel

        channel = TelegramChannel(
            bot_token="fake-token",
            project_path=Path("/tmp/test"),
        )

        update = MagicMock()
        update.message = None

        ctx = MagicMock()
        _run(channel._handle_message(update, ctx))
        # Should not raise

    def test_ignores_none_effective_user(self):
        from seeknal.ask.gateway.channels.telegram import TelegramChannel

        channel = TelegramChannel(
            bot_token="fake-token",
            project_path=Path("/tmp/test"),
        )

        update = MagicMock()
        update.message.text = "hello"
        update.effective_user = None
        update.effective_chat.id = 42
        update.message.chat.send_action = AsyncMock()
        update.message.reply_text = AsyncMock()

        ctx = MagicMock()
        _run(channel._handle_message(update, ctx))

        update.message.reply_text.assert_not_called()

    def test_ignores_none_effective_chat(self):
        from seeknal.ask.gateway.channels.telegram import TelegramChannel

        channel = TelegramChannel(
            bot_token="fake-token",
            project_path=Path("/tmp/test"),
        )

        update = MagicMock()
        update.message.text = "hello"
        update.effective_user.id = 42
        update.effective_chat = None
        update.message.chat.send_action = AsyncMock()
        update.message.reply_text = AsyncMock()

        ctx = MagicMock()
        _run(channel._handle_message(update, ctx))

        update.message.reply_text.assert_not_called()


# ---------------------------------------------------------------------------
# Deliver
# ---------------------------------------------------------------------------


class TestDeliver:
    """Test the deliver() method."""

    def test_deliver_ignores_non_telegram_session(self):
        from seeknal.ask.gateway.channels.telegram import TelegramChannel

        channel = TelegramChannel(
            bot_token="fake-token",
            project_path=Path("/tmp/test"),
        )
        channel._application = MagicMock()
        channel._application.bot.send_message = AsyncMock()

        _run(channel.deliver("websocket:abc", "hello"))

        channel._application.bot.send_message.assert_not_called()

    def test_deliver_sends_to_chat(self):
        from seeknal.ask.gateway.channels.telegram import TelegramChannel

        channel = TelegramChannel(
            bot_token="fake-token",
            project_path=Path("/tmp/test"),
        )
        channel._application = MagicMock()
        channel._application.bot.send_message = AsyncMock()

        _run(channel.deliver("telegram:12345", "hello there"))

        channel._application.bot.send_message.assert_called_once_with(
            chat_id=12345, text="hello there"
        )

    def test_deliver_noop_when_not_started(self):
        from seeknal.ask.gateway.channels.telegram import TelegramChannel

        channel = TelegramChannel(
            bot_token="fake-token",
            project_path=Path("/tmp/test"),
        )
        # _application is None (not started)
        _run(channel.deliver("telegram:12345", "hello"))
        # Should not raise


# ---------------------------------------------------------------------------
# Webhook handler
# ---------------------------------------------------------------------------


class TestWebhookHandler:
    """Test the webhook_handler Starlette route."""

    def test_webhook_returns_503_when_not_initialized(self):
        from seeknal.ask.gateway.channels.telegram import TelegramChannel

        channel = TelegramChannel(
            bot_token="fake-token",
            project_path=Path("/tmp/test"),
        )

        request = MagicMock()
        request.json = AsyncMock(return_value={"update_id": 1})

        response = _run(channel.webhook_handler(request))
        assert response.status_code == 503


# ---------------------------------------------------------------------------
# Safe chat_id parsing tests
# ---------------------------------------------------------------------------


class TestSafeChatIdParsing:
    """Test deliver() handles malformed session_id gracefully."""

    def test_deliver_handles_malformed_session_id(self):
        from seeknal.ask.gateway.channels.telegram import TelegramChannel

        channel = TelegramChannel(
            bot_token="fake-token",
            project_path=Path("/tmp/test"),
        )
        channel._application = MagicMock()
        channel._application.bot.send_message = AsyncMock()

        # Malformed: "telegram:" with no number
        _run(channel.deliver("telegram:", "hello"))
        channel._application.bot.send_message.assert_not_awaited()

    def test_deliver_handles_non_numeric_chat_id(self):
        from seeknal.ask.gateway.channels.telegram import TelegramChannel

        channel = TelegramChannel(
            bot_token="fake-token",
            project_path=Path("/tmp/test"),
        )
        channel._application = MagicMock()
        channel._application.bot.send_message = AsyncMock()

        # Malformed: non-numeric chat_id
        _run(channel.deliver("telegram:not-a-number", "hello"))
        channel._application.bot.send_message.assert_not_awaited()


# ---------------------------------------------------------------------------
# Agent timeout tests
# ---------------------------------------------------------------------------


class TestAgentTimeout:
    """Test _run_agent respects timeout."""

    def test_timeout_returns_error_message(self):
        """Verifies timeout fires and returns error within bounded time."""
        import time as time_mod

        from seeknal.ask.gateway.channels.telegram import TelegramChannel

        channel = TelegramChannel(
            bot_token="fake-token",
            project_path=Path("/tmp/test"),
        )

        async def slow_stream(*args, **kwargs):
            """Async generator that hangs."""
            await asyncio.sleep(10)
            yield  # Never reached

        async def _test():
            mock_agent = MagicMock()
            mock_agent.astream_events = slow_stream
            mock_config = {"configurable": {"thread_id": "test"}}

            with patch(
                "seeknal.ask.agents.agent.create_agent",
                return_value=(mock_agent, mock_config),
            ), patch(
                "seeknal.ask.gateway.channels.telegram._AGENT_TIMEOUT", 0.5,
            ), patch(
                "seeknal.ask.sessions.SessionStore",
            ):
                start = time_mod.monotonic()
                result = await channel._run_agent("telegram:123", "hello")
                elapsed = time_mod.monotonic() - start

                assert "timed out" in result
                assert elapsed < 3.0

        asyncio.run(_test())
