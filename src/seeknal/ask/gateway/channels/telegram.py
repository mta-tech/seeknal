"""Telegram channel for seeknal ask gateway.

Receives messages from Telegram users, runs the seeknal ask agent,
and streams responses back. Uses pydantic-ai's agent.iter() for
step-by-step event streaming.

Requires: python-telegram-bot>=21.0
Configure: TELEGRAM_BOT_TOKEN environment variable
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Telegram message size limit
_MAX_MESSAGE_LENGTH = 4096

# Telegram formatting instruction prepended to user messages
_TELEGRAM_FORMAT_HINT = (
    "[Format: plain text for Telegram. No markdown syntax. "
    "Use dashes for bullets. Write in normal sentence case. "
    "No **bold**, *italic*, ```code``` blocks, or ALL CAPS.]\n"
    "[Context: This is a Telegram channel. When the user asks to add, edit, "
    "or modify content in a previously published Proof document, use "
    "read_proof_document to fetch it, then edit_proof_document to update it. "
    "Do not create seeknal pipeline nodes unless explicitly asked.]\n"
    "[After answering, always suggest 2-3 short follow-up questions the user "
    "could ask next, based on your answer. Write them as a simple list.]\n\n"
)


def _strip_markdown(text: str) -> str:
    """Convert markdown to plain text suitable for Telegram."""
    # Remove code fences
    text = re.sub(r"```\w*\n?", "", text)
    # Convert **bold** and __bold__ to UPPERCASE or just strip
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
    text = re.sub(r"__(.+?)__", r"\1", text)
    # Convert *italic* and _italic_ to plain
    text = re.sub(r"\*(.+?)\*", r"\1", text)
    text = re.sub(r"(?<!\w)_(.+?)_(?!\w)", r"\1", text)
    # Strip heading markers
    text = re.sub(r"^#{1,6}\s+", "", text, flags=re.MULTILINE)
    return text.strip()


def _split_message(text: str, max_len: int = _MAX_MESSAGE_LENGTH) -> list[str]:
    """Split long text into Telegram-safe chunks."""
    if len(text) <= max_len:
        return [text]
    chunks = []
    while text:
        if len(text) <= max_len:
            chunks.append(text)
            break
        # Try to split at newline
        idx = text.rfind("\n", 0, max_len)
        if idx == -1:
            idx = max_len
        chunks.append(text[:idx])
        text = text[idx:].lstrip("\n")
    return chunks


class TelegramChannel:
    """Telegram bot integration for seeknal ask.

    Uses manual lifecycle management (no run_polling) so the gateway
    server controls startup and shutdown.
    """

    def __init__(self, project_path: Path, token: str | None = None) -> None:
        self._project_path = project_path
        self._token = token or os.environ.get("TELEGRAM_BOT_TOKEN", "")
        self._app: Any = None

    async def start(self) -> None:
        """Initialize the Telegram bot application."""
        if not self._token:
            raise ValueError(
                "TELEGRAM_BOT_TOKEN not set. "
                "Set it in your environment or pass token= to TelegramChannel."
            )

        from telegram.ext import (
            ApplicationBuilder,
            CommandHandler,
            MessageHandler,
            filters,
        )

        self._app = (
            ApplicationBuilder()
            .token(self._token)
            .build()
        )

        self._app.add_handler(CommandHandler("start", self._handle_start))
        self._app.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, self._handle_message)
        )

        await self._app.initialize()
        await self._app.start()
        await self._app.updater.start_polling()
        logger.info("Telegram channel started")

    async def stop(self) -> None:
        """Shut down the Telegram bot."""
        if self._app:
            if self._app.updater and self._app.updater.running:
                await self._app.updater.stop()
            await self._app.stop()
            await self._app.shutdown()
            logger.info("Telegram channel stopped")

    async def _handle_start(self, update: Any, context: Any) -> None:
        """Handle /start command — LLM-generated welcome respecting SEEKNAL_ASK.md."""
        chat_id = str(update.effective_chat.id)
        session_id = f"telegram-start-{chat_id}"
        logger.info("[telegram] /start from chat_id=%s", chat_id)

        try:
            answer = await self._run_agent(
                session_id,
                "A colleague just opened this chat. Write: "
                "1) A warm greeting (1 sentence, like a helpful coworker). "
                "2) A brief intro of what you can help with based on your project "
                "context (1-2 sentences, mention key capabilities naturally). "
                "3) Mention that analysis results can be published and shared as "
                "a memo link by saying 'publish ke Proof' or 'publish to Proof'. "
                "4) Then suggest 3 example questions as a bulleted list. "
                "Do not state your system name or role title. "
                "Match the project's language (if project context "
                "is in Indonesian, write in Indonesian). Preserve brand name casing.",
                update,
            )
            if answer and len(answer.strip()) > 20:
                clean = _strip_markdown(answer)
                for chunk in _split_message(clean):
                    await update.message.reply_text(chunk)
                return
            logger.warning("LLM welcome was empty or too short: %r", answer)
        except Exception:
            logger.exception("Failed to generate welcome via LLM")

        # Fallback if LLM fails
        await update.message.reply_text(
            "Hi! Send me a question and I'll analyze it for you."
        )

    async def _handle_message(self, update: Any, context: Any) -> None:
        """Handle incoming text messages — run agent and stream response."""
        from telegram.constants import ChatAction

        question = update.message.text.strip()
        if not question:
            return

        chat_id = str(update.effective_chat.id)
        session_id = f"telegram-{chat_id}"
        logger.info("[telegram] message from chat_id=%s: %s", chat_id, question[:80])

        # Show typing indicator and a single status message (edited in-place)
        await update.effective_chat.send_action(ChatAction.TYPING)
        status_msg = await update.message.reply_text("🔍 Analyzing...")

        try:
            logger.info("[telegram] running agent for session=%s", session_id)
            answer = await self._run_agent(session_id, question, update, status_msg)
            logger.info("[telegram] agent done for session=%s, answer_len=%d",
                        session_id, len(answer) if answer else 0)

            # Delete status message
            try:
                await status_msg.delete()
            except Exception:
                pass

            # Send answer in chunks
            if answer:
                clean = _strip_markdown(answer)
                for chunk in _split_message(clean):
                    await update.message.reply_text(chunk)
                logger.info("[telegram] reply sent to chat_id=%s", chat_id)
            else:
                await update.message.reply_text(
                    "I couldn't generate a response. Please try rephrasing."
                )
                logger.warning("[telegram] empty answer for chat_id=%s", chat_id)

        except Exception as e:
            logger.exception("[telegram] error processing message from chat_id=%s", chat_id)
            try:
                await status_msg.edit_text(f"❌ Error: {e}")
            except Exception:
                await update.message.reply_text(f"❌ Error: {e}")

    # Max tool calls before forcing early return with accumulated text
    _MAX_TOOLS = 30

    async def _run_agent(
        self, session_id: str, question: str, update: Any,
        status_msg: Any = None,
    ) -> str:
        """Run the seeknal ask agent and return the answer."""
        from seeknal.ask.gateway.server import _run_agent_streaming

        text_parts: list[str] = []
        tool_count = 0
        answer: str | None = None

        formatted_question = _TELEGRAM_FORMAT_HINT + question
        logger.info("[telegram] agent stream starting session=%s", session_id)
        stream = _run_agent_streaming(
            self._project_path, session_id, formatted_question,
            auto_approve=True,
        )
        try:
            async for event in stream:
                etype = event["type"]
                if etype == "token":
                    text_parts.append(event["data"])
                elif etype == "tool_start":
                    tool_count += 1
                    tool_name = event["data"]["name"]
                    logger.info("[telegram] tool #%d: %s (session=%s)",
                                tool_count, tool_name, session_id)
                    # Refresh typing indicator
                    try:
                        from telegram.constants import ChatAction
                        await update.effective_chat.send_action(ChatAction.TYPING)
                    except Exception:
                        pass
                    # Edit status message in-place (no new messages)
                    if status_msg and tool_count % 5 == 1:
                        try:
                            await status_msg.edit_text(
                                f"🔍 Analyzing... ({tool_count} steps)"
                            )
                        except Exception:
                            pass
                    # Safety: break if too many tools
                    if tool_count >= self._MAX_TOOLS:
                        logger.warning(
                            "[telegram] tool limit reached (%d), returning partial",
                            self._MAX_TOOLS)
                        break
                elif etype == "tool_end":
                    logger.info("[telegram] tool done (session=%s, total=%d)",
                                session_id, tool_count)
                elif etype == "answer":
                    logger.info("[telegram] answer received (session=%s, len=%d)",
                                session_id, len(event["data"]))
                    answer = event["data"]
                    break
        finally:
            try:
                await stream.aclose()
            except (RuntimeError, GeneratorExit):
                pass  # upstream generator doesn't handle cleanup gracefully

        if answer is not None:
            return answer
        return "".join(text_parts) if text_parts else ""

    async def deliver(self, session_id: str, question: str) -> str:
        """Deliver a question programmatically (for gateway integration)."""
        from seeknal.ask.gateway.server import _run_agent_streaming

        text_parts: list[str] = []
        answer: str | None = None
        formatted_question = _TELEGRAM_FORMAT_HINT + question
        stream = _run_agent_streaming(
            self._project_path, session_id, formatted_question,
            auto_approve=True,
        )
        try:
            async for event in stream:
                if event["type"] == "answer":
                    answer = event["data"]
                    break
                elif event["type"] == "token":
                    text_parts.append(event["data"])
        finally:
            try:
                await stream.aclose()
            except (RuntimeError, GeneratorExit):
                pass

        if answer is not None:
            return answer
        return "".join(text_parts)
