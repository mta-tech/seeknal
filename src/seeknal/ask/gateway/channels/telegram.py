"""Telegram channel plugin for seeknal ask gateway.

Uses python-telegram-bot v21+ with manual lifecycle (no run_polling/run_webhook)
so the Starlette gateway controls startup and shutdown. Incoming updates are
routed through a Starlette webhook endpoint that calls process_update().

Streams agent events (tool calls, reasoning, answer) to the chat in real-time
using LangGraph's astream_events API.

Configuration in seeknal_agent.yml:
    gateway:
      telegram:
        token: ${TELEGRAM_BOT_TOKEN}
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
import warnings
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Telegram message length limit
_MAX_MESSAGE_LENGTH = 4096

# Seconds between typing action re-sends
_TYPING_INTERVAL = 4

# Agent execution timeout (seconds)
_AGENT_TIMEOUT = 300

# Minimum seconds between tool status messages to Telegram
_TOOL_THROTTLE_INTERVAL = 5

# Max retries when agent returns no text (Ralph Loop)
_MAX_RALPH_RETRIES = 2

# Human-readable tool name mapping
_TOOL_LABELS = {
    "execute_sql": "Querying your data",
    "execute_python": "Running analysis",
    "profile_data": "Profiling data files",
    "list_tables": "Looking at available tables",
    "describe_table": "Examining table structure",
    "draft_node": "Drafting pipeline node",
    "dry_run_draft": "Validating draft",
    "apply_draft": "Applying changes",
    "run_pipeline": "Running the pipeline",
    "inspect_output": "Checking the results",
    "plan_pipeline": "Planning the pipeline",
    "task": "Working on a sub-task",
    "generate_report": "Building report",
    "remember": "Saving to memory",
}

# Internal tools that should not produce status messages
_HIDDEN_TOOLS = {"submit_plan", "recall", "ask_user"}


def _strip_markdown(text: str) -> str:
    """Convert markdown to plain text suitable for Telegram.

    Removes markdown formatting that Telegram's default mode doesn't render,
    keeping the content readable as plain text.
    """
    # Code blocks: remove fences, keep content
    text = re.sub(r"```\w*\n?", "", text)
    # Inline code: remove backticks
    text = re.sub(r"`([^`]+)`", r"\1", text)
    # Headers: remove # prefix
    text = re.sub(r"^#{1,6}\s+", "", text, flags=re.MULTILINE)
    # Bold: remove ** and __ markers (but NOT single _ inside words)
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
    text = re.sub(r"__(.+?)__", r"\1", text)
    # Italic with *: only match *word* not mid-word asterisks
    text = re.sub(r"(?<!\w)\*([^*]+?)\*(?!\w)", r"\1", text)
    # Skip single _ italic — too many false positives with identifiers
    # like table_name, feature_group, etc.
    # Links: [text](url) → text (url)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 (\2)", text)
    # Bullet lists: - item → • item
    text = re.sub(r"^[-*]\s+", "• ", text, flags=re.MULTILINE)
    # Horizontal rules
    text = re.sub(r"^---+$", "", text, flags=re.MULTILINE)
    # Collapse multiple blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _split_message(text: str, limit: int = _MAX_MESSAGE_LENGTH) -> list[str]:
    """Split a long message into chunks that fit within the Telegram limit.

    Splits at the last newline before the limit. If no newline is found,
    splits at the limit boundary.

    Args:
        text: The full message text.
        limit: Maximum characters per chunk.

    Returns:
        List of message chunks, each <= limit characters.
    """
    if len(text) <= limit:
        return [text]

    chunks: list[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break
        # Find last newline before the limit
        split_at = remaining.rfind("\n", 0, limit)
        if split_at <= 0:
            # No newline found — hard split at limit
            split_at = limit
        chunk = remaining[:split_at]
        chunks.append(chunk)
        # Skip the newline itself when splitting at newline
        remaining = remaining[split_at:].lstrip("\n") if split_at < limit else remaining[split_at:]
    return chunks


class TelegramChannel:
    """Channel plugin that bridges Telegram Bot API to the seeknal agent.

    Lifecycle (managed by the gateway):
        1. ``start()``  — build Application, add handlers, initialize + start
        2. (gateway runs, webhook receives updates)
        3. ``stop()``   — stop + shutdown the Application

    The channel does NOT use ``run_polling()`` or ``run_webhook()`` — the
    Starlette app provides the webhook endpoint, and we call
    ``process_update()`` directly.
    """

    def __init__(
        self,
        bot_token: str,
        project_path: Path,
        gateway_app: Any = None,
    ) -> None:
        self._bot_token = bot_token
        self._project_path = project_path
        self._gateway_app = gateway_app
        self._application: Any = None  # telegram.ext.Application

    async def start(self, polling: bool = False) -> None:
        """Build the telegram Application with manual lifecycle.

        Args:
            polling: If True, start long-polling for updates instead of
                     waiting for webhook calls. Use for local development.
        """
        from telegram import Update
        from telegram.ext import (
            Application,
            CommandHandler,
            MessageHandler,
            filters,
        )

        builder = Application.builder().token(self._bot_token)
        if not polling:
            builder = builder.updater(None)  # Webhook mode — we handle updates

        self._application = builder.build()

        # Register handlers
        self._application.add_handler(CommandHandler("start", self._handle_start))
        self._application.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, self._handle_message)
        )

        await self._application.initialize()
        await self._application.start()

        if polling:
            await self._application.updater.start_polling()
            logger.info("Telegram channel started (polling mode)")
        else:
            logger.info("Telegram channel started (webhook mode)")

    async def stop(self) -> None:
        """Shut down the telegram Application gracefully."""
        if self._application is None:
            return
        if self._application.updater and self._application.updater.running:
            await self._application.updater.stop()
        await self._application.stop()
        await self._application.shutdown()
        self._application = None
        logger.info("Telegram channel stopped")

    async def deliver(self, session_id: str, text: str) -> None:
        """Deliver a message to a Telegram chat.

        The session_id must be in the format ``telegram:{chat_id}``.
        """
        if self._application is None:
            return
        if not session_id.startswith("telegram:"):
            return
        try:
            chat_id = int(session_id.split(":", 1)[1])
        except (ValueError, IndexError):
            logger.warning("Invalid telegram session_id: %s", session_id)
            return
        chunks = _split_message(text)
        for chunk in chunks:
            await self._application.bot.send_message(chat_id=chat_id, text=chunk)

    # -- Handlers -----------------------------------------------------------

    async def _handle_start(self, update: Any, context: Any) -> None:
        """Handle the /start command with a welcome message."""
        if update.message is None:
            return
        welcome = (
            "Welcome to Seeknal Ask! I'm your data engineering assistant.\n\n"
            "Send me a question about your data, and I'll analyze it or "
            "build pipelines for you."
        )
        await update.message.reply_text(welcome)

    async def _handle_message(self, update: Any, context: Any) -> None:
        """Handle an incoming text message.

        Resolves the session as ``telegram:{user_id}``, creates/resumes the
        agent session, streams agent events to the chat, and sends the
        final response back.
        """
        if update.message is None or update.message.text is None:
            return
        if update.effective_user is None or update.effective_chat is None:
            return

        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        session_id = f"telegram:{user_id}"
        question = update.message.text.strip()

        if not question:
            return

        # Send typing indicator
        await update.message.chat.send_action("typing")

        # Start periodic typing task (Telegram typing expires after ~5s)
        typing_task = asyncio.create_task(
            self._periodic_typing(update.message.chat)
        )

        try:
            answer = await self._run_agent(
                session_id, question, chat=update.message.chat,
            )
        except Exception:
            logger.exception("Agent error for session=%s", session_id)
            answer = "Sorry, something went wrong processing your request."
        finally:
            typing_task.cancel()
            try:
                await typing_task
            except asyncio.CancelledError:
                pass

        # Strip markdown and send final response
        answer = _strip_markdown(answer)
        chunks = _split_message(answer)
        for chunk in chunks:
            await update.message.reply_text(chunk)

    async def _periodic_typing(self, chat: Any) -> None:
        """Re-send typing action every few seconds until cancelled."""
        try:
            while True:
                await asyncio.sleep(_TYPING_INTERVAL)
                await chat.send_action("typing")
        except asyncio.CancelledError:
            pass

    async def _run_agent(
        self, session_id: str, question: str, chat: Any = None,
    ) -> str:
        """Create/resume agent session and stream events to chat.

        Uses LangGraph's astream_events API for real-time progress.
        Falls back to synchronous ask() if streaming is not available.
        """
        from seeknal.ask.agents.agent import create_agent
        from seeknal.ask.sessions import SessionStore

        with SessionStore(self._project_path) as store:
            try:
                if store.get(session_id) is None:
                    store.create(session_id)

                agent, config = create_agent(
                    project_path=self._project_path,
                    question=question,
                    session_store=store,
                    session_name=session_id,
                    output_channel="telegram",
                )

                store.update(session_id, last_question=question[:500])

                answer = await asyncio.wait_for(
                    self._stream_agent_to_chat(agent, config, question, chat),
                    timeout=_AGENT_TIMEOUT,
                )
                return answer or "No response generated."
            except asyncio.TimeoutError:
                logger.error(
                    "Agent timed out after %ds: session=%s",
                    _AGENT_TIMEOUT, session_id,
                )
                return "Sorry, the request timed out. Please try a simpler question."

    async def _stream_agent_to_chat(
        self,
        agent: Any,
        config: dict,
        question: str,
        chat: Any,
    ) -> str:
        """Stream agent events to Telegram chat in real-time.

        Sends tool usage status messages as the agent works, then returns
        the final answer text. Includes Ralph Loop: if the agent finishes
        without producing text (only tool calls), nudge it to summarize.
        """
        from langchain_core.messages import HumanMessage

        from seeknal.ask.agents.agent import _normalize_content

        answer = await self._run_single_stream(agent, config, question, chat)
        if answer:
            return answer

        # Ralph Loop: agent produced no text — nudge to summarize
        for attempt in range(_MAX_RALPH_RETRIES):
            logger.info("Ralph Loop nudge %d: no text response, nudging agent", attempt + 1)
            nudge = (
                "You have not provided a final response. "
                "Summarize your findings as text for the user."
            )
            answer = await self._run_single_stream(agent, config, nudge, chat)
            if answer:
                return answer

        return ""

    async def _run_single_stream(
        self,
        agent: Any,
        config: dict,
        question: str,
        chat: Any,
    ) -> str:
        """Run one streaming pass and return extracted text.

        Returns empty string if no text was produced (only tool calls).
        """
        from langchain_core.messages import HumanMessage

        from seeknal.ask.agents.agent import _normalize_content

        text_buffer: list[str] = []
        accumulated = ""
        last_ai_text = ""
        last_tool_msg_time = 0.0
        last_tool_label = ""

        try:
            # Suppress "coroutine was never awaited" warnings from aiohttp/GenAI
            # internals — caused by hasattr() on coroutine methods during type checks
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message="coroutine.*was never awaited")
                async for event in agent.astream_events(
                    {"messages": [HumanMessage(content=question)]},
                    config=config,
                    version="v2",
                ):
                    kind = event.get("event", "")

                    if kind == "on_chat_model_stream":
                        chunk = event.get("data", {}).get("chunk")
                        if chunk is None:
                            continue
                        content = getattr(chunk, "content", "")
                        token = _normalize_content(content)
                        if not token:
                            continue
                        # Gemini cumulative dedup
                        if accumulated and token.startswith(accumulated) and len(token) > len(accumulated):
                            text_buffer.clear()
                            text_buffer.append(token)
                            accumulated = token
                        else:
                            text_buffer.append(token)
                            accumulated += token

                    elif kind == "on_chat_model_end":
                        output = event.get("data", {}).get("output")
                        if output is not None:
                            content = getattr(output, "content", "")
                            text = _normalize_content(content)
                            if text:
                                last_ai_text = text

                    elif kind == "on_tool_start":
                        name = event.get("name", "unknown")
                        if name in _HIDDEN_TOOLS:
                            continue
                        label = _TOOL_LABELS.get(name, f"Running {name}")
                        if label == last_tool_label:
                            continue
                        now = time.monotonic()
                        if chat and (now - last_tool_msg_time) >= _TOOL_THROTTLE_INTERVAL:
                            try:
                                await chat.send_message(f"{label}...")
                            except Exception:
                                pass
                            last_tool_msg_time = now
                            last_tool_label = label

        except Exception as exc:
            logger.exception("Streaming error: %s", type(exc).__name__)
            if accumulated:
                return accumulated
            if last_ai_text:
                return last_ai_text
            exc_name = type(exc).__name__
            if "Timeout" in exc_name or "CancelledError" in exc_name:
                return (
                    "The AI service timed out while processing your request. "
                    "Please try again or simplify your question."
                )
            return f"Sorry, an error occurred ({exc_name}). Please try again."

        if accumulated:
            return accumulated
        if last_ai_text:
            return last_ai_text
        return ""

    # -- Webhook endpoint ---------------------------------------------------

    async def webhook_handler(self, request: Any) -> Any:
        """Starlette route handler for Telegram webhook updates.

        Mount this as a POST route on the gateway app:
            Route("/telegram/webhook", channel.webhook_handler, methods=["POST"])

        Args:
            request: Starlette Request object.

        Returns:
            Starlette JSONResponse.
        """
        from starlette.responses import JSONResponse

        if self._application is None:
            return JSONResponse({"error": "not initialized"}, status_code=503)

        from telegram import Update

        body = await request.json()
        update = Update.de_json(body, self._application.bot)
        await self._application.process_update(update)
        return JSONResponse({"ok": True})
