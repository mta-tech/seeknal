"""Telegram channel plugin for seeknal ask gateway.

Uses python-telegram-bot v21+ with manual lifecycle (no run_polling/run_webhook)
so the Starlette gateway controls startup and shutdown. Incoming updates are
routed through a Starlette webhook endpoint that calls process_update().

Configuration in seeknal_agent.yml:
    gateway:
      telegram:
        token: ${TELEGRAM_BOT_TOKEN}
"""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Telegram message length limit
_MAX_MESSAGE_LENGTH = 4096

# Seconds between typing action re-sends
_TYPING_INTERVAL = 4


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

    async def start(self) -> None:
        """Build the telegram Application with manual lifecycle."""
        from telegram import Update
        from telegram.ext import (
            Application,
            CommandHandler,
            MessageHandler,
            filters,
        )

        self._application = (
            Application.builder()
            .token(self._bot_token)
            .updater(None)  # No built-in polling/webhook — we handle updates
            .build()
        )

        # Register handlers
        self._application.add_handler(CommandHandler("start", self._handle_start))
        self._application.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, self._handle_message)
        )

        await self._application.initialize()
        await self._application.start()
        logger.info("Telegram channel started")

    async def stop(self) -> None:
        """Shut down the telegram Application gracefully."""
        if self._application is None:
            return
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
        chat_id = int(session_id.split(":", 1)[1])
        chunks = _split_message(text)
        for chunk in chunks:
            await self._application.bot.send_message(chat_id=chat_id, text=chunk)

    # -- Handlers -----------------------------------------------------------

    async def _handle_start(self, update: Any, context: Any) -> None:
        """Handle the /start command with a welcome message."""
        welcome = (
            "Welcome to Seeknal Ask! I'm your data engineering assistant.\n\n"
            "Send me a question about your data, and I'll analyze it or "
            "build pipelines for you."
        )
        await update.message.reply_text(welcome)

    async def _handle_message(self, update: Any, context: Any) -> None:
        """Handle an incoming text message.

        Resolves the session as ``telegram:{user_id}``, creates/resumes the
        agent session, runs the agent, and sends the response back.
        """
        if update.message is None or update.message.text is None:
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
            answer = await self._run_agent(session_id, question)
        except Exception:
            logger.exception("Agent error for session=%s", session_id)
            answer = "Sorry, something went wrong processing your request."
        finally:
            typing_task.cancel()
            try:
                await typing_task
            except asyncio.CancelledError:
                pass

        # Split and send response
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

    async def _run_agent(self, session_id: str, question: str) -> str:
        """Create/resume agent session and run a question.

        Uses asyncio.to_thread to run the synchronous agent in a thread
        pool, keeping the event loop responsive.
        """
        from seeknal.ask.agents.agent import ask as sync_ask, create_agent
        from seeknal.ask.sessions import SessionStore

        store = SessionStore(self._project_path)
        try:
            # Create session if it doesn't exist
            if store.get(session_id) is None:
                store.create(session_id)

            agent, config = create_agent(
                project_path=self._project_path,
                question=question,
                session_store=store,
                session_name=session_id,
            )

            store.update(session_id, last_question=question[:500])

            answer = await asyncio.to_thread(sync_ask, agent, config, question)
            return answer or "No response generated."
        finally:
            store.close()

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
