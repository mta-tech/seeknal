"""Telegram channel for seeknal ask gateway.

Receives messages from Telegram users, runs the seeknal ask agent,
and streams responses back. Uses pydantic-ai's agent.iter() for
step-by-step event streaming.

Requires: python-telegram-bot>=21.0
Configure: TELEGRAM_BOT_TOKEN environment variable
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any

from seeknal.ask.gateway.pairing import (
    PairCodeExpiredError,
    PairCodeInvalidError,
    PairCodeUsedError,
)
from seeknal.ask.gateway.tenant import DEFAULT_TENANT

logger = logging.getLogger(__name__)

# Telegram message size limit
_MAX_MESSAGE_LENGTH = 4096

# Document upload constraints (match the gateway /upload endpoint)
_UPLOAD_MAX_BYTES = 200 * 1024 * 1024  # 200 MB
_UPLOAD_ALLOWED_SUFFIXES = {".xlsx", ".csv", ".tsv", ".json"}

# Image upload constraints (routed into record-entry skill)
_IMAGE_MAX_BYTES = 20 * 1024 * 1024  # 20 MB
_IMAGE_SUFFIX_FOR_MIME = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/heic": ".heic",
    "image/heif": ".heif",
}

# Telegram formatting instruction prepended to user messages
_TELEGRAM_FORMAT_HINT = (
    "[Format: plain text for Telegram. No markdown syntax. "
    "Use dashes for bullets. Write in normal sentence case. "
    "No **bold**, *italic*, ```code``` blocks, or ALL CAPS.]\n"
    "[Context: This is a Telegram channel. When the user asks to 'add', "
    "'tambahkan', 'edit', or modify content — or says 'ke proof yang diatas' / "
    "'to the proof above' — they mean the most recently published Proof document. "
    "Do NOT ask the user for the URL. Look in your conversation history for the "
    "Proof share link you previously sent (starts with http://memokami or similar). "
    "Use read_proof_document with that URL to fetch current content, then use "
    "edit_proof_document to append or modify it. Never create seeknal pipeline "
    "nodes or start a new analysis unless explicitly asked.]\n"
    "[Always include the full answer in your response. Never say 'as mentioned "
    "above' or 'already provided' — the user only sees your current message. "
    "If data was gathered via tools, present the actual findings.]\n"
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
        self._pairing_store: Any = None
        self._link_store: Any = None
        self._public_session_store: Any = None
        self._last_polling_conflict_log_at = 0.0

    def set_pairing_store(self, pairing_store: Any) -> None:
        """Inject a pairing store shared with the gateway app."""
        self._pairing_store = pairing_store

    def set_link_store(self, link_store: Any) -> None:
        """Inject a Telegram chat -> session mapping store."""
        self._link_store = link_store

    def set_public_session_store(self, public_session_store: Any) -> None:
        """Inject a public-session store for unpaired Telegram access."""
        self._public_session_store = public_session_store

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
        self._app.add_handler(CommandHandler("pair", self._handle_pair))
        self._app.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, self._handle_message)
        )
        # Document uploads (xlsx/csv/tsv/json) are routed into the data-ingest
        # workflow so the user can converse with the ingested data.
        self._app.add_handler(
            MessageHandler(filters.Document.ALL, self._handle_document)
        )
        # Photo uploads (receipt / transfer-proof images) are routed into the
        # record-entry workflow: download -> Gemini vision -> ask_user ->
        # write_ingested_table.
        self._app.add_handler(
            MessageHandler(filters.PHOTO, self._handle_photo)
        )

        await self._app.initialize()
        await self._app.start()
        await self._app.updater.start_polling(
            error_callback=self._handle_polling_error,
        )
        logger.info("Telegram channel started")

    async def stop(self) -> None:
        """Shut down the Telegram bot."""
        if self._app:
            if self._app.updater and self._app.updater.running:
                await self._app.updater.stop()
            await self._app.stop()
            await self._app.shutdown()
            logger.info("Telegram channel stopped")

    def _handle_polling_error(self, exc: Any) -> None:
        """Handle Telegram polling errors without noisy retry tracebacks.

        ``python-telegram-bot`` logs a full traceback for every polling error by
        default. A Telegram 409 Conflict is usually operational: another bot
        poller is active, or Telegram still has a just-stopped long-poll request
        open. The polling loop already retries, so surfacing repeated tracebacks
        makes the gateway look broken even while HTTP remains healthy. Log a
        concise throttled warning instead and let the retry loop recover.
        """
        try:
            from telegram.error import Conflict
        except Exception:  # pragma: no cover - defensive for optional dependency
            Conflict = ()  # type: ignore[assignment]

        if isinstance(exc, Conflict):
            now = time.monotonic()
            if now - self._last_polling_conflict_log_at >= 60:
                self._last_polling_conflict_log_at = now
                logger.warning(
                    "Telegram polling conflict; another getUpdates request is "
                    "active or a previous long poll is still closing. The gateway "
                    "will keep retrying. Ensure only one Telegram poller uses this "
                    "bot token."
                )
            return

        logger.exception("Telegram polling error.", exc_info=exc)

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

    async def _handle_pair(self, update: Any, context: Any) -> None:
        """Redeem an admin-generated one-time pair code."""
        if self._pairing_store is None or self._link_store is None:
            await update.message.reply_text(
                "Pairing is not available right now. Please try again later."
            )
            return

        chat_id = str(update.effective_chat.id)
        args = list(getattr(context, "args", []) or [])
        if not args:
            await update.message.reply_text(
                "Ask the admin for a pair code, then send it here as /pair <code>."
            )
            return

        try:
            record = await self._pairing_store.redeem_pair_code(
                " ".join(args),
                tenant_id=DEFAULT_TENANT,
            )
        except (PairCodeInvalidError, PairCodeExpiredError, PairCodeUsedError) as exc:
            await update.message.reply_text(str(exc))
            return
        self._link_store.link_chat(
            chat_id,
            record.session_id,
            tenant_id=DEFAULT_TENANT,
        )

        await update.message.reply_text(
            "Paired successfully. This Telegram chat is now connected to session "
            f"{record.session_id}."
        )

    def _session_id_for_chat(self, chat_id: str) -> str | None:
        if self._link_store is not None:
            linked_session = self._link_store.get_session_id(
                chat_id,
                tenant_id=DEFAULT_TENANT,
            )
            if linked_session:
                return linked_session
        if self._public_session_store is not None:
            public_session = self._public_session_store.get_session_id(
                tenant_id=DEFAULT_TENANT,
            )
            if public_session:
                return public_session
        return None

    async def _handle_message(self, update: Any, context: Any) -> None:
        """Handle incoming text messages — run agent and stream response."""
        from telegram.constants import ChatAction

        question = update.message.text.strip()
        if not question:
            return

        chat_id = str(update.effective_chat.id)
        session_id = self._session_id_for_chat(chat_id)
        if not session_id:
            await update.message.reply_text(
                "This Telegram chat is not paired yet. Ask the admin for a pair code, "
                "then send /pair <code> first."
            )
            return
        logger.info("[telegram] message from chat_id=%s: %s", chat_id, question[:80])

        # Show typing indicator and a single status message (edited in-place)
        await update.effective_chat.send_action(ChatAction.TYPING)
        status_msg = await update.message.reply_text("🔍 Starting...")

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

    # ------------------------------------------------------------------
    # Manifest-aware photo intake
    # ------------------------------------------------------------------
    # When the project ships ``data/telegram_uploads/ingestion_manifest.json``
    # with a route whose ``file_pattern`` matches the user's caption, the
    # destination table is already decided. Drive the agent straight to
    # ``extract_from_image`` -> ``write_ingested_table`` -> ``execute_sql``
    # verify, skipping the ``propose_record_table`` round-trip. Without a
    # manifest hit, fall back to the legacy generic prompt.

    def _load_ingestion_manifest(self) -> dict[str, Any] | None:
        """Read the project's ``data/telegram_uploads/ingestion_manifest.json``.

        Returns the parsed dict, or ``None`` when missing, unreadable, or
        lacking a non-empty ``routes`` list.
        """
        manifest_path = (
            self._project_path
            / "data"
            / "telegram_uploads"
            / "ingestion_manifest.json"
        )
        if not manifest_path.exists():
            return None
        try:
            data = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            logger.warning(
                "[telegram] failed to read ingestion_manifest.json: %s", exc
            )
            return None
        if not isinstance(data, dict):
            return None
        routes = data.get("routes")
        if not isinstance(routes, list) or not routes:
            return None
        return data

    def _match_manifest_route(
        self, hint_text: str, manifest: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Return the unique manifest route whose ``file_pattern`` matches.

        Photos from Telegram do not carry meaningful filenames, so we match
        the ``file_pattern`` token (``*order*`` -> ``order``) against the
        lowercased caption text. When zero or multiple routes match we
        return ``None`` and the legacy generic flow handles disambiguation.
        """
        if not hint_text:
            return None
        hint = hint_text.lower()
        matched: list[dict[str, Any]] = []
        for route in manifest.get("routes", []):
            if not isinstance(route, dict):
                continue
            pattern = route.get("file_pattern", "")
            if not isinstance(pattern, str):
                continue
            token = pattern.replace("*", "").strip().lower()
            if not token:
                continue
            if token in hint:
                matched.append(route)
        if len(matched) == 1:
            return matched[0]
        return None

    def _load_photo_intake_config(self) -> dict[str, Any]:
        """Read ``telegram.photo_intake`` from ``seeknal_agent.yml``.

        Defaults to ``require_confirmation=True`` (the safer setting) when
        the file is missing, unreadable, or omits the key.
        """
        cfg_path = self._project_path / "seeknal_agent.yml"
        if not cfg_path.exists():
            cfg_path = self._project_path / "seeknal_agent.yaml"
        if not cfg_path.exists():
            return {"require_confirmation": True}
        try:
            import yaml

            data = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
        except (OSError, ValueError) as exc:
            logger.warning(
                "[telegram] failed to read seeknal_agent.yml: %s", exc
            )
            return {"require_confirmation": True}
        if not isinstance(data, dict):
            return {"require_confirmation": True}
        telegram_cfg = data.get("telegram") or {}
        intake = telegram_cfg.get("photo_intake") or {}
        return {
            "require_confirmation": bool(
                intake.get("require_confirmation", True)
            )
        }

    async def _handle_photo(self, update: Any, context: Any) -> None:
        """Handle photo uploads — stage and route into record-entry skill."""
        from telegram.constants import ChatAction

        photos = update.message.photo
        if not photos:
            return

        # Telegram serves multiple sizes; the largest is the last element.
        photo = photos[-1]

        chat_id = str(update.effective_chat.id)
        session_id = f"telegram-{chat_id}"
        logger.info(
            "[telegram] photo from chat_id=%s: file_id=%s size=%s",
            chat_id, photo.file_id, photo.file_size,
        )

        if photo.file_size and photo.file_size > _IMAGE_MAX_BYTES:
            await update.message.reply_text(
                f"Image too large ({photo.file_size / 1024 / 1024:.1f} MB). "
                f"Limit is {_IMAGE_MAX_BYTES / 1024 / 1024:.0f} MB."
            )
            return

        staging_dir = (
            self._project_path / "target" / "ask_ingest" / "_staging"
            / f"telegram-{chat_id}"
        )
        staging_dir.mkdir(parents=True, exist_ok=True)
        staged_path = staging_dir / f"{photo.file_unique_id}.jpg"

        await update.effective_chat.send_action(ChatAction.TYPING)
        status_msg = await update.message.reply_text(
            "📷 Received image. Downloading..."
        )

        try:
            tg_file = await context.bot.get_file(photo.file_id)
            await tg_file.download_to_drive(custom_path=str(staged_path))
            logger.info(
                "[telegram] photo staged: %s (%d bytes)",
                staged_path, staged_path.stat().st_size,
            )
        except Exception as exc:
            logger.exception(
                "[telegram] photo download failed for chat_id=%s", chat_id
            )
            try:
                await status_msg.edit_text(f"❌ Download failed: {exc}")
            except Exception:
                await update.message.reply_text(f"❌ Download failed: {exc}")
            return

        caption = (update.message.caption or "").strip()

        manifest = self._load_ingestion_manifest()
        matched_route = (
            self._match_manifest_route(caption, manifest) if manifest else None
        )

        if matched_route:
            target_table = str(matched_route.get("target_table", "")).strip()
            required_cols = matched_route.get("required_columns") or []
            if not isinstance(required_cols, list):
                required_cols = []
            require_confirmation = self._load_photo_intake_config()[
                "require_confirmation"
            ]
            ingest_table = f"ingest_{target_table}"
            cols_line = (
                ", ".join(str(c) for c in required_cols)
                if required_cols
                else "(see manifest)"
            )
            logger.info(
                "[telegram] manifest route matched: target=%s, confirm=%s",
                target_table,
                require_confirmation,
            )

            user_prompt = (
                "The user sent a photo via Telegram and the project's "
                "ingestion manifest routes this upload to a known canonical "
                "table. The destination is already decided — do NOT call "
                "`propose_record_table`.\n\n"
                f"Absolute image path: {staged_path}\n"
            )
            if caption:
                user_prompt += f"User caption / hint: {caption}\n"
            user_prompt += (
                f"\nCanonical destination: source.{target_table}\n"
                f"Ingest staging table: {ingest_table}\n"
                f"Required columns: {cols_line}\n\n"
                "Workflow:\n"
                "1. Call `extract_from_image(image_path=<path>, "
                "hint=<caption if any>)`.\n"
                "2. Coerce the extracted fields onto the required columns "
                "listed above.\n"
            )
            if require_confirmation:
                user_prompt += (
                    "3. Show the draft row to the user in plain Indonesian "
                    "and ask: Ya / Edit / Salah / Batal.\n"
                    "4. After the user confirms, call "
                    f"`write_ingested_table(table_name='{ingest_table}', "
                    "mode='append', user_confirmed=True)`.\n"
                )
            else:
                user_prompt += (
                    "3. Auto-save is enabled — call "
                    f"`write_ingested_table(table_name='{ingest_table}', "
                    "mode='append', user_confirmed=True)` directly.\n"
                )
            user_prompt += (
                "5. Verify by calling `execute_sql("
                f"\"SELECT COUNT(*) FROM {ingest_table}\")` and quote the "
                "row count in your reply. Never claim 'tersimpan' or "
                "'saved' without this verification step.\n"
                "Reply in plain Indonesian owner language."
            )
        else:
            user_prompt = (
                "The user sent a photo via Telegram — most likely a "
                "fund-transfer proof "
                "(BCA/Mandiri/BNI/BRI/GoPay/OVO/DANA/QRIS), a shop receipt, "
                "or an order photo. Load the 'record-entry' skill and walk "
                "through the full workflow on this image:\n\n"
                f"Absolute image path: {staged_path}\n"
            )
            if caption:
                user_prompt += f"User caption / hint: {caption}\n"
            user_prompt += (
                "\nStart with `extract_from_image(image_path=<path>, "
                "hint=<caption if any>)`, then `list_tables` + "
                "`propose_record_table`, then `ask_user` until every "
                "required field is resolved, then `write_ingested_table`. "
                "Strict clarification — do not record until the draft is "
                "fully confirmed."
            )

        try:
            await status_msg.edit_text("🧐 Reading the image...")
        except Exception:
            pass

        try:
            answer = await self._run_agent(session_id, user_prompt, update, status_msg)
            logger.info(
                "[telegram] record-entry agent done for session=%s, answer_len=%d",
                session_id, len(answer) if answer else 0,
            )
            try:
                await status_msg.delete()
            except Exception:
                pass
            if answer:
                clean = _strip_markdown(answer)
                for chunk in _split_message(clean):
                    await update.message.reply_text(chunk)
                logger.info(
                    "[telegram] record-entry reply sent to chat_id=%s", chat_id
                )
            else:
                await update.message.reply_text(
                    "I could read the image but didn't produce a reply. "
                    "Ask me 'what did you extract?' to recover."
                )
        except Exception as exc:
            logger.exception(
                "[telegram] error on photo from chat_id=%s", chat_id
            )
            try:
                await status_msg.edit_text(f"❌ Error: {exc}")
            except Exception:
                await update.message.reply_text(f"❌ Error: {exc}")

    # ------------------------------------------------------------------
    # Step 7 — Telegram → heartbeat-inbox bridge
    # ------------------------------------------------------------------
    # The Telegram channel stages every uploaded file under
    # ``target/ask_ingest/_staging/`` and runs the interactive ``data-ingest``
    # skill (existing behavior, untouched). Optionally, projects that also
    # run ``seeknal heartbeat`` can mirror the staged file into the
    # heartbeat's inbox folder so the deterministic tick picks it up on
    # its next scan. The bridge is a *copy*, not a move, so the interactive
    # path keeps working in parallel.
    #
    # The lint contract in ``tests/heartbeat/test_telegram_bridge.py::
    # test_no_telegram_imports_in_heartbeat`` forbids the reverse direction
    # — heartbeat must never import from this module. We honour the same
    # boundary from this side by reading ``HEARTBEAT.md`` inline rather
    # than importing ``seeknal.heartbeat.config``.

    def _inbox_drop_settings(self) -> dict[str, Any]:
        """Return the Telegram → heartbeat-inbox bridge settings.

        Returns a dict with two keys:

        - ``enabled`` (bool): when False, :meth:`_copy_to_inbox` is a no-op
          and the existing staging-only behavior is preserved.
        - ``folder`` (str | None): when set, overrides HEARTBEAT.md's first
          ``inbox_folders`` entry. ``None`` defers to that lookup and finally
          to the ``inbox`` default.

        Default is disabled so existing deployments keep their current
        behavior. Projects opt in by subclassing or future config wiring.
        """
        return {"enabled": False, "folder": None}

    def _copy_to_inbox(self, staged_path: Path) -> Path | None:
        """Copy a staged Telegram upload into the heartbeat's inbox folder.

        Returns the destination path on success, or ``None`` when the
        bridge is disabled OR the resolved inbox path would escape the
        project root (containment guard).

        Folder resolution order:

        1. ``_inbox_drop_settings()["folder"]`` (explicit override)
        2. HEARTBEAT.md's first ``inbox_folders`` entry
        3. ``inbox`` (project-root default)

        On filename collision, appends a UTC timestamp to the stem so the
        original uploaded name is preserved as the prefix. The staged file
        itself is left in place.
        """
        settings = self._inbox_drop_settings()
        if not settings.get("enabled"):
            return None

        folder_name = settings.get("folder")
        if folder_name is None:
            folder_name = (
                self._read_inbox_folder_from_heartbeat_md() or "inbox"
            )

        # Containment guard: resolved inbox must stay under project root.
        try:
            project_root = self._project_path.resolve()
            inbox_dir = (project_root / folder_name).resolve()
            inbox_dir.relative_to(project_root)
        except (ValueError, OSError):
            logger.warning(
                "[telegram] inbox drop refused — resolved path escapes "
                "project root: %s",
                folder_name,
            )
            return None

        try:
            inbox_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            logger.warning(
                "[telegram] inbox drop refused — mkdir failed: %s", exc
            )
            return None

        dest = inbox_dir / staged_path.name
        if dest.exists():
            from datetime import datetime, timezone

            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            dest = inbox_dir / f"{staged_path.stem}.{ts}{staged_path.suffix}"

        import shutil

        try:
            shutil.copy2(staged_path, dest)
        except OSError as exc:
            logger.warning("[telegram] inbox drop copy failed: %s", exc)
            return None
        return dest

    def _read_inbox_folder_from_heartbeat_md(self) -> str | None:
        """Best-effort: return HEARTBEAT.md's first ``inbox_folders`` entry.

        Done inline so this module stays independent of the heartbeat
        package surface; the lint contract on the heartbeat side prevents
        the reverse import.
        """
        md = self._project_path / "HEARTBEAT.md"
        if not md.exists():
            return None
        try:
            text = md.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            return None
        match = re.search(r"```yaml\s*(.*?)```", text, re.DOTALL)
        body = match.group(1) if match else text
        try:
            import yaml

            data = yaml.safe_load(body)
        except Exception:
            return None
        if not isinstance(data, dict):
            return None
        folders = data.get("inbox_folders")
        if isinstance(folders, list) and folders:
            first = folders[0]
            if isinstance(first, str) and first.strip():
                return first.strip()
        return None

    async def _handle_document(self, update: Any, context: Any) -> None:
        """Handle document uploads — stage file and trigger data-ingest skill."""
        from telegram.constants import ChatAction

        doc = update.message.document
        if doc is None:
            return

        chat_id = str(update.effective_chat.id)
        session_id = f"telegram-{chat_id}"
        original_name = Path(doc.file_name or "upload").name
        suffix = Path(original_name).suffix.lower()
        logger.info(
            "[telegram] document from chat_id=%s: %s (size=%s)",
            chat_id, original_name, doc.file_size,
        )

        if suffix not in _UPLOAD_ALLOWED_SUFFIXES:
            await update.message.reply_text(
                f"Sorry, I can't ingest '{suffix}' files. "
                f"Supported: {', '.join(sorted(_UPLOAD_ALLOWED_SUFFIXES))}."
            )
            return

        if doc.file_size and doc.file_size > _UPLOAD_MAX_BYTES:
            await update.message.reply_text(
                f"File too large ({doc.file_size / 1024 / 1024:.1f} MB). "
                f"Limit is 200 MB."
            )
            return

        staging_dir = (
            self._project_path / "target" / "ask_ingest" / "_staging"
            / f"telegram-{chat_id}"
        )
        staging_dir.mkdir(parents=True, exist_ok=True)
        staged_path = staging_dir / original_name

        await update.effective_chat.send_action(ChatAction.TYPING)
        status_msg = await update.message.reply_text(
            f"📥 Downloading {original_name}..."
        )

        try:
            tg_file = await context.bot.get_file(doc.file_id)
            await tg_file.download_to_drive(custom_path=str(staged_path))
            logger.info(
                "[telegram] document staged: %s (%d bytes)",
                staged_path, staged_path.stat().st_size,
            )
        except Exception as exc:
            logger.exception(
                "[telegram] download failed for chat_id=%s", chat_id
            )
            try:
                await status_msg.edit_text(f"❌ Download failed: {exc}")
            except Exception:
                await update.message.reply_text(f"❌ Download failed: {exc}")
            return

        caption = (update.message.caption or "").strip()
        user_prompt = (
            f"The user uploaded a tabular file via Telegram. "
            f"Absolute file path: {staged_path}. "
            f"Please load the 'data-ingest' skill and walk through the ingestion "
            f"workflow: use read_tabular to preview the file, propose a table "
            f"name and business key via ask_user, write it via "
            f"write_ingested_table, and save a reusable skill via "
            f"save_ingestion_skill. "
        )
        if caption:
            user_prompt += f"User note: {caption}"

        try:
            await status_msg.edit_text(f"🔍 Inspecting {original_name}...")
        except Exception:
            pass

        try:
            answer = await self._run_agent(session_id, user_prompt, update, status_msg)
            logger.info(
                "[telegram] ingest agent done for session=%s, answer_len=%d",
                session_id, len(answer) if answer else 0,
            )
            try:
                await status_msg.delete()
            except Exception:
                pass
            if answer:
                clean = _strip_markdown(answer)
                for chunk in _split_message(clean):
                    await update.message.reply_text(chunk)
                logger.info("[telegram] ingest reply sent to chat_id=%s", chat_id)
            else:
                await update.message.reply_text(
                    f"Staged {original_name}, but the agent didn't respond. "
                    f"Try asking: 'ingest the file at {staged_path}'."
                )
        except Exception as exc:
            logger.exception(
                "[telegram] error ingesting document from chat_id=%s", chat_id
            )
            try:
                await status_msg.edit_text(f"❌ Error: {exc}")
            except Exception:
                await update.message.reply_text(f"❌ Error: {exc}")

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
        writing_started = False
        last_status = ""

        async def _update_status(text: str) -> None:
            nonlocal last_status
            if not status_msg or text == last_status:
                return
            try:
                await status_msg.edit_text(text)
                last_status = text
            except Exception:
                pass

        formatted_question = _TELEGRAM_FORMAT_HINT + question
        logger.info("[telegram] agent stream starting session=%s", session_id)
        stream = _run_agent_streaming(
            self._project_path, session_id, formatted_question,
            auto_approve=True, include_web=True,
        )
        try:
            async for event in stream:
                etype = event["type"]
                if etype == "token":
                    text_parts.append(event["data"])
                    if not writing_started:
                        writing_started = True
                        await _update_status(
                            f"✍️ Writing response... ({tool_count} steps)")
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
                    # Cycle status phases based on tool count
                    if tool_count <= 2:
                        await _update_status(
                            f"🔍 Starting... ({tool_count} steps)")
                    elif tool_count <= 10:
                        await _update_status(
                            f"🔧 Running... ({tool_count} steps)")
                    else:
                        await _update_status(
                            f"📊 Analyzing... ({tool_count} steps)")
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
            auto_approve=True, include_web=True,
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
