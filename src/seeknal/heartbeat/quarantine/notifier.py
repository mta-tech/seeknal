"""QuarantineNotifier — formatter for operator-facing review messages.

The notifier produces the *content* of the notification (title, summary,
preview, inline-keyboard button payloads). Actual delivery is the Telegram
channel's job; this module is delivery-agnostic so it can also drive the
``seeknal heartbeat review`` CLI.

Per the file-as-boundary principle, the heartbeat does not import telegram.
The telegram channel reads ``QuarantineNotice`` from this module instead.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from seeknal.heartbeat.quarantine.review import QuarantineSidecar


@dataclass
class QuarantineNotice:
    sidecar_path: Path
    run_id: str
    file: str
    target_table: str
    confidence: float
    concerns: list[str] = field(default_factory=list)
    preview_text: str = ""
    callback_tokens: dict[str, str] = field(default_factory=dict)

    def summary(self) -> str:
        concerns = ", ".join(self.concerns) if self.concerns else "—"
        return (
            f"📨 New file needs review: {Path(self.file).name}\n"
            f"Best guess: {self.target_table or '?'} "
            f"({self.confidence * 100:.0f}% confidence)\n"
            f"Concerns: {concerns}\n"
        )


def build_notice(
    sidecar_path: Path,
    *,
    preview_text: str = "",
    confirm_token: str = "",
    reject_token: str = "",
    edit_token: str = "",
) -> QuarantineNotice:
    sidecar = QuarantineSidecar.load(sidecar_path)
    notice = QuarantineNotice(
        sidecar_path=sidecar_path,
        run_id=sidecar.run_id,
        file=sidecar.file,
        target_table=sidecar.target_table,
        confidence=sidecar.confidence,
        concerns=list(sidecar.concerns),
        preview_text=preview_text,
        callback_tokens={
            "confirm": confirm_token,
            "reject": reject_token,
            "edit": edit_token,
        },
    )
    return notice


def render_callback(action: str, token: str, idx: int = 0) -> str:
    """Build a base36 callback payload — 9 bytes max (well under Telegram's 64)."""
    short = {"confirm": "c", "reject": "r", "edit": "e"}.get(action, action[0])
    return f"q:{short}:{token}:{idx}"


def iter_pending(project_root: Path) -> Iterable[Path]:
    from seeknal.heartbeat.quarantine.review import list_pending_reviews
    yield from list_pending_reviews(project_root)


__all__ = ["QuarantineNotice", "build_notice", "render_callback", "iter_pending"]
