"""Quarantine review primitives — read sidecar, apply Confirm/Reject/Edit decisions."""

from __future__ import annotations

import logging
import re
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Optional

import yaml

logger = logging.getLogger("seeknal.heartbeat.quarantine.review")

_REVIEW_SUFFIX = ".review.yml"


@dataclass
class QuarantineSidecar:
    """The ``*.review.yml`` next to each quarantined file."""

    schema_version: int = 1
    run_id: str = ""
    file: str = ""
    classified_at: str = ""
    target_table: str = ""
    confidence: float = 0.0
    source: str = "llm"
    llm_call_id: Optional[str] = None
    reasoning: str = ""
    column_mapping: dict[str, str] = field(default_factory=dict)
    concerns: list[str] = field(default_factory=list)
    status: Literal[
        "pending_review", "confirmed", "rejected", "edited"
    ] = "pending_review"
    notified_at: str = ""
    notified_chats: list[int] = field(default_factory=list)
    resolution: Optional[str] = None
    resolved_by: Optional[str] = None
    resolved_at: Optional[str] = None
    write_mode: str = "append"
    business_key: list[str] = field(default_factory=list)
    source_path: Optional[Path] = None

    @classmethod
    def load(cls, path: Path) -> "QuarantineSidecar":
        with open(path, "r") as fh:
            raw = yaml.safe_load(fh) or {}
        if not isinstance(raw, dict):
            raise ValueError(f"Sidecar at {path} is not a mapping")
        decision = raw.get("classifier_decision") or {}
        sidecar = cls(
            schema_version=int(raw.get("schema_version", 1)),
            run_id=str(raw.get("run_id", "") or ""),
            file=str(raw.get("file", "") or ""),
            classified_at=str(raw.get("classified_at", "") or ""),
            target_table=str(decision.get("target_table", "") or ""),
            confidence=float(decision.get("confidence", 0.0) or 0.0),
            source=str(decision.get("source", "llm") or "llm"),
            llm_call_id=decision.get("llm_call_id"),
            reasoning=str(decision.get("reasoning", "") or ""),
            column_mapping=dict(decision.get("column_mapping") or {}),
            concerns=list(decision.get("concerns") or []),
            status=str(raw.get("status", "pending_review") or "pending_review"),
            notified_at=str(raw.get("notified_at", "") or ""),
            notified_chats=list(raw.get("notified_chats") or []),
            resolution=raw.get("resolution"),
            resolved_by=raw.get("resolved_by"),
            resolved_at=raw.get("resolved_at"),
            write_mode=str(raw.get("write_mode", "append") or "append"),
            business_key=list(raw.get("business_key") or []),
        )
        sidecar.source_path = path
        return sidecar

    def save(self, path: Optional[Path] = None) -> Path:
        target = Path(path or self.source_path)
        payload: dict[str, Any] = {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "file": self.file,
            "classified_at": self.classified_at,
            "classifier_decision": {
                "target_table": self.target_table,
                "confidence": self.confidence,
                "source": self.source,
                "llm_call_id": self.llm_call_id,
                "reasoning": self.reasoning,
                "column_mapping": dict(self.column_mapping),
                "concerns": list(self.concerns),
            },
            "status": self.status,
            "write_mode": self.write_mode,
            "business_key": list(self.business_key),
        }
        if self.notified_at:
            payload["notified_at"] = self.notified_at
        if self.notified_chats:
            payload["notified_chats"] = list(self.notified_chats)
        if self.resolution is not None:
            payload["resolution"] = self.resolution
        if self.resolved_by is not None:
            payload["resolved_by"] = self.resolved_by
        if self.resolved_at is not None:
            payload["resolved_at"] = self.resolved_at
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(yaml.safe_dump(payload, sort_keys=False))
        self.source_path = target
        return target


def list_pending_reviews(project_root: Path) -> list[Path]:
    """Return sidecar files in needs_review/ that are still pending."""
    needs = project_root / "target" / "heartbeat" / "quarantine" / "needs_review"
    if not needs.exists():
        return []
    out: list[Path] = []
    for sidecar in sorted(needs.rglob(f"*{_REVIEW_SUFFIX}")):
        try:
            doc = yaml.safe_load(sidecar.read_text()) or {}
        except yaml.YAMLError:
            continue
        if isinstance(doc, dict) and doc.get("status", "pending_review") == "pending_review":
            out.append(sidecar)
    return out


def _utcnow_iso() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


_EDIT_LINE_RE = re.compile(r"^([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*(.+)$")


def parse_edit_input(text: str) -> tuple[dict[str, str], dict[str, Any], list[str]]:
    """Parse operator free-text edit input — mechanical, no LLM fallback.

    Supported keys:
    - ``target=<table>`` → override target_table.
    - ``mode=upsert|append|overwrite`` → override write_mode.
    - ``key=col`` or ``key=col1,col2`` → override business_key.
    - ``<file_col>=<target_col>`` → column mapping.

    Returns: (column_mapping_overrides, metadata_overrides, errors)
    """
    column_mapping: dict[str, str] = {}
    metadata: dict[str, Any] = {}
    errors: list[str] = []
    for lineno, raw_line in enumerate(text.splitlines(), 1):
        line = raw_line.strip()
        if not line:
            continue
        if line.lower() == "cancel":
            return {}, {"cancelled": True}, []
        match = _EDIT_LINE_RE.match(line)
        if not match:
            errors.append(
                f"line {lineno}: expected `key=value` (alphanumeric+underscores), got: {raw_line!r}"
            )
            continue
        key = match.group(1).strip()
        value = match.group(2).strip()
        if key.lower() == "target":
            metadata["target_table"] = value
        elif key.lower() == "mode":
            if value.lower() not in {"append", "upsert", "overwrite"}:
                errors.append(
                    f"line {lineno}: unknown mode {value!r}. "
                    "Use append, upsert, or overwrite."
                )
            else:
                metadata["write_mode"] = value.lower()
        elif key.lower() == "key":
            cols = [c.strip() for c in value.split(",") if c.strip()]
            if not cols:
                errors.append(f"line {lineno}: empty key list")
            else:
                metadata["business_key"] = cols
        else:
            column_mapping[key] = value
    return column_mapping, metadata, errors


def apply_review_decision(
    sidecar_path: Path,
    *,
    decision: Literal["confirm", "reject", "edit"],
    resolved_by: str = "",
    edit_text: Optional[str] = None,
    rejected_dir: Optional[Path] = None,
) -> dict[str, Any]:
    """Apply an operator decision to the sidecar. Returns a result summary.

    The classifier-confirmation effect (catalog update + IngestWriter call) is
    intentionally NOT wired in here — that lives in the heartbeat runner, which
    knows about the IngestWriter and Catalog. This helper is the deterministic
    state-transition primitive.
    """
    sidecar = QuarantineSidecar.load(sidecar_path)
    if sidecar.status != "pending_review":
        return {
            "ok": False,
            "error": f"Sidecar already {sidecar.status!r}; skipping.",
        }

    if decision == "confirm":
        sidecar.status = "confirmed"
        sidecar.resolution = "confirm"
        sidecar.resolved_by = resolved_by or "operator"
        sidecar.resolved_at = _utcnow_iso()
        sidecar.save()
        return {"ok": True, "decision": "confirm", "sidecar": sidecar}

    if decision == "reject":
        sidecar.status = "rejected"
        sidecar.resolution = "reject"
        sidecar.resolved_by = resolved_by or "operator"
        sidecar.resolved_at = _utcnow_iso()
        sidecar.save()
        # Move the data file aside.
        data_file = Path(sidecar.file)
        if not data_file.is_absolute():
            data_file = sidecar_path.parent / data_file.name
        rejected = rejected_dir or (
            sidecar_path.parents[3] / "rejected" / sidecar.run_id
        )
        if data_file.exists():
            rejected.mkdir(parents=True, exist_ok=True)
            try:
                shutil.move(str(data_file), str(rejected / data_file.name))
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to move rejected file: %s", exc)
        return {"ok": True, "decision": "reject", "sidecar": sidecar}

    if decision == "edit":
        if edit_text is None:
            return {"ok": False, "error": "edit requires edit_text"}
        column_overrides, meta_overrides, errors = parse_edit_input(edit_text)
        if errors:
            return {"ok": False, "errors": errors}
        if meta_overrides.get("cancelled"):
            return {"ok": False, "cancelled": True}
        if column_overrides:
            sidecar.column_mapping.update(column_overrides)
        if "target_table" in meta_overrides:
            sidecar.target_table = str(meta_overrides["target_table"])
        if "write_mode" in meta_overrides:
            sidecar.write_mode = str(meta_overrides["write_mode"])
        if "business_key" in meta_overrides:
            sidecar.business_key = list(meta_overrides["business_key"])
        sidecar.status = "edited"
        sidecar.resolution = "edit"
        sidecar.resolved_by = resolved_by or "operator"
        sidecar.resolved_at = _utcnow_iso()
        sidecar.save()
        return {"ok": True, "decision": "edit", "sidecar": sidecar}

    raise ValueError(f"Unknown decision {decision!r}")


__all__ = [
    "QuarantineSidecar",
    "apply_review_decision",
    "list_pending_reviews",
    "parse_edit_input",
]
