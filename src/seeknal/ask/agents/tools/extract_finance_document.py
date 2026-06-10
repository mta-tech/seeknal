"""Extract finance documents into reviewable drafts.

Supports PDF invoices plus receipt/payment proof images. The tool returns a
structured JSON draft only; it never writes trusted records. Agents must ask
for human review before calling write_ingested_table or any project writer.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

try:
    from pydantic_ai import Agent, BinaryContent  # type: ignore[import]
except ImportError:  # pragma: no cover - pydantic_ai is a hard dependency
    Agent = None  # type: ignore[assignment]
    BinaryContent = None  # type: ignore[assignment]

_MAX_DOCUMENT_BYTES = 25 * 1024 * 1024
_MIME_BY_SUFFIX = {
    ".pdf": "application/pdf",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".webp": "image/webp",
    ".heic": "image/heic",
    ".heif": "image/heif",
}

_EXTRACTION_PROMPT = (
    "You are extracting a finance document for a small business operations "
    "workflow. The document may be a supplier invoice, customer invoice, "
    "receipt, payment proof, bank statement page, purchase order, or other "
    "finance artifact. Prioritize Malaysian MSME fields, including SST and "
    "e-Invoice readiness fields, but do not provide tax advice.\n\n"
    "Return one JSON object only. Never invent values. Use null when uncertain. "
    "Amounts must be numeric decimal values, not formatted strings. Dates must "
    "be ISO 8601 when visible. Currency defaults to MYR only when no other "
    "currency is visible.\n\n"
    "Required JSON schema:\n"
    "{\n"
    "  \"document_kind\": \"supplier_invoice\" | \"customer_invoice\" | "
    "\"receipt\" | \"payment_proof\" | \"bank_statement\" | "
    "\"purchase_order\" | \"other\",\n"
    "  \"direction\": \"ar\" | \"ap\" | \"payment\" | \"other\",\n"
    "  \"document_number\": string | null,\n"
    "  \"counterparty_name\": string | null,\n"
    "  \"counterparty_tax_id\": string | null,\n"
    "  \"issue_date\": string | null,\n"
    "  \"due_date\": string | null,\n"
    "  \"payment_date\": string | null,\n"
    "  \"po_number\": string | null,\n"
    "  \"bank_reference\": string | null,\n"
    "  \"currency\": string,\n"
    "  \"subtotal_amount\": number | null,\n"
    "  \"tax_amount\": number | null,\n"
    "  \"sst_amount\": number | null,\n"
    "  \"total_amount\": number | null,\n"
    "  \"line_items\": [{\"description\": string, \"sku\": string | null, "
    "\"quantity\": number | null, \"unit_price\": number | null, "
    "\"amount\": number | null}] | [],\n"
    "  \"readiness_flags\": [string],\n"
    "  \"confidence\": number,\n"
    "  \"raw_text\": string\n"
    "}\n\n"
    "Readiness flags should call out missing tax info, missing due date, "
    "missing PO, duplicate-looking document number, amount ambiguity, or "
    "low OCR confidence when visible. Return JSON only. No markdown."
)


def extract_finance_document(document_path: str, hint: str = "") -> str:
    """Extract finance fields from a PDF or receipt/payment image.

    The returned JSON is a DRAFT only. Use this for PDF invoices, purchase
    orders, receipts, bank statement pages, and payment proof images. The
    agent must present the draft for human review before writing normalized
    records.

    Args:
        document_path: Absolute or project-relative local path to PDF/image.
        hint: Optional free-text hint, e.g. "supplier invoice from textile mill".
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()
    path = _resolve_document_path(document_path, ctx.project_path)
    if path is None:
        return f"Error: document not found at {document_path}"

    suffix = path.suffix.lower()
    mime = _MIME_BY_SUFFIX.get(suffix)
    if not mime:
        return (
            f"Error: unsupported finance document format '{suffix}'. "
            f"Supported: {', '.join(sorted(_MIME_BY_SUFFIX))}."
        )

    try:
        size = path.stat().st_size
    except OSError as exc:
        return f"Error: cannot access document: {exc}"
    if size > _MAX_DOCUMENT_BYTES:
        return (
            f"Error: document is {size / 1024 / 1024:.1f} MB, exceeds "
            f"{_MAX_DOCUMENT_BYTES / 1024 / 1024:.0f} MB limit."
        )

    try:
        draft = _call_model(path, mime, hint)
    except Exception as exc:
        return f"Error: finance document extraction failed: {exc}"

    try:
        ctx.last_read_staging_path = str(path)
    except Exception:
        pass

    return _format_draft(path, draft, hint)


def _resolve_document_path(raw_path: str, project_path: Path) -> Path | None:
    candidate = Path(raw_path or "").expanduser()
    if not candidate:
        return None
    if not candidate.is_absolute():
        candidate = (project_path / candidate).resolve()
    else:
        candidate = candidate.resolve()
    if not candidate.exists() or not candidate.is_file():
        return None
    return candidate


def _call_model(path: Path, mime: str, hint: str) -> dict:
    from seeknal.ask.agents.providers import get_model_string

    if Agent is None or BinaryContent is None:
        raise RuntimeError(
            "pydantic_ai is required for finance document extraction."
        )

    prompt = _EXTRACTION_PROMPT
    if hint:
        prompt = f"{prompt}\n\nUser hint: {hint.strip()}"

    agent = Agent(get_model_string())
    result = agent.run_sync([
        prompt,
        BinaryContent(data=path.read_bytes(), media_type=mime),
    ])
    text = (result.output or "").strip()
    return _parse_json_output(text)


def _parse_json_output(raw: str) -> dict:
    text = raw.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"model returned non-JSON output: {exc}. Raw (first 500 chars): {text[:500]!r}"
        ) from exc
    if not isinstance(payload, dict):
        raise ValueError(f"model output was not a JSON object: {type(payload).__name__}")

    payload.setdefault("currency", "MYR")
    payload.setdefault("line_items", [])
    payload.setdefault("readiness_flags", [])
    payload.setdefault("confidence", 0)
    return payload


def _format_draft(path: Path, draft: dict, hint: str) -> str:
    missing = _missing_review_fields(draft)
    lines = ["## Extracted finance document draft"]
    lines.append(f"- **Source:** `{path.name}`")
    if hint:
        lines.append(f"- **Hint:** {hint}")
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps(draft, ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")
    if missing:
        lines.append("**Fields needing review:** " + ", ".join(missing))
    else:
        lines.append("No required review fields are obviously missing.")
    lines.append("")
    lines.append(
        "This is not a trusted accounting record yet. Present the draft to the "
        "user with `ask_user`, resolve missing fields, then normalize into the "
        "KAFI canonical tables or route through `propose_record_table` and "
        "`write_ingested_table`."
    )
    return "\n".join(lines)


def _missing_review_fields(draft: dict) -> list[str]:
    kind = str(draft.get("document_kind") or "").lower()
    required = ["document_kind", "counterparty_name", "total_amount"]
    if kind in {"supplier_invoice", "customer_invoice"}:
        required.extend(["document_number", "issue_date"])
    if kind == "supplier_invoice":
        required.append("due_date")
    if kind == "payment_proof":
        required.extend(["payment_date", "bank_reference"])
    return [field for field in required if not draft.get(field)]
