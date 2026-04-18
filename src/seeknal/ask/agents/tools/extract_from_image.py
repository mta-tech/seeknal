"""Extract from image tool — Gemini-vision OCR for receipts and transfer proofs.

Spawns a short-lived pydantic-ai Agent bound to the same Google Gemini model
used by the main ask agent, passes the image as a ``BinaryContent`` part,
and asks for a structured JSON draft. The tool does NOT write anywhere — it
returns the draft for the agent to inspect and confirm via ``ask_user``.

Prioritises Indonesian banking / e-wallet terminology (BCA, Mandiri, GoPay,
OVO, DANA, QRIS, ShopeePay) since fund-transfer proof screenshots are the
most common input in that market.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

# Top-level imports so tests can patch `extract_from_image.Agent` /
# `extract_from_image.BinaryContent` without triggering the deferred import.
try:
    from pydantic_ai import Agent, BinaryContent  # type: ignore[import]
except ImportError:  # pragma: no cover — pydantic_ai is a hard dep of seeknal[ask]
    Agent = None  # type: ignore[assignment]
    BinaryContent = None  # type: ignore[assignment]

_MAX_IMAGE_BYTES = 20 * 1024 * 1024  # 20 MB
_MIME_BY_SUFFIX = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".webp": "image/webp",
    ".heic": "image/heic",
    ".heif": "image/heif",
}

_EXTRACTION_PROMPT = (
    "You are analysing an image commonly shared in Indonesian chats: a bank "
    "transfer proof (BCA, Mandiri, BNI, BRI, CIMB, BSI), an e-wallet receipt "
    "(GoPay, OVO, DANA, ShopeePay, LinkAja), a QRIS confirmation, or a shop "
    "receipt.\n\n"
    "Extract the fields below. Use null if you are not confident. Never "
    "invent values. All amounts must be plain integers in the smallest unit "
    "of the currency (so IDR 1,500,000 becomes 1500000). Dates must be ISO "
    "8601.\n\n"
    "Required JSON schema:\n"
    "{\n"
    "  \"kind\": \"transfer\" | \"receipt\" | \"order\" | \"other\",\n"
    "  \"amount\": integer | null,\n"
    "  \"currency\": string (default \"IDR\"),\n"
    "  \"sender_name\": string | null,\n"
    "  \"sender_account\": string | null,\n"
    "  \"recipient_name\": string | null,\n"
    "  \"recipient_account\": string | null,\n"
    "  \"bank\": string | null,     // e.g. BCA, Mandiri, GoPay, QRIS\n"
    "  \"transaction_date\": string | null,\n"
    "  \"reference_number\": string | null,\n"
    "  \"merchant\": string | null, // for receipts / orders\n"
    "  \"items\": [{\"name\": string, \"quantity\": integer | null, "
    "\"price\": integer | null}] | [],\n"
    "  \"note\": string | null,    // memo / berita / keterangan\n"
    "  \"raw_text\": string        // full OCR text for audit\n"
    "}\n\n"
    "Return the JSON object only. No markdown fences. No prose before or "
    "after."
)


def extract_from_image(image_path: str, hint: str = "") -> str:
    """Run Gemini vision on an image and return a structured JSON draft.

    Use this for fund-transfer proofs, receipts, QRIS confirmations, and
    order photos. Do NOT use it for tabular data (spreadsheets, PDFs of
    tables, screenshots of CSVs) — use ``read_tabular`` for those.

    The returned JSON is a DRAFT only. The agent must confirm or resolve
    missing fields via ``ask_user`` before calling ``write_ingested_table``.

    Args:
        image_path: Absolute path to a local image file (jpg, png, webp, heic).
        hint: Optional free-text hint about what the image contains, e.g.
            'BCA mobile transfer receipt' or 'Warteg dinner bill'.
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()

    path = Path(image_path).expanduser()
    if not path.is_absolute():
        path = (ctx.project_path / path).resolve()
    else:
        path = path.resolve()
    if not path.exists() or not path.is_file():
        return f"Error: image not found at {image_path}"

    suffix = path.suffix.lower()
    mime = _MIME_BY_SUFFIX.get(suffix)
    if not mime:
        return (
            f"Error: unsupported image format '{suffix}'. "
            f"Supported: {', '.join(sorted(_MIME_BY_SUFFIX))}."
        )

    size = path.stat().st_size
    if size > _MAX_IMAGE_BYTES:
        return (
            f"Error: image is {size / 1024 / 1024:.1f} MB, exceeds "
            f"{_MAX_IMAGE_BYTES / 1024 / 1024:.0f} MB limit."
        )

    try:
        draft = _call_gemini(path, mime, hint)
    except Exception as exc:
        return f"Error: vision extraction failed: {exc}"

    # Stash the image path so write_ingested_table can emit it in provenance
    # (source_file will hash the image, giving us dedup-by-image-content).
    try:
        ctx.last_read_staging_path = str(path)
    except Exception:
        pass

    lines = ["## Extracted draft from image"]
    lines.append(f"- **Source:** `{path.name}`")
    if hint:
        lines.append(f"- **Hint:** {hint}")
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps(draft, ensure_ascii=False, indent=2))
    lines.append("```")

    missing = _missing_required(draft)
    if missing:
        lines.append("")
        lines.append(
            "**Required fields not yet resolved:** " + ", ".join(missing)
        )
        lines.append(
            "Call `ask_user` to confirm each missing field before calling "
            "`write_ingested_table`."
        )
    else:
        lines.append("")
        lines.append(
            "All required fields extracted. Confirm the draft with the user "
            "via `ask_user` (they may spot OCR mistakes), then propose a "
            "target table with `propose_record_table` before writing."
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _call_gemini(path: Path, mime: str, hint: str) -> dict:
    """Run the vision model and return the parsed JSON draft."""
    from seeknal.ask.agents.providers import get_model_string

    if Agent is None or BinaryContent is None:
        raise RuntimeError(
            "pydantic_ai is required for image extraction; install seeknal[ask]."
        )

    image_bytes = path.read_bytes()
    model_string = get_model_string()

    prompt = _EXTRACTION_PROMPT
    if hint:
        prompt = f"{prompt}\n\nUser hint: {hint.strip()}"

    agent = Agent(model_string)
    result = agent.run_sync(
        [
            prompt,
            BinaryContent(data=image_bytes, media_type=mime),
        ]
    )
    text = (result.output or "").strip()
    return _parse_json_output(text)


def _parse_json_output(raw: str) -> dict:
    """Strip markdown fences (if any) and parse JSON defensively."""
    text = raw.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Gemini returned non-JSON output: {exc}. Raw (first 500 chars): {text[:500]!r}"
        )
    if not isinstance(data, dict):
        raise ValueError(f"Gemini output was not a JSON object: {type(data).__name__}")
    # Normalise defaults
    data.setdefault("currency", "IDR")
    data.setdefault("items", [])
    return data


def _missing_required(draft: dict) -> list[str]:
    """Required fields depend on the `kind`."""
    kind = (draft.get("kind") or "").lower()
    required: list[str] = []
    if kind == "transfer":
        required = ["amount", "recipient_name", "transaction_date"]
    elif kind in {"receipt", "order"}:
        required = ["amount", "merchant"]
    else:
        required = ["amount"]
    return [k for k in required if not draft.get(k)]
