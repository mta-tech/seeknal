"""Parse record tool — best-effort hint extractor for /record text messages.

Pure Python, no LLM. The job is NOT to be exhaustive and NOT to dictate
which fields are required. The agent classifies the record kind and
decides which fields matter via SKILL.md reasoning + ``ask_user``; this
tool just surfaces obvious signals so the agent doesn't waste a turn
re-spotting them.

Works for any record type, not just financial:

    "/record fitra, 1 mie ayam, 1 mineral water"
      -> hints: items=[...], person=fitra (agent decides kind = order)

    "/record transfer ke budi Rp 1.500.000 via BCA"
      -> hints: amount=1500000, bank=BCA (agent decides kind = transfer)

    "/record workout, 30 min running, 10 pushups"
      -> hints: items=[{qty:30,...}, {qty:10,...}] (agent decides
         kind = activity, treats items as exercises)

    "/record meeting with budi, discussed Q2 plan"
      -> hints: note="meeting with budi | discussed Q2 plan" (agent
         decides kind = meeting, extracts attendees + summary)

    "/record weight 72.5 kg, bp 120/80"
      -> hints: note preserved (agent decides kind = health, extracts
         metrics from raw_text)
"""

from __future__ import annotations

import json
import re
from typing import Any

_PREFIXES = ("/record ", "/record:", "/record", "record:", "record ", "/r ")

# Quantity-first patterns: "1 mie ayam", "2x nasi goreng", "3 * kopi"
_QTY_ITEM_RE = re.compile(r"^(\d{1,3})\s*[x*]?\s+(.{2,})$", re.IGNORECASE)

# Amount patterns (Indonesian-aware)
# Matches: 1.500.000 | 1,500,000 | 45000 | 1.5jt | 12.5k | 2500
# The `(?![a-zA-Z])` guard stops "m" in "mie ayam" from being mis-read as
# a "million" suffix during full-body sweeps.
_AMOUNT_TOKEN_RE = re.compile(
    r"(?i)"
    r"(?:Rp\.?\s*|IDR\s*|USD\s*|\$)?"
    r"(\d{1,3}(?:[.,]\d{3})+|\d+(?:[.,]\d+)?|\d+)"
    r"\s*(k|rb|ribu|m|jt|juta|mil|million)?(?![a-zA-Z])"
)
# Decimal form with a magnitude suffix (e.g. "1.5jt" = 1,500,000)
_DECIMAL_AMOUNT_RE = re.compile(r"^\d+[.,]\d+$")

# Bank / e-wallet hints (Indonesian context)
_BANK_TOKENS = {
    "bca", "mandiri", "bni", "bri", "cimb", "permata", "danamon",
    "btpn", "maybank", "panin", "bsi", "mega",
    "gopay", "ovo", "dana", "shopeepay", "linkaja", "qris",
}


def parse_record(text: str) -> str:
    """Extract best-effort hints from a free-form /record message.

    Returns a markdown scaffold that includes a JSON hint object plus the
    kind most likely implied by detected signals. The agent MUST:

      1. Treat the JSON as *hints*, not as the final schema.
      2. Classify the record kind itself (financial / activity / meeting
         / health / note / custom) based on the raw message — use
         `kind_hint` only as a starting point.
      3. Decide which fields are required for that kind and resolve any
         ambiguity via `ask_user` before writing.

    Accepts Indonesian and English. Handles amount shorthand (45rb,
    1.5jt) and bank/e-wallet keywords (BCA, GoPay, OVO, ...). No field
    is ever marked "required" — requiredness is the agent's call.

    Args:
        text: The raw message, e.g. "/record fitra, 1 mie ayam, 1 mineral water".
    """
    raw = (text or "").strip()
    if not raw:
        return json.dumps({"error": "empty record"}, ensure_ascii=False)

    body = _strip_prefix(raw)
    parts = [p.strip() for p in body.split(",") if p.strip()]

    person: str | None = None
    items: list[dict[str, Any]] = []
    amount: int | None = None
    currency: str = "IDR"
    bank: str | None = None
    note_parts: list[str] = []

    for idx, part in enumerate(parts):
        qi = _QTY_ITEM_RE.match(part)
        if qi:
            qty = int(qi.group(1))
            name = qi.group(2).strip()
            # Guard: a big leading number (>99) is more likely an amount than a qty
            if qty > 99 and not re.search(r"[a-zA-Z]", name):
                amt = _coerce_amount(part)
                if amt is not None:
                    amount = _prefer_amount(amount, amt)
                    continue
            items.append({"quantity": qty, "name": name})
            continue

        amt = _coerce_amount(part)
        if amt is not None and (amount is None or amt > amount):
            amount = amt
            continue

        bank_hit = _find_bank(part)
        if bank_hit and bank is None:
            bank = bank_hit

        # First non-item-looking segment with no digits is the likely person
        if (
            person is None
            and idx == 0
            and len(part.split()) <= 3
            and not any(ch.isdigit() for ch in part)
            and not bank_hit
        ):
            person = part.lower()
            continue

        note_parts.append(part)

    # Second pass: sweep the whole string for an amount if nothing found yet.
    if amount is None:
        matches = list(_AMOUNT_TOKEN_RE.finditer(body))
        candidates = [
            _coerce_amount_match(m) for m in matches
        ]
        candidates = [c for c in candidates if c is not None]
        if candidates:
            amount = max(candidates)

    # Second pass: sweep for a bank keyword anywhere
    if bank is None:
        low = body.lower()
        for b in _BANK_TOKENS:
            if re.search(rf"\b{re.escape(b)}\b", low):
                bank = b.upper() if len(b) <= 4 else b.capitalize()
                break

    kind_hint = _guess_kind(amount, bank, items, body)

    draft = {
        "kind_hint": kind_hint,
        "person": person,
        "items": items,
        "amount": amount,
        "currency": currency,
        "bank": bank,
        "note": " | ".join(note_parts) or None,
        "raw_text": body,
    }

    lines = ["## Detected hints"]
    lines.append("```json")
    lines.append(json.dumps(draft, ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")
    lines.append(
        f"**Kind hint:** `{kind_hint}` — a first guess. Re-classify from the "
        "raw message if this doesn't fit."
    )
    lines.append("")
    lines.append(
        "Next steps (record-entry SKILL.md):\n"
        "1. Classify the record kind yourself (financial, activity, meeting, "
        "health, note, or custom). Do not accept `kind_hint` blindly.\n"
        "2. Decide which fields are required for that kind.\n"
        "3. Call `ask_user` to resolve any required fields still null.\n"
        "4. Call `propose_record_table` with the enriched draft (pass "
        "`_columns` in the draft to override the template if needed) and "
        "route through `write_ingested_table`."
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _strip_prefix(text: str) -> str:
    for prefix in _PREFIXES:
        if text.lower().startswith(prefix):
            return text[len(prefix):].strip()
    return text


def _coerce_amount(token: str) -> int | None:
    """Return an integer amount from a single token, or None."""
    m = _AMOUNT_TOKEN_RE.fullmatch(token.strip())
    if not m:
        return None
    return _coerce_amount_match(m)


def _coerce_amount_match(m: "re.Match[str]") -> int | None:
    raw = m.group(1)
    suffix = (m.group(2) or "").lower()

    # Decimal form with magnitude suffix (e.g. "1.5jt" -> 1,500,000).
    if _DECIMAL_AMOUNT_RE.match(raw) and suffix:
        try:
            val = float(raw.replace(",", "."))
        except ValueError:
            return None
        if suffix in {"k", "rb", "ribu"}:
            val *= 1000
        elif suffix in {"m", "jt", "juta", "mil", "million"}:
            val *= 1_000_000
        return int(val)

    digits = raw.replace(".", "").replace(",", "")
    if not digits.isdigit():
        return None
    try:
        val = int(digits)
    except ValueError:
        return None
    if suffix in {"k", "rb", "ribu"}:
        val *= 1000
    elif suffix in {"m", "jt", "juta", "mil", "million"}:
        val *= 1_000_000
    # Sanity: anything under 100 as a standalone amount is probably a qty.
    if val < 100 and not suffix:
        return None
    return val


def _prefer_amount(current: int | None, candidate: int) -> int:
    return candidate if current is None or candidate > current else current


def _find_bank(token: str) -> str | None:
    low = token.lower().strip()
    for b in _BANK_TOKENS:
        if low == b or low.endswith(f" {b}") or low.startswith(f"{b} "):
            return b.upper() if len(b) <= 4 else b.capitalize()
    return None


_ACTIVITY_WORDS = {
    "workout", "running", "run", "jog", "walk", "gym", "yoga", "swim",
    "pushup", "pushups", "pullup", "pullups", "squat", "squats",
    "cycling", "bike", "lari", "jalan", "olahraga",
}
_MEETING_WORDS = {
    "meeting", "rapat", "call", "standup", "sync", "interview", "discussed",
    "bahas", "diskusi", "ngobrol",
}
_HEALTH_WORDS = {
    "weight", "berat", "bp", "tekanan", "blood", "pulse", "detak", "sugar",
    "glucose", "gula", "temp", "suhu", "sleep", "tidur",
}


def _guess_kind(
    amount: int | None,
    bank: str | None,
    items: list[dict],
    body: str,
) -> str:
    """Heuristic kind classifier used only as a hint — the agent has final say."""
    low = body.lower()
    tokens = set(re.findall(r"[a-z]+", low))

    if tokens & _HEALTH_WORDS:
        return "health"
    if tokens & _MEETING_WORDS:
        return "meeting"
    if bank:
        return "transfer"
    if amount and items:
        return "order"
    if amount and not items:
        return "expense"
    if items and tokens & _ACTIVITY_WORDS:
        return "activity"
    if tokens & _ACTIVITY_WORDS:
        return "activity"
    if items:
        return "order"
    return "note"
