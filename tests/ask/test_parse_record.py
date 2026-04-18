"""Tests for parse_record."""

from __future__ import annotations

import json
import re

from seeknal.ask.agents.tools.parse_record import parse_record


def _payload(markdown: str) -> dict:
    match = re.search(r"```json\n(.+?)\n```", markdown, flags=re.DOTALL)
    assert match, f"no JSON block in output:\n{markdown}"
    return json.loads(match.group(1))


def test_person_plus_items():
    out = parse_record("/record fitra, 1 mie ayam, 1 mineral water")
    draft = _payload(out)
    assert draft["person"] == "fitra"
    assert draft["items"] == [
        {"quantity": 1, "name": "mie ayam"},
        {"quantity": 1, "name": "mineral water"},
    ]
    assert draft["amount"] is None
    # No "required fields" language — parse_record reports hints only.
    assert "Detected hints" in out
    assert "kind_hint" in out


def test_amount_with_thousands_separator():
    out = parse_record("/record transfer ke budi Rp 1.500.000 via BCA")
    draft = _payload(out)
    assert draft["amount"] == 1_500_000
    assert draft["bank"] == "BCA"


def test_amount_shorthand_rb():
    out = parse_record("/record 2 nasi goreng, 1 es teh, total 45rb")
    draft = _payload(out)
    assert draft["amount"] == 45_000
    assert draft["items"] == [
        {"quantity": 2, "name": "nasi goreng"},
        {"quantity": 1, "name": "es teh"},
    ]


def test_amount_shorthand_juta():
    out = parse_record("/record saham, 1.5jt, broker XYZ")
    draft = _payload(out)
    assert draft["amount"] == 1_500_000


def test_ewallet_detected():
    out = parse_record("record: 3 kopi, 75rb, dana")
    draft = _payload(out)
    assert draft["bank"] == "DANA"
    assert draft["amount"] == 75_000


def test_meeting_kind_hint():
    out = parse_record("/record meeting budi pagi")
    draft = _payload(out)
    assert draft["kind_hint"] == "meeting"
    # No "required" language — the agent decides what's required for a meeting.
    assert "Required fields" not in out


def test_transfer_kind_hint_with_qris():
    out = parse_record("/record transfer 500000 qris")
    draft = _payload(out)
    assert draft["amount"] == 500_000
    assert draft["bank"] == "QRIS"
    assert draft["kind_hint"] == "transfer"


def test_activity_kind_hint():
    out = parse_record("/record workout, 30 min running, 10 pushups")
    draft = _payload(out)
    assert draft["kind_hint"] == "activity"
    assert draft["amount"] is None  # amount is not required for activities


def test_health_kind_hint():
    out = parse_record("/record weight 72.5 kg, bp 120/80")
    draft = _payload(out)
    assert draft["kind_hint"] == "health"


def test_note_kind_hint_for_freeform():
    out = parse_record("/record just a random thought")
    draft = _payload(out)
    assert draft["kind_hint"] == "note"


def test_strips_prefix_variants():
    for prefix in ("/record ", "record: ", "/r "):
        out = parse_record(f"{prefix}budi, 10000")
        draft = _payload(out)
        assert draft["person"] == "budi"
        assert draft["amount"] == 10_000


def test_empty_input():
    out = parse_record("")
    assert json.loads(out) == {"error": "empty record"}


def test_qty_above_99_is_treated_as_amount():
    # "2500 mineral" is weird — lean toward item with qty; but "2500, mineral"
    # hits separate tokens and 2500 becomes the amount.
    out = parse_record("/record 2500, mineral water")
    draft = _payload(out)
    assert draft["amount"] == 2500


def test_raw_text_preserved():
    msg = "/record fitra, 1 mie ayam"
    out = parse_record(msg)
    draft = _payload(out)
    assert draft["raw_text"] == "fitra, 1 mie ayam"
