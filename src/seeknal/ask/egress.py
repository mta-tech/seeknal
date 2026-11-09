"""Atlas Runtime Egress Tiers and provenance label filtering.

Governed Ask filters data before it reaches the model, ensuring model provider
policies (E0-E3) are strictly enforced based on Catalog provenance labels.
"""

from __future__ import annotations

import os
import re
from enum import Enum
from typing import Any, Sequence


class EgressTier(str, Enum):
    """Egress tiers defining what data may reach the model."""

    E0 = "E0"  # Schema, descriptions and metric definitions; no values
    E1 = "E1"  # E0 plus values only from columns with classification == null
    E2 = "E2"  # E1 plus masked values of sensitive columns (masked: true)
    E3 = "E3"  # All values the user may read


INJECTION_NOTICE = "possible instruction-like text in data; treated as data"

INJECTION_PATTERNS = [
    re.compile(r"ignore (?:(?:all|any|previous|prior)\s*)+instructions", re.IGNORECASE),
    re.compile(r"you are now", re.IGNORECASE),
    re.compile(r"system prompt", re.IGNORECASE),
    re.compile(r"call the .* tool", re.IGNORECASE),
]


class DroppedColumn(dict):
    """Metadata describing a column withheld from model egress."""

    def __init__(self, column: str, reason: str):
        super().__init__(column=column, reason=reason)
        self.column = column
        self.reason = reason

    def __str__(self) -> str:
        return f"column `{self.column}` withheld: {self.reason}"


def tier_from_env() -> EgressTier:
    """Read ATLAS_ASK_EGRESS_TIER from environment, defaulting to E1.

    Refuses governed startup on an invalid tier value.
    """
    raw = os.environ.get("ATLAS_ASK_EGRESS_TIER")
    if raw is None or not raw.strip():
        return EgressTier.E1
    val = raw.strip().upper()
    try:
        return EgressTier(val)
    except (ValueError, KeyError):
        from seeknal.ask.governed import GovernedAskConfigError

        raise GovernedAskConfigError(
            f"Invalid ATLAS_ASK_EGRESS_TIER: {raw!r}. Allowed tiers: E0, E1, E2, E3"
        )


def detect_instruction_text(rows: Sequence[Any]) -> bool:
    """Detect if any string cell matches common prompt-injection phrasing."""
    if not rows:
        return False
    for row in rows:
        if isinstance(row, dict):
            cells = row.values()
        elif isinstance(row, (tuple, list)):
            cells = row
        elif isinstance(row, str):
            cells = [row]
        else:
            continue
        for cell in cells:
            if isinstance(cell, str):
                for pat in INJECTION_PATTERNS:
                    if pat.search(cell):
                        return True
    return False


def filter_result(
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
    labels: Any,
    tier: EgressTier | str,
    *,
    is_schema: bool = False,
) -> tuple[list[str], list[Any], list[DroppedColumn]]:
    """Filter columns and rows by egress tier and Catalog provenance labels.

    Args:
        columns: Column names of the result set.
        rows: Data rows (tuples or lists).
        labels: Per-column metadata from Catalog response. Missing labels are
            treated as classification='phi' (fail-safe).
        tier: EgressTier enum or string ('E0', 'E1', 'E2', 'E3').
        is_schema: If True, indicates result is schema/introspection definitions
            which are allowed in all tiers (including E0).

    Returns:
        (filtered_columns, filtered_rows, report)
    """
    if isinstance(tier, str):
        tier = EgressTier(tier.strip().upper())

    columns_list = list(columns)
    report: list[DroppedColumn] = []

    # E0: keeps column names and types, drops all rows (unless is_schema=True)
    if tier == EgressTier.E0:
        if is_schema:
            return columns_list, list(rows), []
        return columns_list, [], []

    # E3: allows all values the user may read
    if tier == EgressTier.E3:
        return columns_list, list(rows), []

    # Map labels by column name
    col_labels: dict[str, dict[str, Any]] = {}
    if isinstance(labels, dict):
        col_labels = labels
    elif isinstance(labels, (list, tuple)):
        has_names = any(isinstance(item, dict) and "name" in item for item in labels)
        if has_names:
            for item in labels:
                if isinstance(item, dict) and "name" in item:
                    col_labels[item["name"]] = item
        elif len(labels) == len(columns_list):
            for col_name, item in zip(columns_list, labels):
                if isinstance(item, dict):
                    col_labels[col_name] = item
                else:
                    col_labels[col_name] = {"classification": item, "masked": False}

    kept_indices: list[int] = []
    for idx, col in enumerate(columns_list):
        if col in col_labels:
            info = col_labels[col]
            if isinstance(info, dict):
                classification = info.get("classification") if "classification" in info else "phi"
                masked = bool(info.get("masked", False))
            else:
                classification = str(info)
                masked = False
        else:
            # Missing labels treated as phi (fail-safe)
            classification = "phi"
            masked = False

        if classification is None:
            # Unclassified column is allowed in E1 and E2
            kept_indices.append(idx)
        elif tier == EgressTier.E1:
            # In E1, any pii, phi or mask column is dropped entirely, not sent masked
            reason = f"classified {classification}, egress tier E1"
            report.append(DroppedColumn(col, reason))
        elif tier == EgressTier.E2:
            # In E2, masked values of sensitive columns are allowed; unmasked are dropped
            if masked:
                kept_indices.append(idx)
            else:
                reason = f"classified {classification}, egress tier E2"
                report.append(DroppedColumn(col, reason))

    filtered_columns = [columns_list[i] for i in kept_indices]
    filtered_rows: list[Any] = []
    for row in rows:
        if isinstance(row, tuple):
            filtered_rows.append(tuple(row[i] for i in kept_indices if i < len(row)))
        elif isinstance(row, list):
            filtered_rows.append([row[i] for i in kept_indices if i < len(row)])
        elif isinstance(row, dict):
            filtered_rows.append({col: row.get(col) for col in filtered_columns})
        else:
            filtered_rows.append(row)

    return filtered_columns, filtered_rows, report


__all__ = [
    "EgressTier",
    "DroppedColumn",
    "INJECTION_NOTICE",
    "INJECTION_PATTERNS",
    "tier_from_env",
    "detect_instruction_text",
    "filter_result",
]
