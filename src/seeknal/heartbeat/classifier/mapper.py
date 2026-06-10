"""Column mapper — Pass 1 deterministic, Pass 2 LLM-assisted (optional).

Pass 1 is always run and covers:
- Exact match.
- Snake_case / camelCase normalization.
- Catalog alias lookup.
- Levenshtein distance ≤ 2 (cheap stdlib fallback).

Pass 2 (LLM) is invoked only when Pass 1 leaves >10% of the target schema
unmapped AND ``classifier.enabled`` is true. Unit-sensitive numeric columns
are never auto-mapped regardless of confidence — they force a quarantine.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable, Optional

from seeknal.heartbeat.classifier.models import ColumnMapping

DEFAULT_UNIT_SENSITIVE_KEYWORDS = [
    # English
    "amount", "total", "revenue", "price", "cost", "weight", "duration",
    "tax", "fee", "discount",
    # Indonesian
    "jumlah", "harga", "biaya", "pendapatan", "berat", "durasi", "pajak", "diskon",
]


def _normalize(name: str) -> str:
    snake = re.sub(r"(?<!^)(?=[A-Z])", "_", name.strip()).lower()
    return re.sub(r"[^a-z0-9]+", "_", snake).strip("_")


def _levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if len(a) > len(b):
        a, b = b, a
    if not a:
        return len(b)
    previous = list(range(len(a) + 1))
    for j, bc in enumerate(b, 1):
        current = [j]
        for i, ac in enumerate(a, 1):
            cost = 0 if ac == bc else 1
            current.append(min(
                current[-1] + 1,
                previous[i] + 1,
                previous[i - 1] + cost,
            ))
        previous = current
    return previous[-1]


@dataclass
class Mapper:
    """Maps file column names to target schema column names."""

    unit_sensitive_keywords: list[str] = field(
        default_factory=lambda: list(DEFAULT_UNIT_SENSITIVE_KEYWORDS)
    )

    def map_columns(
        self,
        file_columns: Iterable[str],
        target_columns: Iterable[str],
        *,
        aliases: Optional[dict[str, list[str]]] = None,
    ) -> ColumnMapping:
        """Run Pass 1 deterministic mapping."""
        aliases = aliases or {}
        alias_lookup: dict[str, str] = {}
        for canonical, alts in aliases.items():
            for alt in alts:
                alias_lookup[_normalize(alt)] = canonical

        target_norm = {_normalize(c): c for c in target_columns}

        mapping: dict[str, str] = {}
        claimed: set[str] = set()
        unmapped: list[str] = []
        for col in file_columns:
            norm = _normalize(col)
            if norm in target_norm and target_norm[norm] not in claimed:
                mapping[col] = target_norm[norm]
                claimed.add(target_norm[norm])
                continue
            if norm in alias_lookup and alias_lookup[norm] not in claimed:
                mapping[col] = alias_lookup[norm]
                claimed.add(alias_lookup[norm])
                continue
            # Fuzzy: Levenshtein ≤ 2
            best: Optional[str] = None
            best_dist = 3
            for tnorm, tcol in target_norm.items():
                if tcol in claimed:
                    continue
                dist = _levenshtein(norm, tnorm)
                if dist < best_dist:
                    best_dist = dist
                    best = tcol
            if best is not None and best_dist <= 2:
                mapping[col] = best
                claimed.add(best)
                continue
            unmapped.append(col)

        unmapped_target = [
            t for t in target_columns if t not in set(mapping.values())
        ]

        unit_uncertain = any(
            any(kw in _normalize(target_col) for kw in self.unit_sensitive_keywords)
            for target_col in mapping.values()
        )

        return ColumnMapping(
            mapping=mapping,
            unmapped_file_cols=unmapped,
            unmapped_target_cols=unmapped_target,
            has_unit_uncertainty=unit_uncertain,
        )

    def has_unit_sensitive(self, target_col: str) -> bool:
        norm = _normalize(target_col)
        return any(kw in norm for kw in self.unit_sensitive_keywords)


__all__ = ["Mapper", "DEFAULT_UNIT_SENSITIVE_KEYWORDS"]
