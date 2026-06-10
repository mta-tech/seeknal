"""Classifier dataclasses."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Optional


@dataclass
class Fingerprint:
    column_names: list[str]
    column_types: list[str]
    value_regexes: list[str]
    row_count_sample: int = 0
    column_count: int = 0


@dataclass
class ColumnMapping:
    mapping: dict[str, str] = field(default_factory=dict)  # file_col -> target_col
    unmapped_file_cols: list[str] = field(default_factory=list)
    unmapped_target_cols: list[str] = field(default_factory=list)
    has_unit_uncertainty: bool = False

    @property
    def coverage(self) -> float:
        total = len(self.mapping) + len(self.unmapped_file_cols)
        if total == 0:
            return 1.0
        return len(self.mapping) / total


@dataclass
class ClassificationDecision:
    file: Path
    outcome: Literal["routed", "quarantined", "default_fallback"]
    target_table: str
    source: Literal["fingerprint", "llm", "fallback"] = "fallback"
    confidence: float = 0.0
    mapping: ColumnMapping = field(default_factory=ColumnMapping)
    write_mode: Literal["append", "upsert", "overwrite"] = "append"
    business_key: list[str] = field(default_factory=list)
    llm_call_id: Optional[str] = None
    reason: Optional[str] = None  # populated for quarantine

    @classmethod
    def default_for(cls, path: Path) -> "ClassificationDecision":
        """v0.4 fallback: filename-stem → ``ingested.<stem>``."""
        stem = path.stem.lower()
        return cls(
            file=path,
            outcome="default_fallback",
            target_table=f"ingested.{stem}",
            source="fallback",
            confidence=0.0,
        )

    @classmethod
    def routed(
        cls,
        *,
        file: Path,
        target_table: str,
        mapping: ColumnMapping,
        source: Literal["fingerprint", "llm"],
        confidence: float,
        write_mode: Literal["append", "upsert", "overwrite"] = "append",
        business_key: Optional[list[str]] = None,
        llm_call_id: Optional[str] = None,
    ) -> "ClassificationDecision":
        return cls(
            file=file,
            outcome="routed",
            target_table=target_table,
            mapping=mapping,
            source=source,
            confidence=confidence,
            write_mode=write_mode,
            business_key=business_key or [],
            llm_call_id=llm_call_id,
        )

    @classmethod
    def quarantined(
        cls,
        *,
        file: Path,
        reason: str,
        proposed_table: str = "",
        confidence: float = 0.0,
        llm_call_id: Optional[str] = None,
        mapping: Optional[ColumnMapping] = None,
    ) -> "ClassificationDecision":
        return cls(
            file=file,
            outcome="quarantined",
            target_table=proposed_table,
            source="llm" if llm_call_id else "fingerprint",
            confidence=confidence,
            mapping=mapping or ColumnMapping(),
            llm_call_id=llm_call_id,
            reason=reason,
        )


__all__ = ["ClassificationDecision", "ColumnMapping", "Fingerprint"]
