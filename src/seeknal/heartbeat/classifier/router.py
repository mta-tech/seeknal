"""ClassificationRouter — Phase A.5 orchestrator.

Inputs: a freshly-discovered file path.
Outputs: a :class:`ClassificationDecision` (routed / quarantined / fallback).

The router runs:
1. Fingerprint extraction (always; deterministic).
2. Catalog fuzzy match (deterministic).
3. Pass 1 column mapping (deterministic; uses catalog aliases).
4. If catalog match is high-confidence AND mapping covers ≥95% AND no
   unit-sensitive uncertainty → ROUTED.
5. Else if classifier disabled or LLM unavailable → DEFAULT_FALLBACK.
6. Else LLM proposal; high-confidence + no concerns → ROUTED.
7. Otherwise QUARANTINED.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from seeknal.heartbeat.classifier.catalog import Catalog, CatalogEntry
from seeknal.heartbeat.classifier.fingerprint import compute_fingerprint
from seeknal.heartbeat.classifier.llm_classifier import (
    LLMClassifier,
    LLMProposal,
    NullLLMClassifier,
)
from seeknal.heartbeat.classifier.mapper import Mapper
from seeknal.heartbeat.classifier.models import (
    ClassificationDecision,
    ColumnMapping,
    Fingerprint,
)

logger = logging.getLogger("seeknal.heartbeat.classifier.router")


@dataclass
class ClassifierConfig:
    enabled: bool = False
    provider: str = "anthropic"
    model: str = "claude-haiku-4-5"
    api_key_env: str = "ANTHROPIC_API_KEY"
    confidence_threshold: float = 0.85
    catalog_path: Optional[Path] = None
    unit_sensitive_keywords: Optional[list[str]] = None


class ClassificationRouter:
    def __init__(
        self,
        *,
        config: ClassifierConfig,
        catalog: Catalog,
        llm: Optional[LLMClassifier] = None,
        mapper: Optional[Mapper] = None,
    ):
        self._config = config
        self._catalog = catalog
        self._llm = llm or NullLLMClassifier()
        keywords = (
            config.unit_sensitive_keywords
            if config.unit_sensitive_keywords is not None
            else None
        )
        if keywords is None:
            self._mapper = mapper or Mapper()
        else:
            self._mapper = mapper or Mapper(unit_sensitive_keywords=list(keywords))

    def classify(self, path: Path) -> ClassificationDecision:
        if not self._config.enabled:
            return ClassificationDecision.default_for(path)
        try:
            fp = compute_fingerprint(path)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Fingerprint failed for %s: %s", path, exc)
            return ClassificationDecision.default_for(path)

        match = self._catalog.find_best_match(
            fp, threshold=self._config.confidence_threshold
        )
        if match is not None:
            mapping = self._build_mapping(fp, match.entry)
            if (
                mapping.coverage >= 0.95
                and not mapping.has_unit_uncertainty
            ):
                return ClassificationDecision.routed(
                    file=path,
                    target_table=match.source_table,
                    mapping=mapping,
                    source="fingerprint",
                    confidence=match.confidence,
                    write_mode=_to_write_mode(match.entry.write_mode),
                    business_key=list(match.entry.business_key),
                )

        # Below catalog threshold (or unit-sensitive uncertainty) → LLM
        try:
            proposal = self._llm.classify(
                fp,
                catalog_summary=self._summarize_catalog(),
                file_sample_preview="",
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("LLM classify failed (%s); falling back.", exc)
            return ClassificationDecision.default_for(path)

        return self._decide_from_proposal(path, fp, proposal)

    def _build_mapping(
        self, fp: Fingerprint, entry: CatalogEntry
    ) -> ColumnMapping:
        target_columns = list(entry.canonical_columns.keys()) or list(
            entry.fingerprint.column_names
        )
        aliases: dict[str, list[str]] = {}
        for canonical, body in entry.canonical_columns.items():
            if isinstance(body, dict):
                aliases[canonical] = list(body.get("aliases") or [])
        return self._mapper.map_columns(
            file_columns=fp.column_names,
            target_columns=target_columns,
            aliases=aliases,
        )

    def _summarize_catalog(self) -> str:
        lines: list[str] = []
        for name, entry in self._catalog.entries.items():
            lines.append(
                f"- {name}: cols={entry.fingerprint.column_names[:8]}"
            )
        return "\n".join(lines) or "(empty)"

    def _decide_from_proposal(
        self,
        path: Path,
        fp: Fingerprint,
        proposal: LLMProposal,
    ) -> ClassificationDecision:
        if not proposal.target_table:
            return ClassificationDecision.quarantined(
                file=path,
                reason="LLM did not propose a target table",
                confidence=proposal.confidence,
                llm_call_id=proposal.call_id,
            )

        # Unit-sensitive check on the proposed mapping.
        unit_keywords = self._mapper.unit_sensitive_keywords
        unit_concern = any(
            any(kw in tgt.lower() for kw in unit_keywords)
            for tgt in proposal.column_mapping.values()
        )

        threshold = self._config.confidence_threshold
        if (
            proposal.confidence < threshold
            or proposal.concerns
            or unit_concern
        ):
            reason_parts: list[str] = []
            if proposal.confidence < threshold:
                reason_parts.append("below_threshold")
            if proposal.concerns:
                reason_parts.append("has_concerns")
            if unit_concern:
                reason_parts.append("unit_sensitive_columns")
            return ClassificationDecision.quarantined(
                file=path,
                reason=";".join(reason_parts),
                proposed_table=proposal.target_table,
                confidence=proposal.confidence,
                llm_call_id=proposal.call_id,
                mapping=ColumnMapping(mapping=dict(proposal.column_mapping)),
            )

        mapping = ColumnMapping(mapping=dict(proposal.column_mapping))
        return ClassificationDecision.routed(
            file=path,
            target_table=proposal.target_table,
            mapping=mapping,
            source="llm",
            confidence=proposal.confidence,
            llm_call_id=proposal.call_id,
        )


def _to_write_mode(value: str):
    v = (value or "append").lower()
    if v in {"append", "upsert", "overwrite"}:
        return v  # type: ignore[return-value]
    return "append"


__all__ = ["ClassificationRouter", "ClassifierConfig"]
