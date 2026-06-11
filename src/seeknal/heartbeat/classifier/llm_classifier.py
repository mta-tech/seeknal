"""LLM-backed classifier (optional, requires ``pip install seeknal[classifier]``)."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional, Protocol

from seeknal.heartbeat.classifier.models import Fingerprint

logger = logging.getLogger("seeknal.heartbeat.classifier.llm")


@dataclass
class LLMProposal:
    target_table: str
    confidence: float
    column_mapping: dict[str, str] = field(default_factory=dict)
    reasoning: str = ""
    concerns: list[str] = field(default_factory=list)
    call_id: str = ""


class LLMClassifier(Protocol):
    """Provider-pluggable classifier."""

    def classify(
        self,
        fp: Fingerprint,
        catalog_summary: str,
        file_sample_preview: str,
    ) -> LLMProposal:
        ...


class NullLLMClassifier:
    """No-op classifier — returns a zero-confidence proposal."""

    def classify(
        self,
        fp: Fingerprint,
        catalog_summary: str,
        file_sample_preview: str,
    ) -> LLMProposal:  # pragma: no cover — trivial
        del fp, catalog_summary, file_sample_preview
        return LLMProposal(
            target_table="",
            confidence=0.0,
            concerns=["LLM classifier disabled"],
        )


def _require_anthropic():
    try:
        import anthropic  # type: ignore  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "Classifier requires `pip install seeknal[classifier]` "
            "(anthropic SDK not found). "
            "Disable the classifier in HEARTBEAT.md (classifier.enabled: false) "
            "to run without it."
        ) from exc


class AnthropicLLMClassifier:
    """Anthropic Claude Haiku wrapper. Lazy SDK import for soft dependency.

    Behavior on outage/timeout: raises RuntimeError; the router catches and
    falls back to the deterministic default.
    """

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        model: str = "claude-haiku-4-5",
        timeout_s: float = 30.0,
    ):
        self.api_key = api_key
        self.model = model
        self.timeout_s = timeout_s

    def classify(
        self,
        fp: Fingerprint,
        catalog_summary: str,
        file_sample_preview: str,
    ) -> LLMProposal:
        _require_anthropic()
        import anthropic  # type: ignore

        client = anthropic.Anthropic(api_key=self.api_key, timeout=self.timeout_s)
        prompt = self._build_prompt(fp, catalog_summary, file_sample_preview)
        try:
            response = client.messages.create(
                model=self.model,
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}],
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Anthropic call failed: %s", exc)
            raise RuntimeError(f"LLM provider error: {exc}") from exc

        call_id = getattr(response, "id", "") or ""
        text = ""
        for block in getattr(response, "content", []) or []:
            text += getattr(block, "text", "")
        proposal = self._parse_response(text, call_id)
        return proposal

    def _build_prompt(
        self,
        fp: Fingerprint,
        catalog_summary: str,
        file_sample_preview: str,
    ) -> str:
        return (
            "You are a strict data-routing classifier. Decide whether the file "
            "matches one of the known source tables in the catalog, or whether "
            "it should be quarantined for human review.\n\n"
            f"FILE FINGERPRINT:\n"
            f"  columns: {fp.column_names}\n"
            f"  types:   {fp.column_types}\n"
            f"  regexes: {fp.value_regexes}\n"
            f"  rows_in_sample: {fp.row_count_sample}\n\n"
            f"FILE PREVIEW (first rows):\n{file_sample_preview}\n\n"
            f"CATALOG SUMMARY:\n{catalog_summary}\n\n"
            "Respond ONLY with a JSON object: "
            '{"target_table": "<name or empty>", "confidence": <0..1>, '
            '"column_mapping": {"<file_col>": "<target_col>"}, '
            '"reasoning": "<short>", "concerns": ["..."]}'
        )

    def _parse_response(self, text: str, call_id: str) -> LLMProposal:
        import json
        import re

        json_match = re.search(r"\{.*\}", text, re.DOTALL)
        if not json_match:
            raise RuntimeError("LLM response did not contain JSON")
        try:
            payload = json.loads(json_match.group(0))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"LLM response JSON parse failed: {exc}") from exc

        return LLMProposal(
            target_table=str(payload.get("target_table", "") or ""),
            confidence=float(payload.get("confidence", 0.0)),
            column_mapping=dict(payload.get("column_mapping") or {}),
            reasoning=str(payload.get("reasoning", "") or ""),
            concerns=list(payload.get("concerns", []) or []),
            call_id=call_id,
        )


__all__ = [
    "AnthropicLLMClassifier",
    "LLMClassifier",
    "LLMProposal",
    "NullLLMClassifier",
]
