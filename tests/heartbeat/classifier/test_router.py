"""Router tests — high-confidence, low-confidence, disabled, fallback."""

from __future__ import annotations

from pathlib import Path

import pytest

from seeknal.heartbeat.classifier.catalog import Catalog, CatalogEntry
from seeknal.heartbeat.classifier.fingerprint import compute_fingerprint
from seeknal.heartbeat.classifier.llm_classifier import LLMProposal, NullLLMClassifier
from seeknal.heartbeat.classifier.mapper import Mapper
from seeknal.heartbeat.classifier.models import Fingerprint
from seeknal.heartbeat.classifier.router import (
    ClassificationRouter,
    ClassifierConfig,
)


def _csv(tmp_path: Path, name: str, body: str) -> Path:
    f = tmp_path / name
    f.write_text(body)
    return f


def test_disabled_falls_back_to_filename(tmp_path: Path):
    config = ClassifierConfig(enabled=False)
    router = ClassificationRouter(
        config=config, catalog=Catalog(), llm=NullLLMClassifier()
    )
    decision = router.classify(_csv(tmp_path, "sales.csv", "a\n1\n"))
    assert decision.outcome == "default_fallback"
    assert decision.target_table == "ingested.sales"


def test_high_confidence_auto_route(tmp_path: Path):
    catalog = Catalog()
    catalog.upsert(
        CatalogEntry(
            source_table="bronze.sales",
            fingerprint=Fingerprint(
                column_names=["order_id", "customer_id"],
                column_types=["string", "string"],
                value_regexes=["uuid", "uuid"],
            ),
            canonical_columns={
                "order_id": {"aliases": []},
                "customer_id": {"aliases": []},
            },
            write_mode="append",
            business_key=["order_id"],
        )
    )
    src = _csv(
        tmp_path,
        "sales.csv",
        "order_id,customer_id\nA,B\nC,D\n",
    )
    config = ClassifierConfig(enabled=True, confidence_threshold=0.5)
    router = ClassificationRouter(
        config=config, catalog=catalog, llm=NullLLMClassifier()
    )
    decision = router.classify(src)
    assert decision.outcome == "routed"
    assert decision.target_table == "bronze.sales"
    assert decision.source == "fingerprint"
    assert decision.business_key == ["order_id"]


def test_low_confidence_quarantine(tmp_path: Path):
    catalog = Catalog()  # empty
    src = _csv(tmp_path, "novel.csv", "a,b\n1,2\n")

    class FakeLLM:
        def classify(self, fp, catalog_summary, file_sample_preview):
            return LLMProposal(
                target_table="bronze.maybe",
                confidence=0.5,
                column_mapping={"a": "x"},
                concerns=["weak match"],
                call_id="msg-1",
            )

    config = ClassifierConfig(enabled=True, confidence_threshold=0.85)
    router = ClassificationRouter(config=config, catalog=catalog, llm=FakeLLM())
    decision = router.classify(src)
    assert decision.outcome == "quarantined"
    assert "below_threshold" in (decision.reason or "")
    assert decision.llm_call_id == "msg-1"


def test_unit_sensitive_quarantine(tmp_path: Path):
    catalog = Catalog()
    src = _csv(tmp_path, "rev.csv", "amount\n1.0\n")

    class FakeLLM:
        def classify(self, fp, catalog_summary, file_sample_preview):
            return LLMProposal(
                target_table="bronze.revenue",
                confidence=0.95,
                column_mapping={"amount": "amount"},
                call_id="msg-2",
            )

    config = ClassifierConfig(enabled=True)
    router = ClassificationRouter(config=config, catalog=catalog, llm=FakeLLM())
    decision = router.classify(src)
    assert decision.outcome == "quarantined"
    assert "unit_sensitive_columns" in (decision.reason or "")


def test_llm_outage_falls_back(tmp_path: Path):
    catalog = Catalog()
    src = _csv(tmp_path, "x.csv", "a\n1\n")

    class BoomLLM:
        def classify(self, *args, **kwargs):
            raise RuntimeError("anthropic offline")

    config = ClassifierConfig(enabled=True)
    router = ClassificationRouter(config=config, catalog=catalog, llm=BoomLLM())
    decision = router.classify(src)
    assert decision.outcome == "default_fallback"


def test_llm_success_routes(tmp_path: Path):
    catalog = Catalog()
    src = _csv(tmp_path, "x.csv", "a,b\n1,2\n")

    class GoodLLM:
        def classify(self, fp, catalog_summary, file_sample_preview):
            return LLMProposal(
                target_table="bronze.ok",
                confidence=0.95,
                column_mapping={"a": "a", "b": "b"},
                call_id="msg-3",
            )

    config = ClassifierConfig(enabled=True)
    router = ClassificationRouter(config=config, catalog=catalog, llm=GoodLLM())
    decision = router.classify(src)
    assert decision.outcome == "routed"
    assert decision.source == "llm"
    assert decision.target_table == "bronze.ok"
