"""AC20: classifier.enabled: false → no LLM/catalog reads; v0.4 fallback intact."""

from __future__ import annotations

from pathlib import Path

from seeknal.heartbeat.classifier.catalog import Catalog
from seeknal.heartbeat.classifier.llm_classifier import LLMProposal
from seeknal.heartbeat.classifier.router import (
    ClassificationRouter,
    ClassifierConfig,
)


class CountingLLM:
    def __init__(self):
        self.calls = 0

    def classify(self, *args, **kwargs):
        self.calls += 1
        return LLMProposal(target_table="x", confidence=1.0)


def test_disabled_skips_llm(tmp_path: Path):
    src = tmp_path / "sales.csv"
    src.write_text("a\n1\n")
    config = ClassifierConfig(enabled=False)
    llm = CountingLLM()
    router = ClassificationRouter(config=config, catalog=Catalog(), llm=llm)
    decision = router.classify(src)
    assert decision.outcome == "default_fallback"
    assert decision.target_table == "ingested.sales"
    assert llm.calls == 0


def test_disabled_skips_catalog_match(tmp_path: Path):
    src = tmp_path / "any.csv"
    src.write_text("a\n1\n")
    # Even with a catalog that would match perfectly, disabled returns fallback.
    from seeknal.heartbeat.classifier.catalog import CatalogEntry
    from seeknal.heartbeat.classifier.models import Fingerprint

    catalog = Catalog()
    catalog.upsert(
        CatalogEntry(
            source_table="bronze.any",
            fingerprint=Fingerprint(column_names=["a"], column_types=["int"], value_regexes=[""]),
        )
    )
    config = ClassifierConfig(enabled=False)
    router = ClassificationRouter(config=config, catalog=catalog)
    decision = router.classify(src)
    assert decision.outcome == "default_fallback"
    assert decision.target_table == "ingested.any"
