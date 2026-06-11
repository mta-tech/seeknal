"""Shared fixtures for classifier tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from seeknal.heartbeat.classifier.catalog import Catalog, CatalogEntry
from seeknal.heartbeat.classifier.models import Fingerprint


@pytest.fixture
def sales_fingerprint() -> Fingerprint:
    return Fingerprint(
        column_names=["order_id", "customer_id", "amount", "order_date"],
        column_types=["string", "string", "float", "date"],
        value_regexes=["uuid", "uuid", "currency_usd", "iso_date"],
        row_count_sample=100,
        column_count=4,
    )


@pytest.fixture
def sales_catalog_entry(sales_fingerprint: Fingerprint) -> CatalogEntry:
    return CatalogEntry(
        source_table="bronze.sales",
        fingerprint=sales_fingerprint,
        canonical_columns={
            "order_id": {"aliases": ["orderid", "order_no"]},
            "customer_id": {"aliases": ["cust_id", "customer"]},
            "amount": {"aliases": ["total"]},
            "order_date": {"aliases": ["date", "transaction_date"]},
        },
        write_mode="upsert",
        business_key=["order_id"],
    )


@pytest.fixture
def catalog_with_sales(tmp_path: Path, sales_catalog_entry: CatalogEntry) -> Catalog:
    catalog = Catalog(source_path=tmp_path / ".seeknal/source_catalog.yml")
    catalog.upsert(sales_catalog_entry)
    return catalog


class FakeLLM:
    """A scripted LLM stub for router tests."""

    def __init__(self, proposal):
        self._proposal = proposal
        self.calls = 0

    def classify(self, fp, catalog_summary, file_sample_preview):
        self.calls += 1
        return self._proposal


@pytest.fixture
def fake_llm():
    return FakeLLM
