"""Catalog persistence tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from seeknal.heartbeat.classifier.catalog import Catalog, CatalogEntry
from seeknal.heartbeat.classifier.models import Fingerprint


def test_catalog_missing_file_returns_empty(tmp_path: Path):
    catalog = Catalog.load(tmp_path)
    assert catalog.entries == {}


def test_catalog_insecure_path():
    with pytest.raises(ValueError, match="Insecure catalog"):
        Catalog.load(Path("/tmp"))


def test_save_and_load_round_trip(tmp_path: Path):
    catalog = Catalog(source_path=tmp_path / ".seeknal/source_catalog.yml")
    catalog.upsert(
        CatalogEntry(
            source_table="bronze.sales",
            fingerprint=Fingerprint(
                column_names=["id", "amount"],
                column_types=["string", "float"],
                value_regexes=["uuid", "currency_usd"],
            ),
            canonical_columns={"id": {"aliases": ["order_id"]}},
            write_mode="upsert",
            business_key=["id"],
        )
    )
    catalog.save()

    loaded = Catalog.load(tmp_path)
    assert "bronze.sales" in loaded.entries
    entry = loaded.entries["bronze.sales"]
    assert entry.write_mode == "upsert"
    assert entry.business_key == ["id"]


def test_find_best_match_above_threshold(tmp_path: Path):
    catalog = Catalog(source_path=tmp_path / "x.yml")
    catalog.upsert(
        CatalogEntry(
            source_table="orders",
            fingerprint=Fingerprint(
                column_names=["order_id", "customer_id", "amount"],
                column_types=["string", "string", "float"],
                value_regexes=["uuid", "uuid", "currency_usd"],
            ),
        )
    )
    fp = Fingerprint(
        column_names=["order_id", "customer_id", "amount"],
        column_types=["string", "string", "float"],
        value_regexes=["uuid", "uuid", "currency_usd"],
    )
    match = catalog.find_best_match(fp, threshold=0.8)
    assert match is not None
    assert match.source_table == "orders"


def test_find_best_match_below_threshold(tmp_path: Path):
    catalog = Catalog(source_path=tmp_path / "x.yml")
    catalog.upsert(
        CatalogEntry(
            source_table="orders",
            fingerprint=Fingerprint(
                column_names=["a", "b"],
                column_types=["int", "int"],
                value_regexes=["", ""],
            ),
        )
    )
    fp = Fingerprint(
        column_names=["x", "y", "z"],
        column_types=["string", "string", "string"],
        value_regexes=["", "", ""],
    )
    assert catalog.find_best_match(fp, threshold=0.9) is None


def test_corrupt_yaml_starts_empty(tmp_path: Path):
    path = tmp_path / ".seeknal" / "source_catalog.yml"
    path.parent.mkdir()
    path.write_text("not: [valid yaml")
    catalog = Catalog.load(tmp_path)
    assert catalog.entries == {}
    # Corrupt file renamed aside
    corrupted = list(path.parent.glob("source_catalog.yml.corrupt.*"))
    assert corrupted, "Expected corrupt file rename"


def test_alias_learning(tmp_path: Path):
    """AC14 surface: confirming a mapping adds aliases that route the next file."""
    catalog = Catalog(source_path=tmp_path / ".seeknal/source_catalog.yml")
    entry = CatalogEntry(
        source_table="bronze.sales",
        fingerprint=Fingerprint(
            column_names=["order_id", "amount"],
            column_types=["string", "float"],
            value_regexes=["uuid", "currency_usd"],
        ),
        canonical_columns={"order_id": {"aliases": []}, "amount": {"aliases": []}},
    )
    catalog.upsert(entry)
    # Operator confirms: new alias added.
    entry.canonical_columns["order_id"]["aliases"].append("orderid")
    catalog.upsert(entry)
    catalog.save()

    loaded = Catalog.load(tmp_path)
    aliases = loaded.entries["bronze.sales"].canonical_columns["order_id"]["aliases"]
    assert "orderid" in aliases
