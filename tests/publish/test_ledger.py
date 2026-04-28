from __future__ import annotations

import warnings
import shutil
from pathlib import Path

import pytest

from seeknal.publish.ledger import LedgerEntry, append_entry, find_by_report_name, list_entries


def _monkeypatch_ledger_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect the ledger path to a secure project-scoped temp directory."""
    ledger_home = Path.cwd() / ".seeknal" / "test-tmp" / tmp_path.name
    shutil.rmtree(ledger_home, ignore_errors=True)
    ledger_home.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("XDG_DATA_HOME", str(ledger_home))
    return ledger_home


def _make_entry(
    slug: str = "slug-001",
    server: str = "http://reports.local",
    share_url: str = "http://reports.local/r/slug-001",
    report_name: str = "my-report",
    published_at: str = "2026-04-01T10:00:00Z",
) -> LedgerEntry:
    return LedgerEntry(
        slug=slug,
        server=server,
        share_url=share_url,
        report_name=report_name,
        published_at=published_at,
    )


class TestAppendAndList:
    def test_append_and_list(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        _monkeypatch_ledger_home(tmp_path, monkeypatch)

        entry = _make_entry()
        append_entry(entry)

        entries = list_entries()
        assert len(entries) == 1
        assert entries[0].slug == "slug-001"
        assert entries[0].report_name == "my-report"
        assert entries[0].server == "http://reports.local"

    def test_append_multiple_accumulates(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        _monkeypatch_ledger_home(tmp_path, monkeypatch)

        append_entry(_make_entry(slug="slug-a", report_name="report-a"))
        append_entry(_make_entry(slug="slug-b", report_name="report-b"))

        entries = list_entries()
        assert len(entries) == 2
        slugs = {e.slug for e in entries}
        assert slugs == {"slug-a", "slug-b"}

    def test_entry_fields_round_trip(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        _monkeypatch_ledger_home(tmp_path, monkeypatch)

        original = LedgerEntry(
            slug="round-trip",
            server="https://srv.example.com",
            share_url="https://srv.example.com/r/round-trip",
            report_name="rt-report",
            published_at="2026-03-15T08:30:00Z",
        )
        append_entry(original)

        loaded = list_entries()[0]
        assert loaded.slug == original.slug
        assert loaded.server == original.server
        assert loaded.share_url == original.share_url
        assert loaded.report_name == original.report_name
        assert loaded.published_at == original.published_at


class TestFindByReportName:
    def test_find_by_report_name(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        _monkeypatch_ledger_home(tmp_path, monkeypatch)

        append_entry(_make_entry(slug="s1", report_name="alpha"))
        append_entry(_make_entry(slug="s2", report_name="beta"))
        append_entry(_make_entry(slug="s3", report_name="alpha"))

        results = find_by_report_name("alpha")
        assert len(results) == 2
        assert all(e.report_name == "alpha" for e in results)
        slugs = {e.slug for e in results}
        assert slugs == {"s1", "s3"}

    def test_find_by_report_name_no_match(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        _monkeypatch_ledger_home(tmp_path, monkeypatch)

        append_entry(_make_entry(slug="s1", report_name="alpha"))

        results = find_by_report_name("nonexistent")
        assert results == []

    def test_find_by_report_name_exact_match(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        _monkeypatch_ledger_home(tmp_path, monkeypatch)

        append_entry(_make_entry(slug="s1", report_name="alpha"))
        append_entry(_make_entry(slug="s2", report_name="alpha-v2"))

        results = find_by_report_name("alpha")
        assert len(results) == 1
        assert results[0].slug == "s1"


class TestEmptyLedger:
    def test_empty_ledger_returns_empty_list(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        _monkeypatch_ledger_home(tmp_path, monkeypatch)

        entries = list_entries()
        assert entries == []

    def test_find_on_empty_ledger_returns_empty_list(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        _monkeypatch_ledger_home(tmp_path, monkeypatch)

        results = find_by_report_name("any-name")
        assert results == []


class TestCorruptLedger:
    def test_corrupt_ledger_returns_empty_with_warning(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        ledger_home = _monkeypatch_ledger_home(tmp_path, monkeypatch)

        # Write garbage YAML that cannot be parsed as a list
        ledger_file = ledger_home / "seeknal" / "published_reports.yml"
        ledger_file.parent.mkdir(parents=True, exist_ok=True)
        ledger_file.write_text(":::not valid yaml:::\n\t\t\tbad indent")

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            entries = list_entries()

        assert entries == []
        # A warning should have been emitted
        assert len(caught) >= 1
        warning_messages = [str(w.message) for w in caught]
        assert any("ledger" in msg.lower() or "published_reports" in msg.lower() for msg in warning_messages)

    def test_non_list_yaml_returns_empty(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        ledger_home = _monkeypatch_ledger_home(tmp_path, monkeypatch)

        ledger_file = ledger_home / "seeknal" / "published_reports.yml"
        ledger_file.parent.mkdir(parents=True, exist_ok=True)
        # Valid YAML but not a list — _load_raw returns []
        ledger_file.write_text("key: value\nother: stuff\n")

        entries = list_entries()
        assert entries == []
