"""Tests for Lakekeeper catalog URI env lookup."""

import pytest

from seeknal.utils.lakekeeper import lakekeeper_uri_from_env


@pytest.fixture(autouse=True)
def _clear_lakekeeper_env(monkeypatch):
    monkeypatch.delenv("LAKEKEEPER_URI", raising=False)
    monkeypatch.delenv("LAKEKEEPER_URL", raising=False)


def test_reads_canonical_uri(monkeypatch):
    monkeypatch.setenv("LAKEKEEPER_URI", "http://uri:8181")
    assert lakekeeper_uri_from_env() == "http://uri:8181"


def test_reads_url_alias(monkeypatch):
    monkeypatch.setenv("LAKEKEEPER_URL", "http://url:8181")
    assert lakekeeper_uri_from_env() == "http://url:8181"


def test_uri_takes_precedence_over_url(monkeypatch):
    monkeypatch.setenv("LAKEKEEPER_URI", "http://uri:8181")
    monkeypatch.setenv("LAKEKEEPER_URL", "http://url:8181")
    assert lakekeeper_uri_from_env() == "http://uri:8181"


def test_empty_uri_falls_through_to_url(monkeypatch):
    monkeypatch.setenv("LAKEKEEPER_URI", "  ")
    monkeypatch.setenv("LAKEKEEPER_URL", "http://url:8181")
    assert lakekeeper_uri_from_env() == "http://url:8181"


def test_returns_default_when_unset():
    assert lakekeeper_uri_from_env() == ""
    assert lakekeeper_uri_from_env("http://fallback:8181") == "http://fallback:8181"
