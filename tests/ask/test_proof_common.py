"""Tests for the shared Proof helper module."""

from __future__ import annotations


class TestParseProofUrl:
    def test_full_share_url_with_token(self):
        from seeknal.ask.agents.tools._proof_common import parse_proof_url

        r = parse_proof_url("https://memokami.exe.xyz/d/abc1234?token=secrettok")
        assert isinstance(r, dict)
        assert r["base"] == "https://memokami.exe.xyz"
        assert r["slug"] == "abc1234"
        assert r["token"] == "secrettok"

    def test_share_url_without_token(self):
        from seeknal.ask.agents.tools._proof_common import parse_proof_url

        r = parse_proof_url("https://proofeditor.ai/d/xyz")
        assert isinstance(r, dict)
        assert r["base"] == "https://proofeditor.ai"
        assert r["slug"] == "xyz"
        assert r["token"] == ""

    def test_bare_slug(self):
        from seeknal.ask.agents.tools._proof_common import parse_proof_url

        r = parse_proof_url("abc123")
        assert isinstance(r, dict)
        assert r["slug"] == "abc123"
        assert r["base"] == ""
        assert r["token"] == ""

    def test_empty_returns_error(self):
        from seeknal.ask.agents.tools._proof_common import parse_proof_url

        r = parse_proof_url("")
        assert isinstance(r, str)
        assert "empty" in r.lower()

    def test_invalid_scheme_returns_error(self):
        from seeknal.ask.agents.tools._proof_common import parse_proof_url

        r = parse_proof_url("ftp://bad/d/x")
        assert isinstance(r, str)
        assert "http" in r

    def test_http_scheme_accepted(self):
        from seeknal.ask.agents.tools._proof_common import parse_proof_url

        r = parse_proof_url("http://localhost:4000/d/doc1?token=t")
        assert isinstance(r, dict)
        assert r["slug"] == "doc1"
        assert r["base"] == "http://localhost:4000"
        assert r["token"] == "t"

    def test_missing_host_returns_error(self):
        from seeknal.ask.agents.tools._proof_common import parse_proof_url

        r = parse_proof_url("https:///d/x")
        assert isinstance(r, str)
        assert "host" in r.lower()


class TestResolveBaseUrl:
    def test_default_is_memokami(self, monkeypatch):
        from seeknal.ask.agents.tools.publish_to_proof import resolve_proof_base_url

        monkeypatch.delenv("PROOF_BASE_URL", raising=False)
        assert resolve_proof_base_url() == "https://memokami.exe.xyz"

    def test_env_overrides_default(self, monkeypatch):
        from seeknal.ask.agents.tools.publish_to_proof import resolve_proof_base_url

        monkeypatch.setenv("PROOF_BASE_URL", "https://envset.example")
        assert resolve_proof_base_url() == "https://envset.example"

    def test_param_overrides_env(self, monkeypatch):
        from seeknal.ask.agents.tools.publish_to_proof import resolve_proof_base_url

        monkeypatch.setenv("PROOF_BASE_URL", "https://envset.example")
        assert resolve_proof_base_url("https://paramset.example") == "https://paramset.example"

    def test_empty_param_falls_back_to_env(self, monkeypatch):
        from seeknal.ask.agents.tools.publish_to_proof import resolve_proof_base_url

        monkeypatch.setenv("PROOF_BASE_URL", "https://envset.example")
        assert resolve_proof_base_url("   ") == "https://envset.example"

    def test_empty_env_falls_back_to_default(self, monkeypatch):
        from seeknal.ask.agents.tools.publish_to_proof import resolve_proof_base_url

        monkeypatch.setenv("PROOF_BASE_URL", "")
        assert resolve_proof_base_url() == "https://memokami.exe.xyz"


class TestUpgradeScheme:
    def test_upgrades_hosted_http_to_https(self):
        from seeknal.ask.agents.tools._proof_common import upgrade_scheme_if_remote

        assert upgrade_scheme_if_remote("http://memokami.exe.xyz") == "https://memokami.exe.xyz"
        assert upgrade_scheme_if_remote("http://proofeditor.ai/") == "https://proofeditor.ai/"

    def test_preserves_localhost_http(self):
        from seeknal.ask.agents.tools._proof_common import upgrade_scheme_if_remote

        assert upgrade_scheme_if_remote("http://localhost:4000") == "http://localhost:4000"
        assert upgrade_scheme_if_remote("http://127.0.0.1:4000") == "http://127.0.0.1:4000"

    def test_preserves_https(self):
        from seeknal.ask.agents.tools._proof_common import upgrade_scheme_if_remote

        assert upgrade_scheme_if_remote("https://memokami.exe.xyz") == "https://memokami.exe.xyz"


class TestSanitizeHelpers:
    def test_sanitize_strips_ansi(self):
        from seeknal.ask.agents.tools._proof_common import sanitize

        assert "\x1b" not in sanitize("a\x1b[31mb\x1b[0mc")
        assert sanitize("a\x00b\x07c") == "abc"

    def test_sanitize_truncates(self):
        from seeknal.ask.agents.tools._proof_common import sanitize

        out = sanitize("x" * 600, max_len=100)
        assert len(out) <= 101  # 100 + ellipsis
        assert out.endswith("…")

    def test_mask_secret_short(self):
        from seeknal.ask.agents.tools._proof_common import mask_secret

        assert mask_secret("") == ""
        assert mask_secret("short") == "****"

    def test_mask_secret_long(self):
        from seeknal.ask.agents.tools._proof_common import mask_secret

        assert mask_secret("supersecretvalue1234") == "supe…1234"

    def test_validate_base_url_rejects_ftp(self):
        from seeknal.ask.agents.tools._proof_common import validate_base_url

        err = validate_base_url("ftp://bad")
        assert err is not None
        assert "http" in err

    def test_validate_base_url_rejects_missing_host(self):
        from seeknal.ask.agents.tools._proof_common import validate_base_url

        err = validate_base_url("https://")
        assert err is not None
        assert "host" in err.lower()

    def test_validate_base_url_accepts_https(self):
        from seeknal.ask.agents.tools._proof_common import validate_base_url

        assert validate_base_url("https://memokami.exe.xyz") is None
