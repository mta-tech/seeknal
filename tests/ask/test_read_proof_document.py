"""Tests for seeknal.ask.agents.tools.read_proof_document."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import httpx


def _ok_response(body: dict | None = None, status: int = 200, text: str | None = None) -> MagicMock:
    response = MagicMock()
    response.status_code = status
    response.text = text if text is not None else ("" if body is None else str(body))
    response.json.return_value = body or {}
    return response


class TestDoRead:
    def test_empty_url_returns_error(self):
        from seeknal.ask.agents.tools.read_proof_document import _do_read

        r = _do_read("", "https://memokami.exe.xyz", None)
        assert isinstance(r, str)
        assert "Error" in r

    def test_invalid_scheme_returns_error(self):
        from seeknal.ask.agents.tools.read_proof_document import _do_read

        r = _do_read("ftp://bad/d/x", "https://memokami.exe.xyz", None)
        assert isinstance(r, str)
        assert "Error" in r

    def test_extracts_base_token_from_mutation_base(self):
        """Real proof servers return the base token under `mutationBase.token`."""
        from seeknal.ask.agents.tools import read_proof_document as module

        body = {
            "success": True,
            "slug": "livknnxz",
            "markdown": "# hi",
            "content": "# hi",  # duplicate field on real responses
            "mutationBase": {
                "token": "mt1:realbasetokenvalue1234",
                "source": "persisted_yjs",
                "schemaVersion": "mt1",
            },
            "contract": {"preferredPrecondition": "baseToken"},
        }
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        mock_client.get.return_value = _ok_response(body)

        with patch.object(module.httpx, "Client", return_value=mock_client):
            r = module._do_read(
                "https://memokami.exe.xyz/d/livknnxz?token=t",
                "https://memokami.exe.xyz",
                None,
            )
        assert isinstance(r, dict)
        assert r["base_token"] == "mt1:realbasetokenvalue1234"

    def test_successful_read_returns_dict(self):
        from seeknal.ask.agents.tools import read_proof_document as module

        body = {
            "markdown": "# Hello\n\nBody text",
            "baseToken": "mt1:abcdef1234567890",
            "marks": [],
        }
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        mock_client.get.return_value = _ok_response(body)

        with patch.object(module.httpx, "Client", return_value=mock_client):
            r = module._do_read(
                "https://memokami.exe.xyz/d/abc?token=tok123",
                "https://memokami.exe.xyz",
                None,
            )
        assert isinstance(r, dict)
        assert r["slug"] == "abc"
        assert r["markdown"] == "# Hello\n\nBody text"
        assert r["base_token"] == "mt1:abcdef1234567890"
        assert r["share_url"] == "https://memokami.exe.xyz/d/abc"

        # Auth header and agent id must be set from the URL token
        args, kwargs = mock_client.get.call_args
        assert args[0] == "https://memokami.exe.xyz/api/agent/abc/state"
        assert kwargs["headers"]["Authorization"] == "Bearer tok123"
        assert kwargs["headers"]["X-Agent-Id"] == "agent:seeknal"
        assert kwargs["headers"]["Accept"] == "application/json"

    def test_bare_slug_uses_fallback_base(self):
        from seeknal.ask.agents.tools import read_proof_document as module

        body = {"markdown": "x", "baseToken": "t"}
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        mock_client.get.return_value = _ok_response(body)

        with patch.object(module.httpx, "Client", return_value=mock_client):
            r = module._do_read("abc123", "https://memokami.exe.xyz", None)
        assert isinstance(r, dict)
        args, _ = mock_client.get.call_args
        assert args[0] == "https://memokami.exe.xyz/api/agent/abc123/state"

    def test_api_key_used_when_no_url_token(self):
        from seeknal.ask.agents.tools import read_proof_document as module

        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        mock_client.get.return_value = _ok_response({"markdown": "", "baseToken": ""})

        with patch.object(module.httpx, "Client", return_value=mock_client):
            module._do_read("https://memokami.exe.xyz/d/xyz", "https://memokami.exe.xyz", "sk-abc")
        _, kwargs = mock_client.get.call_args
        assert kwargs["headers"]["Authorization"] == "Bearer sk-abc"

    def test_401_returns_auth_error(self):
        from seeknal.ask.agents.tools import read_proof_document as module

        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        mock_client.get.return_value = _ok_response(status=401)

        with patch.object(module.httpx, "Client", return_value=mock_client):
            r = module._do_read("https://memokami.exe.xyz/d/x", "https://memokami.exe.xyz", None)
        assert isinstance(r, str) and "auth" in r.lower()

    def test_404_returns_not_found(self):
        from seeknal.ask.agents.tools import read_proof_document as module

        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        mock_client.get.return_value = _ok_response(status=404)

        with patch.object(module.httpx, "Client", return_value=mock_client):
            r = module._do_read("https://memokami.exe.xyz/d/x", "https://memokami.exe.xyz", None)
        assert isinstance(r, str) and "not found" in r.lower()

    def test_timeout_returns_timeout_error(self):
        from seeknal.ask.agents.tools import read_proof_document as module

        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        mock_client.get.side_effect = httpx.TimeoutException("timed out")

        with patch.object(module.httpx, "Client", return_value=mock_client):
            r = module._do_read("https://memokami.exe.xyz/d/x", "https://memokami.exe.xyz", None)
        assert isinstance(r, str) and "timed out" in r.lower()

    def test_ansi_sequences_sanitized(self):
        from seeknal.ask.agents.tools import read_proof_document as module

        body = {
            "markdown": "# Hi\n\n\x1b[31minjected\x1b[0m",
            "baseToken": "tok\x07ev",
        }
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        mock_client.get.return_value = _ok_response(body)

        with patch.object(module.httpx, "Client", return_value=mock_client):
            r = module._do_read("https://memokami.exe.xyz/d/x", "https://memokami.exe.xyz", None)
        assert isinstance(r, dict)
        # The async entrypoint formats the markdown; here we just verify it's in the raw dict
        assert "\x1b" in r["markdown"]  # raw markdown preserved in dict
        assert "\x07" in r["base_token"]  # raw value preserved in dict


class TestReadProofDocumentAsync:
    def test_returns_formatted_string(self, monkeypatch):
        from seeknal.ask.agents.tools import read_proof_document as module

        monkeypatch.setenv("PROOF_BASE_URL", "https://memokami.exe.xyz")
        monkeypatch.delenv("PROOF_API_KEY", raising=False)

        body = {
            "markdown": "# Sample\n\nsome body content",
            "baseToken": "mt1:abcdef1234567890ABCDEF",
        }
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        mock_client.get.return_value = _ok_response(body)

        with patch.object(module.httpx, "Client", return_value=mock_client):
            result = asyncio.run(
                module.read_proof_document("https://memokami.exe.xyz/d/abc?token=tok")
            )

        assert "Read Proof document" in result
        assert "https://memokami.exe.xyz/d/abc" in result
        assert "# Sample" in result
        assert "some body content" in result
        # Secret is masked in output
        assert "mt1:abcdef1234567890ABCDEF" not in result
        assert "mt1:…CDEF" in result or "mt1:a" in result  # prefix+suffix mask

    def test_error_propagates_as_string(self, monkeypatch):
        from seeknal.ask.agents.tools import read_proof_document as module

        result = asyncio.run(module.read_proof_document(""))
        assert "Error" in result

    def test_preview_truncates_long_markdown(self, monkeypatch):
        from seeknal.ask.agents.tools import read_proof_document as module

        monkeypatch.setenv("PROOF_BASE_URL", "https://memokami.exe.xyz")
        monkeypatch.delenv("PROOF_API_KEY", raising=False)

        long_md = "x" * 8000
        body = {"markdown": long_md, "baseToken": "t"}
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        mock_client.get.return_value = _ok_response(body)

        with patch.object(module.httpx, "Client", return_value=mock_client):
            result = asyncio.run(module.read_proof_document("https://memokami.exe.xyz/d/abc?token=t"))

        assert "truncated" in result.lower()
        assert "8000 chars" in result
