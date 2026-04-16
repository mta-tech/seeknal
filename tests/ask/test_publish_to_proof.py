"""Tests for seeknal.ask.agents.tools.publish_to_proof."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httpx
import pytest


def _ok_response(body: dict | None = None, status: int = 200) -> MagicMock:
    response = MagicMock()
    response.status_code = status
    response.text = "" if body is None else str(body)
    response.json.return_value = body or {}
    return response


class TestDoPublish:
    """Unit tests for the _do_publish helper — no approval gate involved."""

    def test_empty_title_returns_error(self):
        from seeknal.ask.agents.tools.publish_to_proof import _do_publish

        result = _do_publish(
            title="",
            content="body",
            role="commenter",
            base_url="https://proofeditor.ai",
            api_key=None,
        )
        assert "Error" in result
        assert "title" in result.lower()

    def test_empty_content_returns_error(self):
        from seeknal.ask.agents.tools.publish_to_proof import _do_publish

        result = _do_publish(
            title="My memo",
            content="   ",
            role="commenter",
            base_url="https://proofeditor.ai",
            api_key=None,
        )
        assert "Error" in result
        assert "content" in result.lower()

    def test_invalid_role_returns_error(self):
        from seeknal.ask.agents.tools.publish_to_proof import _do_publish

        result = _do_publish(
            title="My memo",
            content="# Hi",
            role="owner",
            base_url="https://proofeditor.ai",
            api_key=None,
        )
        assert "Error" in result
        assert "role" in result.lower()

    def test_successful_publish_returns_share_url(self):
        from seeknal.ask.agents.tools import publish_to_proof as module

        body = {
            "success": True,
            "slug": "abc123xy",
            "shareUrl": "https://proofeditor.ai/d/abc123xy",
            "tokenUrl": "https://proofeditor.ai/d/abc123xy?token=xyz",
            "ownerSecret": "supersecretvalue1234",
        }
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        mock_client.post.return_value = _ok_response(body)

        with patch.object(module.httpx, "Client", return_value=mock_client):
            result = module._do_publish(
                title="Weekly memo",
                content="# Summary\n\nDetails here.",
                role="commenter",
                base_url="https://proofeditor.ai",
                api_key=None,
            )

        assert "Memo published" in result
        assert "https://proofeditor.ai/d/abc123xy" in result
        assert "Slug: abc123xy" in result
        # Secret must be masked — full value never appears in TUI output.
        assert "supersecretvalue1234" not in result
        assert "Owner secret (preview)" in result
        assert "supe…1234" in result
        # Payload verification
        _, kwargs = mock_client.post.call_args
        assert kwargs["json"]["markdown"] == "# Summary\n\nDetails here."
        assert kwargs["json"]["title"] == "Weekly memo"
        assert kwargs["json"]["role"] == "commenter"
        assert "Authorization" not in kwargs["headers"]

    def test_api_key_sets_authorization_header(self):
        from seeknal.ask.agents.tools import publish_to_proof as module

        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        mock_client.post.return_value = _ok_response(
            {"shareUrl": "https://example.com/d/x", "slug": "x"}
        )

        with patch.object(module.httpx, "Client", return_value=mock_client):
            module._do_publish(
                title="T",
                content="body",
                role="commenter",
                base_url="https://example.com/",  # trailing slash intentional
                api_key="sk-test-123",
            )

        args, kwargs = mock_client.post.call_args
        assert args[0] == "https://example.com/documents"  # no double slash
        assert kwargs["headers"]["Authorization"] == "Bearer sk-test-123"

    def test_401_returns_auth_error(self):
        from seeknal.ask.agents.tools import publish_to_proof as module

        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        mock_client.post.return_value = _ok_response(status=401)

        with patch.object(module.httpx, "Client", return_value=mock_client):
            result = module._do_publish(
                title="T",
                content="body",
                role="commenter",
                base_url="https://proofeditor.ai",
                api_key=None,
            )
        assert "Error" in result
        assert "auth" in result.lower()

    def test_404_returns_endpoint_error(self):
        from seeknal.ask.agents.tools import publish_to_proof as module

        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        mock_client.post.return_value = _ok_response(status=404)

        with patch.object(module.httpx, "Client", return_value=mock_client):
            result = module._do_publish(
                title="T",
                content="body",
                role="commenter",
                base_url="https://proofeditor.ai",
                api_key=None,
            )
        assert "Error" in result
        assert "not found" in result.lower()

    def test_timeout_returns_timeout_error(self):
        from seeknal.ask.agents.tools import publish_to_proof as module

        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        mock_client.post.side_effect = httpx.TimeoutException("timed out")

        with patch.object(module.httpx, "Client", return_value=mock_client):
            result = module._do_publish(
                title="T",
                content="body",
                role="commenter",
                base_url="https://proofeditor.ai",
                api_key=None,
            )
        assert "Error" in result
        assert "timed out" in result.lower()

    def test_invalid_base_url_scheme_returns_error(self):
        from seeknal.ask.agents.tools.publish_to_proof import _do_publish

        result = _do_publish(
            title="T",
            content="body",
            role="commenter",
            base_url="file:///etc/passwd",
            api_key=None,
        )
        assert "Error" in result
        assert "http or https" in result

    def test_base_url_missing_host_returns_error(self):
        from seeknal.ask.agents.tools.publish_to_proof import _do_publish

        result = _do_publish(
            title="T",
            content="body",
            role="commenter",
            base_url="https://",
            api_key=None,
        )
        assert "Error" in result
        assert "missing a host" in result

    def test_server_response_ansi_sequences_sanitized(self):
        """A malicious Proof server must not inject ANSI escapes into the TUI."""
        from seeknal.ask.agents.tools import publish_to_proof as module

        body = {
            "shareUrl": "https://proofeditor.ai/d/abc\x1b[31mINJECT\x1b[0m",
            "slug": "slug\x00\x07evil",
            "ownerSecret": "secret\x1b[2Jvalue123",
        }
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        mock_client.post.return_value = _ok_response(body)

        with patch.object(module.httpx, "Client", return_value=mock_client):
            result = module._do_publish(
                title="T",
                content="body",
                role="commenter",
                base_url="https://proofeditor.ai",
                api_key=None,
            )

        assert "\x1b" not in result
        assert "\x00" not in result
        assert "\x07" not in result

    def test_error_body_ansi_sequences_sanitized(self):
        """Error-body snippet from the server must also be sanitized."""
        from seeknal.ask.agents.tools import publish_to_proof as module

        response = MagicMock()
        response.status_code = 500
        response.text = "boom \x1b[31mRED\x1b[0m \x07bell"
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        mock_client.post.return_value = response

        with patch.object(module.httpx, "Client", return_value=mock_client):
            result = module._do_publish(
                title="T",
                content="body",
                role="commenter",
                base_url="https://proofeditor.ai",
                api_key=None,
            )
        assert "Error" in result
        assert "HTTP 500" in result
        assert "\x1b" not in result
        assert "\x07" not in result

    def test_missing_share_url_in_response_returns_error(self):
        from seeknal.ask.agents.tools import publish_to_proof as module

        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        mock_client.post.return_value = _ok_response({"success": True})

        with patch.object(module.httpx, "Client", return_value=mock_client):
            result = module._do_publish(
                title="T",
                content="body",
                role="commenter",
                base_url="https://proofeditor.ai",
                api_key=None,
            )
        assert "Error" in result
        assert "shareUrl" in result


class TestPublishToProofApprovalGate:
    """Verify the confirmation gate blocks unapproved publishes."""

    def test_requires_approval_by_default(self, monkeypatch):
        from seeknal.ask.agents.tools.publish_to_proof import publish_to_proof

        ctx = SimpleNamespace(
            project_path=Path("/tmp/test"),
            require_proof_publish_approval=True,
            proof_publish_approval_granted=False,
        )
        monkeypatch.setattr(
            "seeknal.ask.agents.tools._context.get_tool_context",
            lambda: ctx,
        )

        result = asyncio.run(publish_to_proof("Memo", "# Body"))
        assert "Approval required before publish_to_proof" in result
        assert "Publish memo to Proof" in result

    def test_skips_publish_when_not_approved(self, monkeypatch):
        """Ensure the guard short-circuits BEFORE any HTTP call is made."""
        from seeknal.ask.agents.tools import publish_to_proof as module

        ctx = SimpleNamespace(
            project_path=Path("/tmp/test"),
            require_proof_publish_approval=True,
            proof_publish_approval_granted=False,
        )
        monkeypatch.setattr(
            "seeknal.ask.agents.tools._context.get_tool_context",
            lambda: ctx,
        )

        with patch.object(module.httpx, "Client") as mock_client_cls:
            result = asyncio.run(module.publish_to_proof("Memo", "# Body"))
            mock_client_cls.assert_not_called()
        assert "Approval required" in result

    def test_publishes_when_approval_granted(self, monkeypatch):
        from seeknal.ask.agents.tools import publish_to_proof as module

        ctx = SimpleNamespace(
            project_path=Path("/tmp/test"),
            require_proof_publish_approval=True,
            proof_publish_approval_granted=True,
        )
        monkeypatch.setattr(
            "seeknal.ask.agents.tools._context.get_tool_context",
            lambda: ctx,
        )
        monkeypatch.delenv("PROOF_API_KEY", raising=False)
        monkeypatch.setenv("PROOF_BASE_URL", "https://example.com")

        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        mock_client.post.return_value = _ok_response(
            {"shareUrl": "https://example.com/d/xyz", "slug": "xyz"}
        )

        with patch.object(module.httpx, "Client", return_value=mock_client):
            result = asyncio.run(module.publish_to_proof("Memo", "# Body"))

        assert "Memo published" in result
        assert "https://example.com/d/xyz" in result


class TestProofPublishApprovalContext:
    """Verify the context-level approval tracking for proof publishes."""

    def test_reset_resets_both_report_and_proof_flags(self, monkeypatch):
        from seeknal.ask.agents.tools import _context

        ctx = SimpleNamespace(
            require_report_approval=False,
            report_approval_granted=True,
            require_proof_publish_approval=False,
            proof_publish_approval_granted=True,
        )
        monkeypatch.setattr(_context, "get_tool_context", lambda: ctx)

        _context.reset_report_approval()

        assert ctx.require_report_approval is True
        assert ctx.report_approval_granted is False
        assert ctx.require_proof_publish_approval is True
        assert ctx.proof_publish_approval_granted is False

    def test_record_ask_user_response_grants_proof_publish(self, monkeypatch):
        from seeknal.ask.agents.tools import _context

        ctx = SimpleNamespace(
            require_report_approval=True,
            report_approval_granted=False,
            require_proof_publish_approval=True,
            proof_publish_approval_granted=False,
        )
        monkeypatch.setattr(_context, "get_tool_context", lambda: ctx)

        options = [
            {"label": "Continue analysis"},
            {"label": "Publish memo to Proof"},
            {"label": "Done for now"},
            {"label": "Type your own"},
        ]
        _context.record_ask_user_response(options, "Publish memo to Proof")

        assert ctx.proof_publish_approval_granted is True
        # Report approval should remain unchanged — different option set
        assert ctx.report_approval_granted is False

    def test_record_ask_user_response_declines_proof_publish(self, monkeypatch):
        from seeknal.ask.agents.tools import _context

        ctx = SimpleNamespace(
            require_report_approval=True,
            report_approval_granted=False,
            require_proof_publish_approval=True,
            proof_publish_approval_granted=False,
        )
        monkeypatch.setattr(_context, "get_tool_context", lambda: ctx)

        options = [
            {"label": "Continue analysis"},
            {"label": "Publish memo to Proof"},
            {"label": "Done for now"},
            {"label": "Type your own"},
        ]
        _context.record_ask_user_response(options, "Continue analysis")

        assert ctx.proof_publish_approval_granted is False

    def test_report_menu_does_not_grant_proof_approval(self, monkeypatch):
        from seeknal.ask.agents.tools import _context

        ctx = SimpleNamespace(
            require_report_approval=True,
            report_approval_granted=False,
            require_proof_publish_approval=True,
            proof_publish_approval_granted=False,
        )
        monkeypatch.setattr(_context, "get_tool_context", lambda: ctx)

        options = [
            {"label": "Continue analysis"},
            {"label": "Generate report now"},
            {"label": "Done for now"},
            {"label": "Type your own"},
        ]
        _context.record_ask_user_response(options, "Generate report now")

        assert ctx.report_approval_granted is True
        assert ctx.proof_publish_approval_granted is False

    def test_require_proof_publish_approval_message(self, monkeypatch):
        from seeknal.ask.agents.tools import _context

        ctx = SimpleNamespace(
            require_proof_publish_approval=True,
            proof_publish_approval_granted=False,
        )
        monkeypatch.setattr(_context, "get_tool_context", lambda: ctx)

        msg = _context.require_proof_publish_approval("publish_to_proof")
        assert msg is not None
        assert "Publish memo to Proof" in msg
        assert "Continue analysis" in msg

    def test_require_proof_publish_approval_returns_none_when_granted(self, monkeypatch):
        from seeknal.ask.agents.tools import _context

        ctx = SimpleNamespace(
            require_proof_publish_approval=True,
            proof_publish_approval_granted=True,
        )
        monkeypatch.setattr(_context, "get_tool_context", lambda: ctx)

        assert _context.require_proof_publish_approval("publish_to_proof") is None

    def test_record_ask_user_grants_proof_with_paraphrased_menu(self, monkeypatch):
        """Regression for the session lockout where the agent offered
        ``Publish memo to Proof`` alongside paraphrased siblings like
        ``Deepen analysis`` and ``Just done``. The old strict 4-element
        subset check silently dropped approval; the new check only
        requires the discriminator label to be present and the user to
        select it.
        """
        from seeknal.ask.agents.tools import _context

        ctx = SimpleNamespace(
            require_report_approval=True,
            report_approval_granted=False,
            require_proof_publish_approval=True,
            proof_publish_approval_granted=False,
            require_proof_edit_approval=True,
            proof_edit_approval_granted=False,
        )
        monkeypatch.setattr(_context, "get_tool_context", lambda: ctx)

        # None of these siblings match the old strict 4-element set.
        options = [
            {"label": "Deepen analysis"},
            {"label": "Publish memo to Proof"},
            {"label": "Just done"},
        ]
        _context.record_ask_user_response(options, "Publish memo to Proof")

        assert ctx.proof_publish_approval_granted is True
        # And sibling approvals must stay locked.
        assert ctx.report_approval_granted is False
        assert ctx.proof_edit_approval_granted is False

    def test_record_ask_user_is_case_insensitive(self, monkeypatch):
        """LLM tool calls sometimes lowercase or uppercase the menu label.
        Approval must still be granted as long as the normalized answer
        matches the discriminator.
        """
        from seeknal.ask.agents.tools import _context

        ctx = SimpleNamespace(
            require_proof_publish_approval=True,
            proof_publish_approval_granted=False,
        )
        monkeypatch.setattr(_context, "get_tool_context", lambda: ctx)

        options = [
            {"label": "Continue analysis"},
            {"label": "publish memo to proof"},  # lowercased by LLM
        ]
        _context.record_ask_user_response(options, "  PUBLISH MEMO TO PROOF  ")

        assert ctx.proof_publish_approval_granted is True


class TestToolsetRegistration:
    def test_publish_to_proof_is_registered(self):
        pytest.importorskip("pydantic_ai")
        from seeknal.ask.agents.tools.toolset import create_ask_toolset

        toolset = create_ask_toolset()
        tool_names: set[str] = set()
        for tool in toolset.tools:
            if isinstance(tool, str):
                tool_names.add(tool)
            elif hasattr(tool, "__name__"):
                tool_names.add(tool.__name__)
            elif hasattr(tool, "name"):
                tool_names.add(tool.name)
        assert "publish_to_proof" in tool_names
