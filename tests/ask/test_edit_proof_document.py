"""Tests for seeknal.ask.agents.tools.edit_proof_document."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httpx


def _ok_response(body: dict | None = None, status: int = 200, text: str | None = None) -> MagicMock:
    response = MagicMock()
    response.status_code = status
    response.text = text if text is not None else ("" if body is None else str(body))
    response.json.return_value = body or {}
    return response


class TestDoEdit:
    def test_empty_markdown_returns_error(self):
        from seeknal.ask.agents.tools.edit_proof_document import _do_edit

        r = _do_edit(
            "https://memokami.exe.xyz/d/abc?token=t",
            "",
            "https://memokami.exe.xyz",
            None,
        )
        assert "Error" in r
        assert "empty" in r.lower()

    def test_invalid_url_returns_error(self):
        from seeknal.ask.agents.tools.edit_proof_document import _do_edit

        r = _do_edit("ftp://bad/x", "new md", "https://memokami.exe.xyz", None)
        assert "Error" in r

    def test_successful_edit_fetches_state_then_posts_rewrite(self):
        """Verify the two-step protocol: GET state → POST ops with baseToken."""
        from seeknal.ask.agents.tools import edit_proof_document as module

        # First call: GET /api/agent/abc/state → returns markdown + baseToken + revision
        state_body = {
            "markdown": "# old body",
            "baseToken": "mt1:basetoken-xyz-12345",
            "revision": 7,
        }
        # Second call: POST /api/agent/abc/ops → success
        ops_body = {
            "success": True,
            "revision": "rev-42",
            "baseToken": "mt1:new-basetoken-67890",
        }

        state_client = MagicMock()
        state_client.__enter__.return_value = state_client
        state_client.__exit__.return_value = False
        state_client.get.return_value = _ok_response(state_body)

        ops_client = MagicMock()
        ops_client.__enter__.return_value = ops_client
        ops_client.__exit__.return_value = False
        ops_client.post.return_value = _ok_response(ops_body)

        # Two separate httpx.Client() calls — patch to return them in order
        clients_iter = iter([state_client, ops_client])
        with patch.object(
            module.httpx, "Client", side_effect=lambda *a, **kw: next(clients_iter)
        ):
            r = module._do_edit(
                "https://memokami.exe.xyz/d/abc?token=accesstok",
                "# new body\n\nupdated content",
                "https://memokami.exe.xyz",
                None,
            )

        assert "Proof document updated" in r
        assert "https://memokami.exe.xyz/d/abc" in r

        # Verify the state fetch used the URL token
        get_args, get_kwargs = state_client.get.call_args
        assert get_args[0] == "https://memokami.exe.xyz/api/agent/abc/state"
        assert get_kwargs["headers"]["Authorization"] == "Bearer accesstok"

        # Verify the ops POST used the baseToken from state and the URL token
        post_args, post_kwargs = ops_client.post.call_args
        assert post_args[0] == "https://memokami.exe.xyz/api/agent/abc/ops"
        payload = post_kwargs["json"]
        assert payload["type"] == "rewrite.apply"
        assert payload["by"] == "ai:seeknal"
        # Real server expects `content`; we also send `markdown` for back-compat
        assert payload["content"] == "# new body\n\nupdated content"
        assert payload["markdown"] == "# new body\n\nupdated content"
        # Precondition: baseRevision takes precedence and baseToken must NOT
        # be combined with it (server returns CONFLICTING_BASE otherwise).
        assert payload["baseRevision"] == 7
        assert "baseToken" not in payload
        assert "expectedRevision" not in payload
        assert post_kwargs["headers"]["Authorization"] == "Bearer accesstok"
        assert post_kwargs["headers"]["X-Agent-Id"] == "agent:seeknal"

    def test_falls_back_to_base_token_when_no_revision(self):
        """When state lacks a `revision`, fall back to baseToken precondition."""
        from seeknal.ask.agents.tools import edit_proof_document as module

        state_body = {
            "markdown": "# old",
            "baseToken": "mt1:fallbacktoken",
            # no revision!
        }
        ops_body = {"success": True}

        state_client = MagicMock()
        state_client.__enter__.return_value = state_client
        state_client.__exit__.return_value = False
        state_client.get.return_value = _ok_response(state_body)

        ops_client = MagicMock()
        ops_client.__enter__.return_value = ops_client
        ops_client.__exit__.return_value = False
        ops_client.post.return_value = _ok_response(ops_body)

        clients_iter = iter([state_client, ops_client])
        with patch.object(module.httpx, "Client", side_effect=lambda *a, **kw: next(clients_iter)):
            r = module._do_edit(
                "https://memokami.exe.xyz/d/abc?token=t",
                "new",
                "https://memokami.exe.xyz",
                None,
            )
        assert "Proof document updated" in r
        _, post_kwargs = ops_client.post.call_args
        payload = post_kwargs["json"]
        assert payload["baseToken"] == "mt1:fallbacktoken"
        assert "baseRevision" not in payload

    def test_edit_fails_when_state_has_no_precondition(self):
        """If state has neither revision nor baseToken, ops still fires but
        the server will likely reject — regression guard for the dispatcher."""
        from seeknal.ask.agents.tools import edit_proof_document as module

        state_body = {"markdown": "# old", "baseToken": ""}
        # No revision either
        state_client = MagicMock()
        state_client.__enter__.return_value = state_client
        state_client.__exit__.return_value = False
        state_client.get.return_value = _ok_response(state_body)

        with patch.object(module.httpx, "Client", return_value=state_client):
            r = module._do_edit(
                "https://memokami.exe.xyz/d/abc?token=t",
                "new",
                "https://memokami.exe.xyz",
                None,
            )
        # Tool refuses to send an unanchored rewrite — returns an error
        # before any POST fires.
        assert "Error" in r

    def test_edit_fails_when_state_read_errors(self):
        from seeknal.ask.agents.tools import edit_proof_document as module

        state_client = MagicMock()
        state_client.__enter__.return_value = state_client
        state_client.__exit__.return_value = False
        state_client.get.return_value = _ok_response(status=404)

        with patch.object(module.httpx, "Client", return_value=state_client):
            r = module._do_edit(
                "https://memokami.exe.xyz/d/abc?token=t",
                "new",
                "https://memokami.exe.xyz",
                None,
            )
        assert "Error" in r
        assert "could not read current state" in r.lower()

    def test_409_conflict_message(self):
        from seeknal.ask.agents.tools import edit_proof_document as module

        state_body = {"markdown": "# old", "baseToken": "mt1:t"}
        state_client = MagicMock()
        state_client.__enter__.return_value = state_client
        state_client.__exit__.return_value = False
        state_client.get.return_value = _ok_response(state_body)

        ops_client = MagicMock()
        ops_client.__enter__.return_value = ops_client
        ops_client.__exit__.return_value = False
        ops_client.post.return_value = _ok_response(
            status=409, text='{"code":"LIVE_CLIENTS_PRESENT"}'
        )

        clients_iter = iter([state_client, ops_client])
        with patch.object(module.httpx, "Client", side_effect=lambda *a, **kw: next(clients_iter)):
            r = module._do_edit(
                "https://memokami.exe.xyz/d/abc?token=t",
                "new",
                "https://memokami.exe.xyz",
                None,
            )
        assert "409" in r or "CONFLICT" in r
        assert "collaborators" in r.lower() or "stale" in r.lower()

    def test_timeout_returns_timeout_error(self):
        from seeknal.ask.agents.tools import edit_proof_document as module

        state_body = {"markdown": "# old", "baseToken": "mt1:t"}
        state_client = MagicMock()
        state_client.__enter__.return_value = state_client
        state_client.__exit__.return_value = False
        state_client.get.return_value = _ok_response(state_body)

        ops_client = MagicMock()
        ops_client.__enter__.return_value = ops_client
        ops_client.__exit__.return_value = False
        ops_client.post.side_effect = httpx.TimeoutException("slow")

        clients_iter = iter([state_client, ops_client])
        with patch.object(module.httpx, "Client", side_effect=lambda *a, **kw: next(clients_iter)):
            r = module._do_edit(
                "https://memokami.exe.xyz/d/abc?token=t",
                "new",
                "https://memokami.exe.xyz",
                None,
            )
        assert "timed out" in r.lower()


class TestEditApprovalGate:
    def test_requires_approval_by_default(self, monkeypatch):
        from seeknal.ask.agents.tools.edit_proof_document import edit_proof_document

        ctx = SimpleNamespace(
            project_path=Path("/tmp/test"),
            require_proof_edit_approval=True,
            proof_edit_approval_granted=False,
        )
        monkeypatch.setattr(
            "seeknal.ask.agents.tools._context.get_tool_context",
            lambda: ctx,
        )

        result = asyncio.run(
            edit_proof_document("https://memokami.exe.xyz/d/abc?token=t", "new body")
        )
        assert "Approval required before edit_proof_document" in result
        assert "Apply edit to Proof" in result

    def test_skips_edit_when_not_approved(self, monkeypatch):
        from seeknal.ask.agents.tools import edit_proof_document as module

        ctx = SimpleNamespace(
            project_path=Path("/tmp/test"),
            require_proof_edit_approval=True,
            proof_edit_approval_granted=False,
        )
        monkeypatch.setattr(
            "seeknal.ask.agents.tools._context.get_tool_context",
            lambda: ctx,
        )

        with patch.object(module.httpx, "Client") as mock_client_cls:
            result = asyncio.run(
                module.edit_proof_document("https://memokami.exe.xyz/d/abc?token=t", "new")
            )
            mock_client_cls.assert_not_called()
        assert "Approval required" in result

    def test_executes_when_approved(self, monkeypatch):
        from seeknal.ask.agents.tools import edit_proof_document as module

        ctx = SimpleNamespace(
            project_path=Path("/tmp/test"),
            require_proof_edit_approval=True,
            proof_edit_approval_granted=True,
        )
        monkeypatch.setattr(
            "seeknal.ask.agents.tools._context.get_tool_context",
            lambda: ctx,
        )
        monkeypatch.delenv("PROOF_API_KEY", raising=False)
        monkeypatch.setenv("PROOF_BASE_URL", "https://memokami.exe.xyz")

        state_client = MagicMock()
        state_client.__enter__.return_value = state_client
        state_client.__exit__.return_value = False
        state_client.get.return_value = _ok_response(
            {"markdown": "# old", "baseToken": "mt1:t"}
        )
        ops_client = MagicMock()
        ops_client.__enter__.return_value = ops_client
        ops_client.__exit__.return_value = False
        ops_client.post.return_value = _ok_response({"success": True})

        clients_iter = iter([state_client, ops_client])
        with patch.object(module.httpx, "Client", side_effect=lambda *a, **kw: next(clients_iter)):
            result = asyncio.run(
                module.edit_proof_document(
                    "https://memokami.exe.xyz/d/abc?token=tok", "# new body"
                )
            )
        assert "Proof document updated" in result


class TestEditApprovalContext:
    def test_reset_report_approval_clears_edit_flag(self, monkeypatch):
        from seeknal.ask.agents.tools import _context

        ctx = SimpleNamespace(
            require_report_approval=False,
            report_approval_granted=True,
            require_proof_publish_approval=False,
            proof_publish_approval_granted=True,
            require_proof_edit_approval=False,
            proof_edit_approval_granted=True,
        )
        monkeypatch.setattr(_context, "get_tool_context", lambda: ctx)

        _context.reset_report_approval()

        assert ctx.require_proof_edit_approval is True
        assert ctx.proof_edit_approval_granted is False

    def test_record_ask_user_response_grants_edit_only_on_exact_discriminator(self, monkeypatch):
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

        options = [
            {"label": "Continue analysis"},
            {"label": "Apply edit to Proof"},
            {"label": "Done for now"},
            {"label": "Type your own"},
        ]
        _context.record_ask_user_response(options, "Apply edit to Proof")

        assert ctx.proof_edit_approval_granted is True
        assert ctx.proof_publish_approval_granted is False
        assert ctx.report_approval_granted is False

    def test_publish_menu_does_not_grant_edit_approval(self, monkeypatch):
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

        options = [
            {"label": "Continue analysis"},
            {"label": "Publish memo to Proof"},
            {"label": "Done for now"},
            {"label": "Type your own"},
        ]
        _context.record_ask_user_response(options, "Publish memo to Proof")

        assert ctx.proof_publish_approval_granted is True
        assert ctx.proof_edit_approval_granted is False

    def test_edit_menu_does_not_grant_publish_approval(self, monkeypatch):
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

        options = [
            {"label": "Continue analysis"},
            {"label": "Apply edit to Proof"},
            {"label": "Done for now"},
            {"label": "Type your own"},
        ]
        _context.record_ask_user_response(options, "Apply edit to Proof")

        assert ctx.proof_edit_approval_granted is True
        assert ctx.proof_publish_approval_granted is False
