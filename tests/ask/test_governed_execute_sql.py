"""Tests for execute_sql in governed mode (TASK-SPEC.md § Governed mode)."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import httpx
import pytest

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.agents.tools.execute_sql import execute_sql
from seeknal.ask.governed import AskCatalogClient, GovernedRepl


@pytest.fixture
def token_file(tmp_path: Path) -> Path:
    p = tmp_path / "token.jwt"
    p.write_text("governed-execute-sql-token", encoding="utf-8")
    os.chmod(p, 0o600)
    return p


@pytest.fixture
def governed_setup(tmp_path: Path, token_file: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ATLAS_ASK_MODE", "governed")

    query_requests: list[httpx.Request] = []

    def handle_request(request: httpx.Request) -> httpx.Response:
        query_requests.append(request)
        if request.url.path == "/api/ask/v1/query":
            body = json.loads(request.content)
            sql = body.get("sql", "")
            if "forbidden" in sql.lower():
                return httpx.Response(
                    403,
                    json={
                        "error": {
                            "code": "access_denied",
                            "message": "User alice does not have SELECT permission on forbidden",
                        }
                    },
                )
            # Normal successful query
            return httpx.Response(
                200,
                json={
                    "columns": [
                        {"name": "id", "type": "INTEGER"},
                        {"name": "customer", "type": "VARCHAR"},
                    ],
                    "rows": [
                        {"id": 1, "customer": "Acme Corp"},
                        {"id": 2, "customer": "Beta LLC"},
                    ],
                    "rowCount": 2,
                    "elapsedMs": 8,
                    "truncated": False,
                    "queryId": "q-success-1",
                },
            )
        return httpx.Response(404, json={"error": {"message": "Not found"}})

    transport = httpx.MockTransport(handle_request)
    client = AskCatalogClient("https://catalog.atlas.local", token_file, transport=transport)
    repl = GovernedRepl(client)

    ctx = ToolContext(
        repl=repl,
        artifact_discovery=MagicMock(),
        project_path=tmp_path,
        is_governed=True,
    )
    set_tool_context(ctx)
    return ctx, repl, client, query_requests


def test_execute_sql_calls_catalog_once_and_skips_count_total(governed_setup):
    ctx, repl, client, query_requests = governed_setup

    with patch("seeknal.ask.agents.tools.execute_sql._count_total") as mock_count_total:
        result = execute_sql("SELECT id, customer FROM orders", limit=100)

        assert mock_count_total.call_count == 0
        assert len(query_requests) == 1
        assert query_requests[0].url.path == "/api/ask/v1/query"
        body = json.loads(query_requests[0].content)
        assert body["sql"] == "SELECT id, customer FROM orders"
        assert body["maxRows"] == 101  # fetch_limit is asked + 1

        assert "Acme Corp" in result
        assert "Beta LLC" in result


def test_execute_sql_does_not_call_govern_query(governed_setup):
    ctx, repl, client, query_requests = governed_setup

    with patch("seeknal.integrations.atlas_governance.govern_query") as mock_govern_query:
        result = execute_sql("SELECT id, customer FROM orders")

        assert mock_govern_query.call_count == 0
        assert "Acme Corp" in result


def test_execute_sql_renders_access_denied_on_403(governed_setup):
    ctx, repl, client, query_requests = governed_setup

    result = execute_sql("SELECT * FROM forbidden")

    assert "Access denied" in result
    assert "User alice does not have SELECT permission on forbidden" in result


def test_execute_sql_never_touches_duckdb_connect_attachments(governed_setup, monkeypatch: pytest.MonkeyPatch):
    ctx, repl, client, query_requests = governed_setup

    connect_calls: list[tuple] = []
    real_connect = duckdb.connect

    def spy_connect(*args, **kwargs):
        connect_calls.append((args, kwargs))
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(duckdb, "connect", spy_connect)

    result = execute_sql("SELECT id, customer FROM orders")

    assert "Acme Corp" in result
    # In governed mode, execute_sql routes exclusively to the Catalog;
    # it must never touch duckdb.connect or attach database/storage credentials.
    assert len(connect_calls) == 0
