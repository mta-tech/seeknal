"""Tests for GovernedRepl and AskCatalogClient (TASK-SPEC.md § Governed mode)."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import httpx
import pytest

from seeknal.ask.governed import (
    AskAccessDenied,
    AskCatalogClient,
    AskCatalogError,
    AskCatalogUnavailable,
    AskSessionExpired,
    GovernedRepl,
)


@pytest.fixture
def token_file(tmp_path: Path) -> Path:
    p = tmp_path / "session_token"
    p.write_text("governed-ask-bearer-token-12345", encoding="utf-8")
    os.chmod(p, 0o600)
    return p


def test_query_rows_converted_to_tuples_in_column_order(token_file: Path):
    def handle_request(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/ask/v1/query"
        body = json.loads(request.content)
        assert body["sql"] == "SELECT id, name, score FROM students"
        return httpx.Response(
            200,
            json={
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "VARCHAR"},
                    {"name": "score", "type": "FLOAT"},
                ],
                "rows": [
                    # Notice dictionary keys in row are scrambled vs column order
                    {"score": 95.5, "id": 1, "name": "Alice"},
                    {"name": "Bob", "score": 88.0, "id": 2},
                ],
                "rowCount": 2,
                "elapsedMs": 12,
                "truncated": False,
                "queryId": "q-101",
            },
        )

    transport = httpx.MockTransport(handle_request)
    client = AskCatalogClient(
        "https://catalog.atlas.local",
        token_file,
        transport=transport,
    )
    repl = GovernedRepl(client)

    columns, rows = repl.execute_oneshot("SELECT id, name, score FROM students")

    assert columns == ["id", "name", "score"]
    assert rows == [
        (1, "Alice", 95.5),
        (2, "Bob", 88.0),
    ]
    assert repl.last_result_meta == {
        "rowCount": 2,
        "truncated": False,
        "elapsedMs": 12,
        "queryId": "q-101",
    }


def test_query_sends_max_rows(token_file: Path):
    recorded_bodies: list[dict] = []

    def handle_request(request: httpx.Request) -> httpx.Response:
        recorded_bodies.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={"columns": [{"name": "x"}], "rows": [{"x": 1}], "rowCount": 1},
        )

    transport = httpx.MockTransport(handle_request)
    client = AskCatalogClient("https://catalog.atlas.local", token_file, transport=transport)
    repl = GovernedRepl(client)

    # With explicit limit
    repl.execute_oneshot("SELECT 1", limit=42)
    assert recorded_bodies[-1]["maxRows"] == 42

    # Without limit: defaults to 500
    repl.execute_oneshot("SELECT 1", limit=None)
    assert recorded_bodies[-1]["maxRows"] == 500


@pytest.mark.parametrize(
    ("status_code", "expected_exception"),
    [
        (403, AskAccessDenied),
        (401, AskSessionExpired),
        (429, AskCatalogUnavailable),
        (503, AskCatalogUnavailable),
        (504, AskCatalogUnavailable),
        (400, AskCatalogError),
        (413, AskCatalogError),
        (500, AskCatalogError),
    ],
)
def test_error_code_mapping(
    token_file: Path,
    status_code: int,
    expected_exception: type[Exception],
):
    def handle_request(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code,
            json={"error": {"code": f"ERR_{status_code}", "message": f"Failure {status_code}"}},
        )

    transport = httpx.MockTransport(handle_request)
    client = AskCatalogClient("https://catalog.atlas.local", token_file, transport=transport)
    repl = GovernedRepl(client)

    with pytest.raises(expected_exception) as exc_info:
        repl.execute_oneshot("SELECT 1")

    assert f"Failure {status_code}" in str(exc_info.value) or str(status_code) in str(exc_info.value)


def test_introspection_forms_go_to_schema_not_query(token_file: Path):
    requests_log: list[httpx.Request] = []

    def handle_request(request: httpx.Request) -> httpx.Response:
        requests_log.append(request)
        if request.url.path == "/api/ask/v1/schema":
            if "table" in request.url.params:
                return httpx.Response(
                    200,
                    json={
                        "items": [
                            {"name": "col_a", "type": "INTEGER"},
                            {"name": "col_b", "type": "VARCHAR"},
                        ]
                    },
                )
            return httpx.Response(
                200,
                json={
                    "items": [
                        {"name": "orders", "type": "BASE TABLE"},
                        {"name": "revenue_view", "type": "VIEW"},
                    ]
                },
            )
        return httpx.Response(500, json={"error": {"message": "Should not call query"}})

    transport = httpx.MockTransport(handle_request)
    client = AskCatalogClient("https://catalog.atlas.local", token_file, transport=transport)
    repl = GovernedRepl(client)

    # 1. SHOW TABLES
    cols, rows = repl.execute_oneshot("SHOW TABLES")
    assert any(r.url.path == "/api/ask/v1/schema" for r in requests_log)
    assert not any(r.url.path == "/api/ask/v1/query" for r in requests_log)
    assert cols == ["name"]
    assert ("orders",) in rows
    assert ("revenue_view",) in rows

    # 2. DESCRIBE <table_name>
    requests_log.clear()
    cols, rows = repl.execute_oneshot("DESCRIBE warehouse.analytics.orders")
    assert any(r.url.path == "/api/ask/v1/schema" for r in requests_log)
    assert not any(r.url.path == "/api/ask/v1/query" for r in requests_log)
    assert "column_name" in cols
    assert ("col_a", "INTEGER", "YES") in rows

    # 3. information_schema.tables
    requests_log.clear()
    cols, rows = repl.execute_oneshot(
        'SELECT table_schema, table_name, table_type FROM "wh".information_schema.tables'
    )
    assert any(r.url.path == "/api/ask/v1/schema" for r in requests_log)
    assert not any(r.url.path == "/api/ask/v1/query" for r in requests_log)
    assert "table_name" in cols

    # 4. information_schema.columns
    requests_log.clear()
    cols, rows = repl.execute_oneshot(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'orders'"
    )
    assert any(r.url.path == "/api/ask/v1/schema" for r in requests_log)
    assert not any(r.url.path == "/api/ask/v1/query" for r in requests_log)
    assert cols == ["column_name", "data_type"]
    assert ("col_a", "INTEGER") in rows


def test_scratch_conn_refuses_filesystem_and_attach(token_file: Path):
    client = AskCatalogClient("https://catalog.atlas.local", token_file)
    repl = GovernedRepl(client)

    assert repl.attached == set()

    with pytest.raises(Exception, match=r"disabled by configuration|Permission Error"):
        repl.conn.execute("SELECT * FROM read_csv('/etc/hosts')")

    with pytest.raises(Exception, match=r"disabled by configuration|Permission Error"):
        repl.conn.execute("ATTACH 'secrets.db'")


def test_token_never_appears_in_caplog_or_exception_text(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
):
    secret_token = "secret-token-uncompromised-99887766"
    p = tmp_path / "secret.jwt"
    p.write_text(secret_token, encoding="utf-8")
    os.chmod(p, 0o600)

    def handle_request(request: httpx.Request) -> httpx.Response:
        # Server refuses request with 403
        return httpx.Response(
            403,
            json={"error": {"code": "access_denied", "message": "Permission denied for role user"}},
        )

    transport = httpx.MockTransport(handle_request)
    client = AskCatalogClient("https://catalog.atlas.local", p, transport=transport)
    repl = GovernedRepl(client)

    caplog.set_level(logging.DEBUG)

    with pytest.raises(AskAccessDenied) as exc_info:
        repl.execute_oneshot("SELECT * FROM restricted_table")

    # Verify secret token is NOT leaked in exception text
    assert secret_token not in str(exc_info.value)
    # Verify secret token is NOT leaked in caplog
    assert secret_token not in caplog.text
