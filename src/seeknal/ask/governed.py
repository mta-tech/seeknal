"""Governed Seeknal Ask — Catalog-backed REPL and ask client.

In Atlas Runtime governed mode:
- Seeknal Ask runs in a sandbox pod as the user and holds no engine or storage credentials.
- Every data tool must get rows only from the Atlas Catalog via the Catalog Ask API.
- All database queries go through AskCatalogClient.
- The DuckDB connection is a scratch hardened in-memory connection with no external access.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

import httpx

from seeknal.serving.duckdb_policy import connect_hardened
from seeknal.serving.server.config import ServerConfigError, check_file_not_group_world_readable

logger = logging.getLogger("seeknal.ask.governed")

# Static credential variables strictly prohibited in governed mode.
PROHIBITED_CREDENTIAL_VARS: tuple[str, ...] = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "KEYCLOAK_CLIENT_SECRET",
    "LAKEKEEPER_TOKEN",
    "ATLAS_API_TOKEN",
    "IBA_ENGINE_API_KEY",
    "IBA_STORAGE_API_KEY",
)


class AskCatalogError(Exception):
    """Base error for Ask Catalog client issues."""


class AskAccessDenied(AskCatalogError):
    """Raised when Catalog returns 403 Forbidden."""


class AskSessionExpired(AskCatalogError):
    """Raised when Catalog returns 401 Unauthorized."""


class AskCatalogUnavailable(AskCatalogError):
    """Raised when Catalog returns 429, 503, or 504."""


class GovernedAskConfigError(ValueError, ServerConfigError):
    """Raised on configuration or environment errors in governed mode."""


def is_governed() -> bool:
    """Return True if governed Ask mode is enabled via environment."""
    return os.environ.get("ATLAS_ASK_MODE") == "governed"


def parse_schema_items(data: Any) -> list[dict[str, Any]]:
    """Parse progressive schema response items in one centralized function."""
    if not isinstance(data, dict):
        return []
    items = data.get("items", [])
    if not isinstance(items, list):
        return []
    result: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, dict):
            result.append(item)
        elif isinstance(item, str):
            result.append({"name": item})
    return result


def _extract_error_message(response: httpx.Response) -> str:
    """Extract a user-facing error message without leaking sensitive data."""
    try:
        data = response.json()
        if isinstance(data, dict):
            err = data.get("error")
            if isinstance(err, dict):
                msg = err.get("message") or err.get("code")
                if msg:
                    return str(msg)
            elif isinstance(err, str):
                return err
            if "message" in data:
                return str(data["message"])
    except Exception:
        pass
    return f"Catalog request failed with status {response.status_code}"


class AskCatalogClient:
    """HTTP client for Atlas Catalog Ask API (query and progressive schema)."""

    def __init__(
        self,
        base_url: str,
        token_file: str | Path,
        *,
        timeout_seconds: float = 60.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.base_url = str(base_url).rstrip("/")
        self.token_file = str(token_file)
        self.timeout_seconds = timeout_seconds
        self.transport = transport
        self.last_result_meta: dict[str, Any] | None = None

    def _read_token(self) -> str:
        """Re-read session token from file, enforcing file permission 0600."""
        check_file_not_group_world_readable(self.token_file, "Ask session token file")
        with open(self.token_file, "r", encoding="utf-8") as f:
            return f.read().strip()

    def _get_client(self) -> httpx.Client:
        return httpx.Client(
            transport=self.transport,
            timeout=self.timeout_seconds,
        )

    def _handle_error_response(self, response: httpx.Response) -> None:
        message = _extract_error_message(response)
        if response.status_code == 403:
            raise AskAccessDenied(message)
        if response.status_code == 401:
            raise AskSessionExpired(message)
        if response.status_code in (429, 503, 504):
            raise AskCatalogUnavailable(message)
        raise AskCatalogError(message)

    def query(
        self,
        sql: str,
        max_rows: int | None = None,
        catalog: str | None = None,
    ) -> tuple[list[str], list[tuple]]:
        """Send a SQL query to the Catalog and return (columns, rows)."""
        token = self._read_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        body: dict[str, Any] = {"sql": sql}
        if max_rows is not None:
            body["maxRows"] = max_rows
        if catalog is not None:
            body["catalog"] = catalog

        url = f"{self.base_url}/api/ask/v1/query"
        with self._get_client() as client:
            response = client.post(url, headers=headers, json=body)

        if not response.is_success:
            self._handle_error_response(response)

        payload = response.json()
        self.last_result_meta = {
            "rowCount": payload.get("rowCount"),
            "truncated": payload.get("truncated", False),
            "elapsedMs": payload.get("elapsedMs"),
            "queryId": payload.get("queryId"),
        }

        raw_columns = payload.get("columns", [])
        columns = [
            c["name"] if isinstance(c, dict) else str(c) for c in raw_columns
        ]
        raw_rows = payload.get("rows", [])
        rows: list[tuple] = []
        for r in raw_rows:
            if isinstance(r, dict):
                rows.append(tuple(r.get(col) for col in columns))
            elif isinstance(r, (list, tuple)):
                rows.append(tuple(r))
            else:
                rows.append((r,))

        return columns, rows

    def schema(
        self,
        catalog: str | None = None,
        db: str | None = None,
        table: str | None = None,
    ) -> dict[str, Any]:
        """Fetch progressive schema metadata from the Catalog."""
        token = self._read_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        params: dict[str, str] = {}
        if catalog is not None:
            params["catalog"] = catalog
        if db is not None:
            params["db"] = db
        if table is not None:
            params["table"] = table

        url = f"{self.base_url}/api/ask/v1/schema"
        with self._get_client() as client:
            response = client.get(url, headers=headers, params=params)

        if not response.is_success:
            self._handle_error_response(response)

        return response.json()


def _is_show_tables(sql: str) -> bool:
    norm = " ".join(sql.strip().rstrip(";").split()).lower()
    return norm == "show tables" or norm.startswith("show tables ")


def _is_describe_table(sql: str) -> bool:
    norm = " ".join(sql.strip().rstrip(";").split()).lower()
    if not norm.startswith("describe "):
        return False
    target = norm[9:].strip()
    if target.startswith("select ") or target.startswith("(") or " select " in target:
        return False
    return True


def _is_information_schema(sql: str) -> bool:
    norm = sql.lower()
    return "information_schema.tables" in norm or "information_schema.columns" in norm


def _connect_scratch_duckdb():
    try:
        return connect_hardened(
            ":memory:",
            read_only=False,
            allowed_directories=(),
            memory_limit="512MB",
            threads=2,
        )
    except Exception:
        import duckdb

        conn = duckdb.connect(":memory:", read_only=False)
        conn.execute("SET enable_external_access = false")
        conn.execute("SET autoinstall_known_extensions = false")
        conn.execute("SET autoload_known_extensions = false")
        conn.execute("SET memory_limit = '512MB'")
        conn.execute("SET threads = 2")
        conn.execute("SET lock_configuration = true")
        return conn


class GovernedRepl:
    """Drop-in REPL replacement for governed mode.

    Passes statements to AskCatalogClient. Introspection statements (SHOW TABLES,
    DESCRIBE <table_name>, and information_schema queries) are serviced from
    the Catalog schema endpoint.
    """

    def __init__(self, client: AskCatalogClient) -> None:
        self.client = client
        self.attached: set[str] = set()
        self.is_governed: bool = True
        self.last_result_meta: dict[str, Any] | None = None
        self.conn = _connect_scratch_duckdb()

    def execute_oneshot(
        self,
        sql: str,
        limit: int | None = None,
    ) -> tuple[list[str], list[tuple]]:
        """Execute a query through the Catalog or service introspection via schema."""
        trimmed = sql.strip().rstrip(";").strip()

        if _is_show_tables(trimmed):
            return self._handle_show_tables()

        if _is_describe_table(trimmed):
            return self._handle_describe_table(trimmed)

        if _is_information_schema(trimmed):
            return self._handle_information_schema(trimmed)

        max_rows = limit if limit is not None else 500
        columns, rows = self.client.query(sql, max_rows=max_rows)
        self.last_result_meta = getattr(self.client, "last_result_meta", None)
        return columns, rows

    def _handle_show_tables(self) -> tuple[list[str], list[tuple]]:
        res = self.client.schema()
        items = parse_schema_items(res)
        columns = ["name"]
        rows = [(item["name"],) for item in items if "name" in item]
        return columns, rows

    def _handle_describe_table(self, sql: str) -> tuple[list[str], list[tuple]]:
        table_ref = sql.strip().rstrip(";")[len("describe"):].strip()
        table_ref = table_ref.replace('"', "").replace("`", "")
        parts = [p for p in table_ref.split(".") if p]

        catalog: str | None = None
        db: str | None = None
        table: str | None = None

        if len(parts) >= 3:
            catalog, db, table = parts[0], parts[1], parts[2]
        elif len(parts) == 2:
            db, table = parts[0], parts[1]
        elif len(parts) == 1:
            table = parts[0]

        res = self.client.schema(catalog=catalog, db=db, table=table)
        items = parse_schema_items(res)
        columns = ["column_name", "column_type", "null"]
        rows = [
            (item["name"], item.get("type", "VARCHAR"), "YES")
            for item in items
            if "name" in item
        ]
        return columns, rows

    def _handle_information_schema(self, sql: str) -> tuple[list[str], list[tuple]]:
        norm = sql.lower()

        # Extract catalog if qualified: e.g. "wh".information_schema.tables
        cat_match = re.search(r'["\']?([a-zA-Z0-9_]+)["\']?\s*\.\s*information_schema', sql, re.IGNORECASE)
        catalog = cat_match.group(1) if cat_match and cat_match.group(1).lower() not in ("information_schema", "pg_catalog") else None

        if "information_schema.columns" in norm:
            tbl_match = re.search(r"table_name\s*=\s*['\"]([^'\"]+)['\"]", sql, re.IGNORECASE)
            table = tbl_match.group(1) if tbl_match else None
            res = self.client.schema(catalog=catalog, table=table)
            items = parse_schema_items(res)
            columns = ["column_name", "data_type"]
            rows = [
                (item["name"], item.get("type", "VARCHAR"))
                for item in items
                if "name" in item
            ]
            return columns, rows

        # information_schema.tables
        res = self.client.schema(catalog=catalog)
        items = parse_schema_items(res)
        columns = ["table_schema", "table_name", "table_type"]
        rows = [
            (
                item.get("schema") or item.get("db") or "main",
                item["name"],
                item.get("type") or item.get("table_type") or "BASE TABLE",
            )
            for item in items
            if "name" in item
        ]
        return columns, rows


@dataclass
class GovernedAskContext:
    """Runtime context for governed Ask mode."""

    catalog_url: str
    token_file: str
    client: AskCatalogClient
    repl: GovernedRepl

    @classmethod
    def from_env(
        cls,
        *,
        timeout_seconds: float = 60.0,
        transport: httpx.BaseTransport | None = None,
    ) -> GovernedAskContext:
        """Construct governed context from environment variables, validating policy."""
        # 1. Refuse prohibited static-credential variables
        for var in PROHIBITED_CREDENTIAL_VARS:
            if var in os.environ:
                raise GovernedAskConfigError(
                    f"Prohibited static-credential environment variable is set: {var}"
                )

        for key in os.environ:
            if key.startswith("SEEKNAL_PG_") and "PASSWORD" in key:
                raise GovernedAskConfigError(
                    f"Prohibited static-credential environment variable is set: {key}"
                )

        # 2. Check ATLAS_CATALOG_URL
        catalog_url = os.environ.get("ATLAS_CATALOG_URL", "").strip()
        if not catalog_url:
            raise GovernedAskConfigError("ATLAS_CATALOG_URL environment variable is required")

        allow_http = os.environ.get("ATLAS_ASK_ALLOW_HTTP", "").lower() in ("true", "1")
        if catalog_url.startswith("http://"):
            if not allow_http:
                raise GovernedAskConfigError(
                    "ATLAS_CATALOG_URL must use https:// (or set ATLAS_ASK_ALLOW_HTTP=true for http)"
                )
        elif not catalog_url.startswith("https://"):
            raise GovernedAskConfigError(
                "ATLAS_CATALOG_URL must start with https:// or http://"
            )

        # 3. Check ATLAS_ASK_SESSION_TOKEN_FILE
        token_file = os.environ.get("ATLAS_ASK_SESSION_TOKEN_FILE", "").strip()
        if not token_file:
            raise GovernedAskConfigError(
                "ATLAS_ASK_SESSION_TOKEN_FILE environment variable is required"
            )

        if not os.path.exists(token_file):
            raise GovernedAskConfigError(
                f"Session token file does not exist: {token_file}"
            )

        # Reuses check_file_not_group_world_readable (mode 0600 check)
        check_file_not_group_world_readable(token_file, "Ask session token file")

        client = AskCatalogClient(
            base_url=catalog_url,
            token_file=token_file,
            timeout_seconds=timeout_seconds,
            transport=transport,
        )
        repl = GovernedRepl(client=client)

        return cls(
            catalog_url=catalog_url,
            token_file=token_file,
            client=client,
            repl=repl,
        )
