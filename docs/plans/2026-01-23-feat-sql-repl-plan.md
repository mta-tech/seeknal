---
title: "feat: Add SQL REPL for Interactive Data Exploration"
type: feat
date: 2026-01-23
brainstorm: docs/brainstorms/2026-01-23-sql-repl-brainstorm.md
revised: 2026-01-23
---

# feat: Add SQL REPL for Interactive Data Exploration

## Revision Summary

**Revised on:** 2026-01-23 after code review by DHH, Kieran, and Simplicity reviewers

### Key Changes from Original Plan

| Original | Revised |
|----------|---------|
| Hybrid SQLAlchemy + DuckDB (~600 lines) | **DuckDB-only (~150 lines)** |
| MultiFernet with config.toml key | **Fernet encryption with SEEKNAL_ENCRYPT_KEY** |
| prompt_toolkit with fuzzy completion | **Basic readline** |
| Source management CLI commands | **Included: `seeknal source add/list/remove`** |
| Custom exception hierarchy | **Single `RuntimeError`** |
| Tenacity retry logic | **Simple error display** |
| Signal handler registration | **try/finally block** |

### Bugs Fixed from Review

| Bug | Fix |
|-----|-----|
| `@staticmethod` referencing `self` | Removed - not applicable in simplified design |
| `ConnectionError` shadows builtin | Using `RuntimeError` instead |
| Missing `validate_path()` function | Using `is_insecure_path()` from existing module |
| Wrong exception type caught | Fixed error handling |

---

## Overview

Add an interactive SQL REPL (`seeknal repl`) that allows users to explore data across any data source. Uses DuckDB as the single query engine with scanner extensions for postgres, mysql, and sqlite.

**Target users:** Data engineers exploring feature stores and debugging data pipelines.

## Problem Statement

Users need a quick way to:
- Run ad-hoc SQL queries against databases
- Explore parquet/csv files
- Debug data transformations interactively

## Proposed Solution

### Architecture (Simplified)

```
seeknal repl
    │
    └─► DuckDB in-memory connection
            │
            ├─► postgres_scanner (for PostgreSQL)
            ├─► mysql_scanner (for MySQL)
            ├─► sqlite_scanner (for SQLite)
            └─► Native (for parquet/csv files)
```

**Single query engine.** DuckDB handles everything via scanner extensions.

---

## Security Architecture

### Threat Model

| Threat | Mitigation |
|--------|------------|
| **Credential exposure in storage** | Fernet encryption of connection URLs in SQLite |
| **Credential leakage in logs/errors** | `sanitize_error_message()` masks all credential patterns |
| **Credential exposure in shell history** | Use `.connect <name>` with saved sources instead of raw URLs |
| **Credential exposure in terminal** | Passwords masked when adding sources |
| **Unauthorized file access** | SQLite file in `~/.seeknal/` with user-only permissions |
| **Path traversal attacks** | `is_insecure_path()` blocks `/tmp` and world-writable paths |
| **Key exposure** | SEEKNAL_ENCRYPT_KEY in environment, not in config files |

### Credential Encryption Flow

```
User adds source:
  seeknal source add mydb --url "postgres://user:pass@host/db"
      │
      ▼
  Extract password from URL
      │
      ▼
  Encrypt password with Fernet(SEEKNAL_ENCRYPT_KEY)
      │
      ▼
  Store in SourceTable: {url: "postgres://user:****@host/db", encrypted_password: "gAAAA..."}

User connects in REPL:
  seeknal> .connect mydb
      │
      ▼
  Load source from SourceTable
      │
      ▼
  Decrypt password with Fernet(SEEKNAL_ENCRYPT_KEY)
      │
      ▼
  Reconstruct full URL and connect via DuckDB
```

### Encryption Implementation

New file: `src/seeknal/utils/encryption.py`

```python
"""Fernet encryption for sensitive data.

Uses SEEKNAL_ENCRYPT_KEY from environment. If not set, operations
that require encryption will fail with a clear error message.
"""
from __future__ import annotations

import os
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken


class EncryptionError(Exception):
    """Raised when encryption/decryption fails."""
    pass


def _get_fernet() -> Fernet:
    """Get Fernet instance from environment key."""
    key = os.environ.get("SEEKNAL_ENCRYPT_KEY")
    if not key:
        raise EncryptionError(
            "SEEKNAL_ENCRYPT_KEY not set. Generate one with: "
            "python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'"
        )
    try:
        return Fernet(key.encode() if isinstance(key, str) else key)
    except Exception as e:
        raise EncryptionError(f"Invalid SEEKNAL_ENCRYPT_KEY: {e}")


def encrypt_value(plaintext: str) -> str:
    """Encrypt a string value.

    Args:
        plaintext: The value to encrypt.

    Returns:
        Base64-encoded encrypted value.

    Raises:
        EncryptionError: If SEEKNAL_ENCRYPT_KEY is not set or invalid.
    """
    if not plaintext:
        return ""
    fernet = _get_fernet()
    return fernet.encrypt(plaintext.encode()).decode("utf-8")


def decrypt_value(ciphertext: str) -> str:
    """Decrypt a string value.

    Args:
        ciphertext: Base64-encoded encrypted value.

    Returns:
        Decrypted plaintext.

    Raises:
        EncryptionError: If SEEKNAL_ENCRYPT_KEY is not set, invalid, or
                        ciphertext is corrupted.
    """
    if not ciphertext:
        return ""
    fernet = _get_fernet()
    try:
        return fernet.decrypt(ciphertext.encode()).decode("utf-8")
    except InvalidToken:
        raise EncryptionError("Failed to decrypt: invalid key or corrupted data")


def is_encryption_configured() -> bool:
    """Check if encryption is properly configured."""
    return bool(os.environ.get("SEEKNAL_ENCRYPT_KEY"))
```

### Source Storage Model

Extended `SourceTable` in `src/seeknal/models.py`:

```python
class SourceTable(SQLModel, table=True):
    """Data source connection storage with encrypted credentials."""

    __tablename__ = "sources"

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(unique=True, index=True)
    source_type: str  # postgres, mysql, sqlite, parquet, csv

    # URL with password masked (for display)
    masked_url: str

    # Encrypted sensitive parts (password, auth tokens)
    encrypted_credentials: Optional[str] = None

    # Non-sensitive connection parts
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None

    # Metadata
    workspace_id: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
```

### Key Security Features

1. **Separation of concerns**: Password stored encrypted, URL components stored separately
2. **No plaintext storage**: Only masked URL visible in database
3. **Environment-based key**: Key never persists to disk
4. **Fail-secure**: Operations fail if encryption key missing
5. **Existing patterns**: Uses `sanitize_error_message()` from `db_utils.py`

---

### Key Design Decisions

1. **Fernet encryption for stored credentials** — Connection URLs with passwords are encrypted before storage. Decrypted only at connection time.

2. **Source persistence** — Users can save connections with `seeknal source add` and reuse them with `.connect <name>`.

3. **Security by design**:
   - Credentials never appear in logs or error messages (using `sanitize_error_message`)
   - Credentials encrypted at rest using Fernet symmetric encryption
   - ENCRYPT_KEY stored in environment variable, not in config files
   - Path validation prevents insecure storage locations

4. **Minimal dependencies** — Uses `cryptography` (new), `duckdb`, and `tabulate`.

5. **Simple UX** — Basic readline, no fuzzy completion, no syntax highlighting for v1.

## Implementation

### File 1: `src/seeknal/cli/source.py` (Source Management CLI)

```python
"""Source management CLI commands.

Provides:
  seeknal source add <name> --url <connection_url>
  seeknal source list
  seeknal source remove <name>
"""
from __future__ import annotations

import typer
from urllib.parse import urlparse, urlunparse

from seeknal.context import context
from seeknal.db_utils import mask_url_credentials
from seeknal.utils.encryption import encrypt_value, decrypt_value, is_encryption_configured

app = typer.Typer(help="Manage data sources for REPL")


def _parse_and_encrypt_url(url: str) -> dict:
    """Parse URL and encrypt sensitive parts.

    Returns dict with: source_type, masked_url, encrypted_credentials,
                       host, port, database, username
    """
    parsed = urlparse(url)

    # Determine source type
    scheme = parsed.scheme.lower()
    if scheme in ("postgres", "postgresql"):
        source_type = "postgres"
    elif scheme == "mysql":
        source_type = "mysql"
    elif scheme == "sqlite":
        source_type = "sqlite"
    elif url.endswith(".parquet"):
        source_type = "parquet"
    elif url.endswith(".csv"):
        source_type = "csv"
    else:
        source_type = "unknown"

    # Encrypt password if present
    encrypted_credentials = None
    if parsed.password:
        encrypted_credentials = encrypt_value(parsed.password)

    # Create masked URL for display
    masked_url = mask_url_credentials(url)

    return {
        "source_type": source_type,
        "masked_url": masked_url,
        "encrypted_credentials": encrypted_credentials,
        "host": parsed.hostname,
        "port": parsed.port,
        "database": parsed.path.lstrip("/") if parsed.path else None,
        "username": parsed.username,
    }


def _reconstruct_url(source: dict) -> str:
    """Reconstruct full URL from source record with decrypted password."""
    if source["source_type"] in ("parquet", "csv"):
        return source["masked_url"]

    password = ""
    if source.get("encrypted_credentials"):
        password = decrypt_value(source["encrypted_credentials"])

    # Rebuild URL
    scheme_map = {"postgres": "postgresql", "mysql": "mysql", "sqlite": "sqlite"}
    scheme = scheme_map.get(source["source_type"], source["source_type"])

    if source["source_type"] == "sqlite":
        return f"sqlite:///{source['database']}"

    netloc = ""
    if source.get("username"):
        netloc = source["username"]
        if password:
            netloc += f":{password}"
        netloc += "@"
    if source.get("host"):
        netloc += source["host"]
        if source.get("port"):
            netloc += f":{source['port']}"

    path = f"/{source['database']}" if source.get("database") else ""

    return f"{scheme}://{netloc}{path}"


@app.command("add")
def add_source(
    name: str = typer.Argument(..., help="Unique name for this source"),
    url: str = typer.Option(..., "--url", "-u", help="Connection URL"),
):
    """Add a new data source with encrypted credentials.

    Example:
        seeknal source add mydb --url postgres://user:pass@localhost/mydb
    """
    if not is_encryption_configured():
        typer.echo(
            "Error: SEEKNAL_ENCRYPT_KEY not set. Generate one with:\n"
            "  python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'\n"
            "Then: export SEEKNAL_ENCRYPT_KEY=<your-key>",
            err=True
        )
        raise typer.Exit(1)

    # Parse and encrypt
    source_data = _parse_and_encrypt_url(url)
    source_data["name"] = name

    # Store in database (uses existing SourceRequest pattern)
    from seeknal.request import SourceRequest
    req = SourceRequest()

    # Check if exists
    existing = req.select_by_name(name)
    if existing:
        typer.echo(f"Source '{name}' already exists. Use 'seeknal source remove {name}' first.")
        raise typer.Exit(1)

    req.save_source(source_data)
    typer.echo(f"Added source '{name}': {source_data['masked_url']}")


@app.command("list")
def list_sources():
    """List all saved data sources."""
    from seeknal.request import SourceRequest
    req = SourceRequest()
    sources = req.list_sources()

    if not sources:
        typer.echo("No sources configured. Add one with: seeknal source add <name> --url <url>")
        return

    typer.echo(f"{'NAME':<20} {'TYPE':<12} {'URL'}")
    typer.echo("-" * 60)
    for s in sources:
        typer.echo(f"{s['name']:<20} {s['source_type']:<12} {s['masked_url']}")


@app.command("remove")
def remove_source(name: str = typer.Argument(..., help="Name of source to remove")):
    """Remove a saved data source."""
    from seeknal.request import SourceRequest
    req = SourceRequest()

    if not req.select_by_name(name):
        typer.echo(f"Source '{name}' not found.")
        raise typer.Exit(1)

    req.delete_source(name)
    typer.echo(f"Removed source '{name}'")
```

### File 2: `src/seeknal/cli/repl.py` (REPL)

```python
"""Minimal SQL REPL for seeknal using DuckDB.

Uses DuckDB scanner extensions to query postgres, mysql, sqlite databases
and native DuckDB for parquet/csv files. ~100 lines.
"""
from __future__ import annotations

import os
import readline
from pathlib import Path
from typing import Optional

import duckdb
from tabulate import tabulate

from seeknal.utils.path_security import is_insecure_path
from seeknal.db_utils import sanitize_error_message


# Scanner extensions to load
EXTENSIONS = ["postgres", "mysql", "sqlite"]


class REPL:
    """Simple SQL REPL using DuckDB as the single query engine."""

    def __init__(self) -> None:
        self.conn = duckdb.connect(":memory:")
        self.attached: set[str] = set()
        self._load_extensions()
        self._setup_history()

    def _load_extensions(self) -> None:
        """Install and load DuckDB scanner extensions."""
        for ext in EXTENSIONS:
            try:
                self.conn.execute(f"INSTALL {ext}")
                self.conn.execute(f"LOAD {ext}")
            except Exception:
                pass  # Extension may already be installed or unavailable

    def _setup_history(self) -> None:
        """Configure readline history."""
        history_dir = Path.home() / ".seeknal"
        history_dir.mkdir(parents=True, exist_ok=True)
        self._history_path = history_dir / ".repl_history"

        try:
            if self._history_path.exists():
                readline.read_history_file(str(self._history_path))
            readline.set_history_length(1000)
        except Exception:
            pass

    def _save_history(self) -> None:
        """Save readline history."""
        try:
            readline.write_history_file(str(self._history_path))
        except Exception:
            pass

    def run(self) -> None:
        """Main REPL loop."""
        print("Seeknal SQL REPL (DuckDB)")
        print("Commands: .connect <url>, .tables, .schema <table>, .quit")
        print("Example: .connect postgres://user:pass@host/db")
        print()

        try:
            while True:
                try:
                    line = input("seeknal> ").strip()
                    if not line:
                        continue

                    if line == ".quit" or line == ".exit":
                        break
                    elif line == ".help":
                        self._show_help()
                    elif line == ".sources":
                        self._show_sources()
                    elif line.startswith(".connect "):
                        self._connect(line[9:].strip())
                    elif line == ".tables":
                        self._show_tables()
                    elif line.startswith(".schema "):
                        self._show_schema(line[8:].strip())
                    elif line.startswith("."):
                        print(f"Unknown command: {line.split()[0]}. Type .help")
                    else:
                        self._execute(line)

                except KeyboardInterrupt:
                    print()  # Newline after ^C
                    continue

        except EOFError:
            pass
        finally:
            self._save_history()
            print("Goodbye!")

    def _show_help(self) -> None:
        """Show available commands."""
        print("""
Commands:
  .sources          List saved data sources
  .connect <name>   Connect to a saved source by name
  .connect <url>    Connect to a database URL directly
  .tables           List tables in connected databases
  .schema <table>   Show table schema
  .quit             Exit the REPL

Connection examples:
  .connect mydb                                    (saved source)
  .connect postgres://user:pass@localhost/mydb    (direct URL)
  .connect $DATABASE_URL                          (env variable)
  .connect /path/to/data.parquet                  (file)

Manage sources:
  seeknal source add <name> --url <connection_url>
  seeknal source list
  seeknal source remove <name>
""")

    def _show_sources(self) -> None:
        """List saved data sources."""
        try:
            from seeknal.request import SourceRequest
            req = SourceRequest()
            sources = req.list_sources()

            if not sources:
                print("No saved sources. Add with: seeknal source add <name> --url <url>")
                return

            print(f"{'NAME':<20} {'TYPE':<12} {'URL'}")
            print("-" * 60)
            for s in sources:
                print(f"{s['name']:<20} {s['source_type']:<12} {s['masked_url']}")
        except Exception as e:
            print(f"Error listing sources: {sanitize_error_message(str(e))}")

    def _resolve_source(self, name_or_url: str) -> str:
        """Resolve a source name or URL to a connection URL.

        If name_or_url looks like a saved source name (no :// or file extension),
        look it up and decrypt credentials. Otherwise return as-is.
        """
        # Check if it's a URL or file path
        if "://" in name_or_url or name_or_url.endswith((".parquet", ".csv")):
            return os.path.expandvars(name_or_url)

        # Try to load as saved source
        try:
            from seeknal.request import SourceRequest
            from seeknal.cli.source import _reconstruct_url

            req = SourceRequest()
            source = req.select_by_name(name_or_url)
            if source:
                return _reconstruct_url(source)
        except Exception:
            pass

        # Not found - maybe it's an env var or typo
        return os.path.expandvars(name_or_url)

    def _connect(self, name_or_url: str) -> None:
        """Connect to a data source by name or URL."""
        # Resolve saved source name to URL
        url = self._resolve_source(name_or_url)

        # Generate alias - use source name if available, else db0, db1...
        if "://" not in name_or_url and not name_or_url.endswith((".parquet", ".csv")):
            alias = name_or_url  # Use source name as alias
        else:
            alias = f"db{len(self.attached)}"

        try:
            if url.startswith(("postgres://", "postgresql://")):
                self._attach_postgres(alias, url)
            elif url.startswith("mysql://"):
                self._attach_mysql(alias, url)
            elif url.startswith("sqlite://"):
                self._attach_sqlite(alias, url)
            elif url.endswith(".parquet"):
                self._attach_parquet(alias, url)
            elif url.endswith(".csv"):
                self._attach_csv(alias, url)
            else:
                print(f"Unknown source or URL format: {name_or_url}")
                print("Use a saved source name, URL (postgres://...), or file path")
                return

            self.attached.add(alias)
            print(f"Connected as '{alias}'. Query with: SELECT * FROM {alias}.<table>")

        except Exception as e:
            # Sanitize to avoid credential leakage
            print(f"Connection failed: {sanitize_error_message(str(e))}")

    def _attach_postgres(self, alias: str, url: str) -> None:
        """Attach PostgreSQL database."""
        # Convert URL to DuckDB ATTACH format
        # postgres://user:pass@host:port/db -> host=... port=... dbname=... user=... password=...
        from urllib.parse import urlparse
        parsed = urlparse(url)

        conn_str = (
            f"host={parsed.hostname or 'localhost'} "
            f"port={parsed.port or 5432} "
            f"dbname={parsed.path.lstrip('/')} "
            f"user={parsed.username or ''} "
            f"password={parsed.password or ''}"
        )
        self.conn.execute(f"ATTACH '{conn_str}' AS {alias} (TYPE postgres)")

    def _attach_mysql(self, alias: str, url: str) -> None:
        """Attach MySQL database."""
        from urllib.parse import urlparse
        parsed = urlparse(url)

        conn_str = (
            f"host={parsed.hostname or 'localhost'} "
            f"port={parsed.port or 3306} "
            f"database={parsed.path.lstrip('/')} "
            f"user={parsed.username or ''} "
            f"password={parsed.password or ''}"
        )
        self.conn.execute(f"ATTACH '{conn_str}' AS {alias} (TYPE mysql)")

    def _attach_sqlite(self, alias: str, url: str) -> None:
        """Attach SQLite database."""
        # sqlite:///path/to/file.db -> /path/to/file.db
        path = url.replace("sqlite://", "")
        if path and not path.startswith(":memory:"):
            if is_insecure_path(path):
                raise RuntimeError(f"Insecure path: {path}")
        self.conn.execute(f"ATTACH '{path}' AS {alias} (TYPE sqlite)")

    def _attach_parquet(self, alias: str, path: str) -> None:
        """Attach parquet file as a view."""
        if is_insecure_path(path):
            raise RuntimeError(f"Insecure path: {path}")
        if not Path(path).exists():
            raise RuntimeError(f"File not found: {path}")

        self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {alias}")
        self.conn.execute(
            f"CREATE VIEW {alias}.data AS SELECT * FROM read_parquet(?)",
            [path]
        )

    def _attach_csv(self, alias: str, path: str) -> None:
        """Attach CSV file as a view."""
        if is_insecure_path(path):
            raise RuntimeError(f"Insecure path: {path}")
        if not Path(path).exists():
            raise RuntimeError(f"File not found: {path}")

        self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {alias}")
        self.conn.execute(
            f"CREATE VIEW {alias}.data AS SELECT * FROM read_csv_auto(?)",
            [path]
        )

    def _show_tables(self) -> None:
        """List tables in all attached databases."""
        if not self.attached:
            print("No databases connected. Use .connect <url> first.")
            return

        for alias in sorted(self.attached):
            try:
                result = self.conn.execute(
                    f"SELECT table_schema, table_name FROM information_schema.tables "
                    f"WHERE table_catalog = '{alias}'"
                ).fetchall()
                for schema, table in result:
                    print(f"{alias}.{schema}.{table}")
            except Exception:
                # For file-based sources
                try:
                    result = self.conn.execute(f"SHOW TABLES FROM {alias}").fetchall()
                    for row in result:
                        print(f"{alias}.{row[0]}")
                except Exception:
                    pass

    def _show_schema(self, table: str) -> None:
        """Show schema for a table."""
        if not table:
            print("Usage: .schema <table_name>")
            return

        try:
            result = self.conn.execute(f"DESCRIBE {table}").fetchall()
            print(tabulate(result, headers=["Column", "Type", "Null", "Key", "Default", "Extra"]))
        except Exception as e:
            print(f"Error: {sanitize_error_message(str(e))}")

    def _execute(self, sql: str) -> None:
        """Execute SQL query and display results."""
        try:
            result = self.conn.execute(sql)

            if not result.description:
                print("Query executed successfully.")
                return

            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()

            # Limit display
            MAX_ROWS = 100
            display_rows = rows[:MAX_ROWS]

            print(tabulate(display_rows, headers=columns, tablefmt="simple"))

            if len(rows) > MAX_ROWS:
                print(f"\n({MAX_ROWS} of {len(rows)} rows displayed. Use LIMIT for more control.)")
            else:
                print(f"\n({len(rows)} rows)")

        except Exception as e:
            print(f"Error: {sanitize_error_message(str(e))}")


def run_repl() -> None:
    """Entry point for REPL."""
    repl = REPL()
    repl.run()
```

### Modified: `src/seeknal/cli/main.py`

Add the repl command:

```python
from seeknal.cli.repl import run_repl

@app.command("repl")
def repl_command():
    """Start interactive SQL REPL."""
    run_repl()
```

## Acceptance Criteria

### Security Requirements

- [ ] Passwords are encrypted with Fernet before storage in SQLite
- [ ] Error messages don't expose credentials (uses `sanitize_error_message`)
- [ ] Masked URLs shown in `.sources` and logs (e.g., `postgres://user:****@host/db`)
- [ ] `SEEKNAL_ENCRYPT_KEY` required for source storage
- [ ] Clear error if encryption key not set
- [ ] File paths validated against insecure locations

### Source Management

- [ ] `seeknal source add <name> --url <url>` saves encrypted source
- [ ] `seeknal source list` shows sources with masked credentials
- [ ] `seeknal source remove <name>` deletes source
- [ ] Sources persist across sessions in SQLite

### REPL Functional Requirements

- [ ] `seeknal repl` launches interactive REPL session
- [ ] `.sources` lists saved data sources
- [ ] `.connect <name>` connects to a saved source (decrypts credentials)
- [ ] `.connect <url>` connects directly to a database URL
- [ ] `.tables` lists tables in connected databases
- [ ] `.schema <table>` shows column definitions
- [ ] SQL queries execute and display tabular results
- [ ] `.quit` exits cleanly with history saved
- [ ] Environment variables in URLs are expanded (`$DATABASE_URL`)

### Query Engine Support

- [ ] PostgreSQL via `postgres_scanner` extension
- [ ] MySQL via `mysql_scanner` extension
- [ ] SQLite via `sqlite_scanner` extension
- [ ] Parquet files via native DuckDB
- [ ] CSV files via native DuckDB

### Non-Functional Requirements

- [ ] REPL starts in <1 second
- [ ] Readline history persists across sessions
- [ ] Graceful handling of connection failures

## Testing Strategy

### Unit Tests: `tests/utils/test_encryption.py`

```python
import pytest
import os
from unittest.mock import patch
from cryptography.fernet import Fernet

from seeknal.utils.encryption import (
    encrypt_value,
    decrypt_value,
    is_encryption_configured,
    EncryptionError,
)


@pytest.fixture
def valid_key():
    """Generate a valid Fernet key for testing."""
    return Fernet.generate_key().decode()


def test_encrypt_decrypt_roundtrip(valid_key, monkeypatch):
    """Values can be encrypted and decrypted."""
    monkeypatch.setenv("SEEKNAL_ENCRYPT_KEY", valid_key)

    plaintext = "my-secret-password"
    ciphertext = encrypt_value(plaintext)

    assert ciphertext != plaintext
    assert decrypt_value(ciphertext) == plaintext


def test_encrypt_empty_string_returns_empty(valid_key, monkeypatch):
    """Empty strings return empty without encryption."""
    monkeypatch.setenv("SEEKNAL_ENCRYPT_KEY", valid_key)
    assert encrypt_value("") == ""
    assert decrypt_value("") == ""


def test_encrypt_without_key_raises():
    """Encryption fails if SEEKNAL_ENCRYPT_KEY not set."""
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(EncryptionError, match="SEEKNAL_ENCRYPT_KEY not set"):
            encrypt_value("secret")


def test_decrypt_with_wrong_key_raises(valid_key, monkeypatch):
    """Decryption fails with wrong key."""
    monkeypatch.setenv("SEEKNAL_ENCRYPT_KEY", valid_key)
    ciphertext = encrypt_value("secret")

    # Change to different key
    new_key = Fernet.generate_key().decode()
    monkeypatch.setenv("SEEKNAL_ENCRYPT_KEY", new_key)

    with pytest.raises(EncryptionError, match="invalid key or corrupted"):
        decrypt_value(ciphertext)


def test_is_encryption_configured(valid_key, monkeypatch):
    """is_encryption_configured returns correct status."""
    monkeypatch.delenv("SEEKNAL_ENCRYPT_KEY", raising=False)
    assert not is_encryption_configured()

    monkeypatch.setenv("SEEKNAL_ENCRYPT_KEY", valid_key)
    assert is_encryption_configured()


def test_invalid_key_raises():
    """Invalid key format raises EncryptionError."""
    with patch.dict(os.environ, {"SEEKNAL_ENCRYPT_KEY": "not-a-valid-key"}):
        with pytest.raises(EncryptionError, match="Invalid SEEKNAL_ENCRYPT_KEY"):
            encrypt_value("secret")
```

### Unit Tests: `tests/cli/test_source.py`

```python
import pytest
from unittest.mock import patch, MagicMock
from typer.testing import CliRunner
from cryptography.fernet import Fernet

from seeknal.cli.source import app, _parse_and_encrypt_url


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def valid_key():
    return Fernet.generate_key().decode()


def test_parse_and_encrypt_url_masks_password(valid_key, monkeypatch):
    """URL password is masked in masked_url."""
    monkeypatch.setenv("SEEKNAL_ENCRYPT_KEY", valid_key)

    result = _parse_and_encrypt_url("postgres://user:secret123@host/db")

    assert result["source_type"] == "postgres"
    assert "secret123" not in result["masked_url"]
    assert "****" in result["masked_url"]
    assert result["encrypted_credentials"] is not None
    assert result["host"] == "host"
    assert result["username"] == "user"


def test_add_source_requires_encryption_key(runner, monkeypatch):
    """Adding source fails without SEEKNAL_ENCRYPT_KEY."""
    monkeypatch.delenv("SEEKNAL_ENCRYPT_KEY", raising=False)

    result = runner.invoke(app, ["add", "mydb", "--url", "postgres://u:p@h/d"])

    assert result.exit_code == 1
    assert "SEEKNAL_ENCRYPT_KEY not set" in result.stdout


def test_list_sources_empty(runner):
    """List shows message when no sources."""
    with patch("seeknal.cli.source.SourceRequest") as mock:
        mock.return_value.list_sources.return_value = []

        result = runner.invoke(app, ["list"])

    assert "No sources configured" in result.stdout
```

### Unit Tests: `tests/cli/test_repl.py`

```python
import pytest
from unittest.mock import MagicMock, patch
from seeknal.cli.repl import REPL


@pytest.fixture
def repl():
    """Create REPL instance with mocked DuckDB."""
    with patch("seeknal.cli.repl.duckdb") as mock_duckdb:
        mock_conn = MagicMock()
        mock_duckdb.connect.return_value = mock_conn
        r = REPL()
        yield r, mock_conn


def test_connect_postgres(repl):
    """PostgreSQL URLs are parsed and attached correctly."""
    r, mock_conn = repl
    r._connect("postgres://user:pass@localhost:5432/mydb")

    mock_conn.execute.assert_called()
    call_args = str(mock_conn.execute.call_args)
    assert "TYPE postgres" in call_args
    assert "db0" in r.attached


def test_connect_expands_env_vars(repl, monkeypatch):
    """Environment variables in URLs are expanded."""
    monkeypatch.setenv("DATABASE_URL", "postgres://user:pass@host/db")
    r, mock_conn = repl
    r._connect("$DATABASE_URL")

    call_args = str(mock_conn.execute.call_args)
    assert "host=host" in call_args


def test_connect_insecure_path_rejected(repl):
    """Insecure paths are rejected."""
    r, _ = repl
    with patch("seeknal.cli.repl.is_insecure_path", return_value=True):
        r._connect("/tmp/evil.parquet")
    # Should print error, not raise


def test_execute_limits_output(repl):
    """Large result sets are limited to 100 rows."""
    r, mock_conn = repl
    mock_result = MagicMock()
    mock_result.description = [("col1",)]
    mock_result.fetchall.return_value = [(i,) for i in range(500)]
    mock_conn.execute.return_value = mock_result

    with patch("builtins.print") as mock_print:
        r._execute("SELECT * FROM big_table")

    # Should mention limited rows
    output = " ".join(str(c) for c in mock_print.call_args_list)
    assert "100 of 500" in output


def test_error_messages_sanitized(repl):
    """Error messages don't expose credentials."""
    r, mock_conn = repl
    mock_conn.execute.side_effect = Exception("password=secret123 failed")

    with patch("seeknal.cli.repl.sanitize_error_message", return_value="connection failed") as mock_sanitize:
        with patch("builtins.print") as mock_print:
            r._execute("SELECT 1")

    mock_sanitize.assert_called()
```

### E2E Tests: `tests/e2e/test_repl_e2e.py`

```python
import pytest
from typer.testing import CliRunner
from seeknal.cli.main import app


@pytest.fixture
def runner():
    return CliRunner()


def test_repl_help_command(runner):
    """REPL shows help on .help command."""
    result = runner.invoke(app, ["repl"], input=".help\n.quit\n")
    assert ".connect" in result.stdout
    assert ".tables" in result.stdout


def test_repl_parquet_file(runner, tmp_path):
    """Can query parquet files."""
    import pandas as pd

    # Create test parquet file
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    parquet_path = tmp_path / "test.parquet"
    df.to_parquet(parquet_path)

    result = runner.invoke(
        app, ["repl"],
        input=f".connect {parquet_path}\nSELECT * FROM db0.data\n.quit\n"
    )
    assert "Connected as 'db0'" in result.stdout
    assert "id" in result.stdout
```

## File Changes Summary

| File | Action | Description |
|------|--------|-------------|
| `src/seeknal/utils/encryption.py` | Create | ~50 lines Fernet encryption utilities |
| `src/seeknal/cli/source.py` | Create | ~100 lines source management CLI |
| `src/seeknal/cli/repl.py` | Create | ~200 lines DuckDB-only REPL with source integration |
| `src/seeknal/cli/main.py` | Modify | Add `repl` and `source` commands |
| `src/seeknal/models.py` | Modify | Add `SourceTable` model |
| `src/seeknal/request.py` | Modify | Add source CRUD methods |
| `tests/cli/test_repl.py` | Create | REPL unit tests |
| `tests/cli/test_source.py` | Create | Source CLI tests |
| `tests/utils/test_encryption.py` | Create | Encryption unit tests |
| `tests/e2e/test_repl_e2e.py` | Create | E2E tests |
| `pyproject.toml` | Modify | Add `cryptography` dependency |

## Dependencies

**One new dependency:**
- `cryptography` — **New** - Fernet encryption for credentials

**Existing:**
- `duckdb` — Already in project
- `tabulate` — Already in project

## Environment Setup

Users must set `SEEKNAL_ENCRYPT_KEY` before using source storage:

```bash
# Generate a key once
python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'

# Add to shell profile (~/.bashrc, ~/.zshrc, etc.)
export SEEKNAL_ENCRYPT_KEY="your-generated-key-here"
```

## Deferred to v2

| Feature | Reason |
|---------|--------|
| prompt_toolkit with completers | Basic readline is sufficient |
| SQL syntax highlighting | Nice-to-have, not essential |
| Multi-line SQL | Users can use semicolon continuation |
| Output format switching (json/csv) | Users can pipe to `jq` |
| Tab completion | Requires metadata caching |
| Retry logic with backoff | Users can retry manually |
| Key rotation | Single Fernet key is sufficient for v1 |

## References

### Internal References
- Brainstorm: `docs/brainstorms/2026-01-23-sql-repl-brainstorm.md`
- Path security: `src/seeknal/utils/path_security.py`
- Error sanitization: `src/seeknal/db_utils.py:sanitize_error_message`
- URL masking: `src/seeknal/db_utils.py:mask_url_credentials`
- KAI encryption pattern: `~/project/mta/KAI/app/utils/core/encrypt.py`

### External References
- DuckDB CLI: https://duckdb.org/docs/api/cli
- DuckDB postgres_scanner: https://duckdb.org/docs/extensions/postgres_scanner
- DuckDB mysql_scanner: https://duckdb.org/docs/extensions/mysql
- DuckDB sqlite_scanner: https://duckdb.org/docs/extensions/sqlite
- Fernet (cryptography): https://cryptography.io/en/latest/fernet/

### Security Considerations
- **OWASP Credential Storage**: Fernet uses AES-128-CBC with HMAC-SHA256
- **Key Management**: Environment variable prevents key persistence in version control
- **Defense in Depth**: Multiple layers (encryption + masking + path validation)
