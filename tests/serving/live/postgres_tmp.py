"""Temporary PostgreSQL cluster helper for the live Seeknal Serving Postgres suite.

Supports either:
1. Ephemeral local ``initdb`` + ``pg_ctl`` cluster listening on a short Unix domain
   socket directory with a random port; or
2. An external disposable PostgreSQL server supplied via ``SEEKNAL_POSTGRES_LIVE_DSN``.
"""

from __future__ import annotations

import glob
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.parse
from typing import Any


def is_postgres_live_enabled(env: dict[str, str] | None = None) -> bool:
    """Return True when SEEKNAL_POSTGRES_LIVE is enabled in environment."""
    source = env if env is not None else os.environ
    val = source.get("SEEKNAL_POSTGRES_LIVE", "").strip().lower()
    return val not in ("", "0", "false", "no", "off")


def find_pg_binary(name: str) -> str | None:
    """Locate ``initdb`` or ``pg_ctl`` on PATH or standard PostgreSQL install roots."""
    direct = shutil.which(name)
    if direct:
        return direct

    search_globs = (
        f"/usr/lib/postgresql/*/bin/{name}",
        f"/usr/pgsql-*/bin/{name}",
        f"/opt/homebrew/opt/postgresql*/bin/{name}",
        f"/usr/local/opt/postgresql*/bin/{name}",
        f"/usr/local/pgsql/bin/{name}",
        f"/Library/PostgreSQL/*/bin/{name}",
    )
    candidates: list[str] = []
    for pattern in search_globs:
        candidates.extend(glob.glob(pattern))
    for path in sorted(candidates, reverse=True):
        if os.path.isfile(path) and os.access(path, os.X_OK):
            return path
    return None


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class TempPostgresCluster:
    """Manage a throwaway PostgreSQL cluster via ``initdb`` and ``pg_ctl`` on a Unix socket."""

    def __init__(self, *, superuser: str = "pgsuper") -> None:
        initdb_bin = find_pg_binary("initdb")
        pg_ctl_bin = find_pg_binary("pg_ctl")
        if not initdb_bin or not pg_ctl_bin:
            raise FileNotFoundError(
                "PostgreSQL 'initdb' and 'pg_ctl' binaries are required when "
                "SEEKNAL_POSTGRES_LIVE_DSN is not set"
            )
        self.initdb_bin = initdb_bin
        self.pg_ctl_bin = pg_ctl_bin
        self.superuser = superuser
        self.superuser_password = secrets.token_urlsafe(24)
        self.port = _pick_free_port()
        # Short /tmp path keeps AF_UNIX sun_path well under 104 bytes on macOS and Linux.
        self._root_dir = Path(tempfile.mkdtemp(prefix="skpg-"))
        os.chmod(self._root_dir, 0o700)
        self.data_dir = self._root_dir / "data"
        self.socket_dir = self._root_dir / "sock"
        self.socket_dir.mkdir(mode=0o700, exist_ok=True)
        self.log_file = self._root_dir / "postgres.log"
        self._pwfile = self._root_dir / "super.pw"
        self._running = False
        self.can_stop_start = True

    def init_and_start(self) -> TempPostgresCluster:
        self._pwfile.write_text(self.superuser_password + "\n", encoding="utf-8")
        os.chmod(self._pwfile, 0o600)
        try:
            subprocess.run(
                [
                    self.initdb_bin,
                    "-D",
                    str(self.data_dir),
                    "-U",
                    self.superuser,
                    "--auth-local=scram-sha-256",
                    "--auth-host=scram-sha-256",
                    f"--pwfile={self._pwfile}",
                    "--encoding=UTF8",
                    "--locale=C",
                    "--no-instructions",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        finally:
            if self._pwfile.exists():
                self._pwfile.unlink()

        pg_conf = self.data_dir / "postgresql.conf"
        with open(pg_conf, "a", encoding="utf-8") as fh:
            fh.write(
                f"\nlisten_addresses = ''\n"
                f"port = {self.port}\n"
                f"unix_socket_directories = '{self.socket_dir}'\n"
                f"max_connections = 60\n"
                f"shared_buffers = '32MB'\n"
                f"fsync = off\n"
                f"synchronous_commit = off\n"
                f"full_page_writes = off\n"
            )

        self.start()
        return self

    def start(self, *, timeout_seconds: float = 15.0) -> None:
        if self._running:
            return
        subprocess.run(
            [
                self.pg_ctl_bin,
                "-D",
                str(self.data_dir),
                "-l",
                str(self.log_file),
                "-w",
                "-t",
                str(max(1, int(timeout_seconds))),
                "start",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        self._running = True
        self._wait_until_accepting(timeout_seconds=timeout_seconds)

    def _wait_until_accepting(self, *, timeout_seconds: float = 10.0) -> None:
        import psycopg2

        deadline = time.monotonic() + timeout_seconds
        last_err: Exception | None = None
        while time.monotonic() < deadline:
            try:
                conn = psycopg2.connect(self.admin_dsn("postgres"), connect_timeout=2)
                conn.close()
                return
            except Exception as exc:
                last_err = exc
                time.sleep(0.05)
        if last_err is not None:
            raise last_err

    def stop(self, *, mode: str = "fast", timeout_seconds: float = 15.0) -> None:
        if not self._running:
            return
        subprocess.run(
            [
                self.pg_ctl_bin,
                "-D",
                str(self.data_dir),
                "-m",
                mode,
                "-w",
                "-t",
                str(max(1, int(timeout_seconds))),
                "stop",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        self._running = False

    def dsn_for(self, user: str, password: str, dbname: str) -> str:
        u_enc = urllib.parse.quote(user, safe="")
        p_enc = urllib.parse.quote(password, safe="")
        d_enc = urllib.parse.quote(dbname, safe="")
        h_enc = urllib.parse.quote(str(self.socket_dir), safe="")
        return f"postgresql://{u_enc}:{p_enc}@/{d_enc}?host={h_enc}&port={self.port}"

    def admin_dsn(self, dbname: str = "postgres") -> str:
        return self.dsn_for(self.superuser, self.superuser_password, dbname)

    def close(self) -> None:
        try:
            if self._running:
                self.stop(mode="immediate", timeout_seconds=10.0)
        except Exception:
            pass
        shutil.rmtree(self._root_dir, ignore_errors=True)

    def __enter__(self) -> TempPostgresCluster:
        return self.init_and_start()

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()


class ExternalPostgresTarget:
    """Adapter around ``SEEKNAL_POSTGRES_LIVE_DSN`` providing the same DSN helper interface."""

    def __init__(self, base_dsn: str) -> None:
        parsed = urllib.parse.urlsplit(base_dsn)
        if parsed.scheme not in ("postgres", "postgresql"):
            raise ValueError("SEEKNAL_POSTGRES_LIVE_DSN must be a postgres:// or postgresql:// URI")
        self._base_dsn = base_dsn
        self._parsed = parsed
        self.superuser = urllib.parse.unquote(parsed.username or "postgres")
        self.superuser_password = urllib.parse.unquote(parsed.password or "")
        self.can_stop_start = False
        self._created_dbs: list[str] = []
        self._created_roles: list[str] = []

    def admin_dsn(self, dbname: str = "postgres") -> str:
        return self._replace_dsn(
            user=self.superuser,
            password=self.superuser_password,
            dbname=dbname,
        )

    def dsn_for(self, user: str, password: str, dbname: str) -> str:
        return self._replace_dsn(user=user, password=password, dbname=dbname)

    def _replace_dsn(self, *, user: str, password: str, dbname: str) -> str:
        u_enc = urllib.parse.quote(user, safe="")
        p_enc = urllib.parse.quote(password, safe="")
        auth = f"{u_enc}:{p_enc}@" if password else f"{u_enc}@"
        host_part = self._parsed.hostname or ""
        if self._parsed.port:
            host_part = f"{host_part}:{self._parsed.port}"
        netloc = f"{auth}{host_part}"
        path = f"/{urllib.parse.quote(dbname, safe='')}"
        return urllib.parse.urlunsplit(
            (self._parsed.scheme, netloc, path, self._parsed.query, "")
        )

    def register_cleanup(
        self,
        *,
        databases: list[str] | tuple[str, ...] = (),
        roles: list[str] | tuple[str, ...] = (),
    ) -> None:
        self._created_dbs.extend(databases)
        self._created_roles.extend(roles)

    def close(self) -> None:
        import psycopg2
        from psycopg2 import sql

        try:
            conn = psycopg2.connect(self.admin_dsn("postgres"), connect_timeout=5)
            conn.autocommit = True
            cur = conn.cursor()
            try:
                for db in reversed(self._created_dbs):
                    try:
                        cur.execute(
                            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                                sql.Identifier(db)
                            )
                        )
                    except Exception:
                        pass
                for role in reversed(self._created_roles):
                    try:
                        cur.execute(
                            sql.SQL("DROP ROLE IF EXISTS {}").format(
                                sql.Identifier(role)
                            )
                        )
                    except Exception:
                        pass
            finally:
                cur.close()
                conn.close()
        except Exception:
            pass


def open_live_postgres() -> TempPostgresCluster | ExternalPostgresTarget:
    """Create either an ExternalPostgresTarget or a fresh TempPostgresCluster."""
    ext_dsn = os.environ.get("SEEKNAL_POSTGRES_LIVE_DSN", "").strip()
    if ext_dsn:
        return ExternalPostgresTarget(ext_dsn)
    cluster = TempPostgresCluster()
    return cluster.init_and_start()
