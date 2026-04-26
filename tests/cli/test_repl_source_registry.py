"""Tests for REPL attachment using Ask source registry namespaces."""

from __future__ import annotations

from pathlib import Path
import textwrap


class _FakeDuckDBConnection:
    def __init__(self) -> None:
        self.sql: list[str] = []

    def execute(self, sql: str):
        self.sql.append(sql)
        return self


class _FakeDuckDBModule:
    def __init__(self, conn: _FakeDuckDBConnection) -> None:
        self.conn = conn

    def connect(self, _target: str):
        return self.conn


def test_repl_attaches_explicit_postgres_source_under_namespace(tmp_path: Path, monkeypatch):
    (tmp_path / "seeknal_agent.yml").write_text(
        textwrap.dedent(
            """\
            mode:
              default: auto
            sources:
              warehouse:
                source_kind: connected
                source_type: database
                connector: postgresql
                namespace: wh
                access: read_only
                role: business_source_of_truth
            """
        )
    )
    (tmp_path / "profiles.yml").write_text(
        textwrap.dedent(
            """\
            connections:
              warehouse:
                type: postgresql
                host: localhost
                port: 5432
                user: analyst
                password: secret
                database: analytics
            """
        )
    )

    fake_conn = _FakeDuckDBConnection()
    monkeypatch.setattr("seeknal.cli.repl.duckdb", _FakeDuckDBModule(fake_conn))

    from seeknal.cli.repl import REPL

    repl = REPL(project_path=tmp_path, skip_history=True)

    attach_sql = [sql for sql in fake_conn.sql if sql.startswith("ATTACH ")]
    assert len(attach_sql) == 1
    assert ' AS "wh" ' in attach_sql[0]
    assert 'READ_ONLY' in attach_sql[0]
    assert repl.attached == {"wh"}
