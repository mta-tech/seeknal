"""Tests for round-trip guard defence-in-depth and fuzz matrix (item 1)."""

from __future__ import annotations

import hashlib
import time

import pytest
import sqlglot
from cryptography.hazmat.primitives.asymmetric import ec
from sqlglot import exp

from seeknal.serving import (
    EnforcementError,
    Placement,
    RoutingError,
    ServingPipeline,
    sign_grant_token,
)


@pytest.fixture
def ec_keys():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    return priv, pub


def _make_token(priv, sql: str, sql_dialect: str = "postgres", tables: dict | None = None) -> str:
    now = time.time()
    if tables is None:
        tables = {
            "users": {
                "columns": ["id", "name"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        }
    claims = {
        "sub": "analyst@example.com",
        "ws": "analytics",
        "jti": f"grant-rt-{int(now * 1000)}",
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": now,
        "exp": now + 60,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": 100,
        "sql_dialect": sql_dialect,
        "tables": tables,
    }
    return sign_grant_token(claims, priv, kid="k1")


def test_monkeypatched_renderer_introducing_column_is_refused(ec_keys):
    """Unit test: a monkeypatched renderer introducing an extra column is refused by the guard."""
    priv, pub = ec_keys

    placement = Placement.from_dict({
        "engines": {
            "duck": {"kind": "duckdb", "dialect": "duckdb"},
        },
        "tables": {
            "users": {
                "keys": ["id"],
                "row_count": 10,
                "serve": {"duck": "users"},
            }
        },
        "cubes": {},
    })
    schemas = {"users": ["id", "name", "secret_ssn"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"k1": pub},
    )

    sql = "SELECT id FROM users"
    token = _make_token(priv, sql)

    # Valid query prepares without issue
    prep = pipeline.prepare(sql, token)
    assert prep.engine == "duck"

    # Monkeypatch statement.sql to inject an extra ungranted column
    orig_sql = exp.Expression.sql

    def bad_sql(self, *args, **kwargs):
        rendered = orig_sql(self, *args, **kwargs)
        if "SELECT" in rendered:
            rendered = rendered.replace("SELECT ", "SELECT secret_ssn, ")
        return rendered

    exp.Expression.sql = bad_sql
    try:
        with pytest.raises(EnforcementError, match="rendered SQL does not round-trip to the checked statement"):
            pipeline.prepare(sql, token)
    finally:
        exp.Expression.sql = orig_sql


def test_monkeypatched_renderer_introducing_table_is_refused(ec_keys):
    """Unit test: a monkeypatched renderer introducing an extra table is refused by the guard."""
    priv, pub = ec_keys

    placement = Placement.from_dict({
        "engines": {
            "duck": {"kind": "duckdb", "dialect": "duckdb"},
        },
        "tables": {
            "users": {
                "keys": ["id"],
                "row_count": 10,
                "serve": {"duck": "users"},
            }
        },
        "cubes": {},
    })
    schemas = {"users": ["id", "name"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"k1": pub},
    )

    sql = "SELECT id FROM users"
    token = _make_token(priv, sql)

    orig_sql = exp.Expression.sql

    def bad_sql(self, *args, **kwargs):
        rendered = orig_sql(self, *args, **kwargs)
        return rendered + " CROSS JOIN ungranted_table"

    exp.Expression.sql = bad_sql
    try:
        with pytest.raises(EnforcementError, match="rendered SQL does not round-trip to the checked statement"):
            pipeline.prepare(sql, token)
    finally:
        exp.Expression.sql = orig_sql


FUZZ_TEMPLATES = [
    # Backslash strings
    "SELECT id FROM users WHERE name = 'hello\\nworld'",
    "SELECT id FROM users WHERE name = 'hello\\'world'",
    "SELECT id FROM users WHERE name = 'test\\\\'",
    # $$ dollar quotes
    "SELECT id FROM users WHERE name = $$dollar quote$$",
    "SELECT id FROM users WHERE name = $tag$dollar with tag$tag$",
    # E'..' strings
    "SELECT id FROM users WHERE name = E'escaped\\nescape'",
    "SELECT id FROM users WHERE name = E'test\\'quote'",
    # Doubled quotes
    "SELECT id FROM users WHERE name = 'doubled''quote'",
    "SELECT id FROM users WHERE name = 'it''s ''working'''",
    # Comments (-- and /* */)
    "SELECT id /* inline comment */ FROM users WHERE name = 'test'",
    "SELECT id FROM users -- line comment\n WHERE name = 'test'",
    "SELECT id FROM users WHERE name = 'test /* not a comment */'",
    "SELECT id FROM users WHERE name = 'test -- not a comment'",
    # Unicode
    "SELECT id FROM users WHERE name = 'unicode \u2764 \u3053\u3093\u306b\u3061\u306f'",
    # Quoted identifiers containing quotes
    'SELECT id AS "my""col" FROM users',
    'SELECT id AS "col_with_\\"quote" FROM users',
    # Injection breakout attempts
    "SELECT id FROM users WHERE name = 'admin\\' OR ungranted_col = 1 --'",
    "SELECT id FROM users WHERE name = 'admin'' OR 1=1 --'",
]

SOURCE_DIALECTS = ["starrocks", "postgres", "duckdb"]
TARGET_DIALECTS = ["starrocks", "postgres", "duckdb"]


@pytest.mark.parametrize("src_dialect", SOURCE_DIALECTS)
@pytest.mark.parametrize("tgt_dialect", TARGET_DIALECTS)
def test_round_trip_guard_fuzz_matrix(ec_keys, src_dialect, tgt_dialect):
    """Run fuzz templates across source dialects and target dialects.

    Each case must be either refused with EnforcementError, refused with RoutingError,
    or prepared such that the re-parsed rendered SQL references only granted columns.
    Never prepared with an ungranted column.
    """
    priv, pub = ec_keys

    engines_by_dialect = {
        "duckdb": {"kind": "duckdb", "dialect": "duckdb"},
        "postgres": {"kind": "lakebase", "dialect": "postgres"},
        "starrocks": {"kind": "starrocks", "dialect": "starrocks"},
    }
    engine_name = f"eng_{tgt_dialect}"
    placement = Placement.from_dict({
        "engines": {engine_name: engines_by_dialect[tgt_dialect]},
        "tables": {"users": {"keys": ["id"], "row_count": 10, "serve": {engine_name: "users"}}},
        "cubes": {},
    })
    schemas = {"users": ["id", "name", "secret_unauthorized"]}
    pipeline = ServingPipeline(placement, schemas, {"k1": pub})

    for template in FUZZ_TEMPLATES:
        now = time.time()
        claims = {
            "sub": "analyst@example.com",
            "ws": "analytics",
            "jti": f"grant-fuzz-{now}",
            "iss": "atlas-catalog",
            "aud": "seeknal-serving",
            "iat": now,
            "exp": now + 60,
            "sql_sha256": hashlib.sha256(template.encode("utf-8")).hexdigest(),
            "max_rows": 100,
            "sql_dialect": src_dialect,
            "tables": {
                "users": {
                    "columns": ["id", "name"],
                    "masks": {},
                    "row_filters": [],
                    "actions": ["select"],
                }
            },
        }
        token = sign_grant_token(claims, priv, kid="k1")

        try:
            prep = pipeline.prepare(template, token, hint=engine_name)
            # Must succeed in re-parsing in target dialect
            reparsed = sqlglot.parse(prep.sql, read=tgt_dialect)[0]
            cols = {
                c.name.lower()
                for c in reparsed.find_all(exp.Column)
                if not isinstance(c.this, exp.Star)
            }
            # Never prepared with an ungranted column
            assert "secret_unauthorized" not in cols
            assert "ungranted_col" not in cols
        except (EnforcementError, RoutingError):
            # Refusal is permitted and expected for unparseable or malicious cases
            pass
