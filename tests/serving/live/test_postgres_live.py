"""Live PostgreSQL suite for Seeknal Serving (§8 least-privilege SQL, introspection, guardrails, cold start, role guard).

Skipped cleanly unless ``SEEKNAL_POSTGRES_LIVE`` is set in ``os.environ``.
Uses ``TempPostgresCluster`` (``initdb`` + ``pg_ctl`` on a short Unix socket) or
``SEEKNAL_POSTGRES_LIVE_DSN`` when provided.
"""

from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import secrets
import threading
import time
from typing import Any

from cryptography.hazmat.primitives.asymmetric import ec
import psycopg2
from psycopg2 import sql as pgsql
import pytest
from starlette.testclient import TestClient

from seeknal.serving.adapters import EngineError
from seeknal.serving.discovery.build import discover
from seeknal.serving.grant_token import sign_grant_token
from seeknal.serving.lakebase_endpoint import (
    ColdStartConnector,
    EngineStartingError,
    LakebaseColdStartConfig,
    check_role_guard,
)
from seeknal.serving.ops import ReadinessProbe
from seeknal.serving.parameter_binding import parameter_digest
from seeknal.serving.postgres_session import (
    PostgresSessionConfig,
    begin_query_transaction,
    build_connect_kwargs,
    verify_and_harden_session,
)
from seeknal.serving.server.app import build_server
from seeknal.serving.server.config import ServerConfig

from tests.serving.live.postgres_tmp import (
    ExternalPostgresTarget,
    TempPostgresCluster,
    is_postgres_live_enabled,
    open_live_postgres,
)

pytestmark = pytest.mark.skipif(
    not is_postgres_live_enabled(),
    reason="Live PostgreSQL suite requires SEEKNAL_POSTGRES_LIVE=1",
)


@pytest.fixture(scope="module")
def live_pg() -> Any:
    try:
        pg = open_live_postgres()
    except FileNotFoundError as exc:
        pytest.skip(str(exc))
    try:
        yield pg
    finally:
        pg.close()


@pytest.fixture(scope="module")
def seeded_cbn_cluster(live_pg: TempPostgresCluster | ExternalPostgresTarget) -> dict[str, Any]:
    """Apply §8 least-privilege SQL with generated passwords, seed 1M synthetic rows, and write sync-state."""
    suffix = secrets.token_hex(4)
    dbname = f"cbn_report_{suffix}"
    ops_dbname = f"cbn_ops_{suffix}"
    role_read_rpt = f"seeknal_read_rpt_{suffix}"
    role_read_pii = f"seeknal_read_pii_{suffix}"
    role_bi = f"seeknal_bi_{suffix}"
    role_ask = f"seeknal_ask_{suffix}"
    role_bypass = f"seeknal_bypassrls_{suffix}"

    pw_bi = secrets.token_urlsafe(24)
    pw_ask = secrets.token_urlsafe(24)
    pw_bypass = secrets.token_urlsafe(24)

    if isinstance(live_pg, ExternalPostgresTarget):
        live_pg.register_cleanup(
            databases=[dbname, ops_dbname],
            roles=[role_read_rpt, role_read_pii, role_bi, role_ask, role_bypass],
        )

    admin_conn = psycopg2.connect(live_pg.admin_dsn("postgres"), connect_timeout=5)
    admin_conn.autocommit = True
    cur = admin_conn.cursor()
    try:
        cur.execute(pgsql.SQL("CREATE DATABASE {}").format(pgsql.Identifier(dbname)))
        cur.execute(pgsql.SQL("CREATE DATABASE {}").format(pgsql.Identifier(ops_dbname)))

        cur.execute(pgsql.SQL("CREATE ROLE {} NOLOGIN").format(pgsql.Identifier(role_read_rpt)))
        cur.execute(pgsql.SQL("CREATE ROLE {} NOLOGIN").format(pgsql.Identifier(role_read_pii)))
        cur.execute(
            pgsql.SQL(
                "CREATE ROLE {bi} LOGIN PASSWORD %s NOSUPERUSER NOCREATEDB NOCREATEROLE "
                "NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 30 IN ROLE {r_rpt}, {r_pii}"
            ).format(
                bi=pgsql.Identifier(role_bi),
                r_rpt=pgsql.Identifier(role_read_rpt),
                r_pii=pgsql.Identifier(role_read_pii),
            ),
            (pw_bi,),
        )
        cur.execute(
            pgsql.SQL(
                "CREATE ROLE {ask} LOGIN PASSWORD %s NOSUPERUSER NOCREATEDB NOCREATEROLE "
                "NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 10 IN ROLE {r_rpt}"
            ).format(
                ask=pgsql.Identifier(role_ask),
                r_rpt=pgsql.Identifier(role_read_rpt),
            ),
            (pw_ask,),
        )
        cur.execute(
            pgsql.SQL(
                "CREATE ROLE {bp} LOGIN PASSWORD %s NOSUPERUSER NOCREATEDB NOCREATEROLE "
                "NOREPLICATION BYPASSRLS"
            ).format(bp=pgsql.Identifier(role_bypass)),
            (pw_bypass,),
        )

        for r_ident in (role_bi, role_ask):
            cur.execute(
                pgsql.SQL("ALTER ROLE {} SET default_transaction_read_only = on").format(
                    pgsql.Identifier(r_ident)
                )
            )
            cur.execute(
                pgsql.SQL("ALTER ROLE {} SET statement_timeout = '30s'").format(
                    pgsql.Identifier(r_ident)
                )
            )
            cur.execute(
                pgsql.SQL("ALTER ROLE {} SET lock_timeout = '2s'").format(
                    pgsql.Identifier(r_ident)
                )
            )
            cur.execute(
                pgsql.SQL(
                    "ALTER ROLE {} SET idle_in_transaction_session_timeout = '10s'"
                ).format(pgsql.Identifier(r_ident))
            )
            cur.execute(
                pgsql.SQL("ALTER ROLE {} SET search_path = pg_catalog").format(
                    pgsql.Identifier(r_ident)
                )
            )
    finally:
        cur.close()
        admin_conn.close()

    # Seed cbn_report
    db_conn = psycopg2.connect(live_pg.admin_dsn(dbname), connect_timeout=5)
    db_conn.autocommit = True
    dcur = db_conn.cursor()
    try:
        dcur.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
        dcur.execute("CREATE SCHEMA rpt")
        dcur.execute("CREATE SCHEMA src_pii")
        dcur.execute("CREATE SCHEMA unlisted_internal")

        dcur.execute(
            """
            CREATE TABLE public._lakebase_sync_state (
                target_table TEXT PRIMARY KEY,
                snapshot_id BIGINT,
                synced_at TIMESTAMPTZ NOT NULL
            )
            """
        )
        synced_ts = datetime.now(timezone.utc) - timedelta(seconds=45)
        dcur.execute(
            "INSERT INTO public._lakebase_sync_state (target_table, snapshot_id, synced_at) VALUES (%s, %s, %s)",
            ("rpt.rpt_txn_detail", 9001, synced_ts),
        )

        dcur.execute(
            """
            CREATE TABLE rpt.rpt_txn_detail (
                txn_id BIGINT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                branch_code TEXT NOT NULL,
                amount BIGINT NOT NULL,
                partial_col TEXT,
                invalid_idx_col TEXT
            )
            """
        )
        dcur.execute(
            """
            CREATE TABLE rpt.rpt_daily_branch (
                branch_code TEXT PRIMARY KEY,
                total_txns BIGINT NOT NULL
            )
            """
        )
        dcur.execute(
            """
            CREATE TABLE src_pii.customer (
                customer_id TEXT PRIMARY KEY,
                full_name TEXT NOT NULL,
                msisdn_hash TEXT NOT NULL,
                segment TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                ungranted_national_id TEXT
            )
            """
        )
        dcur.execute("CREATE TABLE unlisted_internal.secret_ledger (id INT PRIMARY KEY)")

        # Seed 1,000,000 synthetic transactions (customer C-000042 gets 25 deterministic rows)
        dcur.execute(
            """
            INSERT INTO rpt.rpt_txn_detail (
                txn_id, customer_id, as_of_date, branch_code, amount, partial_col, invalid_idx_col
            )
            SELECT
                g,
                CASE
                    WHEN g <= 25 THEN 'C-000042'
                    ELSE 'C-' || lpad(((g % 50000) + 100)::text, 6, '0')
                END,
                DATE '2026-10-01' + ((g % 28))::int,
                'B' || lpad(((g % 20) + 1)::text, 2, '0'),
                (g * 10)::bigint,
                'P' || (g % 5)::text,
                'I' || (g % 5)::text
            FROM generate_series(1, 1000000) AS g
            """
        )
        dcur.execute(
            """
            INSERT INTO rpt.rpt_daily_branch (branch_code, total_txns)
            VALUES ('B01', 50000), ('B02', 50000)
            """
        )
        dcur.execute(
            """
            INSERT INTO src_pii.customer (
                customer_id, full_name, msisdn_hash, segment, updated_at, ungranted_national_id
            )
            VALUES
                ('C-000042', 'Budi Santoso', 'hash_42', 'PRIORITY', TIMESTAMPTZ '2026-10-28T00:00:00Z', 'NIK-SECRET-42'),
                ('C-000101', 'Siti Aminah', 'hash_101', 'RETAIL', TIMESTAMPTZ '2026-10-28T00:00:00Z', 'NIK-SECRET-101')
            """
        )

        # Valid composite btree indexes (§8)
        dcur.execute(
            "CREATE INDEX CONCURRENTLY idx_txn_cust_date_id ON rpt.rpt_txn_detail (customer_id, as_of_date, txn_id)"
        )
        dcur.execute(
            "CREATE INDEX CONCURRENTLY idx_txn_date_id ON rpt.rpt_txn_detail (as_of_date, txn_id)"
        )
        dcur.execute(
            "CREATE INDEX CONCURRENTLY idx_cust_msisdn ON src_pii.customer (msisdn_hash)"
        )

        # Partial index, expression index, and invalid index (must be ignored by derivation!)
        dcur.execute(
            "CREATE INDEX idx_txn_partial_ignored ON rpt.rpt_txn_detail (partial_col) WHERE amount > 100"
        )
        dcur.execute(
            "CREATE INDEX idx_txn_expr_ignored ON rpt.rpt_txn_detail ((lower(branch_code)))"
        )
        dcur.execute(
            "CREATE INDEX idx_txn_invalid_ignored ON rpt.rpt_txn_detail (invalid_idx_col)"
        )
        dcur.execute(
            """
            UPDATE pg_catalog.pg_index
               SET indisvalid = false
             WHERE indexrelid = 'rpt.idx_txn_invalid_ignored'::regclass
            """
        )

        # Grants (§8 least-privilege)
        dcur.execute(
            pgsql.SQL("GRANT CONNECT ON DATABASE {} TO {}, {}").format(
                pgsql.Identifier(dbname),
                pgsql.Identifier(role_read_rpt),
                pgsql.Identifier(role_read_pii),
            )
        )
        dcur.execute(
            pgsql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(
                pgsql.Identifier(role_read_rpt)
            )
        )
        dcur.execute(
            pgsql.SQL("GRANT SELECT ON public._lakebase_sync_state TO {}").format(
                pgsql.Identifier(role_read_rpt)
            )
        )
        dcur.execute(
            pgsql.SQL("GRANT USAGE ON SCHEMA rpt TO {}").format(
                pgsql.Identifier(role_read_rpt)
            )
        )
        dcur.execute(
            pgsql.SQL(
                "GRANT SELECT ON rpt.rpt_txn_detail, rpt.rpt_daily_branch TO {}"
            ).format(pgsql.Identifier(role_read_rpt))
        )
        dcur.execute(
            pgsql.SQL("GRANT USAGE ON SCHEMA src_pii TO {}").format(
                pgsql.Identifier(role_read_pii)
            )
        )
        dcur.execute(
            pgsql.SQL(
                "GRANT SELECT (customer_id, full_name, msisdn_hash, segment, updated_at) "
                "ON src_pii.customer TO {}"
            ).format(pgsql.Identifier(role_read_pii))
        )

        dcur.execute("ANALYZE rpt.rpt_txn_detail")
        dcur.execute("ANALYZE rpt.rpt_daily_branch")
        dcur.execute("ANALYZE src_pii.customer")
    finally:
        dcur.close()
        db_conn.close()

    # Seed second operational database for cross-source join / pii_cross_engine tests
    ops_conn = psycopg2.connect(live_pg.admin_dsn(ops_dbname), connect_timeout=5)
    ops_conn.autocommit = True
    ocur = ops_conn.cursor()
    try:
        ocur.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
        ocur.execute("CREATE SCHEMA ops")
        ocur.execute(
            "CREATE TABLE ops.case_status (case_id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, status TEXT NOT NULL)"
        )
        ocur.execute(
            "INSERT INTO ops.case_status VALUES ('CASE-1', 'C-000042', 'OPEN')"
        )
        ocur.execute("ANALYZE ops.case_status")
        ocur.execute(
            pgsql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                pgsql.Identifier(ops_dbname),
                pgsql.Identifier(role_read_rpt),
            )
        )
        ocur.execute(
            pgsql.SQL("GRANT USAGE ON SCHEMA ops TO {}").format(
                pgsql.Identifier(role_read_rpt)
            )
        )
        ocur.execute(
            pgsql.SQL("GRANT SELECT ON ops.case_status TO {}").format(
                pgsql.Identifier(role_read_rpt)
            )
        )
    finally:
        ocur.close()
        ops_conn.close()

    return {
        "cluster": live_pg,
        "dbname": dbname,
        "ops_dbname": ops_dbname,
        "bi_dsn": live_pg.dsn_for(role_bi, pw_bi, dbname),
        "ask_dsn": live_pg.dsn_for(role_ask, pw_ask, dbname),
        "ops_dsn": live_pg.dsn_for(role_bi, pw_bi, ops_dbname),
        "super_dsn": live_pg.admin_dsn(dbname),
        "bypass_dsn": live_pg.dsn_for(role_bypass, pw_bypass, dbname),
        "passwords": (pw_bi, pw_ask, pw_bypass, live_pg.superuser_password),
    }


def _mint_sql_grant(
    priv_key: ec.EllipticCurvePrivateKey,
    *,
    sql_text: str,
    tables: dict[str, Any],
    jti: str,
    workload: str = "interactive",
    max_rows: int = 100,
    params: list[Any] | None = None,
) -> str:
    now = time.time()
    claims: dict[str, Any] = {
        "sub": "analyst@cbn.example",
        "ws": "ws-cbn",
        "jti": jti,
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": now,
        "exp": now + 60.0,
        "typ": "grant",
        "workload": workload,
        "sql_dialect": "postgres",
        "sql_sha256": hashlib.sha256(sql_text.encode("utf-8")).hexdigest(),
        "max_rows": max_rows,
        "tables": tables,
    }
    if params is not None:
        claims["params_sha256"] = parameter_digest(params)
    return sign_grant_token(claims, priv_key, kid="live-kid")


def test_live_introspection_ignores_partial_expression_invalid_indexes_and_reports_sync_lag(
    seeded_cbn_cluster: dict[str, Any],
):
    disc_cfg = {
        "default_catalog": "cbn",
        "refresh_interval_seconds": 60.0,
        "engines": {
            "pg_report": {"kind": "postgres", "dialect": "postgres"},
            "pg_ops": {"kind": "postgres", "dialect": "postgres"},
        },
        "postgres_sources": {
            "pg_report": {
                "account": "interactive",
                "logical_catalog": "cbn",
                "freshness": {
                    "sync_state_table": "public._lakebase_sync_state",
                },
                "schemas": {
                    "rpt": {"include": ["rpt_*"]},
                    "src_pii": {"include": ["customer"], "pii": True},
                },
            },
            "pg_ops": {
                "account": "interactive",
                "logical_catalog": "ops",
                "schemas": {"ops": {"include": ["case_status"]}},
            },
        },
    }

    res = discover(
        disc_cfg,
        postgres_connects={
            "pg_report": lambda: psycopg2.connect(seeded_cbn_cluster["bi_dsn"]),
            "pg_ops": lambda: psycopg2.connect(seeded_cbn_cluster["ops_dsn"]),
        },
        connections={
            "pg_report": {"kind": "postgres"},
            "pg_ops": {"kind": "postgres"},
        },
    )

    tables = res.placement["tables"]
    # 1. Unlisted schema is never exposed
    assert not any("unlisted_internal" in t for t in tables)

    # 2. Ungranted column on src_pii.customer is excluded
    pii_cols = res.schemas["cbn.src_pii.customer"]
    assert pii_cols == [
        "customer_id",
        "full_name",
        "msisdn_hash",
        "segment",
        "updated_at",
    ]
    assert "ungranted_national_id" not in pii_cols

    # 3. Partial, expression, and invalid indexes are ignored in derived filterable/sortable
    txn_access = tables["cbn.rpt.rpt_txn_detail"]["access"]
    assert txn_access["pattern"] == "filter"
    assert txn_access["require_filter"] is True
    assert set(txn_access["filterable"]) == {"txn_id", "customer_id", "as_of_date"}
    assert "partial_col" not in txn_access["filterable"]
    assert "branch_code" not in txn_access["filterable"]
    assert "invalid_idx_col" not in txn_access["filterable"]

    # 4. Sync-state lag is reported from public._lakebase_sync_state (>= 45s + refresh_interval)
    txn_serve = tables["cbn.rpt.rpt_txn_detail"]["serve"]["pg_report"]
    assert txn_serve["max_lag_seconds"] is not None
    assert txn_serve["max_lag_seconds"] >= 100.0


def test_live_readonly_rejects_insert_25006_show_hardening_and_role_guard(
    seeded_cbn_cluster: dict[str, Any],
):
    session_cfg = PostgresSessionConfig(
        statement_timeout_ms=30000,
        lock_timeout_ms=2000,
        idle_in_transaction_session_timeout_ms=10000,
        search_path="pg_catalog",
        application_name="seeknal-live-test",
        startup_options=True,
    )
    ckw = build_connect_kwargs(
        seeded_cbn_cluster["bi_dsn"],
        connect_timeout=5.0,
        session=session_cfg,
        tls=None,
        read_only=True,
        kind="postgres",
    )
    conn = psycopg2.connect(seeded_cbn_cluster["bi_dsn"], **ckw)
    try:
        verify_and_harden_session(
            conn,
            kind="postgres",
            read_only=True,
            session=session_cfg,
            role_guard=True,
        )
        cur = conn.cursor()
        try:
            # 1. SHOW values match
            cur.execute("SHOW default_transaction_read_only")
            assert cur.fetchone()[0] == "on"
            cur.execute("SHOW search_path")
            assert cur.fetchone()[0] == "pg_catalog"

            # 2. INSERT is rejected with SQLSTATE 25006 (read_only_sql_transaction)
            with pytest.raises(psycopg2.Error) as exc_ro:
                cur.execute(
                    "INSERT INTO rpt.rpt_daily_branch (branch_code, total_txns) VALUES ('B99', 1)"
                )
            assert exc_ro.value.pgcode == "25006"
            conn.rollback()
        finally:
            cur.close()
    finally:
        conn.close()

    # 3. Role guard refuses superuser and BYPASSRLS roles
    super_conn = psycopg2.connect(seeded_cbn_cluster["super_dsn"], connect_timeout=5)
    with pytest.raises(EngineError, match="Role guard refused connection"):
        check_role_guard(super_conn, "postgres")

    bypass_conn = psycopg2.connect(seeded_cbn_cluster["bypass_dsn"], connect_timeout=5)
    with pytest.raises(EngineError, match="Role guard refused connection"):
        check_role_guard(bypass_conn, "postgres")

    # 4. B-M1: With session default_transaction_read_only = off on a non-autocommit
    # psycopg2 connection, begin_query_transaction sets transaction_read_only = on
    # and rejects INSERT with SQLSTATE 25006.
    rw_conn = psycopg2.connect(seeded_cbn_cluster["ops_dsn"], connect_timeout=5)
    try:
        rw_cur = rw_conn.cursor()
        try:
            rw_cur.execute("SET default_transaction_read_only = off")
            rw_conn.commit()
            rw_cur.execute("SHOW default_transaction_read_only")
            assert rw_cur.fetchone()[0] == "off"
            rw_conn.rollback()

            begin_query_transaction(
                rw_cur,
                read_only=True,
                timeout_ms=10000,
                workload="interactive",
                audit_id="live-bm1-ro",
                hardened=True,
                has_row_limit=True,
            )
            rw_cur.execute("SHOW transaction_read_only")
            assert rw_cur.fetchone()[0] == "on"
            with pytest.raises(psycopg2.Error) as exc_tx_ro:
                rw_cur.execute(
                    "INSERT INTO ops.case_status (case_id, customer_id, status) VALUES ('C999', 'C-000042', 'OPEN')"
                )
            assert exc_tx_ro.value.pgcode == "25006"
            rw_conn.rollback()
        finally:
            rw_cur.close()
    finally:
        rw_conn.close()


def test_live_http_queries_guardrails_keyset_masks_explain_index_scan_timeout_and_pii_cross_engine(
    tmp_path: Path, seeded_cbn_cluster: dict[str, Any]
):
    priv = ec.generate_private_key(ec.SECP256R1())
    pn = priv.public_key().public_numbers()
    jwks_file = tmp_path / "grants.jwks.json"
    jwks_file.write_text(
        json.dumps(
            {
                "keys": [
                    {
                        "kty": "EC",
                        "crv": "P-256",
                        "x": base64.urlsafe_b64encode(pn.x.to_bytes(32, "big"))
                        .rstrip(b"=")
                        .decode("ascii"),
                        "y": base64.urlsafe_b64encode(pn.y.to_bytes(32, "big"))
                        .rstrip(b"=")
                        .decode("ascii"),
                        "kid": "live-kid",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    cursor_secret = secrets.token_urlsafe(32)
    env = {
        "LIVE_PG_BI_DSN": seeded_cbn_cluster["bi_dsn"],
        "LIVE_PG_ASK_DSN": seeded_cbn_cluster["ask_dsn"],
        "LIVE_PG_OPS_DSN": seeded_cbn_cluster["ops_dsn"],
        "LIVE_CURSOR_KEY": cursor_secret,
    }

    cfg_dict = {
        "default_catalog": "cbn",
        "grant_keys": {"jwks_file": str(jwks_file)},
        "cursor_key_env": "LIVE_CURSOR_KEY",
        "connections": {
            "pg_report": {
                "kind": "postgres",
                "mode": "read_only",
                "accounts": {
                    "interactive": {"dsn_env": "LIVE_PG_BI_DSN"},
                    "ask": {"dsn_env": "LIVE_PG_ASK_DSN"},
                },
                "pool_max": 6,
                "session": {
                    "statement_timeout_ms": 30000,
                    "lock_timeout_ms": 2000,
                    "idle_in_transaction_session_timeout_ms": 10000,
                    "search_path": "pg_catalog",
                },
            },
            "pg_ops": {
                "kind": "postgres",
                "mode": "read_only",
                "dsn_env": "LIVE_PG_OPS_DSN",
                "pool_max": 4,
                "session": {
                    "statement_timeout_ms": 30000,
                    "lock_timeout_ms": 2000,
                    "idle_in_transaction_session_timeout_ms": 10000,
                    "search_path": "pg_catalog",
                },
            },
        },
        "placement": {
            "discovery": {
                "default_catalog": "cbn",
                "refresh_interval_seconds": 300.0,
                "engines": {
                    "pg_report": {"kind": "postgres", "dialect": "postgres"},
                    "pg_ops": {"kind": "postgres", "dialect": "postgres"},
                },
                "postgres_sources": {
                    "pg_report": {
                        "account": "interactive",
                        "logical_catalog": "cbn",
                        "freshness": {
                            "sync_state_table": "public._lakebase_sync_state",
                        },
                        "schemas": {
                            "rpt": {"include": ["rpt_*"]},
                            "src_pii": {"include": ["customer"], "pii": True},
                        },
                    },
                    "pg_ops": {
                        "account": "interactive",
                        "logical_catalog": "ops",
                        "schemas": {"ops": {"include": ["case_status"]}},
                    },
                },
            }
        },
    }

    srv_cfg = ServerConfig.from_dict(cfg_dict, env=env)
    handle = build_server(srv_cfg, env=env, run_initial_refresh=True)

    txn_grant = {
        "cbn.rpt.rpt_txn_detail": {
            "columns": ["txn_id", "customer_id", "as_of_date", "branch_code", "amount"],
            "masks": {},
            "row_filters": [],
            "actions": ["select"],
        }
    }
    pii_masked_grant = {
        "cbn.src_pii.customer": {
            "columns": ["customer_id", "segment"],
            "masks": {"full_name": "redact"},
            "row_filters": [],
            "actions": ["select"],
        }
    }

    with TestClient(handle.app) as client:
        # 1. Unfiltered query on 1M-row filter table -> 422 filter_required
        unfiltered_sql = "SELECT txn_id, amount FROM cbn.rpt.rpt_txn_detail LIMIT 10"
        tok_unfiltered = _mint_sql_grant(
            priv, sql_text=unfiltered_sql, tables=txn_grant, jti="live-unfiltered-1"
        )
        r_unfiltered = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok_unfiltered}"},
            json={"sql": unfiltered_sql},
        )
        assert r_unfiltered.status_code == 422
        assert r_unfiltered.json()["code"] == "filter_required"

        # 2. Filtered query -> 200 OK
        filtered_sql = (
            "SELECT txn_id, customer_id, as_of_date, amount "
            "FROM cbn.rpt.rpt_txn_detail "
            "WHERE customer_id = $1 "
            "ORDER BY as_of_date DESC, txn_id DESC LIMIT 10"
        )
        tok_filtered = _mint_sql_grant(
            priv,
            sql_text=filtered_sql,
            tables=txn_grant,
            jti="live-filtered-1",
            params=["C-000042"],
        )
        r_filtered = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok_filtered}"},
            json={"sql": filtered_sql, "params": ["C-000042"]},
        )
        assert r_filtered.status_code == 200
        assert len(r_filtered.json()["rows"]) == 10

        # 3. EXPLAIN shows an Index Scan on the 1M-row table
        tok_exp = _mint_sql_grant(
            priv,
            sql_text=filtered_sql,
            tables=txn_grant,
            jti="live-explain-1",
            params=["C-000042"],
        )
        r_exp = client.post(
            "/v1/explain",
            headers={"Authorization": f"Bearer {tok_exp}"},
            json={"sql": filtered_sql, "params": ["C-000042"]},
        )
        assert r_exp.status_code == 200
        compiled_sql = r_exp.json()["sql"]
        with psycopg2.connect(seeded_cbn_cluster["bi_dsn"]) as pg_exp_conn:
            with pg_exp_conn.cursor() as pg_exp_cur:
                pg_exp_cur.execute(f"EXPLAIN {compiled_sql}", ("C-000042",))
                plan_lines = "\n".join(row[0] for row in pg_exp_cur.fetchall())
        assert "Index Scan" in plan_lines or "Index Only Scan" in plan_lines
        assert "Seq Scan" not in plan_lines

        # 4. Keyset pagination across all 25 rows of C-000042 has zero gaps and zero duplicates
        route_paths = {getattr(rt, "path", None) for rt in handle.app.routes}
        if "/v1/select" in route_paths:
            collected_ids: list[int] = []
            cursor_after: str | None = None
            page_num = 0
            while True:
                page_num += 1
                sel_body: dict[str, Any] = {
                    "table": "cbn.rpt.rpt_txn_detail",
                    "columns": ["txn_id", "as_of_date", "amount"],
                    "where": {"customer_id": {"_eq": "C-000042"}},
                    "order_by": [{"as_of_date": "desc"}, {"txn_id": "desc"}],
                    "limit": 7,
                }
                if cursor_after is not None:
                    sel_body["after"] = cursor_after
                raw_bytes = json.dumps(sel_body, separators=(",", ":")).encode("utf-8")
                now = time.time()
                sel_tok = sign_grant_token(
                    {
                        "sub": "analyst@cbn.example",
                        "ws": "ws-cbn",
                        "jti": f"live-select-p{page_num}",
                        "iss": "atlas-catalog",
                        "aud": "seeknal-serving",
                        "iat": now,
                        "exp": now + 60.0,
                        "typ": "grant",
                        "api": "select.v1",
                        "workload": "interactive",
                        "sql_dialect": "postgres",
                        "sql_sha256": hashlib.sha256(raw_bytes).hexdigest(),
                        "max_rows": 100,
                        "tables": txn_grant,
                    },
                    priv,
                    kid="live-kid",
                )
                r_sel = client.post(
                    "/v1/select",
                    headers={
                        "Authorization": f"Bearer {sel_tok}",
                        "Content-Type": "application/json",
                    },
                    content=raw_bytes,
                )
                assert r_sel.status_code == 200
                pdata = r_sel.json()
                collected_ids.extend(int(row[0]) for row in pdata["rows"])
                cursor_after = pdata.get("next")
                if not cursor_after:
                    break
            assert sorted(collected_ids) == list(range(1, 26))
            assert len(collected_ids) == 25
        else:
            # SQL keyset fallback when running on a branch before /v1/select is merged
            collected_ids = []
            last_date: str | None = None
            last_id: int | None = None
            page_num = 0
            while True:
                page_num += 1
                if last_date is None:
                    ks_sql = (
                        "SELECT txn_id, as_of_date FROM cbn.rpt.rpt_txn_detail "
                        "WHERE customer_id = $1 ORDER BY as_of_date DESC, txn_id DESC LIMIT 7"
                    )
                    ks_params: list[Any] = ["C-000042"]
                else:
                    ks_sql = (
                        "SELECT txn_id, as_of_date FROM cbn.rpt.rpt_txn_detail "
                        "WHERE customer_id = $1 AND (as_of_date < CAST($2 AS DATE) "
                        "OR (as_of_date = CAST($2 AS DATE) AND txn_id < $3)) "
                        "ORDER BY as_of_date DESC, txn_id DESC LIMIT 7"
                    )
                    ks_params = ["C-000042", last_date, last_id]
                ks_tok = _mint_sql_grant(
                    priv,
                    sql_text=ks_sql,
                    tables=txn_grant,
                    jti=f"live-ks-{page_num}",
                    params=ks_params,
                )
                r_ks = client.post(
                    "/v1/query",
                    headers={"Authorization": f"Bearer {ks_tok}"},
                    json={"sql": ks_sql, "params": ks_params},
                )
                assert r_ks.status_code == 200
                rows = r_ks.json()["rows"]
                if not rows:
                    break
                for r_item in rows:
                    collected_ids.append(int(r_item[0]))
                last_id = int(rows[-1][0])
                last_date = str(rows[-1][1])
                if len(rows) < 7:
                    break
            assert sorted(collected_ids) == list(range(1, 26))

        # 5. Masks render as '****' and never expose plaintext PII
        pii_sql = (
            "SELECT customer_id, full_name, segment FROM cbn.src_pii.customer "
            "WHERE customer_id = $1"
        )
        tok_pii = _mint_sql_grant(
            priv,
            sql_text=pii_sql,
            tables=pii_masked_grant,
            jti="live-pii-mask-1",
            params=["C-000042"],
        )
        r_pii = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok_pii}"},
            json={"sql": pii_sql, "params": ["C-000042"]},
        )
        assert r_pii.status_code == 200
        assert r_pii.json()["rows"] == [["C-000042", "****", "PRIORITY"]]
        assert "Budi Santoso" not in r_pii.text

        # 6. Cross-engine join with PII table fires pii_cross_engine (422)
        cross_sql = (
            "SELECT c.customer_id, s.status FROM cbn.src_pii.customer c "
            "JOIN ops.ops.case_status s ON c.customer_id = s.customer_id "
            "WHERE c.customer_id = $1"
        )
        tok_cross = _mint_sql_grant(
            priv,
            sql_text=cross_sql,
            tables={
                **pii_masked_grant,
                "ops.ops.case_status": {
                    "columns": ["case_id", "customer_id", "status"],
                    "masks": {},
                    "row_filters": [],
                    "actions": ["select"],
                },
            },
            jti="live-pii-cross-1",
            params=["C-000042"],
        )
        r_cross = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok_cross}"},
            json={"sql": cross_sql, "params": ["C-000042"]},
        )
        assert r_cross.status_code == 422
        assert r_cross.json()["code"] == "pii_cross_engine"

        # 7. Statement timeout produces engine_timeout (504)
        slow_sql = (
            "SELECT count(*) FROM cbn.rpt.rpt_txn_detail a "
            "JOIN cbn.rpt.rpt_txn_detail b ON a.as_of_date = b.as_of_date "
            "WHERE a.as_of_date = DATE '2026-10-02' AND b.as_of_date = DATE '2026-10-02'"
        )
        tok_slow = _mint_sql_grant(
            priv,
            sql_text=slow_sql,
            tables=txn_grant,
            jti="live-timeout-1",
        )
        r_slow = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok_slow}"},
            json={"sql": slow_sql, "timeout_s": 0.08},
        )
        assert r_slow.status_code == 504
        assert r_slow.json()["code"] == "engine_timeout"


def test_live_cold_start_simulation_with_pg_ctl_stop_and_start(
    seeded_cbn_cluster: dict[str, Any],
):
    cluster = seeded_cbn_cluster["cluster"]
    if not getattr(cluster, "can_stop_start", False):
        pytest.skip("pg_ctl stop/start simulation requires local TempPostgresCluster")

    bi_dsn = seeded_cbn_cluster["bi_dsn"]

    def raw_connect(**kwargs: Any) -> Any:
        to = max(1, int(kwargs.get("connect_timeout", 2)))
        return psycopg2.connect(bi_dsn, connect_timeout=to)

    # 1. Delayed pg_ctl start during a connect attempt -> bounded retry survives!
    connector_survive = ColdStartConnector(
        raw_connect,
        LakebaseColdStartConfig(
            connect_timeout_seconds=2.0,
            wake="none",
            max_query_wait_seconds=8.0,
            max_wake_seconds=12.0,
            retry_backoff_ms=(200, 400, 600),
        ),
        engine_name="lb_sim",
    )

    cluster.stop(mode="fast")
    try:
        def _delayed_start() -> None:
            time.sleep(0.6)
            cluster.start()

        starter = threading.Thread(target=_delayed_start, daemon=True)
        starter.start()

        conn = connector_survive.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                assert cur.fetchone()[0] == 1
        finally:
            conn.close()
        starter.join(timeout=10.0)
        assert connector_survive.last_cold_start is True

        # 2. Long stop exceeding max_query_wait_seconds -> raises EngineStartingError (503)
        #    and ReadinessProbe reports 'starting' while wake retry is in flight!
        connector_starting = ColdStartConnector(
            raw_connect,
            LakebaseColdStartConfig(
                connect_timeout_seconds=2.0,
                wake="none",
                max_query_wait_seconds=0.5,
                max_wake_seconds=4.0,
                retry_backoff_ms=(300, 500),
            ),
            engine_name="lb_sim",
        )

        class _StartingAdapterStub:
            cold_start_connector = connector_starting

            def check_health(self, timeout_seconds: float = 1.0) -> bool:
                return False

        probe = ReadinessProbe(
            {"lb_sim": _StartingAdapterStub()},
            timeout_seconds=1.0,
            cache_seconds=0.0,
        )

        cluster.stop(mode="fast")
        with pytest.raises(EngineStartingError) as exc_start:
            connector_starting.connect()
        assert exc_start.value.code == "engine_starting"
        assert exc_start.value.status_code == 503
        assert "Retry-After" in exc_start.value.headers

        st_code, payload = probe.check()
        assert st_code == 503
        assert payload == {"ready": False, "engines": {"lb_sim": "starting"}}
    finally:
        cluster.start()


def test_live_unforced_rls_table_ownership_emits_warning_until_forced(
    seeded_cbn_cluster: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    import logging

    bi_probe = psycopg2.connect(seeded_cbn_cluster["bi_dsn"], connect_timeout=5)
    try:
        with bi_probe.cursor() as bcur:
            bcur.execute("SELECT current_user")
            bi_role = bcur.fetchone()[0]
    finally:
        bi_probe.close()

    admin_conn = psycopg2.connect(seeded_cbn_cluster["super_dsn"], connect_timeout=5)
    admin_conn.autocommit = True
    try:
        with admin_conn.cursor() as acur:
            acur.execute("DROP TABLE IF EXISTS rpt.b_l5_unforced_rls_test")
            acur.execute("CREATE TABLE rpt.b_l5_unforced_rls_test (id INT PRIMARY KEY)")
            acur.execute(
                pgsql.SQL("ALTER TABLE rpt.b_l5_unforced_rls_test OWNER TO {}").format(
                    pgsql.Identifier(bi_role)
                )
            )
            acur.execute(
                "ALTER TABLE rpt.b_l5_unforced_rls_test ENABLE ROW LEVEL SECURITY"
            )

        # 1. RLS enabled without FORCE ROW LEVEL SECURITY -> warns without failing
        caplog.clear()
        bi_conn_warn = psycopg2.connect(seeded_cbn_cluster["bi_dsn"], connect_timeout=5)
        try:
            with caplog.at_level(
                logging.WARNING, logger="seeknal.serving.postgres_session"
            ):
                verify_and_harden_session(
                    bi_conn_warn, kind="postgres", role_guard=True
                )
            assert bi_conn_warn.closed == 0
            assert any(
                "rpt.b_l5_unforced_rls_test" in r.getMessage()
                and "FORCE ROW LEVEL SECURITY" in r.getMessage()
                for r in caplog.records
            )
        finally:
            bi_conn_warn.close()

        # 2. Enabling FORCE ROW LEVEL SECURITY clears the warning
        with admin_conn.cursor() as acur:
            acur.execute(
                "ALTER TABLE rpt.b_l5_unforced_rls_test FORCE ROW LEVEL SECURITY"
            )

        caplog.clear()
        bi_conn_ok = psycopg2.connect(seeded_cbn_cluster["bi_dsn"], connect_timeout=5)
        try:
            with caplog.at_level(
                logging.WARNING, logger="seeknal.serving.postgres_session"
            ):
                verify_and_harden_session(
                    bi_conn_ok, kind="postgres", role_guard=True
                )
            assert bi_conn_ok.closed == 0
            assert not any(
                "rpt.b_l5_unforced_rls_test" in r.getMessage()
                for r in caplog.records
            )
        finally:
            bi_conn_ok.close()
    finally:
        try:
            with admin_conn.cursor() as acur:
                acur.execute("DROP TABLE IF EXISTS rpt.b_l5_unforced_rls_test")
        finally:
            admin_conn.close()

