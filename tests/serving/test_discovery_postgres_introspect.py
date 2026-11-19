"""Unit tests for pg_catalog introspection, access derivation, overrides,
freshness cascade, and FakePostgresCatalog.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from seeknal.serving.discovery.config import (
    DiscoveryConfig,
    DiscoveryConfigError,
    DiscoveryError,
    PostgresFreshnessConfig,
    PostgresSchemaConfig,
    PostgresSourceConfig,
    PostgresTableOverrideConfig,
)
from seeknal.serving.discovery.postgres_introspect import introspect_postgres_source
from tests.serving.fake_postgres_catalog import (
    FakeClock,
    FakePostgresCatalog,
    OperationalError,
    ProgrammingError,
)


def test_fake_catalog_clock_connect_failures_and_role_guard():
    clock = FakeClock(datetime(2026, 11, 19, 12, 0, 0, tzinfo=timezone.utc))
    clock.sleep(2.5)
    assert clock.sleeps == [2.5]
    assert clock.now() == datetime(2026, 11, 19, 12, 0, 2, 500000, tzinfo=timezone.utc)

    cat = FakePostgresCatalog(clock=clock)
    cat.set_role(rolsuper=False, rolbypassrls=False, neon_superuser=True)
    cat.fail_connects(2)

    with pytest.raises(OperationalError):
        cat.connect(dsn="postgresql://svc@lb:5432/db", connect_timeout=5)
    with pytest.raises(OperationalError):
        cat.connect(dsn="postgresql://svc@lb:5432/db", connect_timeout=5)

    conn = cat.connect(dsn="postgresql://svc@lb:5432/db", connect_timeout=5)
    assert len(cat.connect_calls) == 3
    with conn.cursor() as cur:
        cur.execute(
            "SELECT r.rolsuper, r.rolbypassrls, pg_catalog.pg_has_role(current_user, 'neon_superuser', 'MEMBER') "
            "FROM pg_catalog.pg_roles r WHERE r.rolname = current_user"
        )
        assert cur.fetchone() == (False, False, True)
        with pytest.raises(ProgrammingError, match="read-only"):
            cur.execute("INSERT INTO t VALUES (1)")
    conn.close()
    assert conn.closed is True


def test_parameterised_queries_and_deny_by_default():
    cat = FakePostgresCatalog()
    # Allowed schema 'rpt':
    # - rpt_daily: exposed (passes include rpt_*, not excluded)
    # - rpt_daily_stg: excluded by glob
    # - other_tbl: excluded by include glob
    # - rpt_no_priv: has_table_privilege=False -> hidden
    # - rpt_BadIdent: uppercase -> hidden
    cat.add_table(
        "rpt",
        "rpt_daily",
        relkind="r",
        reltuples=5000,
        columns=[
            ("branch_code", "text", True, True),
            ("as_of_date", "date", True, True),
            ("secret_col", "text", False, False),  # revoked column privilege
            ("BadCol", "text", False, True),  # invalid identifier
            {"name": "dropped_col", "type_str": "int4", "is_dropped": True, "attnum": 5},
            {"name": "xmin", "type_str": "xid", "attnum": -1},
        ],
        indexes=[
            {
                "index_name": "rpt_daily_pkey",
                "amname": "btree",
                "columns": ["branch_code", "as_of_date"],
                "is_primary": True,
            }
        ],
    )
    cat.add_table(
        "rpt",
        "rpt_daily_stg",
        columns=[("id", "int8", True, True)],
    )
    cat.add_table(
        "rpt",
        "other_tbl",
        columns=[("id", "int8", True, True)],
    )
    cat.add_table(
        "rpt",
        "rpt_no_priv",
        has_table_privilege=False,
        columns=[("id", "int8", True, False)],
    )
    cat.add_table(
        "rpt",
        "rpt_BadIdent",
        columns=[("id", "int8", True, True)],
    )
    # Unlisted schema 'secret_schema' must never be queried or exposed
    cat.add_table(
        "secret_schema",
        "rpt_secret",
        columns=[("id", "int8", True, True)],
    )

    src_cfg = PostgresSourceConfig(
        engine="pg_report",
        logical_catalog="cbn",
        schema_map={"rpt": "mart"},
        schemas={
            "rpt": PostgresSchemaConfig(
                include=("rpt_*",),
                exclude=("rpt_*_stg",),
            )
        },
    )

    res = introspect_postgres_source(
        cat.connect,
        src_cfg,
        engine_kind="postgres",
        read_only=True,
    )

    assert list(res.tables.keys()) == ["cbn.mart.rpt_daily"]
    tbl = res.tables["cbn.mart.rpt_daily"]
    assert tbl.physical_name == "rpt.rpt_daily"
    assert tbl.columns == ("branch_code", "as_of_date")
    assert res.schemas == {"cbn.mart.rpt_daily": ["branch_code", "as_of_date"]}

    # Verify parameterisation: pg_catalog queries pass (['rpt'],) as parameter
    pg_cat_calls = [
        (sql, params)
        for sql, params in cat.executed
        if "ANY(%s)" in sql
    ]
    assert len(pg_cat_calls) == 3
    for sql, params in pg_cat_calls:
        assert params == (["rpt"],)
        assert "secret_schema" not in sql


def test_read_only_server_fact_check_fails_closed():
    cat = FakePostgresCatalog()
    cat.set_show("default_transaction_read_only", "off")
    cat.add_table("ops", "tickets", columns=[("id", "int8", True, True)])

    src_cfg = PostgresSourceConfig(
        engine="pg_ro",
        schemas={"ops": PostgresSchemaConfig()},
    )

    # Read-only postgres source fails closed when default_transaction_read_only != 'on'
    with pytest.raises(
        DiscoveryError,
        match="Read-only source must have default_transaction_read_only = on",
    ):
        introspect_postgres_source(
            cat.connect,
            src_cfg,
            engine_kind="postgres",
            read_only=True,
        )

    # Read-only lakebase endpoint also fails closed
    with pytest.raises(
        DiscoveryError,
        match="Read-only source must have default_transaction_read_only = on",
    ):
        introspect_postgres_source(
            cat.connect,
            src_cfg,
            engine_kind="lakebase",
            endpoint_type="read_only",
        )

    # Writable source allows default_transaction_read_only = 'off'
    res = introspect_postgres_source(
        cat.connect,
        src_cfg,
        engine_kind="lakebase",
        read_only=False,
        endpoint_type="read_write",
    )
    assert "pg_ro.ops.tickets" in res.tables


def test_access_derivation_indexes_views_reltuples_and_pii_patterns():
    cat = FakePostgresCatalog()

    # 1. Table without PK: narrowest unique NOT NULL non-partial non-expression index wins
    # Plus hash, btree, pattern_ops, COLLATE "C", composite btree with nullable break, brin, partial, expression, invalid
    cat.add_table(
        "app",
        "events",
        relkind="r",
        reltuples=500_000,
        columns=[
            ("tenant_id", "text", True),
            ("event_id", "text", True),
            ("ext_ref", "text", True),
            ("nullable_uniq", "text", False),
            ("hash_tok", "text", True),
            ("code_prefix", "varchar", True),
            ("c_coll_col", "text", True),
            ("created_at", "timestamptz", True),
            ("nullable_mid", "int4", False),
            ("after_null", "int4", True),
            ("brin_dt", "date", True),
            ("partial_col", "text", True),
            ("expr_col", "text", True),
            ("invalid_col", "text", True),
        ],
        indexes=[
            # Wide unique NOT NULL (2 cols)
            {
                "index_name": "idx_uniq_wide",
                "amname": "btree",
                "columns": ["tenant_id", "event_id"],
                "is_unique": True,
            },
            # Narrow unique NOT NULL (1 col) -> must win as keys!
            {
                "index_name": "idx_uniq_narrow",
                "amname": "btree",
                "columns": ["ext_ref"],
                "is_unique": True,
            },
            # Even narrower / alphabetical unique with NULLABLE col -> ignored for keys
            {
                "index_name": "aaa_uniq_nullable",
                "amname": "btree",
                "columns": ["nullable_uniq"],
                "is_unique": True,
            },
            # Hash index -> filterable, NOT range_filterable, NOT sortable
            {
                "index_name": "idx_hash_tok",
                "amname": "hash",
                "columns": ["hash_tok"],
            },
            # Btree with varchar_pattern_ops -> filterable, range_filterable, prefix_filterable
            {
                "index_name": "idx_code_pattern",
                "amname": "btree",
                "columns": ["code_prefix"],
                "opclasses": ["varchar_pattern_ops"],
            },
            # Btree with COLLATE "C" -> prefix_filterable
            {
                "index_name": "idx_c_coll",
                "amname": "btree",
                "columns": ["c_coll_col"],
                "collations": ["C"],
            },
            # Composite btree (created_at NOT NULL, nullable_mid NULL, after_null NOT NULL)
            # -> sortable gets created_at ONLY, stops at nullable_mid!
            {
                "index_name": "idx_comp_sort",
                "amname": "btree",
                "columns": ["created_at", "nullable_mid", "after_null"],
            },
            # BRIN -> ignored for filterable/range/sortable
            {
                "index_name": "idx_brin_dt",
                "amname": "brin",
                "columns": ["brin_dt"],
            },
            # Partial -> ignored
            {
                "index_name": "idx_partial",
                "amname": "btree",
                "columns": ["partial_col"],
                "is_partial": True,
                "is_unique": True,
            },
            # Expression -> ignored
            {
                "index_name": "idx_expr",
                "amname": "btree",
                "columns": ["expr_col"],
                "is_expression": True,
            },
            # Invalid -> ignored
            {
                "index_name": "idx_invalid",
                "amname": "btree",
                "columns": ["invalid_col"],
                "is_valid": False,
            },
        ],
    )

    # 2. Small table (< 100,000) -> scan, require_filter=False
    cat.add_table(
        "app",
        "dim_branch",
        relkind="r",
        reltuples=250,
        columns=[("branch_id", "text", True)],
        indexes=[
            {
                "index_name": "dim_branch_pkey",
                "amname": "btree",
                "columns": ["branch_id"],
                "is_primary": True,
            }
        ],
    )

    # 3. View (relkind='v', reltuples=10) -> row_count=None, pattern='filter'
    cat.add_table(
        "app",
        "v_active_events",
        relkind="v",
        reltuples=10,
        columns=[("event_id", "text", False)],
    )

    # 4. Unanalyzed table (reltuples=-1) -> row_count=None, pattern='filter'
    cat.add_table(
        "app",
        "fresh_load",
        relkind="r",
        reltuples=-1,
        columns=[("id", "int8", True)],
    )

    # 5. PII schema: one table with PK (-> point, max_rows=50), one without keys (-> filter, max_rows=50)
    cat.add_table(
        "pii_s",
        "customer",
        relkind="r",
        reltuples=50,
        columns=[("customer_id", "text", True), ("full_name", "text", True)],
        indexes=[
            {
                "index_name": "customer_pkey",
                "amname": "btree",
                "columns": ["customer_id"],
                "is_primary": True,
            }
        ],
    )
    cat.add_table(
        "pii_s",
        "raw_notes",
        relkind="r",
        reltuples=20,
        columns=[("customer_id", "text", True), ("note", "text", False)],
        indexes=[
            {
                "index_name": "idx_notes_cust",
                "amname": "btree",
                "columns": ["customer_id"],
            }
        ],
    )

    src_cfg = PostgresSourceConfig(
        engine="pg",
        schemas={
            "app": PostgresSchemaConfig(),
            "pii_s": PostgresSchemaConfig(pii=True),
        },
    )

    res = introspect_postgres_source(
        cat.connect,
        src_cfg,
        engine_kind="postgres",
        read_only=True,
        require_filter_min_rows=100_000,
    )

    ev = res.tables["pg.app.events"]
    assert ev.keys == ("ext_ref",)
    assert ev.access["keys"] == ["ext_ref"]
    assert set(ev.access["filterable"]) == {
        "ext_ref",
        "tenant_id",
        "nullable_uniq",
        "hash_tok",
        "code_prefix",
        "c_coll_col",
        "created_at",
    }
    assert "hash_tok" not in ev.access["range_filterable"]
    assert "brin_dt" not in ev.access["filterable"]
    assert "partial_col" not in ev.access["filterable"]
    assert "expr_col" not in ev.access["filterable"]
    assert "invalid_col" not in ev.access["filterable"]
    assert set(ev.access["prefix_filterable"]) == {"code_prefix", "c_coll_col"}
    assert set(ev.access["sortable"]) == {
        "ext_ref",
        "tenant_id",
        "event_id",
        "code_prefix",
        "c_coll_col",
        "created_at",
    }
    assert "nullable_mid" not in ev.access["sortable"]
    assert "after_null" not in ev.access["sortable"]
    assert ev.access["pattern"] == "filter"
    assert ev.access["require_filter"] is True
    assert ev.access["max_rows"] is None

    dim = res.tables["pg.app.dim_branch"]
    assert dim.row_count == 250
    assert dim.access["pattern"] == "scan"
    assert dim.access["require_filter"] is False

    vw = res.tables["pg.app.v_active_events"]
    assert vw.row_count is None
    assert vw.access["pattern"] == "filter"
    assert vw.access["require_filter"] is True

    un = res.tables["pg.app.fresh_load"]
    assert un.row_count is None
    assert un.access["pattern"] == "filter"

    cust = res.tables["pg.pii_s.customer"]
    assert cust.pii is True
    assert cust.access["pattern"] == "point"
    assert cust.access["require_filter"] is True
    assert cust.access["max_rows"] == 50

    notes = res.tables["pg.pii_s.raw_notes"]
    assert notes.pii is True
    assert notes.keys == ()
    assert notes.access["pattern"] == "filter"
    assert notes.access["require_filter"] is True
    assert notes.access["max_rows"] == 50


def test_overrides_win_validate_and_warn_on_unindexed():
    cat = FakePostgresCatalog()
    cat.add_table(
        "rpt",
        "txn",
        reltuples=500_000,
        columns=[
            ("txn_id", "text", True),
            ("branch_code", "text", True),  # unindexed NOT NULL
            ("memo", "text", False),  # nullable
        ],
        indexes=[
            {
                "index_name": "txn_pkey",
                "amname": "btree",
                "columns": ["txn_id"],
                "is_primary": True,
            }
        ],
    )

    # 1. Valid override declaring unindexed 'branch_code' -> wins and emits warning
    cfg_ok = PostgresSourceConfig(
        engine="pg",
        schemas={"rpt": PostgresSchemaConfig()},
        tables={
            "pg.rpt.txn": PostgresTableOverrideConfig(
                access={
                    "filterable": ["txn_id", "branch_code"],
                    "max_rows": 2500,
                }
            )
        },
    )
    res = introspect_postgres_source(cat.connect, cfg_ok, engine_kind="postgres", read_only=True)
    assert res.tables["pg.rpt.txn"].access["filterable"] == ["txn_id", "branch_code"]
    assert res.tables["pg.rpt.txn"].access["max_rows"] == 2500
    assert any("branch_code" in w for w in res.warnings)

    # 2. Missing table in override -> DiscoveryError
    cfg_missing_tbl = PostgresSourceConfig(
        engine="pg",
        schemas={"rpt": PostgresSchemaConfig()},
        tables={"pg.rpt.ghost": PostgresTableOverrideConfig(pii=True)},
    )
    with pytest.raises(DiscoveryError, match="pg.rpt.ghost"):
        introspect_postgres_source(
            cat.connect, cfg_missing_tbl, engine_kind="postgres", read_only=True
        )

    # 3. Unknown column in override -> DiscoveryError
    cfg_bad_col = PostgresSourceConfig(
        engine="pg",
        schemas={"rpt": PostgresSchemaConfig()},
        tables={
            "pg.rpt.txn": PostgresTableOverrideConfig(
                access={"filterable": ["nonexistent_col"]}
            )
        },
    )
    with pytest.raises(DiscoveryError, match="nonexistent_col"):
        introspect_postgres_source(
            cat.connect, cfg_bad_col, engine_kind="postgres", read_only=True
        )

    # 4. pattern == 'point' with empty keys -> DiscoveryError
    cfg_point_no_keys = PostgresSourceConfig(
        engine="pg",
        schemas={"rpt": PostgresSchemaConfig()},
        tables={
            "pg.rpt.txn": PostgresTableOverrideConfig(
                access={"pattern": "point", "keys": []}
            )
        },
    )
    with pytest.raises(DiscoveryError, match="point.*keys"):
        introspect_postgres_source(
            cat.connect, cfg_point_no_keys, engine_kind="postgres", read_only=True
        )

    # 5. Nullable column in sortable -> DiscoveryError
    cfg_nullable_sort = PostgresSourceConfig(
        engine="pg",
        schemas={"rpt": PostgresSchemaConfig()},
        tables={
            "pg.rpt.txn": PostgresTableOverrideConfig(
                access={"sortable": ["memo"]}
            )
        },
    )
    with pytest.raises(DiscoveryError, match="memo.*NOT NULL"):
        introspect_postgres_source(
            cat.connect, cfg_nullable_sort, engine_kind="postgres", read_only=True
        )


def test_freshness_cascade_all_four_levels_and_validation():
    now_dt = datetime(2026, 11, 19, 12, 5, 0, tzinfo=timezone.utc)

    # Level 1 & 2 together: table_synced uses _lakebase_sync_state; table_unsynced falls through to freshness_sql
    cat = FakePostgresCatalog()
    cat.add_table("rpt", "t_synced", columns=[("id", "int8", True)])
    cat.add_table("rpt", "t_sql", columns=[("id", "int8", True)])
    cat.add_sync_state(
        "rpt.t_synced",
        snapshot_id=42,
        synced_at=datetime(2026, 11, 19, 12, 3, 0, tzinfo=timezone.utc),  # 120s ago
    )
    fresh_query = "select max(as_of_date) from stg.load_control"
    # 2026-11-19 midnight UTC -> 12h5m = 43500s ago
    cat.set_freshness_value(date(2026, 11, 19), sql=fresh_query)

    src_cfg = PostgresSourceConfig(
        engine="pg_report",
        logical_catalog="cbn",
        freshness=PostgresFreshnessConfig(
            sync_state_table="public._lakebase_sync_state",
            freshness_sql=fresh_query,
            sync_state_explicit=True,
        ),
        schemas={"rpt": PostgresSchemaConfig()},
    )

    # Without matching Iceberg snapshot -> sync lag = 120 + 60 = 180s
    res1 = introspect_postgres_source(
        cat.connect,
        src_cfg,
        engine_kind="postgres",
        read_only=True,
        now=now_dt,
        refresh_interval_seconds=60.0,
    )
    assert res1.tables["cbn.rpt.t_synced"].max_lag_seconds == 180.0
    assert res1.tables["cbn.rpt.t_sql"].max_lag_seconds == 43500.0 + 60.0

    # Verify freshness_sql ran inside BEGIN READ ONLY ... ROLLBACK
    sql_seq = [s.strip().upper() for s, _ in cat.executed]
    begin_idx = sql_seq.index("BEGIN READ ONLY")
    fresh_idx = sql_seq.index(fresh_query.upper())
    rollback_idx = sql_seq.index("ROLLBACK", fresh_idx)
    assert begin_idx < fresh_idx < rollback_idx

    # With matching Iceberg snapshot_id=42 -> sync lag = 0 + 60 = 60s
    res_match = introspect_postgres_source(
        cat.connect,
        src_cfg,
        engine_kind="postgres",
        read_only=True,
        now=now_dt,
        refresh_interval_seconds=60.0,
        iceberg_snapshot_ids={"cbn.rpt.t_synced": 42},
    )
    assert res_match.tables["cbn.rpt.t_synced"].max_lag_seconds == 60.0

    # Level 3: Lakebase read_only endpoint falls back to pg_postmaster_start_time()
    cat_ro_lb = FakePostgresCatalog()
    cat_ro_lb.add_table("ops", "tickets", columns=[("id", "int8", True)])
    cat_ro_lb.set_postmaster_start_time(
        datetime(2026, 11, 19, 12, 0, 0, tzinfo=timezone.utc)  # 300s ago
    )
    lb_ro_cfg = DiscoveryConfig.from_dict(
        {
            "default_catalog": "ops",
            "engines": {"lb_ro": {"kind": "lakebase", "dialect": "postgres"}},
            "postgres_sources": {"lb_ro": {"schemas": {"ops": {}}}},
        }
    ).postgres_sources["lb_ro"]

    res_ro_lb = introspect_postgres_source(
        cat_ro_lb.connect,
        lb_ro_cfg,
        engine_kind="lakebase",
        endpoint_type="read_only",
        now=now_dt,
        refresh_interval_seconds=60.0,
    )
    assert res_ro_lb.tables["lb_ro.ops.tickets"].max_lag_seconds == 360.0

    # Level 4: Live operational read_write source without sync row or freshness_sql -> None
    res_live = introspect_postgres_source(
        cat_ro_lb.connect,
        lb_ro_cfg,
        engine_kind="lakebase",
        endpoint_type="read_write",
        now=now_dt,
        refresh_interval_seconds=60.0,
    )
    assert res_live.tables["lb_ro.ops.tickets"].max_lag_seconds is None

    # Invalid freshness_sql shapes must raise DiscoveryError
    for bad_rows in (
        [],
        [(None,)],
        [(datetime(2026, 11, 19, tzinfo=timezone.utc), "extra")],
        [(datetime(2026, 11, 19, tzinfo=timezone.utc),), (datetime(2026, 11, 18, tzinfo=timezone.utc),)],
        [(12345,)],
        [("not-a-date",)],
    ):
        cat_bad = FakePostgresCatalog()
        cat_bad.add_table("rpt", "t1", columns=[("id", "int8", True)])
        cat_bad.set_freshness_rows(bad_rows, sql=fresh_query)
        bad_cfg = PostgresSourceConfig(
            engine="pg",
            freshness=PostgresFreshnessConfig(freshness_sql=fresh_query),
            schemas={"rpt": PostgresSchemaConfig()},
        )
        with pytest.raises(DiscoveryError):
            introspect_postgres_source(
                cat_bad.connect, bad_cfg, engine_kind="postgres", read_only=True
            )
