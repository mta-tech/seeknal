"""Unit tests for seeknal.serving.accel.tables and placement helpers."""

from __future__ import annotations

import json
import pytest

from seeknal.serving.accel.loader import plan_sql
from seeknal.serving.accel.placement import (
    generate_placement,
    generate_schemas,
    write_placement_files,
)
from seeknal.serving.accel.tables import (
    MAX_SIGNED_INT64,
    ColumnSpec,
    TableSpec,
    build_table_specs,
    map_type,
    validate_duckdb_type,
    validate_identifier,
    validate_snapshot_id,
)


def _synthetic_inputs():
    columns = {
        "views": {
            "daily_orders": {
                "keys": ["order_id"],
                "columns": [
                    {"name": "order_id", "type": "bigint"},
                    {"name": "order_date", "type": "date"},
                    {"name": "net_amount", "type": "decimal(18, 2)"},
                    {"name": "customer_email", "type": "varchar(128)", "pii": "email_hash"},
                ],
            },
            "inventory_snapshot": {
                "keys": ["sku_id"],
                "columns": [
                    {"name": "sku_id", "type": "varchar"},
                    {"name": "on_hand_qty", "type": "integer"},
                    {"name": "is_active", "type": "boolean"},
                ],
            },
            "realtime_alerts": {
                "keys": ["alert_id"],
                "columns": [
                    {"name": "alert_id", "type": "bigint"},
                    {"name": "created_at", "type": "timestamptz"},
                ],
            },
        }
    }
    pii_policy = {
        "email_hash": {
            "source_form": "sha256",
            "mask": "redact",
        }
    }
    registry = {
        "reports": [
            {"view": "daily_orders", "status": "ready"},
            {"view": "inventory_snapshot", "status": "contract"},
            {"view": "realtime_alerts", "status": "ready"},
        ]
    }
    return columns, pii_policy, registry


def test_map_type_and_identifier_validation():
    assert map_type("c", "varchar(50)") == "VARCHAR"
    assert map_type("c", "string") == "VARCHAR"
    assert map_type("c", "int4") == "INTEGER"
    assert map_type("c", "bigint") == "BIGINT"
    assert map_type("c", "long") == "BIGINT"
    assert map_type("c", "bool") == "BOOLEAN"
    assert map_type("c", "date") == "DATE"
    assert map_type("c", "timestamp with time zone") == "TIMESTAMP"
    assert map_type("c", "double precision") == "DOUBLE"
    assert map_type("c", "numeric(12, 4)") == "DECIMAL(12,4)"

    with pytest.raises(ValueError, match="Unknown type"):
        map_type("c", "jsonb")

    validate_identifier("valid_col_1")
    for bad_ident in ("", "UpperCase", "1leading_digit", "bad-dash", "a; DROP TABLE x", "x" * 64):
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier(bad_ident)

    validate_duckdb_type("DECIMAL(10,2)", "amt")
    with pytest.raises(ValueError, match="Invalid DuckDB type"):
        validate_duckdb_type("BLOB", "payload")


def test_build_table_specs_pii_null_mode_and_live_view_filtering():
    columns, pii_policy, registry = _synthetic_inputs()
    specs = build_table_specs(
        columns=columns,
        pii_policy=pii_policy,
        registry=registry,
        live_views=("realtime_alerts",),
        pii_mode="null",
    )

    assert [s.view for s in specs] == ["daily_orders", "inventory_snapshot"]
    orders_spec = specs[0]
    assert orders_spec.name == "rpt.daily_orders"
    assert orders_spec.source == "rpt_base.daily_orders"
    assert orders_spec.keys == ("order_id",)

    col_map = {c.name: c for c in orders_spec.columns}
    assert col_map["customer_email"].is_nulled is True
    assert col_map["customer_email"].is_pii is True
    assert col_map["customer_email"].duckdb_type == "VARCHAR"
    assert col_map["order_id"].is_nulled is False

    stmts = plan_sql(specs, pii_mode="null")
    assert stmts == [
        "CREATE SCHEMA rpt",
        "CREATE TABLE rpt.daily_orders AS SELECT order_id, order_date, net_amount, CAST(NULL AS VARCHAR) AS customer_email FROM pg.rpt_base.daily_orders",
        "CREATE TABLE rpt.inventory_snapshot AS SELECT sku_id, on_hand_qty, is_active FROM pg.rpt_base.inventory_snapshot",
    ]


def test_build_table_specs_pii_exclude_mode_omits_pii_columns():
    columns, _, registry = _synthetic_inputs()
    # Include both sha256 and plaintext PII policies; exclude mode omits all PII columns cleanly
    pii_policy = {
        "email_hash": {"source_form": "plain", "mask": "last4"},
    }
    specs = build_table_specs(
        columns=columns,
        pii_policy=pii_policy,
        registry=registry,
        live_views=("realtime_alerts",),
        pii_mode="exclude",
    )

    orders_spec = specs[0]
    col_names = [c.name for c in orders_spec.columns]
    assert "customer_email" not in col_names
    assert col_names == ["order_id", "order_date", "net_amount"]

    stmts = plan_sql(specs, pii_mode="exclude")
    assert "customer_email" not in stmts[1]
    assert (
        stmts[1]
        == "CREATE TABLE rpt.daily_orders AS SELECT order_id, order_date, net_amount FROM pg.rpt_base.daily_orders"
    )


def test_plan_sql_exclude_mode_also_filters_nulled_column_specs_and_rejects_empty():
    spec = TableSpec(
        name="rpt.accounts",
        source="rpt_base.accounts",
        view="accounts",
        columns=(
            ColumnSpec("account_id", "BIGINT", is_nulled=False),
            ColumnSpec("phone_hash", "VARCHAR", is_nulled=True),
        ),
    )
    stmts = plan_sql([spec], pii_mode="exclude")
    assert stmts == [
        "CREATE SCHEMA rpt",
        "CREATE TABLE rpt.accounts AS SELECT account_id FROM pg.rpt_base.accounts",
    ]

    all_pii_spec = TableSpec(
        name="rpt.only_pii",
        source="rpt_base.only_pii",
        view="only_pii",
        columns=(ColumnSpec("secret_email", "VARCHAR", is_nulled=True),),
    )
    with pytest.raises(ValueError, match="no remaining columns"):
        plan_sql([all_pii_spec], pii_mode="exclude")


def test_pii_policy_validation_rejects_non_hashed_and_non_neutral_masks_in_null_mode():
    columns, _, registry = _synthetic_inputs()

    # Non-hashed PII rejected in null mode unless allow_non_hashed_pii=True
    with pytest.raises(ValueError, match="not sha256"):
        build_table_specs(
            columns=columns,
            pii_policy={"email_hash": {"source_form": "plain", "mask": "redact"}},
            registry=registry,
            pii_mode="null",
        )

    # Non-neutral mask (e.g. last4) on sha256 PII rejected in null mode
    with pytest.raises(ValueError, match="not engine-neutral"):
        build_table_specs(
            columns=columns,
            pii_policy={"email_hash": {"source_form": "sha256", "mask": "last4"}},
            registry=registry,
            pii_mode="null",
        )

    # Missing PII key in policy rejected
    with pytest.raises(ValueError, match="missing from pii_columns policy"):
        build_table_specs(
            columns=columns,
            pii_policy={},
            registry=registry,
            pii_mode="null",
        )


def test_registry_status_enforcement():
    columns, pii_policy, _ = _synthetic_inputs()
    blocked_registry = {
        "reports": [
            {"view": "daily_orders", "status": "blocked", "blocked_reason": "pending audit"},
            {"view": "inventory_snapshot", "status": "contract"},
            {"view": "realtime_alerts", "status": "ready"},
        ]
    }
    with pytest.raises(ValueError, match="pending audit"):
        build_table_specs(columns=columns, pii_policy=pii_policy, registry=blocked_registry)


def test_snapshot_id_validation_and_precedence():
    assert validate_snapshot_id(1) == 1
    assert validate_snapshot_id(MAX_SIGNED_INT64) == MAX_SIGNED_INT64

    for invalid_snap in (0, -42, MAX_SIGNED_INT64 + 1, True, False, "12345", 12.5):
        with pytest.raises(ValueError, match="snapshot_id"):
            TableSpec(
                name="rpt.daily_orders",
                source="lake.analytics.daily_orders",
                view="daily_orders",
                columns=(ColumnSpec("order_id", "BIGINT", False),),
                snapshot_id=invalid_snap,  # type: ignore[arg-type]
            )

    columns, pii_policy, _ = _synthetic_inputs()
    registry = {
        "reports": [
            {"view": "daily_orders", "status": "ready", "snapshot_id": 1001},
            {"view": "inventory_snapshot", "status": "contract", "snapshot_id": 2002},
            {"view": "realtime_alerts", "status": "ready"},
        ]
    }
    specs = build_table_specs(
        columns=columns,
        pii_policy=pii_policy,
        registry=registry,
        live_views=("realtime_alerts",),
        snapshot_pins={"daily_orders": 999001},
    )
    assert specs[0].snapshot_id == 999001
    assert specs[1].snapshot_id == 2002


def test_placement_and_schemas_generation_respects_pii_exclude(tmp_path):
    columns, pii_policy, registry = _synthetic_inputs()
    cols_file = tmp_path / "columns.json"
    pii_file = tmp_path / "pii.json"
    reg_file = tmp_path / "registry.json"
    fresh_file = tmp_path / "freshness.json"
    out_dir = tmp_path / "out"

    cols_file.write_text(json.dumps(columns), encoding="utf-8")
    pii_file.write_text(json.dumps(pii_policy), encoding="utf-8")
    reg_file.write_text(json.dumps(registry), encoding="utf-8")
    fresh_file.write_text(
        json.dumps({"tables": {"rpt.daily_orders": 450, "rpt.inventory_snapshot": 80}}),
        encoding="utf-8",
    )

    placement, schemas_null = write_placement_files(
        columns_path=cols_file,
        registry_path=reg_file,
        freshness_path=fresh_file,
        live_views=("realtime_alerts",),
        out_dir=out_dir,
        pii_policy_path=pii_file,
        pii_mode="null",
    )
    assert placement["tables"]["rpt.daily_orders"]["row_count"] == 450
    assert "duck" in placement["tables"]["rpt.daily_orders"]["serve"]
    assert "duck" not in placement["tables"]["rpt.realtime_alerts"]["serve"]
    assert "customer_email" in schemas_null["rpt.daily_orders"]

    schemas_excl = generate_schemas(
        columns=cols_file,
        registry=reg_file,
        pii_policy=pii_file,
        pii_mode="exclude",
    )
    assert "customer_email" not in schemas_excl["rpt.daily_orders"]
    assert schemas_excl["rpt.daily_orders"] == ["order_id", "order_date", "net_amount"]
