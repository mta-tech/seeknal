"""Behavioral tests for PyIceberg-backed advanced materialization modes."""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DateType, LongType, NestedField, StringType

from seeknal.workflow.materialization.config import CatalogConfig, ConfigurationError
from seeknal.workflow.materialization.iceberg_mutations import (
    IcebergCommitError,
    IcebergMutationError,
)
from seeknal.workflow.materialization.operations import WriteError, write_to_iceberg


ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("region", pa.string()),
        pa.field("value", pa.string()),
        pa.field("business_date", pa.date32()),
    ]
)
ICEBERG_SCHEMA = Schema(
    NestedField(1, "id", LongType()),
    NestedField(2, "region", StringType()),
    NestedField(3, "value", StringType()),
    NestedField(4, "business_date", DateType()),
)


def arrow(rows):
    return pa.Table.from_pylist(rows, schema=ARROW_SCHEMA)


@pytest.fixture
def catalog(tmp_path):
    return SqlCatalog(
        "test",
        uri=f"sqlite:///{tmp_path / 'catalog.db'}",
        warehouse=f"file://{tmp_path / 'warehouse'}",
    )


@pytest.fixture
def connection():
    con = duckdb.connect()
    yield con
    con.close()


def create_table(catalog, rows, partition_columns=()):
    catalog.create_namespace("analytics")
    fields = []
    for index, column in enumerate(partition_columns):
        source = ICEBERG_SCHEMA.find_field(column)
        fields.append(
            PartitionField(source.field_id, 1000 + index, IdentityTransform(), column)
        )
    table = catalog.create_table(
        ("analytics", "target"),
        ICEBERG_SCHEMA,
        partition_spec=PartitionSpec(*fields),
        properties={"format-version": "2"},
    )
    if rows:
        table.append(arrow(rows))
    return table


def run_write(connection, catalog, batch, mode, **options):
    connection.register("incoming-batch", batch)
    with patch(
        "seeknal.workflow.materialization.iceberg_mutations._load_catalog",
        return_value=catalog,
    ):
        return write_to_iceberg(
            connection,
            "test",
            "analytics.target",
            "incoming-batch",
            mode,
            catalog_config=CatalogConfig(),
            **options,
        )


def rows(catalog):
    return sorted(
        catalog.load_table(("analytics", "target")).scan().to_arrow().to_pylist(),
        key=lambda row: (row["id"], row["region"]),
    )


def test_upsert_updates_and_inserts_with_deterministic_metrics(connection, catalog):
    create_table(
        catalog,
        [
            {"id": 1, "region": "id", "value": "one", "business_date": date(2026, 9, 13)},
            {"id": 2, "region": "id", "value": "two", "business_date": date(2026, 9, 14)},
        ],
    )

    result = run_write(
        connection,
        catalog,
        arrow(
            [
                {"id": 2, "region": "id", "value": "updated", "business_date": date(2026, 9, 14)},
                {"id": 3, "region": "id", "value": "three", "business_date": date(2026, 9, 15)},
            ]
        ),
        "upsert",
        unique_keys=["id", "region"],
    )

    assert result.success is True
    assert result.input_row_count == result.row_count == 2
    assert result.inserted_row_count == 1
    assert result.updated_row_count == 1
    assert result.outcome == "committed"
    assert result.snapshot_verified is True
    assert [row["value"] for row in rows(catalog)] == ["one", "updated", "three"]


def test_insert_overwrite_replaces_exact_composite_partition_tuples(connection, catalog):
    create_table(
        catalog,
        [
            {"id": 1, "region": "id", "value": "keep", "business_date": date(2026, 9, 13)},
            {"id": 2, "region": "id", "value": "replace", "business_date": date(2026, 9, 14)},
            {"id": 3, "region": "sg", "value": "keep", "business_date": date(2026, 9, 14)},
        ],
        partition_columns=("region", "business_date"),
    )

    result = run_write(
        connection,
        catalog,
        arrow(
            [
                {"id": 20, "region": "id", "value": "new", "business_date": date(2026, 9, 14)},
                {"id": 21, "region": "au", "value": "new", "business_date": date(2026, 9, 15)},
            ]
        ),
        "insert_overwrite",
        partition_by=["region", "business_date"],
    )

    assert result.inserted_row_count == 2
    assert result.updated_row_count is None
    assert result.affected_partition_count == 2
    assert {(row["id"], row["region"]) for row in rows(catalog)} == {
        (1, "id"),
        (3, "sg"),
        (20, "id"),
        (21, "au"),
    }


def test_duplicate_matching_target_keys_fail_without_a_commit(connection, catalog):
    table = create_table(
        catalog,
        [
            {"id": 1, "region": "id", "value": "first", "business_date": date(2026, 9, 13)},
            {"id": 1, "region": "id", "value": "second", "business_date": date(2026, 9, 14)},
        ],
    )
    snapshot_before = table.metadata.current_snapshot_id

    with pytest.raises(WriteError, match="upsert is ambiguous"):
        run_write(
            connection,
            catalog,
            arrow(
                [{"id": 1, "region": "id", "value": "new", "business_date": date(2026, 9, 15)}]
            ),
            "upsert",
            unique_keys=["id", "region"],
        )

    current = catalog.load_table(("analytics", "target"))
    assert current.metadata.current_snapshot_id == snapshot_before
    assert len(current.scan().to_arrow()) == 2


def test_missing_table_is_created_with_partition_and_data_in_one_transaction(connection, catalog):
    result = run_write(
        connection,
        catalog,
        arrow(
            [{"id": 1, "region": "id", "value": "one", "business_date": date(2026, 9, 14)}]
        ),
        "insert_overwrite",
        partition_by=["business_date"],
    )

    table = catalog.load_table(("analytics", "target"))
    assert result.outcome == "committed"
    assert [table.schema().find_column_name(field.source_id) for field in table.spec().fields] == [
        "business_date"
    ]
    assert len(table.scan().to_arrow()) == 1


def test_empty_missing_table_is_noop_without_namespace_mutation(connection, catalog):
    result = run_write(
        connection,
        catalog,
        arrow([]),
        "insert_overwrite",
        partition_by=["business_date"],
    )

    assert result.outcome == "noop"
    assert result.snapshot_id is None
    assert ("analytics",) not in catalog.list_namespaces()


def test_invalid_mode_and_batch_budget_fail_before_catalog_loading(connection, catalog):
    batch = arrow(
        [{"id": 1, "region": "id", "value": "one", "business_date": date(2026, 9, 14)}]
    )
    connection.register("incoming-batch", batch)

    with pytest.raises(ConfigurationError, match="Invalid materialization mode"):
        write_to_iceberg(connection, "test", "analytics.target", "incoming-batch", "typo")

    with patch("seeknal.workflow.materialization.iceberg_mutations._load_catalog") as loader:
        with pytest.raises(WriteError, match="exceeds max_batch_bytes"):
            write_to_iceberg(
                connection,
                "test",
                "analytics.target",
                "incoming-batch",
                "upsert",
                unique_keys=["id"],
                max_batch_bytes=batch.nbytes - 1,
                catalog_config=CatalogConfig(),
            )
    loader.assert_not_called()


def test_upsert_partition_preflight_fails_before_catalog_loading(connection):
    batch = arrow(
        [{"id": 1, "region": "id", "value": "one", "business_date": None}]
    )
    connection.register("incoming-batch", batch)

    with patch("seeknal.workflow.materialization.iceberg_mutations._load_catalog") as loader:
        with pytest.raises(WriteError, match="partition_by columns must not contain null"):
            write_to_iceberg(
                connection,
                "test",
                "analytics.target",
                "incoming-batch",
                "upsert",
                unique_keys=["id"],
                partition_by=["business_date"],
                catalog_config=CatalogConfig(),
            )
    loader.assert_not_called()


def test_duplicate_source_column_names_fail_before_catalog_loading():
    duplicate_columns = pa.Table.from_arrays(
        [pa.array([1]), pa.array([2])],
        names=["id", "id"],
    )

    class ArrowResult:
        def execute(self, _query):
            return self

        def fetch_arrow_table(self):
            return duplicate_columns

    with patch("seeknal.workflow.materialization.iceberg_mutations._load_catalog") as loader:
        with pytest.raises(WriteError, match="duplicate column names"):
            write_to_iceberg(
                ArrowResult(),
                "test",
                "analytics.target",
                "incoming",
                "upsert",
                unique_keys=["id"],
                catalog_config=CatalogConfig(),
            )
    loader.assert_not_called()


@pytest.mark.parametrize("non_finite", [float("nan"), float("inf")])
def test_non_finite_upsert_keys_fail_before_catalog_loading(connection, non_finite):
    batch = pa.Table.from_pylist(
        [
            {
                "id": non_finite,
                "region": "id",
                "value": "one",
                "business_date": date(2026, 9, 14),
            }
        ]
    )
    connection.register("incoming-batch", batch)

    with patch("seeknal.workflow.materialization.iceberg_mutations._load_catalog") as loader:
        with pytest.raises(WriteError, match="finite values"):
            write_to_iceberg(
                connection,
                "test",
                "analytics.target",
                "incoming-batch",
                "upsert",
                unique_keys=["id"],
                catalog_config=CatalogConfig(),
            )
    loader.assert_not_called()


def test_mutation_failure_categories_are_explicit():
    assert IcebergMutationError("invalid input").failure_category == "preflight_failed"
    assert IcebergCommitError("conflict", "conflict").failure_category == "conflict"
    assert IcebergCommitError("unknown", "commit_unknown").failure_category == "commit_unknown"


@pytest.mark.parametrize("policy,expected_ids", [("insert", [1, 2, 6]), ("skip", [2, 6])])
def test_conditional_upsert_nulls_and_skip_metrics(connection, catalog, policy, expected_ids):
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("v", pa.int64())])
    catalog.create_namespace("analytics")
    table = catalog.create_table("analytics.target", schema=schema)
    table.append(pa.table({"id": [2, 3, 4, 5, 6], "v": [2, 2, 2, 2, None]}, schema=schema))
    batch = pa.table({"id": [1, 2, 3, 4, 5, 6], "v": [1, 3, 2, 1, None, 9]}, schema=schema)
    result = run_write(
        connection, catalog, batch, "upsert", unique_keys=["id"],
        when_matched_update_if="target.v IS NULL OR source.v > target.v",
        compare_columns=["v"], when_not_matched=policy,
    )
    assert result.input_row_count == 6
    assert result.row_count == len(expected_ids)
    assert result.updated_row_count == 2
    assert result.inserted_row_count == (policy == "insert")
    assert result.skipped_by_condition == 3
    assert result.skipped_not_matched == (policy == "skip")
    actual = {row["id"]: row["v"] for row in catalog.load_table("analytics.target").scan().to_arrow().to_pylist()}
    assert actual == {**({1: 1} if policy == "insert" else {}), 2: 3, 3: 2, 4: 2, 5: 2, 6: 9}


@pytest.mark.parametrize("empty", [False, True])
def test_condition_binding_precedes_table_creation(connection, catalog, empty):
    batch = arrow([] if empty else [{"id": 1, "region": "id", "value": "x", "business_date": date(2026, 10, 15)}])
    with pytest.raises(WriteError, match="(?i)(condition|column)"):
        run_write(connection, catalog, batch, "upsert", unique_keys=["id"],
                  when_matched_update_if="source.value <> target.value", compare_columns=[])
    assert catalog.list_namespaces() == []


def test_skip_missing_table_is_noop_without_creation(connection, catalog):
    batch = arrow([{"id": 1, "region": "id", "value": "x", "business_date": date(2026, 10, 15)}])
    result = run_write(connection, catalog, batch, "upsert", unique_keys=["id"], when_not_matched="skip")
    assert result.outcome == "noop"
    assert result.input_row_count == 1
    assert result.row_count == 0
    assert result.skipped_not_matched == 1
    assert catalog.list_namespaces() == []


def test_condition_rejection_is_noop_and_preserves_input_count(connection, catalog):
    create_table(catalog, [{"id": 1, "region": "id", "value": "current", "business_date": date(2026, 10, 15)}])
    before = catalog.load_table("analytics.target").metadata.current_snapshot_id
    result = run_write(connection, catalog, arrow([{"id": 1, "region": "id", "value": "old", "business_date": date(2026, 10, 14)}]),
                       "upsert", unique_keys=["id"], when_matched_update_if="FALSE")
    assert result.outcome == "noop"
    assert result.row_count == 0
    assert result.input_row_count == 1
    assert result.skipped_by_condition == 1
    assert catalog.load_table("analytics.target").metadata.current_snapshot_id == before


def test_condition_helper_columns_do_not_collide_and_keys_are_quoted(connection, catalog):
    batch = pa.table({"select": [1], "__seeknal_row_index": [99], "__seeknal_matched": [False], "v": [2]})
    catalog.create_namespace("analytics")
    table = catalog.create_table("analytics.target", schema=batch.schema)
    table.append(batch)
    result = run_write(connection, catalog, batch.set_column(3, "v", pa.array([3])), "upsert",
                       unique_keys=["select"], compare_columns=["v"], when_matched_update_if="source.v > target.v")
    assert result.updated_row_count == 1
    assert catalog.load_table("analytics.target").scan().to_arrow()["__seeknal_row_index"].to_pylist() == [99]


@pytest.mark.parametrize("condition,comparisons", [
    ("NULL", []),
    ("source.id < target.id", []),
    ("source.value <> 'old'", []),
])
def test_null_key_only_and_source_only_conditions(connection, catalog, condition, comparisons):
    create_table(catalog, [{"id": 1, "region": "id", "value": "current", "business_date": date(2026, 10, 15)}])
    result = run_write(
        connection, catalog,
        arrow([{"id": 1, "region": "id", "value": "old", "business_date": date(2026, 10, 14)}]),
        "upsert", unique_keys=["id"], when_matched_update_if=condition, compare_columns=comparisons,
    )
    assert result.outcome == "noop"
    assert result.skipped_by_condition == 1


def test_condition_projection_reuses_the_uniqueness_scan(connection, catalog):
    from pyiceberg.table import Table

    create_table(catalog, [{"id": 1, "region": "id", "value": "current", "business_date": date(2026, 10, 14)}])
    original_scan = Table.scan
    with patch.object(Table, "scan", autospec=True, side_effect=original_scan) as scan:
        result = run_write(
            connection, catalog,
            arrow([{"id": 1, "region": "id", "value": "new", "business_date": date(2026, 10, 15)}]),
            "upsert", unique_keys=["id"], compare_columns=["id", "business_date"],
            when_matched_update_if="source.business_date > target.business_date",
        )
    assert result.updated_row_count == 1
    assert scan.call_count == 1
    assert scan.call_args.kwargs["selected_fields"] == ("id", "business_date")


@pytest.mark.parametrize("condition", ["source IS NOT DISTINCT FROM target", "hash(source) = hash(target)"])
def test_whole_row_predicates_do_not_observe_internal_columns(connection, catalog, condition):
    batch = arrow([{"id": 1, "region": "id", "value": "current", "business_date": date(2026, 10, 15)}])
    create_table(catalog, batch.to_pylist())
    result = run_write(
        connection, catalog, batch, "upsert", unique_keys=["id"],
        compare_columns=["region", "value", "business_date"], when_matched_update_if=condition,
    )
    assert result.row_count == 1
    assert result.skipped_by_condition == 0
    assert result.outcome == "noop"  # Accepted but identical: PyIceberg need not rewrite it.


def test_target_record_projection_order_matches_binding(connection, catalog):
    batch = pa.table({"v": [2], "id": [1]})
    catalog.create_namespace("analytics")
    table = catalog.create_table("analytics.target", schema=batch.schema)
    table.append(batch)
    result = run_write(
        connection, catalog, batch, "upsert", unique_keys=["id"], compare_columns=["v"],
        when_matched_update_if="struct_extract_at(target, 1) = source.id",
    )
    assert result.row_count == 1
    assert result.skipped_by_condition == 0
