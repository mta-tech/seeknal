"""REST integration tests for Iceberg upsert and partition replacement.

These tests call the public materialization writer against an authenticated
loopback REST catalog. No configured catalog, object store, or credential is used.
"""

from __future__ import annotations

import os
from datetime import date
from typing import Any

import duckdb
import pyarrow as pa
import pytest

from seeknal.workflow.materialization.config import CatalogConfig
from seeknal.workflow.materialization.operations import WriteError, write_to_iceberg
from tests.integration.iceberg_rest_fixture import RunningRestFixture, run_rest_fixture


@pytest.fixture(autouse=True)
def scrub_external_catalog_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    sensitive_prefixes = ("AWS_", "S3_", "LAKEKEEPER_", "ICEBERG_")
    for name in tuple(os.environ):
        if name.startswith(sensitive_prefixes):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")


@pytest.fixture
def rest_fixture(tmp_path: Any) -> Any:
    with run_rest_fixture(tmp_path) as fixture:
        yield fixture


@pytest.fixture
def con() -> Any:
    connection = duckdb.connect(":memory:")
    try:
        yield connection
    finally:
        connection.close()


def catalog_config(
    fixture: RunningRestFixture,
    *,
    token: str | None = None,
) -> CatalogConfig:
    return CatalogConfig(
        uri=fixture.uri,
        warehouse=fixture.warehouse,
        bearer_token=fixture.token if token is None else token,
    )


def replace_view(con: Any, rows_sql: str) -> None:
    con.execute("DROP VIEW IF EXISTS incoming")
    con.execute(
        "CREATE VIEW incoming AS "
        f"SELECT * FROM (VALUES {rows_sql}) AS batch(id, value, business_date)"
    )


def replace_composite_view(con: Any, rows_sql: str) -> None:
    con.execute("DROP VIEW IF EXISTS incoming")
    con.execute(
        "CREATE VIEW incoming AS "
        f"SELECT * FROM (VALUES {rows_sql}) "
        "AS batch(id, value, business_date, region)"
    )


def empty_view(con: Any) -> None:
    con.execute("DROP VIEW IF EXISTS incoming")
    con.execute(
        "CREATE VIEW incoming AS SELECT "
        "CAST(NULL AS INTEGER) AS id, "
        "CAST(NULL AS VARCHAR) AS value, "
        "CAST(NULL AS DATE) AS business_date WHERE FALSE"
    )


def table_rows(fixture: RunningRestFixture, name: str) -> list[dict[str, Any]]:
    table = fixture.backend.load_table(tuple(name.split(".")))
    return sorted(table.scan().to_arrow().to_pylist(), key=lambda row: row["id"])


def current_snapshot_id(fixture: RunningRestFixture, name: str) -> int | None:
    table = fixture.backend.load_table(tuple(name.split(".")))
    snapshot = table.current_snapshot()
    return snapshot.snapshot_id if snapshot else None


def namespace_exists(fixture: RunningRestFixture, name: str) -> bool:
    return tuple(name.split(".")) in fixture.backend.list_namespaces()


def assert_duckdb_readback(
    fixture: RunningRestFixture,
    name: str,
    expected: list[tuple[Any, ...]],
    columns: str = "id, value, business_date",
) -> None:
    table = fixture.backend.load_table(tuple(name.split(".")))
    metadata_location = table.metadata_location.removeprefix("file://")
    with duckdb.connect(":memory:") as reader:
        reader.execute("LOAD iceberg")
        actual = reader.execute(
            f"SELECT {columns} FROM iceberg_scan(?) ORDER BY id",
            [metadata_location],
        ).fetchall()
    assert actual == expected


@pytest.mark.integration
def test_upsert_create_update_insert_and_rerun(
    con: Any,
    rest_fixture: RunningRestFixture,
) -> None:
    replace_view(
        con,
        "(1, 'one', DATE '2026-09-13'), (2, 'two', DATE '2026-09-14')",
    )
    created = write_to_iceberg(
        con,
        "local_rest",
        "analytics.dim_value",
        "incoming",
        "upsert",
        unique_keys=["id"],
        partition_by=["business_date"],
        create_table=True,
        catalog_config=catalog_config(rest_fixture),
    )
    assert created.success is True
    assert created.mode == "upsert"
    assert created.input_row_count == 2
    assert created.inserted_row_count == 2
    assert created.updated_row_count == 0
    assert created.outcome == "committed"
    assert created.snapshot_verified is True

    replace_view(
        con,
        "(2, 'two-updated', DATE '2026-09-15'), " "(3, 'three', DATE '2026-09-14')",
    )
    updated = write_to_iceberg(
        con,
        "local_rest",
        "analytics.dim_value",
        "incoming",
        "upsert",
        unique_keys=["id"],
        partition_by=["business_date"],
        catalog_config=catalog_config(rest_fixture),
    )
    assert updated.inserted_row_count == 1
    assert updated.updated_row_count == 1
    assert updated.outcome == "committed"

    expected = [
        {"id": 1, "value": "one", "business_date": date(2026, 9, 13)},
        {"id": 2, "value": "two-updated", "business_date": date(2026, 9, 15)},
        {"id": 3, "value": "three", "business_date": date(2026, 9, 14)},
    ]
    assert table_rows(rest_fixture, "analytics.dim_value") == expected

    snapshot_before_rerun = current_snapshot_id(rest_fixture, "analytics.dim_value")
    rerun = write_to_iceberg(
        con,
        "local_rest",
        "analytics.dim_value",
        "incoming",
        "upsert",
        unique_keys=["id"],
        partition_by=["business_date"],
        catalog_config=catalog_config(rest_fixture),
    )
    assert rerun.inserted_row_count == 0
    assert rerun.updated_row_count == 0
    assert rerun.outcome == "noop"
    assert (
        current_snapshot_id(rest_fixture, "analytics.dim_value")
        == snapshot_before_rerun
    )
    assert table_rows(rest_fixture, "analytics.dim_value") == expected
    assert_duckdb_readback(
        rest_fixture,
        "analytics.dim_value",
        [
            (1, "one", date(2026, 9, 13)),
            (2, "two-updated", date(2026, 9, 15)),
            (3, "three", date(2026, 9, 14)),
        ],
    )


@pytest.mark.integration
def test_insert_overwrite_replaces_composite_partition_tuples_only(
    con: Any,
    rest_fixture: RunningRestFixture,
) -> None:
    replace_composite_view(
        con,
        "(1, 'east-a', DATE '2026-09-13', 'east'), "
        "(2, 'east-b', DATE '2026-09-13', 'east'), "
        "(3, 'west', DATE '2026-09-13', 'west'), "
        "(4, 'next-east', DATE '2026-09-14', 'east')",
    )
    write_to_iceberg(
        con,
        "local_rest",
        "analytics.composite_mart",
        "incoming",
        "insert_overwrite",
        partition_by=["business_date", "region"],
        create_table=True,
        catalog_config=catalog_config(rest_fixture),
    )

    replace_composite_view(
        con,
        "(10, 'east-replacement', DATE '2026-09-13', 'east'), "
        "(20, 'new-west', DATE '2026-09-14', 'west')",
    )
    result = write_to_iceberg(
        con,
        "local_rest",
        "analytics.composite_mart",
        "incoming",
        "insert_overwrite",
        partition_by=["business_date", "region"],
        catalog_config=catalog_config(rest_fixture),
    )
    assert result.input_row_count == 2
    assert result.inserted_row_count == 2
    assert result.updated_row_count is None
    assert result.affected_partition_count == 2
    assert result.outcome == "committed"

    expected = [
        {
            "id": 3,
            "value": "west",
            "business_date": date(2026, 9, 13),
            "region": "west",
        },
        {
            "id": 4,
            "value": "next-east",
            "business_date": date(2026, 9, 14),
            "region": "east",
        },
        {
            "id": 10,
            "value": "east-replacement",
            "business_date": date(2026, 9, 13),
            "region": "east",
        },
        {
            "id": 20,
            "value": "new-west",
            "business_date": date(2026, 9, 14),
            "region": "west",
        },
    ]
    assert table_rows(rest_fixture, "analytics.composite_mart") == expected
    assert_duckdb_readback(
        rest_fixture,
        "analytics.composite_mart",
        [
            (3, "west", date(2026, 9, 13), "west"),
            (4, "next-east", date(2026, 9, 14), "east"),
            (10, "east-replacement", date(2026, 9, 13), "east"),
            (20, "new-west", date(2026, 9, 14), "west"),
        ],
        columns="id, value, business_date, region",
    )


@pytest.mark.integration
def test_missing_empty_batch_is_noop_without_catalog_mutation(
    con: Any,
    rest_fixture: RunningRestFixture,
) -> None:
    empty_view(con)
    result = write_to_iceberg(
        con,
        "local_rest",
        "never_created.empty_target",
        "incoming",
        "insert_overwrite",
        partition_by=["business_date"],
        create_table=True,
        catalog_config=catalog_config(rest_fixture),
    )
    assert result.success is True
    assert result.outcome == "noop"
    assert result.snapshot_id is None
    assert result.snapshot_verified is False
    assert result.row_count == 0
    assert rest_fixture.state.create_table_attempts == 0
    assert namespace_exists(rest_fixture, "never_created") is False


@pytest.mark.integration
def test_existing_empty_batch_is_noop_after_metadata_validation(
    con: Any,
    rest_fixture: RunningRestFixture,
) -> None:
    replace_view(con, "(1, 'one', DATE '2026-09-13')")
    write_to_iceberg(
        con,
        "local_rest",
        "analytics.empty_existing",
        "incoming",
        "insert_overwrite",
        partition_by=["business_date"],
        create_table=True,
        catalog_config=catalog_config(rest_fixture),
    )
    table = rest_fixture.backend.load_table(("analytics", "empty_existing"))
    table.delete("id = 1")
    snapshot_before = current_snapshot_id(rest_fixture, "analytics.empty_existing")

    empty_view(con)
    result = write_to_iceberg(
        con,
        "local_rest",
        "analytics.empty_existing",
        "incoming",
        "insert_overwrite",
        partition_by=["business_date"],
        catalog_config=catalog_config(rest_fixture),
    )
    assert result.outcome == "noop"
    assert result.snapshot_id == str(snapshot_before)
    assert result.snapshot_verified is True
    assert (
        current_snapshot_id(rest_fixture, "analytics.empty_existing") == snapshot_before
    )
    assert table_rows(rest_fixture, "analytics.empty_existing") == []


@pytest.mark.integration
def test_missing_empty_batch_with_create_disabled_fails(
    con: Any,
    rest_fixture: RunningRestFixture,
) -> None:
    empty_view(con)
    with pytest.raises(WriteError, match="(?i)(missing|not found|does not exist)"):
        write_to_iceberg(
            con,
            "local_rest",
            "never_created.disabled_target",
            "incoming",
            "upsert",
            unique_keys=["id"],
            create_table=False,
            catalog_config=catalog_config(rest_fixture),
        )
    assert rest_fixture.state.create_table_attempts == 0
    assert namespace_exists(rest_fixture, "never_created") is False


@pytest.mark.integration
def test_auth_error_is_not_treated_as_missing_table(
    con: Any,
    rest_fixture: RunningRestFixture,
) -> None:
    replace_view(con, "(1, 'one', DATE '2026-09-13')")
    with pytest.raises(WriteError, match="(?i)(unauthorized|401|token)"):
        write_to_iceberg(
            con,
            "local_rest",
            "analytics.auth_target",
            "incoming",
            "upsert",
            unique_keys=["id"],
            create_table=True,
            catalog_config=catalog_config(rest_fixture, token="invalid-token"),
        )
    assert rest_fixture.state.unauthorized_requests >= 1
    assert rest_fixture.state.create_table_attempts == 0
    assert namespace_exists(rest_fixture, "analytics") is False


@pytest.mark.integration
@pytest.mark.parametrize("status", [409, 500])
def test_commit_failure_is_not_retried(
    con: Any,
    rest_fixture: RunningRestFixture,
    status: int,
) -> None:
    replace_view(con, "(1, 'one', DATE '2026-09-13')")
    write_to_iceberg(
        con,
        "local_rest",
        "analytics.commit_failure",
        "incoming",
        "upsert",
        unique_keys=["id"],
        create_table=True,
        catalog_config=catalog_config(rest_fixture),
    )
    before = table_rows(rest_fixture, "analytics.commit_failure")
    attempts_before = rest_fixture.state.commit_attempts
    rest_fixture.state.forced_commit_status = status
    replace_view(con, "(1, 'changed', DATE '2026-09-13')")

    with pytest.raises(WriteError, match="(?i)(commit|conflict|unknown|forced)"):
        write_to_iceberg(
            con,
            "local_rest",
            "analytics.commit_failure",
            "incoming",
            "upsert",
            unique_keys=["id"],
            catalog_config=catalog_config(rest_fixture),
        )

    assert rest_fixture.state.commit_attempts - attempts_before == 1
    assert table_rows(rest_fixture, "analytics.commit_failure") == before


@pytest.mark.integration
def test_concurrent_snapshot_conflict_preserves_winner_without_retry(
    con: Any,
    rest_fixture: RunningRestFixture,
) -> None:
    replace_view(con, "(1, 'base', DATE '2026-09-13')")
    write_to_iceberg(
        con,
        "local_rest",
        "analytics.concurrent_target",
        "incoming",
        "upsert",
        unique_keys=["id"],
        create_table=True,
        catalog_config=catalog_config(rest_fixture),
    )
    attempts_before = rest_fixture.state.commit_attempts
    rest_fixture.state.concurrent_append = pa.Table.from_pylist(
        [{"id": 2, "value": "winner", "business_date": date(2026, 9, 14)}],
        schema=pa.schema(
            [
                pa.field("id", pa.int32(), nullable=True),
                pa.field("value", pa.string(), nullable=True),
                pa.field("business_date", pa.date32(), nullable=True),
            ]
        ),
    )
    replace_view(con, "(1, 'loser', DATE '2026-09-13')")

    with pytest.raises(WriteError, match="(?i)(commit|conflict|changed)"):
        write_to_iceberg(
            con,
            "local_rest",
            "analytics.concurrent_target",
            "incoming",
            "upsert",
            unique_keys=["id"],
            catalog_config=catalog_config(rest_fixture),
        )

    assert rest_fixture.state.commit_attempts - attempts_before == 1
    assert table_rows(rest_fixture, "analytics.concurrent_target") == [
        {"id": 1, "value": "base", "business_date": date(2026, 9, 13)},
        {"id": 2, "value": "winner", "business_date": date(2026, 9, 14)},
    ]


@pytest.mark.integration
def test_every_rest_request_uses_explicit_fixture_token(
    con: Any,
    rest_fixture: RunningRestFixture,
) -> None:
    replace_view(con, "(1, 'one', DATE '2026-09-13')")
    write_to_iceberg(
        con,
        "local_rest",
        "analytics.explicit_auth",
        "incoming",
        "upsert",
        unique_keys=["id"],
        create_table=True,
        catalog_config=catalog_config(rest_fixture),
    )
    assert rest_fixture.state.received_authorization
    assert set(rest_fixture.state.received_authorization) == {
        f"Bearer {rest_fixture.token}"
    }
