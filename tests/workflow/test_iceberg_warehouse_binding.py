"""Per-node Iceberg warehouse binding (FIX-11 b).

Regression: one run attached every write under the constant alias
``iceberg_catalog``; the first node's warehouse won and later nodes' rows
landed there, overwriting same-named tables. A failed overwrite also left the
target empty because DELETE and INSERT were not in one transaction.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import duckdb
import pytest

from seeknal.workflow.materialization.dispatcher import MaterializationDispatcher
from seeknal.workflow.materialization.operations import (
    DuckDBIcebergExtension,
    MaterializationOperationError,
    WriteError,
    WriteResult,
    iceberg_catalog_alias,
    write_to_iceberg,
)

URI = "https://lakekeeper.example/catalog"


# --- alias -----------------------------------------------------------------


def test_alias_is_distinct_per_warehouse_and_stable():
    bronze = iceberg_catalog_alias(URI, "bronze")
    silver = iceberg_catalog_alias(URI, "silver")

    assert bronze != silver
    assert bronze == iceberg_catalog_alias(URI, "bronze")
    assert bronze.startswith("iceberg_bronze_")


def test_alias_separates_endpoints_and_normalizes_catalog_suffix():
    assert iceberg_catalog_alias("https://a.example", "bronze") != iceberg_catalog_alias(
        "https://b.example", "bronze"
    )
    assert iceberg_catalog_alias("https://a.example/", "bronze") == iceberg_catalog_alias(
        "https://a.example/catalog", "bronze"
    )


def test_alias_is_a_plain_identifier_for_awkward_warehouse_names():
    alias = iceberg_catalog_alias(URI, "prod-dwa-hot-bronze")

    assert alias.startswith("iceberg_prod_dwa_hot_bronze_")
    assert alias.replace("_", "").isalnum()
    assert iceberg_catalog_alias(URI, "").startswith("iceberg_default_")


# --- attach ----------------------------------------------------------------


class _AttachCon:
    """Fake connection: tracks attachments like DuckDB's duckdb_databases()."""

    def __init__(self, types: dict[str, str] | None = None):
        self.attached: dict[str, str] = {}
        self.types = types or {}
        self.sql: list[str] = []

    def execute(self, sql, params=None):
        self.sql.append(sql)
        result = MagicMock()
        if sql.startswith("ATTACH"):
            warehouse = sql.split("'")[1]
            alias = sql.split(" AS ")[1].split(" ")[0].strip('"')
            if alias in self.attached:
                raise duckdb.BinderException(
                    f'Failed to attach database: database with name "{alias}" already exists'
                )
            self.attached[alias] = warehouse
        elif sql.startswith("DETACH"):
            self.attached.pop(sql.split()[1].strip('"'))
        elif "duckdb_databases()" in sql:
            path = self.attached.get(params[0])
            kind = self.types.get(params[0], "iceberg")
            result.fetchone.return_value = (path, kind) if params[0] in self.attached else None
        return result


def _attach(con, alias, warehouse):
    DuckDBIcebergExtension.attach_rest_catalog(
        con=con, catalog_name=alias, uri=URI, warehouse_path=warehouse
    )


def test_attach_reuses_alias_already_bound_to_same_warehouse():
    con = _AttachCon()
    _attach(con, "cat", "bronze")
    _attach(con, "cat", "bronze")

    assert con.attached == {"cat": "bronze"}
    assert not any(s.startswith("DETACH") for s in con.sql)


def test_attach_repoints_alias_bound_to_another_warehouse():
    con = _AttachCon()
    _attach(con, "cat", "bronze")
    _attach(con, "cat", "silver")

    assert con.attached == {"cat": "silver"}
    assert any(s.startswith("DETACH") for s in con.sql)


def test_attach_refuses_to_replace_a_non_iceberg_database():
    con = _AttachCon(types={"atlas": "postgres"})
    con.attached["atlas"] = "dbname=warehouse"
    con.attached["mem"] = None  # in-memory databases report no path
    con.types["mem"] = "duckdb"

    with pytest.raises(MaterializationOperationError, match="non-Iceberg"):
        _attach(con, "atlas", "silver")
    with pytest.raises(MaterializationOperationError, match="non-Iceberg"):
        _attach(con, "mem", "silver")
    assert con.attached == {"atlas": "dbname=warehouse", "mem": None}
    assert not any(s.startswith("DETACH") for s in con.sql)


def test_attached_database_reads_real_duckdb_metadata(tmp_path):
    from seeknal.workflow.materialization.operations import _attached_database

    con = duckdb.connect()
    db_file = tmp_path / "side.duckdb"
    con.execute(f"ATTACH '{db_file}' AS side")
    con.execute("ATTACH ':memory:' AS mem")

    assert _attached_database(con, "side") == (str(db_file), "duckdb")
    assert _attached_database(con, "mem") == (None, "duckdb")
    assert _attached_database(con, "missing") is None



def test_attach_escapes_quotes_in_literals():
    con = _AttachCon()
    DuckDBIcebergExtension.attach_rest_catalog(
        con=con, catalog_name="cat", uri=URI, warehouse_path="o'brien", bearer_token="t'k"
    )

    assert "'o''brien'" in con.sql[0]
    assert "TOKEN 't''k'" in con.sql[0]


def test_quoted_identifier_escapes_double_quotes():
    from seeknal.workflow.materialization.operations import _qi

    assert _qi("plain_name") == "plain_name"
    assert _qi("prod-dwa") == '"prod-dwa"'
    assert _qi('a"b') == '"a""b"'


def test_attach_wraps_detach_failure_during_reattach():
    class _DetachFails(_AttachCon):
        def execute(self, sql, params=None):
            if sql.startswith("DETACH"):
                raise duckdb.Error("catalog busy")
            return super().execute(sql, params)

    con = _DetachFails()
    _attach(con, "cat", "bronze")

    with pytest.raises(MaterializationOperationError, match="catalog busy"):
        _attach(con, "cat", "silver")


def test_attach_keeps_reuse_when_attachment_metadata_is_unavailable():
    con = MagicMock()
    con.execute.side_effect = [
        Exception('database with name "cat" already exists'),
        Exception("duckdb_databases() unavailable"),
    ]

    _attach(con, "cat", "silver")  # must not raise

    assert con.execute.call_count == 2


def test_attach_still_raises_other_errors():
    con = MagicMock()
    con.execute.side_effect = Exception("connection refused")

    with pytest.raises(MaterializationOperationError, match="connection refused"):
        _attach(con, "cat", "bronze")


# --- dispatcher: one run, three warehouses ---------------------------------


def test_dispatcher_binds_each_target_to_its_own_warehouse_on_one_connection():
    loader = MagicMock()
    profile = MagicMock()
    profile.default_mode.value = "append"
    profile.catalog.interpolate_env_vars.return_value = MagicMock(
        uri="", warehouse="", bearer_token="t"
    )
    loader.load_profile.return_value = profile
    dispatcher = MaterializationDispatcher(profile_loader=loader)
    con = MagicMock()
    attach = MagicMock()
    write = MagicMock(return_value=WriteResult(success=True, row_count=1))

    targets = [
        ("bronze.external_source.t", "bronze"),
        ("silver.external_source.t", "silver"),
        ("gold.claim_mind.t", "gold"),
    ]
    with (
        patch.object(DuckDBIcebergExtension, "attach_rest_catalog", attach),
        patch.object(DuckDBIcebergExtension, "load_extension", MagicMock()),
        patch.object(DuckDBIcebergExtension, "configure_s3", MagicMock()),
        patch("seeknal.workflow.materialization.operations.write_to_iceberg", write),
    ):
        for table, warehouse in targets:
            dispatcher._materialize_iceberg(
                con,
                "v",
                {"table": table, "mode": "overwrite", "catalog_uri": URI, "warehouse": warehouse},
            )

    attached = [(c.kwargs["catalog_name"], c.kwargs["warehouse_path"]) for c in attach.call_args_list]
    written = [c.kwargs["catalog_name"] for c in write.call_args_list]
    assert [w for _, w in attached] == ["bronze", "silver", "gold"]
    assert len({alias for alias, _ in attached}) == 3
    assert written == [alias for alias, _ in attached]
    assert written == [iceberg_catalog_alias(URI, w) for _, w in targets]


# --- atomic overwrite (real DuckDB transaction semantics) ------------------


@pytest.fixture
def catalog_con():
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS cat")
    con.execute("CREATE SCHEMA cat.ns")
    con.execute("CREATE TABLE cat.ns.t AS SELECT range AS id, 'old' AS type FROM range(7)")
    yield con
    con.close()


def test_failed_overwrite_keeps_previous_rows(catalog_con):
    catalog_con.execute("CREATE VIEW bad AS SELECT 1 AS only_one_column")

    with pytest.raises(WriteError):
        write_to_iceberg(catalog_con, "cat", "cat.ns.t", "bad", mode="overwrite")

    assert catalog_con.execute("SELECT type, COUNT(*) FROM cat.ns.t GROUP BY 1").fetchall() == [
        ("old", 7)
    ]


def test_successful_overwrite_replaces_rows(catalog_con):
    catalog_con.execute("CREATE VIEW good AS SELECT range AS id, 'new' AS type FROM range(3)")

    result = write_to_iceberg(catalog_con, "cat", "cat.ns.t", "good", mode="overwrite")

    assert result.success
    assert catalog_con.execute("SELECT type, COUNT(*) FROM cat.ns.t GROUP BY 1").fetchall() == [
        ("new", 3)
    ]


class _TxCon:
    """Fake connection for transaction-failure paths."""

    def __init__(self, fail_on: str, rollback_error: str | None = None):
        self.fail_on = fail_on
        self.rollback_error = rollback_error
        self.sql: list[str] = []

    def execute(self, sql):
        self.sql.append(sql)
        if sql.startswith(self.fail_on):
            raise duckdb.Error(f"{self.fail_on} failed")
        if sql == "ROLLBACK" and self.rollback_error:
            raise duckdb.TransactionException(self.rollback_error)


def test_commit_failure_is_reported_as_unknown_outcome(caplog):
    from seeknal.workflow.materialization.operations import _overwrite_atomically

    con = _TxCon(fail_on="COMMIT", rollback_error="cannot rollback - no transaction is active")

    with pytest.raises(duckdb.Error, match="COMMIT failed"):
        _overwrite_atomically(con, "cat.ns.t", "v")

    assert con.sql[-2:] == ["COMMIT", "ROLLBACK"]
    assert "outcome unknown" in caplog.text
    assert "Rollback failed" not in caplog.text


def test_insert_failure_rolls_back_and_logs_real_rollback_errors(caplog):
    from seeknal.workflow.materialization.operations import _overwrite_atomically

    con = _TxCon(fail_on="INSERT", rollback_error="connection lost")

    with pytest.raises(duckdb.Error, match="INSERT failed"):
        _overwrite_atomically(con, "cat.ns.t", "v")

    assert "COMMIT" not in con.sql
    assert con.sql[-1] == "ROLLBACK"
    assert "Rollback failed" in caplog.text


def test_append_still_appends(catalog_con):
    catalog_con.execute("CREATE VIEW more AS SELECT 100 AS id, 'more' AS type")

    write_to_iceberg(catalog_con, "cat", "cat.ns.t", "more", mode="append")

    assert catalog_con.execute("SELECT COUNT(*) FROM cat.ns.t").fetchone()[0] == 8


# --- decorator ---------------------------------------------------------------


def test_decorator_attaches_under_the_per_warehouse_alias():
    from seeknal.workflow.materialization.decorator import MaterializationMixin

    decorator = MaterializationMixin.__new__(MaterializationMixin)
    config = MagicMock()
    config.catalog.interpolate_env_vars.return_value = MagicMock(
        uri=URI, warehouse="silver", bearer_token="t"
    )
    decorator._materialization_config = config
    attach = MagicMock()

    with (
        patch.object(DuckDBIcebergExtension, "attach_rest_catalog", attach),
        patch.object(DuckDBIcebergExtension, "configure_s3", MagicMock()),
    ):
        alias = decorator._setup_catalog(MagicMock())

    assert alias == iceberg_catalog_alias(URI, "silver")
    assert attach.call_args.kwargs["catalog_name"] == alias


def test_advanced_modes_keep_the_pyiceberg_catalog_name():
    """upsert/insert_overwrite use PyIceberg with explicit uri/warehouse; the name
    stays 'iceberg_catalog' so existing ~/.pyiceberg.yaml settings still apply."""
    from seeknal.workflow.materialization.config import CatalogConfig

    loader = MagicMock()
    profile = MagicMock()
    profile.default_mode.value = "append"
    profile.catalog = CatalogConfig(uri="", warehouse="", bearer_token="t")
    loader.load_profile.return_value = profile
    dispatcher = MaterializationDispatcher(profile_loader=loader)
    write = MagicMock(return_value=WriteResult(success=True, row_count=1))

    with (
        patch("seeknal.workflow.materialization.operations.write_to_iceberg", write),
        patch("seeknal.workflow.materialization.config.validate_iceberg_write_options"),
    ):
        dispatcher._materialize_iceberg(
            MagicMock(), "v",
            {"table": "silver.ns.t", "mode": "upsert", "unique_keys": ["id"],
             "catalog_uri": URI, "warehouse": "silver"},
        )

    assert write.call_args.kwargs["catalog_name"] == "iceberg_catalog"
    assert write.call_args.kwargs["catalog_config"].warehouse == "silver"
