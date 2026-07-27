"""Conformance tests for the PostgreSQL online-store schema contract.

These encode the contract both repos must honour. Seeknal writes; the catalog
reads. A change that breaks a test here is a contract change and requires a
version bump, not a test edit.

Each group cites the defect it guards against, because several exist only
because an earlier design was wrong in a way that was not obvious.
"""

import pytest

from seeknal.featurestore.online_contract import (
    METADATA_COLUMNS,
    PG_MAX_IDENTIFIER_LENGTH,
    RESERVED_COLUMN_NAMES,
    ColumnSpec,
    OnlineContractError,
    OnlineTableDescriptor,
    resolve_pg_type,
    safe_identifier,
)


def make_descriptor(**overrides):
    kwargs = dict(
        project="retail",
        feature_group="customer_activity",
        version=1,
        entity_keys=(ColumnSpec("customer_id", "BIGINT", nullable=False),),
        features=(ColumnSpec("total_orders", "INTEGER"),),
    )
    kwargs.update(overrides)
    return OnlineTableDescriptor(**kwargs)


# ---------------------------------------------------------------------------
# Type mapping
# ---------------------------------------------------------------------------


class TestTypeMapping:
    """Write-direction typing must not inherit read-direction lossiness.

    ``POSTGRESQL_TO_DUCKDB_TYPES`` deliberately collapses JSON/JSONB/UUID to
    VARCHAR so DuckDB can read them. Applying that on write would silently
    degrade stored features.
    """

    @pytest.mark.parametrize(
        "logical,expected",
        [
            ("BIGINT", "BIGINT"),
            ("DOUBLE", "DOUBLE PRECISION"),
            ("VARCHAR", "TEXT"),
            ("TIMESTAMPTZ", "TIMESTAMPTZ"),
            ("UUID", "UUID"),
            ("JSON", "JSONB"),
            ("JSONB", "JSONB"),
        ],
    )
    def test_scalar_types(self, logical, expected):
        assert resolve_pg_type(logical) == expected

    def test_uuid_is_not_degraded_to_text(self):
        assert resolve_pg_type("UUID") == "UUID"

    def test_json_is_not_degraded_to_text(self):
        assert resolve_pg_type("JSON") == "JSONB"

    @pytest.mark.parametrize(
        "logical,expected",
        [
            ("ARRAY(DOUBLE)", "DOUBLE PRECISION[]"),
            ("ARRAY(VARCHAR)", "TEXT[]"),
            ("BIGINT[]", "BIGINT[]"),
        ],
    )
    def test_arrays_map_to_native_arrays(self, logical, expected):
        """Arrays stay relational rather than being flattened into JSON."""
        assert resolve_pg_type(logical) == expected

    def test_nested_types_become_jsonb(self):
        assert resolve_pg_type("MAP") == "JSONB"
        assert resolve_pg_type("STRUCT") == "JSONB"

    def test_decimal_precision_is_preserved(self):
        assert resolve_pg_type("DECIMAL(18,4)") == "NUMERIC(18,4)"

    def test_unmapped_type_fails_loudly(self):
        """A silent TEXT fallback is how type drift enters a contract."""
        with pytest.raises(OnlineContractError, match="no PostgreSQL mapping"):
            resolve_pg_type("GEOMETRY")

    def test_array_without_element_type_rejected(self):
        with pytest.raises(OnlineContractError):
            resolve_pg_type("ARRAY()")


# ---------------------------------------------------------------------------
# Structural invariants
# ---------------------------------------------------------------------------


class TestStructuralInvariants:
    def test_entity_key_must_be_not_null(self):
        with pytest.raises(OnlineContractError, match="must be NOT NULL"):
            make_descriptor(
                entity_keys=(ColumnSpec("customer_id", "BIGINT", nullable=True),)
            )

    def test_entity_key_must_be_a_valid_key_type(self):
        """Floating point makes unreliable equality semantics for point lookups."""
        with pytest.raises(OnlineContractError, match="not a valid key type"):
            make_descriptor(
                entity_keys=(ColumnSpec("score", "DOUBLE", nullable=False),)
            )

    def test_at_least_one_entity_key_required(self):
        with pytest.raises(OnlineContractError, match="at least one entity key"):
            make_descriptor(entity_keys=())

    def test_feature_cannot_shadow_metadata_column(self):
        with pytest.raises(OnlineContractError, match="reserved metadata column"):
            make_descriptor(features=(ColumnSpec("computed_at", "TIMESTAMPTZ"),))

    def test_duplicate_column_names_rejected(self):
        with pytest.raises(OnlineContractError, match="duplicate column"):
            make_descriptor(
                features=(
                    ColumnSpec("total_orders", "INTEGER"),
                    ColumnSpec("total_orders", "BIGINT"),
                )
            )

    def test_composite_keys_supported(self):
        d = make_descriptor(
            entity_keys=(
                ColumnSpec("tenant_id", "TEXT", nullable=False),
                ColumnSpec("customer_id", "BIGINT", nullable=False),
            )
        )
        ddl = d.create_table_ddl()
        assert 'PRIMARY KEY ("tenant_id", "customer_id")' in ddl


# ---------------------------------------------------------------------------
# Required metadata
# ---------------------------------------------------------------------------


class TestMetadataColumns:
    def test_freshness_columns_present_and_not_null(self):
        by_name = {c.name: c for c in METADATA_COLUMNS}
        for required in ("computed_at", "source_interval_start", "source_interval_end"):
            assert required in by_name
            assert by_name[required].nullable is False

    def test_computed_at_and_interval_end_are_distinct(self):
        """A row recomputed today over last week's window is fresh by one
        measure and stale by the other. Only the second is meaningful to a
        consumer, so both must exist."""
        names = {c.name for c in METADATA_COLUMNS}
        assert {"computed_at", "source_interval_end"} <= names

    def test_provenance_columns_present(self):
        names = {c.name for c in METADATA_COLUMNS}
        assert {"definition_sha", "schema_sha", "publish_run_id"} <= names

    def test_retired_at_is_nullable(self):
        """NULL means live; non-NULL means intentionally retired."""
        retired = next(c for c in METADATA_COLUMNS if c.name == "retired_at")
        assert retired.nullable is True

    def test_reserved_names_match_metadata(self):
        assert RESERVED_COLUMN_NAMES == {c.name for c in METADATA_COLUMNS}


# ---------------------------------------------------------------------------
# Identifiers
# ---------------------------------------------------------------------------


class TestIdentifiers:
    def test_short_identifiers_untouched(self):
        assert safe_identifier("fg_retail", "orders") == "fg_retail__orders"

    def test_long_identifiers_truncated_within_pg_limit(self):
        out = safe_identifier("x" * 60, "y" * 60)
        assert len(out) <= PG_MAX_IDENTIFIER_LENGTH

    def test_truncation_is_collision_resistant(self):
        a = safe_identifier("x" * 60, "y" * 60)
        b = safe_identifier("x" * 60, "z" * 60)
        assert a != b

    def test_truncation_is_deterministic(self):
        assert safe_identifier("x" * 60, "y" * 60) == safe_identifier(
            "x" * 60, "y" * 60
        )


# ---------------------------------------------------------------------------
# Staging names — guards a silent zero-row write
# ---------------------------------------------------------------------------


class TestStagingNames:
    """Reusing a staging name that was dropped out-of-band via
    ``postgres_execute`` and recreated through the attached catalog leaves stale
    catalog metadata; the subsequent CTAS then silently writes ZERO rows with no
    error. Binding the name to publish_run_id makes that unreachable.
    """

    def test_staging_name_varies_by_publish_run(self):
        d = make_descriptor()
        assert d.staging_name("run-aaaa-1111") != d.staging_name("run-bbbb-2222")

    def test_staging_name_is_deterministic_for_a_run(self):
        d = make_descriptor()
        assert d.staging_name("run-aaaa-1111") == d.staging_name("run-aaaa-1111")

    def test_staging_name_requires_run_id(self):
        d = make_descriptor()
        with pytest.raises(OnlineContractError, match="publish_run_id is required"):
            d.staging_name("")

    def test_staging_name_within_pg_limit(self):
        d = make_descriptor(feature_group="a" * 60)
        assert len(d.staging_name("run-" + "c" * 40)) <= PG_MAX_IDENTIFIER_LENGTH


# ---------------------------------------------------------------------------
# Schema evolution
# ---------------------------------------------------------------------------


class TestEvolution:
    """PostgreSQL's CREATE OR REPLACE VIEW may only APPEND columns; it cannot
    change an existing column's name, order, or type. An earlier design assumed
    it could cut over type/key/removal changes. It cannot.
    """

    def test_appending_nullable_column_is_compatible(self):
        old = make_descriptor()
        new = make_descriptor(
            features=(
                ColumnSpec("total_orders", "INTEGER"),
                ColumnSpec("avg_basket", "DOUBLE", nullable=True),
            )
        )
        assert new.compatible_with(old)

    def test_appending_not_null_column_is_incompatible(self):
        old = make_descriptor()
        new = make_descriptor(
            features=(
                ColumnSpec("total_orders", "INTEGER"),
                ColumnSpec("avg_basket", "DOUBLE", nullable=False),
            )
        )
        assert not new.compatible_with(old)

    def test_retyping_existing_column_is_incompatible(self):
        old = make_descriptor()
        new = make_descriptor(features=(ColumnSpec("total_orders", "BIGINT"),))
        assert not new.compatible_with(old)

    def test_removing_column_is_incompatible(self):
        old = make_descriptor(
            features=(ColumnSpec("a", "INTEGER"), ColumnSpec("b", "INTEGER"))
        )
        new = make_descriptor(features=(ColumnSpec("a", "INTEGER"),))
        assert not new.compatible_with(old)

    def test_reordering_columns_is_incompatible(self):
        old = make_descriptor(
            features=(ColumnSpec("a", "INTEGER"), ColumnSpec("b", "INTEGER"))
        )
        new = make_descriptor(
            features=(ColumnSpec("b", "INTEGER"), ColumnSpec("a", "INTEGER"))
        )
        assert not new.compatible_with(old)

    def test_changing_entity_key_is_incompatible(self):
        old = make_descriptor()
        new = make_descriptor(
            entity_keys=(ColumnSpec("user_id", "BIGINT", nullable=False),)
        )
        assert not new.compatible_with(old)


# ---------------------------------------------------------------------------
# schema_sha
# ---------------------------------------------------------------------------


class TestSchemaSha:
    def test_stable_for_identical_shape(self):
        assert make_descriptor().schema_sha == make_descriptor().schema_sha

    def test_changes_when_type_changes(self):
        a = make_descriptor()
        b = make_descriptor(features=(ColumnSpec("total_orders", "BIGINT"),))
        assert a.schema_sha != b.schema_sha

    def test_changes_when_column_added(self):
        a = make_descriptor()
        b = make_descriptor(
            features=(ColumnSpec("total_orders", "INTEGER"), ColumnSpec("x", "INTEGER"))
        )
        assert a.schema_sha != b.schema_sha


# ---------------------------------------------------------------------------
# Emitted SQL
# ---------------------------------------------------------------------------


class TestGeneratedSql:
    def test_create_table_declares_explicit_primary_key(self):
        """CREATE TABLE AS SELECT creates no key and no indexes, which makes
        both point-lookup latency and ON CONFLICT unavailable."""
        assert "PRIMARY KEY" in make_descriptor().create_table_ddl()

    def test_upsert_updates_every_served_column(self):
        """Omitting a served column yields a logically mixed old/new row even
        though PostgreSQL row visibility remains atomic."""
        d = make_descriptor(
            features=(ColumnSpec("a", "INTEGER"), ColumnSpec("b", "INTEGER"))
        )
        sql = d.upsert_sql("feature_online.stg_x")
        for name in d.served_column_names:
            assert f'"{name}" = EXCLUDED."{name}"' in sql

    def test_upsert_conflict_target_is_the_entity_key(self):
        d = make_descriptor()
        assert 'ON CONFLICT ("customer_id")' in d.upsert_sql("feature_online.stg_x")

    def test_no_select_star_in_serving_path(self):
        d = make_descriptor()
        for sql in (d.create_view_ddl(), d.live_view_ddl(), d.upsert_sql("s.t")):
            assert "SELECT *" not in sql

    def test_live_view_excludes_retired_rows(self):
        assert '"retired_at" IS NULL' in make_descriptor().live_view_ddl()

    def test_reader_binds_to_unversioned_view(self):
        """The catalog must never bind to a versioned table name."""
        d = make_descriptor(version=7)
        assert "v7" not in d.view_fqn
        assert "v7" in d.physical_fqn


# ---------------------------------------------------------------------------
# Round-trip
# ---------------------------------------------------------------------------


class TestSerialization:
    def test_round_trip_preserves_shape(self):
        original = make_descriptor(
            entity_keys=(
                ColumnSpec("tenant_id", "TEXT", nullable=False),
                ColumnSpec("customer_id", "BIGINT", nullable=False),
            ),
            features=(ColumnSpec("tags", "ARRAY(VARCHAR)"), ColumnSpec("p", "MAP")),
        )
        restored = OnlineTableDescriptor.from_dict(original.to_dict())
        assert restored.schema_sha == original.schema_sha
        assert restored.create_table_ddl() == original.create_table_ddl()

    def test_dict_exposes_resolved_pg_types_for_readers(self):
        d = make_descriptor(features=(ColumnSpec("tags", "ARRAY(VARCHAR)"),))
        tags = next(c for c in d.to_dict()["features"] if c["name"] == "tags")
        assert tags["pg_type"] == "TEXT[]"
