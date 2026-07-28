"""Unit tests for the online publisher.

Covers the logic that can be tested without a database. The atomic activation
sequence itself is proven by the end-to-end scenarios against a real
PostgreSQL -- mocking a transaction would only test the mock.
"""

from datetime import datetime, timedelta, timezone

import pytest

from seeknal.featurestore.online_contract import ColumnSpec, OnlineTableDescriptor
from seeknal.featurestore.online_publisher import (
    EXECUTIONS_TABLE,
    PUBLICATIONS_TABLE,
    OnlinePublishError,
    OnlinePublisher,
    PublishResult,
    StagingValidationError,
    UpstreamIncompleteError,
    advisory_key,
    is_serialization_failure,
)

NOW = datetime(2026, 7, 27, tzinfo=timezone.utc)
DSN = "host=localhost port=5432 dbname=x user=y password=z"


def descriptor(**over):
    kwargs = dict(
        project="retail",
        feature_group="customer_30d",
        version=1,
        entity_keys=(ColumnSpec("customer_id", "BIGINT", nullable=False),),
        features=(ColumnSpec("orders_30d", "INTEGER"),),
    )
    kwargs.update(over)
    return OnlineTableDescriptor(**kwargs)


class FakeCon:
    """Records SQL and replays scripted scalar answers."""

    def __init__(self, scalars=None, tables=None):
        self.sql: list[str] = []
        self.scalars = list(scalars or [])
        # Rows returned by fetchall(), used by resolve_version's table listing.
        self.tables = [(t,) for t in (tables or [])]

    def execute(self, sql):
        self.sql.append(sql)
        return self

    def fetchone(self):
        return (self.scalars.pop(0),) if self.scalars else (0,)

    def fetchall(self):
        return list(self.tables)

    @property
    def joined(self) -> str:
        return "\n".join(self.sql)


def publisher(d=None):
    return OnlinePublisher(d or descriptor(), DSN)


# ---------------------------------------------------------------------------


class TestAdvisoryKey:
    def test_stable_for_same_table(self):
        assert advisory_key("feature_online.t") == advisory_key("feature_online.t")

    def test_differs_between_tables(self):
        assert advisory_key("feature_online.a") != advisory_key("feature_online.b")

    def test_fits_postgres_bigint_positive_range(self):
        """Must be a positive 63-bit-safe value; a negative or oversized key
        would be rejected or wrap."""
        for name in ("a", "b" * 200, "feature_online.fg_x__y__v1"):
            k = advisory_key(name)
            assert 0 <= k <= 0x7FFFFFFF


class TestStagingValidation:
    """Validation runs before activation so a bad publication cannot replace a
    good served generation."""

    def test_empty_staging_rejected(self):
        with pytest.raises(StagingValidationError, match="empty"):
            publisher().validate_staging(FakeCon(), "s.stg", 0)

    def test_null_entity_key_rejected(self):
        con = FakeCon(scalars=[3])  # null-key count
        with pytest.raises(StagingValidationError, match="NULL entity key"):
            publisher().validate_staging(con, "s.stg", 10)

    def test_duplicate_keys_rejected(self):
        con = FakeCon(scalars=[0, 8])  # no nulls, 8 distinct of 10
        with pytest.raises(StagingValidationError, match="duplicate entity keys"):
            publisher().validate_staging(con, "s.stg", 10)

    def test_clean_staging_returns_distinct_count(self):
        con = FakeCon(scalars=[0, 10])
        assert publisher().validate_staging(con, "s.stg", 10) == 10

    def test_composite_key_uniqueness_uses_all_key_columns(self):
        d = descriptor(
            entity_keys=(
                ColumnSpec("tenant_id", "TEXT", nullable=False),
                ColumnSpec("customer_id", "BIGINT", nullable=False),
            )
        )
        con = FakeCon(scalars=[0, 5])
        publisher(d).validate_staging(con, "s.stg", 5)
        assert '"tenant_id"' in con.joined and '"customer_id"' in con.joined


class TestUpstreamGate:
    """Fail-closed: absence of a success record blocks publication. A derived
    feature over an incomplete window is wrong, not missing."""

    def test_no_upstream_declared_is_a_noop(self):
        con = FakeCon()
        publisher().assert_upstream_complete(con, [], NOW, NOW)
        assert con.sql == []

    def test_missing_upstream_record_blocks(self):
        con = FakeCon(scalars=[0])
        with pytest.raises(UpstreamIncompleteError, match="no successful execution"):
            publisher().assert_upstream_complete(con, ["node_a"], NOW, NOW)

    def test_covering_upstream_record_allows(self):
        con = FakeCon(scalars=[1])
        publisher().assert_upstream_complete(con, ["node_a"], NOW, NOW)

    def test_every_upstream_node_is_checked(self):
        con = FakeCon(scalars=[1, 1, 0])
        with pytest.raises(UpstreamIncompleteError, match="node_c"):
            publisher().assert_upstream_complete(
                con, ["node_a", "node_b", "node_c"], NOW, NOW
            )

    def test_gate_requires_coverage_not_exact_equality(self):
        """An upstream node on a coarser schedule legitimately covers a finer
        downstream interval; exact matching would block it forever."""
        con = FakeCon(scalars=[1])
        publisher().assert_upstream_complete(
            con, ["node_a"], NOW - timedelta(days=1), NOW
        )
        assert "interval_start <=" in con.joined and "interval_end >=" in con.joined

    def test_gate_only_counts_succeeded_status(self):
        con = FakeCon(scalars=[1])
        publisher().assert_upstream_complete(con, ["node_a"], NOW, NOW)
        # Quotes are doubled because the predicate is nested inside a string
        # literal passed to postgres_query.
        assert "status = ''succeeded''" in con.joined


class TestLedgerSchema:
    def test_publications_and_executions_are_separate_tables(self):
        """One ledger holding only online publications cannot answer 'did every
        upstream node succeed', and conflating them deadlocks the gate."""
        assert PUBLICATIONS_TABLE != EXECUTIONS_TABLE

    def test_ensure_schema_creates_both_ledgers(self):
        con = FakeCon()
        publisher().ensure_schema(con)
        assert PUBLICATIONS_TABLE in con.joined
        assert EXECUTIONS_TABLE in con.joined

    def test_ensure_schema_creates_table_and_both_views(self):
        d = descriptor()
        con = FakeCon()
        publisher(d).ensure_schema(con)
        assert "CREATE TABLE IF NOT EXISTS" in con.joined
        assert d.view_fqn in con.joined
        assert "__live" in con.joined

    def test_execution_record_is_idempotent(self):
        con = FakeCon()
        publisher().record_execution(con, "n1", NOW, NOW)
        assert "ON CONFLICT" in con.joined and "DO UPDATE" in con.joined


class TestVersionResolution:
    """Publication used to always target __v1: from_relation defaults to 1 and
    nothing overrode it, so compatible_with was never exercised at runtime and
    a feature added to an existing group would fail against the old table
    instead of cutting over to __v2."""

    def test_first_publication_is_v1(self):
        assert publisher().resolve_version(FakeCon(tables=[])) == 1

    def test_matching_schema_reuses_the_existing_version(self):
        d = descriptor()
        con = FakeCon(scalars=[d.schema_sha], tables=[f"{d.base_name}__v1"])
        assert publisher(d).resolve_version(con) == 1

    def test_incompatible_schema_bumps_to_the_next_version(self):
        d = descriptor()
        con = FakeCon(scalars=["a-different-sha"], tables=[f"{d.base_name}__v1"])
        assert publisher(d).resolve_version(con) == 2

    def test_bumps_past_the_highest_existing_version(self):
        d = descriptor()
        con = FakeCon(
            scalars=["other", "other", "other"],
            tables=[f"{d.base_name}__v{i}" for i in (1, 2, 3)],
        )
        assert publisher(d).resolve_version(con) == 4

    def test_reuses_the_newest_matching_version(self):
        d = descriptor()
        # v2 is inspected first (newest-first) and matches.
        con = FakeCon(scalars=[d.schema_sha], tables=[f"{d.base_name}__v1", f"{d.base_name}__v2"])
        assert publisher(d).resolve_version(con) == 2

    def test_empty_table_is_treated_as_compatible(self):
        """A table with no rows constrains nothing, so publishing into it is
        preferable to stranding it and creating another version."""
        d = descriptor()
        con = FakeCon(scalars=[None], tables=[f"{d.base_name}__v1"])
        assert publisher(d).resolve_version(con) == 1

    def test_unrelated_tables_are_ignored(self):
        d = descriptor()
        con = FakeCon(scalars=[d.schema_sha], tables=["some_other_table", f"{d.base_name}__v1"])
        assert publisher(d).resolve_version(con) == 1


class TestLedgerMigration:
    """CREATE TABLE IF NOT EXISTS keeps an older definition silently, so a
    ledger created before idempotency_key was UNIQUE keeps accepting
    duplicates."""

    # Two probes now run, in order: the idempotency_key UNIQUE constraint, then
    # the feature_group column. Scalars are supplied per probe.

    def test_adds_the_unique_constraint_when_absent(self):
        con = FakeCon(scalars=[0, 1])  # no unique constraint, column present
        applied = publisher().migrate_schema(con)
        assert "idempotency_key UNIQUE" in applied
        assert "ADD CONSTRAINT" in con.joined and "UNIQUE" in con.joined

    def test_is_a_noop_when_already_present(self):
        con = FakeCon(scalars=[1, 1])
        assert publisher().migrate_schema(con) == []
        assert "ADD CONSTRAINT" not in con.joined
        assert "ADD COLUMN" not in con.joined

    def test_refreshes_the_attached_catalog_after_the_alter(self):
        """The ALTER runs out-of-band, so the attached catalog keeps the
        pre-ALTER definition unless the cache is cleared."""
        con = FakeCon(scalars=[0, 1])
        publisher().migrate_schema(con)
        assert "pg_clear_cache" in con.joined

    def test_adds_the_feature_group_column_when_absent(self):
        """A ledger predating the policy table has no feature_group column, and
        CREATE TABLE IF NOT EXISTS will not add it -- leaving publication status
        with no join key to the read policy."""
        con = FakeCon(scalars=[1, 0])  # constraint present, column missing
        applied = publisher().migrate_schema(con)
        assert "feature_group column" in applied
        assert "ADD COLUMN" in con.joined and "feature_group" in con.joined
        assert "pg_clear_cache" in con.joined


class TestExpiry:
    def test_rejects_non_positive_ttl(self):
        with pytest.raises(OnlinePublishError, match="must be positive"):
            publisher().expire_stale(FakeCon(), 0)

    def test_measures_age_from_interval_end_not_computed_at(self):
        """A row recomputed minutes ago over a month-old window is stale;
        measuring from computed_at would call it fresh."""
        con = FakeCon(scalars=[0, 5])
        publisher().expire_stale(con, 7)
        assert "source_interval_end <" in con.joined
        assert "computed_at <" not in con.joined

    def test_retires_rather_than_deletes(self):
        con = FakeCon(scalars=[0, 5])
        publisher().expire_stale(con, 7)
        assert "SET retired_at" in con.joined
        assert "DELETE" not in con.joined.upper()

    def test_skips_already_retired_rows(self):
        con = FakeCon(scalars=[0, 5])
        publisher().expire_stale(con, 7)
        assert "retired_at IS NULL" in con.joined

    def test_returns_number_newly_retired(self):
        con = FakeCon(scalars=[2, 9])  # 2 before, 9 after
        assert publisher().expire_stale(con, 7) == 7


class TestRetirement:
    def test_missing_key_column_rejected(self):
        d = descriptor(
            entity_keys=(
                ColumnSpec("tenant_id", "TEXT", nullable=False),
                ColumnSpec("customer_id", "BIGINT", nullable=False),
            )
        )
        with pytest.raises(Exception, match="missing entity key"):
            publisher(d).retire(FakeCon(), {"tenant_id": "t1"})

    def test_retire_updates_rather_than_deletes(self):
        con = FakeCon(scalars=[1])
        publisher().retire(con, {"customer_id": 1})
        assert "SET retired_at" in con.joined
        assert "DELETE" not in con.joined.upper()

    def test_retire_is_idempotent_on_already_retired_rows(self):
        con = FakeCon(scalars=[1])
        publisher().retire(con, {"customer_id": 1})
        assert "retired_at IS NULL" in con.joined


class TestPublishResult:
    def test_not_succeeded_when_not_activated(self):
        assert PublishResult("r", "t").succeeded is False

    def test_not_succeeded_when_error_present(self):
        assert PublishResult("r", "t", activated=True, error="boom").succeeded is False

    def test_succeeded_requires_activation_and_no_error(self):
        assert PublishResult("r", "t", activated=True).succeeded is True

    def test_serialises_for_logging(self):
        payload = PublishResult("r", "t", activated=True, staged_rows=5).to_dict()
        assert payload["succeeded"] is True and payload["staged_rows"] == 5


class TestSerializationRetry:
    """The advisory lock serializes one target, but forced REPEATABLE READ means
    a conflicting write elsewhere can still abort with SQLSTATE 40001."""

    def test_recognises_sqlstate_40001(self):
        assert is_serialization_failure(Exception("ERROR 40001: oops"))

    def test_recognises_the_message_form(self):
        assert is_serialization_failure(
            Exception("could not serialize access due to concurrent update")
        )

    def test_does_not_treat_unrelated_errors_as_retryable(self):
        assert not is_serialization_failure(Exception("relation does not exist"))

    def test_retries_then_succeeds(self, monkeypatch):
        pub = publisher()
        calls = []

        def flaky(con, view, *, attempt=0, **kw):
            calls.append(attempt)
            if attempt < 2:
                raise RuntimeError("could not serialize access due to concurrent update")
            return PublishResult("r", "t", activated=True)

        monkeypatch.setattr(pub, "publish", flaky)
        result = pub.publish_with_retry(
            FakeCon(), "v", max_retries=3, backoff_seconds=0
        )
        assert result.succeeded
        assert calls == [0, 1, 2]

    def test_each_attempt_uses_a_distinct_staging_name(self):
        """Reusing a staging name after the prior attempt dropped it is the R16
        stale-catalog hazard, which silently yields a zero-row CTAS."""
        pub = publisher()
        names = {
            pub.d.staging_fqn("run-1" if a == 0 else f"run-1a{a}") for a in range(3)
        }
        assert len(names) == 3

    def test_validation_errors_are_not_retried(self, monkeypatch):
        pub = publisher()
        calls = []

        def always_invalid(con, view, *, attempt=0, **kw):
            calls.append(attempt)
            raise StagingValidationError("duplicate keys")

        monkeypatch.setattr(pub, "publish", always_invalid)
        with pytest.raises(StagingValidationError):
            pub.publish_with_retry(FakeCon(), "v", max_retries=3, backoff_seconds=0)
        assert calls == [0], "a deterministic failure must not be retried"

    def test_upstream_gate_failures_are_not_retried(self, monkeypatch):
        pub = publisher()
        calls = []

        def blocked(con, view, *, attempt=0, **kw):
            calls.append(attempt)
            raise UpstreamIncompleteError("incomplete")

        monkeypatch.setattr(pub, "publish", blocked)
        with pytest.raises(UpstreamIncompleteError):
            pub.publish_with_retry(FakeCon(), "v", max_retries=3, backoff_seconds=0)
        assert calls == [0]

    def test_non_retryable_errors_propagate_immediately(self, monkeypatch):
        pub = publisher()
        calls = []

        def boom(con, view, *, attempt=0, **kw):
            calls.append(attempt)
            raise RuntimeError("relation does not exist")

        monkeypatch.setattr(pub, "publish", boom)
        with pytest.raises(RuntimeError, match="relation does not exist"):
            pub.publish_with_retry(FakeCon(), "v", max_retries=3, backoff_seconds=0)
        assert calls == [0]

    def test_gives_up_after_max_retries(self, monkeypatch):
        pub = publisher()
        calls = []

        def always_conflict(con, view, *, attempt=0, **kw):
            calls.append(attempt)
            raise RuntimeError("could not serialize access due to concurrent update")

        monkeypatch.setattr(pub, "publish", always_conflict)
        with pytest.raises(OnlinePublishError, match="after 3 attempts"):
            pub.publish_with_retry(FakeCon(), "v", max_retries=2, backoff_seconds=0)
        assert calls == [0, 1, 2]


class TestSqlSafety:
    def test_string_literals_are_escaped(self):
        """The property that matters is that a quote cannot terminate the
        literal early -- not the exact escape depth.

        Escaping happens twice here: once building the inner SQL, once wrapping
        it for postgres_execute. So a single quote becomes four. Asserting the
        dangerous *unescaped* sequence is absent tests the property directly and
        survives a change in nesting depth.
        """
        con = FakeCon()
        publisher().record_execution(con, "node'; DROP TABLE x; --", NOW, NOW)
        assert "node';" not in con.joined  # would have broken out of the literal
        assert "node''''" in con.joined  # doubled twice, still inert

    def test_error_types_form_a_hierarchy(self):
        """Callers should be able to catch OnlinePublishError broadly."""
        assert issubclass(StagingValidationError, OnlinePublishError)
        assert issubclass(UpstreamIncompleteError, OnlinePublishError)


class TestRollback:
    """Publication is in place, so a successful-but-wrong run overwrites the
    previous values and the ledger records metadata rather than rows. These pin
    the guard rails around undoing one."""

    def test_refuses_when_nothing_was_ever_published(self):
        con = FakeCon(scalars=[None])
        with pytest.raises(OnlinePublishError, match="nothing to roll back"):
            publisher().rollback(con, "r-1")

    def test_refuses_to_roll_back_an_older_run(self):
        """Only one generation of pre-publication state is kept, so rolling
        back an older run would silently discard every publication since."""
        con = FakeCon(scalars=["r-newer"])
        with pytest.raises(OnlinePublishError, match="latest successful"):
            publisher().rollback(con, "r-older")

    def test_refuses_when_the_lock_is_held(self):
        """A rollback must not interleave with a publication."""
        con = FakeCon(scalars=["r-1", False])
        with pytest.raises(OnlinePublishError, match="holds the lock"):
            publisher().rollback(con, "r-1")
        assert "ROLLBACK" in con.joined

    def test_restores_and_deletes_under_one_transaction(self):
        con = FakeCon(scalars=["r-1", True, 3, 5])
        counts = publisher().rollback(con, "r-1")
        assert counts == {"restored": 5, "deleted": 3}
        assert "BEGIN TRANSACTION" in con.joined
        assert "COMMIT" in con.joined
        assert "pg_try_advisory_xact_lock" in con.joined

    def test_rows_the_run_created_are_deleted_not_restored(self):
        """Rows absent from the snapshot had no prior value, so there is
        nothing to restore -- they must go."""
        con = FakeCon(scalars=["r-1", True, 3, 5])
        publisher().rollback(con, "r-1")
        assert "NOT IN" in con.joined

    def test_ledger_is_marked_rolled_back(self):
        con = FakeCon(scalars=["r-1", True, 0, 2])
        publisher().rollback(con, "r-1")
        assert "'rolled_back'" in con.joined

    def test_the_snapshot_is_cleared_so_it_cannot_be_applied_twice(self):
        """Left in place, a second rollback would delete the rows the first one
        correctly restored."""
        d = descriptor()
        con = FakeCon(scalars=["r-1", True, 0, 2])
        publisher(d).rollback(con, "r-1")
        assert f"DELETE FROM pg_online.{d.rollback_fqn}" in con.joined

    def test_snapshot_table_is_bound_to_the_versioned_table(self):
        """A rollback restores rows into a specific physical table; restoring a
        v1 snapshot into v2 would write the wrong shape."""
        d = descriptor()
        assert d.physical_name in d.rollback_name
        assert d.rollback_name != d.base_name
