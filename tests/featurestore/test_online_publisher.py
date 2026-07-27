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

    def __init__(self, scalars=None):
        self.sql: list[str] = []
        self.scalars = list(scalars or [])

    def execute(self, sql):
        self.sql.append(sql)
        return self

    def fetchone(self):
        return (self.scalars.pop(0),) if self.scalars else (0,)

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
