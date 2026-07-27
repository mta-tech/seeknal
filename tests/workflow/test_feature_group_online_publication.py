"""Tests for online publication from the feature-group executor.

The behaviour under test is the deliberate asymmetry with Iceberg
materialization: Iceberg failures are logged and the run continues (ADR-006
best-effort), whereas an online publication failure FAILS the run.

The reasoning is in ADR-006 itself, which accepts partial materialization but
states that consistency across targets must be managed at the pipeline level
where required. Publishing a derived feature over an incomplete upstream window
yields a confidently wrong served value, not a missing one -- so it must not be
downgraded to a warning.
"""

from datetime import datetime
from types import SimpleNamespace

import pytest

from seeknal.workflow.executors.base import ExecutionStatus, ExecutorResult
from seeknal.workflow.executors.feature_group_executor import (
    FeatureGroupExecutor,
    _as_datetime,
)


def make_result(status=ExecutionStatus.SUCCESS):
    return ExecutorResult(
        node_id="feature_group.customer_30d",
        status=status,
        metadata={"view_name": "v_customer_30d"},
    )


def make_executor(config, con=None):
    """A stand-in exposing only what _publish_online touches."""
    return SimpleNamespace(
        node=SimpleNamespace(id="feature_group.customer_30d", name="customer_30d", config=config),
        context=SimpleNamespace(
            params={}, get_duckdb_connection=lambda: con or object()
        ),
    )


def publish(executor, result):
    return FeatureGroupExecutor._publish_online(executor, result)


PG_TARGET = {
    "type": "postgresql",
    "connection": "feature_online_pg",
    "table": "feature_online.fg_retail__customer_30d",
    "mode": "upsert_by_key",
    "unique_keys": ["customer_id"],
    "serve_online": True,
}


class TestOptIn:
    def test_no_materializations_is_a_noop(self):
        result = make_result()
        publish(make_executor({}), result)
        assert result.status is ExecutionStatus.SUCCESS
        assert "online_publication" not in result.metadata

    def test_target_without_serve_online_is_a_noop(self):
        entry = dict(PG_TARGET)
        entry.pop("serve_online")
        result = make_result()
        publish(make_executor({"materializations": [entry]}), result)
        assert result.status is ExecutionStatus.SUCCESS
        assert "online_publication" not in result.metadata

    def test_iceberg_only_target_is_a_noop(self):
        result = make_result()
        publish(
            make_executor({"materializations": [{"type": "iceberg", "table": "a.b"}]}),
            result,
        )
        assert result.status is ExecutionStatus.SUCCESS


class TestFailureFailsTheRun:
    """The distinguishing behaviour. An Iceberg failure is a warning; an online
    publication failure is not."""

    def test_invalid_config_fails_the_run(self):
        bad = dict(PG_TARGET, mode="upsert")  # not a real enum value
        result = make_result()
        publish(make_executor({"materializations": [bad]}), result)
        assert result.status is ExecutionStatus.FAILED
        assert "upsert_by_key" in (result.error_message or "")

    def test_missing_unique_keys_fails_the_run(self):
        bad = dict(PG_TARGET, unique_keys=[])
        result = make_result()
        publish(make_executor({"materializations": [bad]}), result)
        assert result.status is ExecutionStatus.FAILED

    def test_publication_error_fails_the_run(self, monkeypatch):
        """A profile that cannot be loaded stands in for any publication-time
        failure; the run must not be reported as successful."""
        import seeknal.workflow.materialization.profile_loader as pl

        class Boom:
            def load_connection_profile(self, name):
                raise RuntimeError("no such connection profile")

        monkeypatch.setattr(pl, "ProfileLoader", Boom)

        result = make_result()
        publish(make_executor({"materializations": [PG_TARGET]}), result)
        assert result.status is ExecutionStatus.FAILED
        assert "online publication" in (result.error_message or "").lower()
        assert result.metadata["online_publication"]["success"] is False
        assert result.metadata["online_publication"]["table"] == PG_TARGET["table"]

    def test_failure_is_not_silently_downgraded_to_metadata(self, monkeypatch):
        """Regression guard: recording the error in metadata while leaving the
        status SUCCESS is exactly the pattern this replaces."""
        import seeknal.workflow.materialization.profile_loader as pl

        class Boom:
            def load_connection_profile(self, name):
                raise RuntimeError("nope")

        monkeypatch.setattr(pl, "ProfileLoader", Boom)
        result = make_result()
        publish(make_executor({"materializations": [PG_TARGET]}), result)
        assert result.status is not ExecutionStatus.SUCCESS


class TestIntervalCoercion:
    def test_datetime_passes_through(self):
        dt = datetime(2026, 7, 27, 12, 0)
        assert _as_datetime(dt, datetime(2000, 1, 1)) is dt

    def test_iso_string_is_parsed(self):
        assert _as_datetime("2026-07-27T12:00:00", datetime(2000, 1, 1)) == datetime(
            2026, 7, 27, 12, 0
        )

    def test_trailing_z_is_handled(self):
        parsed = _as_datetime("2026-07-27T12:00:00Z", datetime(2000, 1, 1))
        assert parsed.year == 2026 and parsed.tzinfo is not None

    def test_absent_value_falls_back(self):
        default = datetime(2000, 1, 1)
        assert _as_datetime(None, default) is default
        assert _as_datetime("", default) is default

    def test_unparseable_value_falls_back_rather_than_raising(self):
        """A malformed interval must not abort a run whose features are already
        computed."""
        default = datetime(2000, 1, 1)
        assert _as_datetime("not-a-date", default) is default
