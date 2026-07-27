"""Tests for parity and interval-completeness checking.

The central claim under test is that these are two different checks, and that
neither substitutes for the other. Row parity catches wrong computation;
interval completeness catches a value that is internally consistent but derived
from a partial window. Parity is blind to the second, because the offline table
contains the same wrong number.
"""

from datetime import datetime, timedelta, timezone

import duckdb
import pytest

from seeknal.featurestore.online_parity import (
    ParityError,
    ParityReport,
    ValueMismatch,
    verify_interval_completeness,
    verify_parity,
)

START = datetime(2026, 6, 27, tzinfo=timezone.utc)
END = datetime(2026, 7, 27, tzinfo=timezone.utc)


@pytest.fixture
def con():
    c = duckdb.connect()
    yield c
    c.close()


def make_relations(con, online_rows, offline_rows):
    def values(rows):
        return ", ".join(
            f"({i}, {orders}, {spend})" for i, orders, spend in rows
        )

    con.execute(
        f"CREATE OR REPLACE VIEW online_t AS SELECT * FROM (VALUES {values(online_rows)}) "
        "AS t(customer_id, orders_30d, spend_30d)"
    )
    con.execute(
        f"CREATE OR REPLACE VIEW offline_t AS SELECT * FROM (VALUES {values(offline_rows)}) "
        "AS t(customer_id, orders_30d, spend_30d)"
    )


def check(con, **kw):
    params = dict(
        online_relation="online_t",
        offline_relation="offline_t",
        key_columns=["customer_id"],
        feature_columns=["orders_30d", "spend_30d"],
        sample_size=None,
    )
    params.update(kw)
    return verify_parity(con, **params)


class TestValueParity:
    def test_identical_relations_are_clean(self, con):
        rows = [(1, 10, 1.5), (2, 20, 2.5)]
        make_relations(con, rows, rows)
        report = check(con)
        assert report.is_clean
        assert report.matched == report.compared == 2

    def test_detects_a_changed_value(self, con):
        make_relations(con, [(1, 10, 1.5)], [(1, 99, 1.5)])
        report = check(con)
        assert not report.is_clean
        assert report.mismatches[0].column == "orders_30d"

    def test_mismatch_reports_both_sides(self, con):
        make_relations(con, [(1, 10, 1.5)], [(1, 99, 1.5)])
        m = check(con).mismatches[0]
        assert m.online_value == 10 and m.offline_value == 99

    def test_row_present_offline_but_not_online(self, con):
        make_relations(con, [(1, 10, 1.5)], [(1, 10, 1.5), (2, 20, 2.5)])
        report = check(con)
        assert report.missing_online == [{"customer_id": 2}]

    def test_row_present_online_but_not_offline(self, con):
        make_relations(con, [(1, 10, 1.5), (3, 30, 3.5)], [(1, 10, 1.5)])
        report = check(con)
        assert report.extra_online == [{"customer_id": 3}]

    def test_float_rounding_within_tolerance_is_not_a_mismatch(self, con):
        """An offline engine and PostgreSQL can round the last bit differently;
        failing on that would train operators to ignore the check."""
        make_relations(con, [(1, 10, 1.0000000001)], [(1, 10, 1.0)])
        assert check(con).is_clean

    def test_float_difference_beyond_tolerance_is_a_mismatch(self, con):
        make_relations(con, [(1, 10, 1.5)], [(1, 10, 2.5)])
        assert not check(con).is_clean

    def test_reported_mismatches_are_capped(self, con):
        rows_on = [(i, i, float(i)) for i in range(1, 60)]
        rows_off = [(i, i + 1, float(i)) for i in range(1, 60)]
        make_relations(con, rows_on, rows_off)
        report = check(con, max_reported=5)
        assert len(report.mismatches) == 5
        assert report.compared == 59  # all compared, only the report is capped

    def test_requires_key_columns(self, con):
        make_relations(con, [(1, 10, 1.5)], [(1, 10, 1.5)])
        with pytest.raises(ParityError, match="key_columns"):
            check(con, key_columns=[])

    def test_requires_feature_columns(self, con):
        make_relations(con, [(1, 10, 1.5)], [(1, 10, 1.5)])
        with pytest.raises(ParityError, match="feature_columns"):
            check(con, feature_columns=[])

    def test_unreadable_relation_raises(self, con):
        with pytest.raises(ParityError, match="could not read"):
            check(con, online_relation="does_not_exist")


class TestIntervalCompleteness:
    """The check parity cannot perform."""

    def _relation(self, con, rows):
        vals = ", ".join(
            f"({cid}, TIMESTAMPTZ '{s.isoformat()}', TIMESTAMPTZ '{e.isoformat()}')"
            for cid, s, e in rows
        )
        con.execute(
            f"CREATE OR REPLACE VIEW served AS SELECT * FROM (VALUES {vals}) "
            "AS t(customer_id, source_interval_start, source_interval_end)"
        )

    def test_full_coverage_is_clean(self, con):
        self._relation(con, [(1, START, END), (2, START, END)])
        report = verify_interval_completeness(
            con, online_relation="served", expected_start=START,
            expected_end=END, key_columns=["customer_id"],
        )
        assert report.is_clean

    def test_wider_coverage_is_also_fine(self, con):
        """Covering more than asked for is not a defect."""
        self._relation(con, [(1, START - timedelta(days=5), END + timedelta(days=1))])
        report = verify_interval_completeness(
            con, online_relation="served", expected_start=START,
            expected_end=END, key_columns=["customer_id"],
        )
        assert report.is_clean

    def test_partial_window_is_caught(self, con):
        """A 30-day feature computed over 10 days. Parity cannot see this,
        because the offline table holds the same wrong number."""
        self._relation(con, [(1, END - timedelta(days=10), END)])
        report = verify_interval_completeness(
            con, online_relation="served", expected_start=START,
            expected_end=END, key_columns=["customer_id"],
        )
        assert not report.is_clean
        assert report.incomplete_intervals[0]["customer_id"] == 1

    def test_truncated_end_is_caught(self, con):
        self._relation(con, [(1, START, END - timedelta(days=3))])
        report = verify_interval_completeness(
            con, online_relation="served", expected_start=START,
            expected_end=END, key_columns=["customer_id"],
        )
        assert not report.is_clean

    def test_only_offending_rows_are_reported(self, con):
        self._relation(
            con, [(1, START, END), (2, END - timedelta(days=5), END), (3, START, END)]
        )
        report = verify_interval_completeness(
            con, online_relation="served", expected_start=START,
            expected_end=END, key_columns=["customer_id"],
        )
        assert len(report.incomplete_intervals) == 1
        assert report.matched == 2

    def test_slack_absorbs_small_boundary_drift(self, con):
        self._relation(con, [(1, START + timedelta(minutes=5), END)])
        report = verify_interval_completeness(
            con, online_relation="served", expected_start=START, expected_end=END,
            key_columns=["customer_id"], slack=timedelta(hours=1),
        )
        assert report.is_clean


class TestReport:
    def test_clean_summary_states_the_counts(self):
        r = ParityReport(compared=10, matched=10)
        assert "10/10" in r.summary() and "clean" in r.summary()

    def test_failed_summary_names_each_problem_class(self):
        r = ParityReport(
            compared=10,
            matched=7,
            mismatches=[ValueMismatch({"id": 1}, "c", 1, 2)],
            missing_online=[{"id": 2}],
            incomplete_intervals=[{"id": 3}],
        )
        s = r.summary()
        assert "FAILED" in s
        assert "mismatch" in s and "missing online" in s and "incomplete" in s

    def test_serialises_for_logging(self):
        payload = ParityReport(compared=2, matched=2).to_dict()
        assert payload["is_clean"] is True and payload["compared"] == 2
