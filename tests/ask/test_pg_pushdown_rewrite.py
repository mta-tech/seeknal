"""Tests for ``_rewrite_for_pg_pushdown`` — issue #64.

DuckDB's postgres_scanner does not push EXTRACT(...) past the
``COPY (SELECT ... TO STDOUT)`` boundary, so filters using EXTRACT run
locally after the full table is transferred. The rewrite helper
translates the small, deterministic set of EXTRACT patterns documented
in the consensus plan to half-open date-range form, which
postgres_scanner WILL push down.
"""

from __future__ import annotations

import pytest

from seeknal.ask.agents.tools.execute_sql import _rewrite_for_pg_pushdown


NOTICE_TEXT = "rewrote EXTRACT(...) to a pushdown-safe date range"


def _normalize_whitespace(text: str) -> str:
    return " ".join(text.split())


# ---------------------------------------------------------------------------
# AC-A1 — bare column, equality
# ---------------------------------------------------------------------------


def test_ac_a1_year_equals_bare_column():
    sql = "SELECT * FROM t WHERE EXTRACT(YEAR FROM tanggal) = 2023"
    out, notices = _rewrite_for_pg_pushdown(sql)
    expected = "(tanggal >= '2023-01-01' AND tanggal < '2024-01-01')"
    assert expected in out
    assert "EXTRACT" not in out
    assert NOTICE_TEXT in notices


# ---------------------------------------------------------------------------
# AC-A2 — table-qualified column preserved
# ---------------------------------------------------------------------------


def test_ac_a2_year_equals_table_qualified():
    sql = "SELECT * FROM t WHERE EXTRACT(YEAR FROM t.tanggal) = 2023"
    out, notices = _rewrite_for_pg_pushdown(sql)
    expected = "(t.tanggal >= '2023-01-01' AND t.tanggal < '2024-01-01')"
    assert expected in out
    assert NOTICE_TEXT in notices


# ---------------------------------------------------------------------------
# AC-A3 — quoted column preserved on both sides
# ---------------------------------------------------------------------------


def test_ac_a3_year_equals_quoted_column():
    sql = 'SELECT * FROM t WHERE EXTRACT(YEAR FROM "Tanggal Pendaftaran") = 2023'
    out, notices = _rewrite_for_pg_pushdown(sql)
    expected = (
        '("Tanggal Pendaftaran" >= \'2023-01-01\' '
        'AND "Tanggal Pendaftaran" < \'2024-01-01\')'
    )
    assert expected in out
    assert NOTICE_TEXT in notices


# ---------------------------------------------------------------------------
# AC-A4 — inequality (both != and <>)
# ---------------------------------------------------------------------------


def test_ac_a4_year_not_equals_bang_equal():
    sql = "SELECT * FROM t WHERE EXTRACT(YEAR FROM tanggal) != 2023"
    out, notices = _rewrite_for_pg_pushdown(sql)
    expected = "(tanggal < '2023-01-01' OR tanggal >= '2024-01-01')"
    assert expected in out
    assert NOTICE_TEXT in notices


def test_ac_a4_year_not_equals_angle_brackets():
    sql = "SELECT * FROM t WHERE EXTRACT(YEAR FROM tanggal) <> 2023"
    out, notices = _rewrite_for_pg_pushdown(sql)
    expected = "(tanggal < '2023-01-01' OR tanggal >= '2024-01-01')"
    assert expected in out
    assert NOTICE_TEXT in notices


# ---------------------------------------------------------------------------
# AC-A5 — IN list emits OR of ranges
# ---------------------------------------------------------------------------


def test_ac_a5_year_in_list():
    sql = "SELECT * FROM t WHERE EXTRACT(YEAR FROM tanggal) IN (2022, 2023)"
    out, notices = _rewrite_for_pg_pushdown(sql)
    assert "(tanggal >= '2022-01-01' AND tanggal < '2023-01-01')" in out
    assert "(tanggal >= '2023-01-01' AND tanggal < '2024-01-01')" in out
    assert " OR " in out
    assert NOTICE_TEXT in notices


# ---------------------------------------------------------------------------
# AC-A6 — duplicate years deduped
# ---------------------------------------------------------------------------


def test_ac_a6_year_in_list_dedup():
    sql = "SELECT * FROM t WHERE EXTRACT(YEAR FROM tanggal) IN (2023, 2023)"
    out, notices = _rewrite_for_pg_pushdown(sql)
    # Only one range should appear.
    range_count = out.count("(tanggal >= '2023-01-01' AND tanggal < '2024-01-01')")
    assert range_count == 1
    assert NOTICE_TEXT in notices


# ---------------------------------------------------------------------------
# AC-A7 — BETWEEN range
# ---------------------------------------------------------------------------


def test_ac_a7_year_between_range():
    sql = "SELECT * FROM t WHERE EXTRACT(YEAR FROM tanggal) BETWEEN 2022 AND 2024"
    out, notices = _rewrite_for_pg_pushdown(sql)
    expected = "(tanggal >= '2022-01-01' AND tanggal < '2025-01-01')"
    assert expected in out
    assert NOTICE_TEXT in notices


# ---------------------------------------------------------------------------
# AC-A8 — inverted BETWEEN left untouched
# ---------------------------------------------------------------------------


def test_ac_a8_inverted_between_unchanged():
    sql = "SELECT * FROM t WHERE EXTRACT(YEAR FROM tanggal) BETWEEN 2024 AND 2022"
    out, notices = _rewrite_for_pg_pushdown(sql)
    assert _normalize_whitespace(out) == _normalize_whitespace(sql)
    assert notices == []


# ---------------------------------------------------------------------------
# AC-A9 — coupled MONTH+YEAR; both orderings produce same output
# ---------------------------------------------------------------------------


def test_ac_a9_month_year_coupling_month_first():
    sql = (
        "SELECT * FROM t WHERE EXTRACT(MONTH FROM tanggal) = 6 "
        "AND EXTRACT(YEAR FROM tanggal) = 2023"
    )
    out, notices = _rewrite_for_pg_pushdown(sql)
    expected = "(tanggal >= '2023-06-01' AND tanggal < '2023-07-01')"
    assert expected in out
    assert "EXTRACT" not in out
    assert NOTICE_TEXT in notices


def test_ac_a9_month_year_coupling_year_first():
    sql = (
        "SELECT * FROM t WHERE EXTRACT(YEAR FROM tanggal) = 2023 "
        "AND EXTRACT(MONTH FROM tanggal) = 6"
    )
    out, notices = _rewrite_for_pg_pushdown(sql)
    expected = "(tanggal >= '2023-06-01' AND tanggal < '2023-07-01')"
    assert expected in out
    assert "EXTRACT" not in out
    assert NOTICE_TEXT in notices


# ---------------------------------------------------------------------------
# AC-A10 — MONTH=12 rolls over to next year
# ---------------------------------------------------------------------------


def test_ac_a10_month_12_year_rollover():
    sql = (
        "SELECT * FROM t WHERE EXTRACT(MONTH FROM tanggal) = 12 "
        "AND EXTRACT(YEAR FROM tanggal) = 2023"
    )
    out, notices = _rewrite_for_pg_pushdown(sql)
    expected = "(tanggal >= '2023-12-01' AND tanggal < '2024-01-01')"
    assert expected in out
    assert NOTICE_TEXT in notices


# ---------------------------------------------------------------------------
# AC-A11 — coupled QUARTER+YEAR
# ---------------------------------------------------------------------------


def test_ac_a11_quarter_year_q2():
    sql = (
        "SELECT * FROM t WHERE EXTRACT(QUARTER FROM tanggal) = 2 "
        "AND EXTRACT(YEAR FROM tanggal) = 2023"
    )
    out, notices = _rewrite_for_pg_pushdown(sql)
    expected = "(tanggal >= '2023-04-01' AND tanggal < '2023-07-01')"
    assert expected in out
    assert NOTICE_TEXT in notices


def test_ac_a11_quarter_year_q4_rollover():
    sql = (
        "SELECT * FROM t WHERE EXTRACT(QUARTER FROM tanggal) = 4 "
        "AND EXTRACT(YEAR FROM tanggal) = 2023"
    )
    out, notices = _rewrite_for_pg_pushdown(sql)
    expected = "(tanggal >= '2023-10-01' AND tanggal < '2024-01-01')"
    assert expected in out
    assert NOTICE_TEXT in notices


# ---------------------------------------------------------------------------
# AC-A12 — DAY + MONTH + YEAR triple → single-day range
# ---------------------------------------------------------------------------


def test_ac_a12_day_month_year_triple():
    sql = (
        "SELECT * FROM t WHERE EXTRACT(DAY FROM tanggal) = 15 "
        "AND EXTRACT(MONTH FROM tanggal) = 6 "
        "AND EXTRACT(YEAR FROM tanggal) = 2023"
    )
    out, notices = _rewrite_for_pg_pushdown(sql)
    expected = "(tanggal >= '2023-06-15' AND tanggal < '2023-06-16')"
    assert expected in out
    assert "EXTRACT" not in out
    assert NOTICE_TEXT in notices


# ---------------------------------------------------------------------------
# AC-A13 — Feb 29 leap year (valid)
# ---------------------------------------------------------------------------


def test_ac_a13_leap_year_feb_29():
    sql = (
        "SELECT * FROM t WHERE EXTRACT(DAY FROM col) = 29 "
        "AND EXTRACT(MONTH FROM col) = 2 "
        "AND EXTRACT(YEAR FROM col) = 2024"
    )
    out, notices = _rewrite_for_pg_pushdown(sql)
    expected = "(col >= '2024-02-29' AND col < '2024-03-01')"
    assert expected in out
    assert NOTICE_TEXT in notices


# ---------------------------------------------------------------------------
# AC-A14 — Feb 28 non-leap (valid)
# ---------------------------------------------------------------------------


def test_ac_a14_non_leap_feb_28():
    sql = (
        "SELECT * FROM t WHERE EXTRACT(DAY FROM col) = 28 "
        "AND EXTRACT(MONTH FROM col) = 2 "
        "AND EXTRACT(YEAR FROM col) = 2023"
    )
    out, notices = _rewrite_for_pg_pushdown(sql)
    expected = "(col >= '2023-02-28' AND col < '2023-03-01')"
    assert expected in out
    assert NOTICE_TEXT in notices


# ---------------------------------------------------------------------------
# AC-A15 — Feb 29 non-leap (invalid) — leave untouched, no notice
# ---------------------------------------------------------------------------


def test_ac_a15_invalid_date_feb_29_2023_untouched():
    sql = (
        "SELECT * FROM t WHERE EXTRACT(DAY FROM col) = 29 "
        "AND EXTRACT(MONTH FROM col) = 2 "
        "AND EXTRACT(YEAR FROM col) = 2023"
    )
    out, notices = _rewrite_for_pg_pushdown(sql)
    assert _normalize_whitespace(out) == _normalize_whitespace(sql)
    assert notices == []


# ---------------------------------------------------------------------------
# AC-A16 — boundary year 9999
# ---------------------------------------------------------------------------


def test_ac_a16_year_9999():
    sql = "SELECT * FROM t WHERE EXTRACT(YEAR FROM tanggal) = 9999"
    out, notices = _rewrite_for_pg_pushdown(sql)
    expected = "(tanggal >= '9999-01-01' AND tanggal < '10000-01-01')"
    assert expected in out
    assert NOTICE_TEXT in notices


# ---------------------------------------------------------------------------
# AC-A17 — year 0 untouched
# ---------------------------------------------------------------------------


def test_ac_a17_year_zero_untouched():
    sql = "SELECT * FROM t WHERE EXTRACT(YEAR FROM tanggal) = 0"
    out, notices = _rewrite_for_pg_pushdown(sql)
    assert _normalize_whitespace(out) == _normalize_whitespace(sql)
    assert notices == []


# ---------------------------------------------------------------------------
# AC-A18 — negative year untouched
# ---------------------------------------------------------------------------


def test_ac_a18_negative_year_untouched():
    sql = "SELECT * FROM t WHERE EXTRACT(YEAR FROM tanggal) = -1"
    out, notices = _rewrite_for_pg_pushdown(sql)
    assert _normalize_whitespace(out) == _normalize_whitespace(sql)
    assert notices == []


# ---------------------------------------------------------------------------
# AC-A19 — standalone MONTH untouched
# ---------------------------------------------------------------------------


def test_ac_a19_standalone_month_untouched():
    sql = "SELECT * FROM t WHERE EXTRACT(MONTH FROM tanggal) = 6"
    out, notices = _rewrite_for_pg_pushdown(sql)
    assert _normalize_whitespace(out) == _normalize_whitespace(sql)
    assert notices == []


# ---------------------------------------------------------------------------
# AC-A20 — standalone QUARTER untouched
# ---------------------------------------------------------------------------


def test_ac_a20_standalone_quarter_untouched():
    sql = "SELECT * FROM t WHERE EXTRACT(QUARTER FROM tanggal) = 2"
    out, notices = _rewrite_for_pg_pushdown(sql)
    assert _normalize_whitespace(out) == _normalize_whitespace(sql)
    assert notices == []


# ---------------------------------------------------------------------------
# AC-A21 — DATE_PART left untouched (out of scope)
# ---------------------------------------------------------------------------


def test_ac_a21_date_part_untouched():
    sql = "SELECT * FROM t WHERE DATE_PART('year', tanggal) = 2023"
    out, notices = _rewrite_for_pg_pushdown(sql)
    assert _normalize_whitespace(out) == _normalize_whitespace(sql)
    assert notices == []


# ---------------------------------------------------------------------------
# AC-A22 — mixed casing matches
# ---------------------------------------------------------------------------


def test_ac_a22_mixed_casing():
    sql = "SELECT * FROM t WHERE extract(year from tanggal) = 2023"
    out, notices = _rewrite_for_pg_pushdown(sql)
    expected = "(tanggal >= '2023-01-01' AND tanggal < '2024-01-01')"
    assert expected in out
    assert NOTICE_TEXT in notices


# ---------------------------------------------------------------------------
# AC-A23 — whitespace tolerance
# ---------------------------------------------------------------------------


def test_ac_a23_whitespace_tolerance():
    sql = "SELECT * FROM t WHERE EXTRACT (\n  YEAR FROM tanggal\n) = 2023"
    out, notices = _rewrite_for_pg_pushdown(sql)
    expected = "(tanggal >= '2023-01-01' AND tanggal < '2024-01-01')"
    assert expected in out
    assert NOTICE_TEXT in notices


# ---------------------------------------------------------------------------
# AC-A24 — single notice per call regardless of how many rewrites
# ---------------------------------------------------------------------------


def test_ac_a24_single_notice_per_call():
    sql = (
        "SELECT * FROM t WHERE EXTRACT(YEAR FROM a) = 2023 "
        "OR EXTRACT(YEAR FROM b) = 2024 "
        "OR EXTRACT(YEAR FROM c) BETWEEN 2020 AND 2022"
    )
    out, notices = _rewrite_for_pg_pushdown(sql)
    # All three rewrites should have happened
    assert "EXTRACT" not in out
    # But only one notice entry
    assert notices.count(NOTICE_TEXT) == 1
    assert len(notices) == 1


# ---------------------------------------------------------------------------
# AC-A25 — no-match SQL is returned unchanged
# ---------------------------------------------------------------------------


def test_ac_a25_no_match_unchanged():
    sql = "SELECT * FROM t WHERE id = 1 AND name ILIKE '%foo%'"
    out, notices = _rewrite_for_pg_pushdown(sql)
    assert out == sql
    assert notices == []


# ---------------------------------------------------------------------------
# AC-A26 — string-literal canary
# ---------------------------------------------------------------------------


def test_ac_a26_string_literal_canary():
    sql = "SELECT 'EXTRACT(YEAR FROM x) = 2023' AS s"
    out, notices = _rewrite_for_pg_pushdown(sql)
    assert out == sql
    assert notices == []


# ---------------------------------------------------------------------------
# AC-A27 — line-comment canary
# ---------------------------------------------------------------------------


def test_ac_a27_line_comment_canary():
    sql = "SELECT 1 -- EXTRACT(YEAR FROM x) = 2023"
    out, notices = _rewrite_for_pg_pushdown(sql)
    assert out == sql
    assert notices == []
    # The comment text remains intact.
    assert "EXTRACT(YEAR FROM x) = 2023" in out


# ---------------------------------------------------------------------------
# AC-A28 — block-comment canary
# ---------------------------------------------------------------------------


def test_ac_a28_block_comment_canary():
    sql = "SELECT 1 /* EXTRACT(YEAR FROM x) = 2023 */"
    out, notices = _rewrite_for_pg_pushdown(sql)
    assert out == sql
    assert notices == []
    assert "EXTRACT(YEAR FROM x) = 2023" in out


# ---------------------------------------------------------------------------
# AC-A29 — EXTRACT inside CTE projection alias → unchanged
# ---------------------------------------------------------------------------


def test_ac_a29_cte_projection_alias_untouched():
    sql = "SELECT EXTRACT(YEAR FROM t) AS y FROM tbl WHERE y = 2023"
    out, _notices = _rewrite_for_pg_pushdown(sql)
    # The WHERE clause does not contain EXTRACT, so no rewrite should fire
    # for that clause. The projection-side EXTRACT must remain untouched.
    assert "EXTRACT(YEAR FROM t) AS y" in out
    # The bare ``y = 2023`` must not be turned into a date range.
    assert "(y >= '2023-01-01' AND y < '2024-01-01')" not in out


# ---------------------------------------------------------------------------
# AC-A30 — parameterized corpus (≥30 EXTRACT-shape variants)
# ---------------------------------------------------------------------------


_PARAMETERIZED_CASES = [
    # 1: bare column, equality
    (
        "SELECT * FROM t WHERE EXTRACT(YEAR FROM d) = 2020",
        "(d >= '2020-01-01' AND d < '2021-01-01')",
    ),
    # 2: leading newline
    (
        "\nSELECT * FROM t WHERE EXTRACT(YEAR FROM d) = 2020",
        "(d >= '2020-01-01' AND d < '2021-01-01')",
    ),
    # 3: tabs
    (
        "SELECT * FROM t WHERE EXTRACT(YEAR\tFROM\td)\t=\t2020",
        "(d >= '2020-01-01' AND d < '2021-01-01')",
    ),
    # 4: lowercase extract/year
    (
        "SELECT * FROM t WHERE extract(year from d) = 2020",
        "(d >= '2020-01-01' AND d < '2021-01-01')",
    ),
    # 5: mixed case
    (
        "SELECT * FROM t WHERE Extract(Year From d) = 2020",
        "(d >= '2020-01-01' AND d < '2021-01-01')",
    ),
    # 6: spaces between EXTRACT and paren
    (
        "SELECT * FROM t WHERE EXTRACT  (YEAR FROM d) = 2020",
        "(d >= '2020-01-01' AND d < '2021-01-01')",
    ),
    # 7: table-qualified
    (
        "SELECT * FROM t WHERE EXTRACT(YEAR FROM t.d) = 2020",
        "(t.d >= '2020-01-01' AND t.d < '2021-01-01')",
    ),
    # 8: schema.table.col qualifier (3-part)
    (
        "SELECT * FROM s.t WHERE EXTRACT(YEAR FROM s.t.d) = 2020",
        "(s.t.d >= '2020-01-01' AND s.t.d < '2021-01-01')",
    ),
    # 9: quoted column
    (
        'SELECT * FROM t WHERE EXTRACT(YEAR FROM "Tanggal") = 2020',
        '("Tanggal" >= \'2020-01-01\' AND "Tanggal" < \'2021-01-01\')',
    ),
    # 10: quoted table + column
    (
        'SELECT * FROM t WHERE EXTRACT(YEAR FROM "t"."D") = 2020',
        '("t"."D" >= \'2020-01-01\' AND "t"."D" < \'2021-01-01\')',
    ),
    # 11: != inequality
    (
        "SELECT * FROM t WHERE EXTRACT(YEAR FROM d) != 2020",
        "(d < '2020-01-01' OR d >= '2021-01-01')",
    ),
    # 12: <> inequality
    (
        "SELECT * FROM t WHERE EXTRACT(YEAR FROM d) <> 2020",
        "(d < '2020-01-01' OR d >= '2021-01-01')",
    ),
    # 13: IN list (single)
    (
        "SELECT * FROM t WHERE EXTRACT(YEAR FROM d) IN (2020)",
        "(d >= '2020-01-01' AND d < '2021-01-01')",
    ),
    # 14: IN list multi
    (
        "SELECT * FROM t WHERE EXTRACT(YEAR FROM d) IN (2020, 2021)",
        "(d >= '2020-01-01' AND d < '2021-01-01')",
    ),
    # 15: BETWEEN range
    (
        "SELECT * FROM t WHERE EXTRACT(YEAR FROM d) BETWEEN 2020 AND 2021",
        "(d >= '2020-01-01' AND d < '2022-01-01')",
    ),
    # 16: MONTH+YEAR coupling
    (
        "SELECT * FROM t WHERE EXTRACT(MONTH FROM d) = 3 AND EXTRACT(YEAR FROM d) = 2020",
        "(d >= '2020-03-01' AND d < '2020-04-01')",
    ),
    # 17: YEAR+MONTH coupling (other order)
    (
        "SELECT * FROM t WHERE EXTRACT(YEAR FROM d) = 2020 AND EXTRACT(MONTH FROM d) = 3",
        "(d >= '2020-03-01' AND d < '2020-04-01')",
    ),
    # 18: QUARTER+YEAR coupling
    (
        "SELECT * FROM t WHERE EXTRACT(QUARTER FROM d) = 1 AND EXTRACT(YEAR FROM d) = 2020",
        "(d >= '2020-01-01' AND d < '2020-04-01')",
    ),
    # 19: DAY+MONTH+YEAR coupling
    (
        "SELECT * FROM t WHERE EXTRACT(DAY FROM d) = 1 AND EXTRACT(MONTH FROM d) = 6 AND EXTRACT(YEAR FROM d) = 2020",
        "(d >= '2020-06-01' AND d < '2020-06-02')",
    ),
    # 20: block-comment near EXTRACT
    (
        "SELECT * FROM t /* foo */ WHERE EXTRACT(YEAR FROM d) = 2020",
        "(d >= '2020-01-01' AND d < '2021-01-01')",
    ),
    # 21: line-comment after WHERE
    (
        "SELECT * FROM t WHERE EXTRACT(YEAR FROM d) = 2020 -- ok",
        "(d >= '2020-01-01' AND d < '2021-01-01')",
    ),
    # 22: leading whitespace + line comment between clauses
    (
        "SELECT *\n  FROM t\n WHERE EXTRACT(YEAR FROM d) = 2020\n  -- trailing",
        "(d >= '2020-01-01' AND d < '2021-01-01')",
    ),
    # 23: extra parens inside FROM
    (
        "SELECT * FROM t WHERE EXTRACT(YEAR FROM (d)) = 2020",
        # The inner parens make this not match the canonical shape; leave it
        # untouched to be conservative.
        None,
    ),
    # 24: BETWEEN reversed
    (
        "SELECT * FROM t WHERE EXTRACT(YEAR FROM d) BETWEEN 2030 AND 2020",
        None,
    ),
    # 25: year-9999 edge
    (
        "SELECT * FROM t WHERE EXTRACT(YEAR FROM d) = 9999",
        "(d >= '9999-01-01' AND d < '10000-01-01')",
    ),
    # 26: year=1 edge
    (
        "SELECT * FROM t WHERE EXTRACT(YEAR FROM d) = 1",
        "(d >= '1-01-01' AND d < '2-01-01')",
    ),
    # 27: month=12 rollover
    (
        "SELECT * FROM t WHERE EXTRACT(MONTH FROM d) = 12 AND EXTRACT(YEAR FROM d) = 2020",
        "(d >= '2020-12-01' AND d < '2021-01-01')",
    ),
    # 28: lowercase BETWEEN
    (
        "SELECT * FROM t WHERE EXTRACT(YEAR FROM d) between 2020 and 2021",
        "(d >= '2020-01-01' AND d < '2022-01-01')",
    ),
    # 29: lowercase IN
    (
        "SELECT * FROM t WHERE EXTRACT(YEAR FROM d) in (2020, 2021)",
        "(d >= '2020-01-01' AND d < '2021-01-01')",
    ),
    # 30: multi-space inside EXTRACT
    (
        "SELECT * FROM t WHERE EXTRACT(YEAR   FROM   d) = 2020",
        "(d >= '2020-01-01' AND d < '2021-01-01')",
    ),
    # 31: combined IN dedup
    (
        "SELECT * FROM t WHERE EXTRACT(YEAR FROM d) IN (2020, 2020, 2021)",
        "(d >= '2020-01-01' AND d < '2021-01-01')",
    ),
    # 32: trailing newline
    (
        "SELECT * FROM t WHERE EXTRACT(YEAR FROM d) = 2020\n",
        "(d >= '2020-01-01' AND d < '2021-01-01')",
    ),
]


@pytest.mark.parametrize(
    "input_sql,expected_substring",
    _PARAMETERIZED_CASES,
)
def test_ac_a30_parameterized_corpus(input_sql, expected_substring):
    out, notices = _rewrite_for_pg_pushdown(input_sql)
    if expected_substring is None:
        # Should be left untouched.
        assert _normalize_whitespace(out) == _normalize_whitespace(input_sql)
        assert notices == []
    else:
        assert expected_substring in out, (
            f"expected {expected_substring!r} in output {out!r}"
        )
        assert NOTICE_TEXT in notices


# ---------------------------------------------------------------------------
# AC-A31 — composite ILIKE + EXTRACT — both rewrites applied via the wrapper
# ---------------------------------------------------------------------------


def test_ac_a31_composite_ilike_and_extract_both_notices():
    from seeknal.ask.agents.tools.execute_sql import (
        _repair_common_sql_before_execution,
    )

    sql = (
        "SELECT * FROM t WHERE ILIKE(name, '%foo%') "
        "AND EXTRACT(YEAR FROM d) = 2023"
    )
    repaired, notices = _repair_common_sql_before_execution(sql)
    assert "name ILIKE '%foo%'" in repaired
    assert "(d >= '2023-01-01' AND d < '2024-01-01')" in repaired
    # Two notices, EXTRACT first, then ILIKE (EXTRACT runs first).
    assert len(notices) == 2
    assert notices[0] == NOTICE_TEXT
    assert "ILIKE" in notices[1]
