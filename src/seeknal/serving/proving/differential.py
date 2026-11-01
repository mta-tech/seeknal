"""Differential engine query execution harness for Seeknal Serving.

Compares query execution results across disparate database engines (e.g. StarRocks,
Postgres, DuckDB) via DB-API and reports row counts, match status, and differences.
"""

from __future__ import annotations

import datetime
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class DiffCase:
    """A test case for differential query evaluation."""

    name: str
    sql: str
    params: Sequence = ()
    ordered: bool = False


@dataclass(frozen=True)
class DiffResult:
    """Result of running a test case across multiple engines."""

    name: str
    row_counts: dict[str, int]
    matched: bool
    first_difference: str | None = None


def default_normalise(val: Any) -> Any:
    """Normalize database values into canonical Python representations.

    - Decimal: string with fixed scale (2 decimal places)
    - datetime: naive UTC ISO-8601 string
    - date/time: ISO-8601 string
    - float: rounded to 9 significant digits
    - bytes: lowercase hex string
    """
    if val is None:
        return None
    if isinstance(val, Decimal):
        return f"{val:.2f}"
    if isinstance(val, datetime.datetime):
        if val.tzinfo is not None:
            val = val.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return val.isoformat()
    if isinstance(val, (datetime.date, datetime.time)):
        return val.isoformat()
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return None
        return float(f"{val:.9g}")
    if isinstance(val, (bytes, bytearray)):
        return val.hex()
    return val


def _safe_sort_key(row: tuple) -> tuple:
    """Construct a sort key tuple that safely handles None and disparate types."""
    def _cell_key(c: Any) -> tuple:
        if c is None:
            return (0, "")
        return (1, str(c))
    return tuple(_cell_key(c) for c in row)


def _coerce_case(case: Any) -> DiffCase:
    if isinstance(case, DiffCase):
        return case
    if isinstance(case, (tuple, list)):
        if len(case) == 2:
            return DiffCase(name=str(case[0]), sql=str(case[1]))
        if len(case) == 3:
            return DiffCase(name=str(case[0]), sql=str(case[1]), params=tuple(case[2]))
        if len(case) >= 4:
            return DiffCase(
                name=str(case[0]),
                sql=str(case[1]),
                params=tuple(case[2]),
                ordered=bool(case[3]),
            )
    name = getattr(case, "name", "unknown")
    sql = getattr(case, "sql", "")
    params = getattr(case, "params", ())
    ordered = getattr(case, "ordered", False)
    return DiffCase(name=name, sql=sql, params=tuple(params), ordered=ordered)


def run_differential(
    cases: Sequence[DiffCase | tuple],
    engines: Mapping[str, Callable[[], Any]],
    *,
    normalise: Callable[[Any], Any] = default_normalise,
) -> list[DiffResult]:
    """Execute test cases on each engine via DB-API and compare results."""
    results: list[DiffResult] = []
    engine_names = list(engines.keys())
    active_conns = {eng_name: connect_fn() for eng_name, connect_fn in engines.items()}

    try:
        for raw_case in cases:
            case = _coerce_case(raw_case)
            engine_rows: dict[str, list[tuple]] = {}
            row_counts: dict[str, int] = {}

            for eng_name, conn in active_conns.items():
                cur = conn.cursor()
                try:
                    cur.execute(case.sql, tuple(case.params))
                    raw_rows = cur.fetchall() if cur.description else []
                    norm_rows = [tuple(normalise(c) for c in r) for r in raw_rows]
                    if not case.ordered:
                        norm_rows.sort(key=_safe_sort_key)
                    engine_rows[eng_name] = norm_rows
                    row_counts[eng_name] = len(norm_rows)
                finally:
                    try:
                        cur.close()
                    except Exception:
                        pass

            # Evaluate differential outcome across engines
            matched = True
            first_diff: str | None = None

            if len(set(row_counts.values())) > 1:
                matched = False
                first_diff = f"Row count mismatch: {row_counts}"
            elif len(engine_names) >= 2:
                ref_eng = engine_names[0]
                ref_rows = engine_rows[ref_eng]
                for other_eng in engine_names[1:]:
                    other_rows = engine_rows[other_eng]
                    for idx, (r1, r2) in enumerate(zip(ref_rows, other_rows)):
                        if r1 != r2:
                            matched = False
                            first_diff = (
                                f"Engines '{ref_eng}' and '{other_eng}' differ at row {idx}: "
                                f"{r1} != {r2}"
                            )
                            break
                    if not matched:
                        break

            results.append(
                DiffResult(
                    name=case.name,
                    row_counts=row_counts,
                    matched=matched,
                    first_difference=first_diff,
                )
            )
    finally:
        for conn in active_conns.values():
            try:
                conn.close()
            except Exception:
                pass

    return results


def setup_synthetic_claims_table(conn: Any) -> None:
    """Create and seed the synthetic claims table on a DB-API connection."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS claims (
                claim_id BIGINT,
                claim_no VARCHAR(32),
                amount NUMERIC(10, 2),
                status VARCHAR(16),
                province VARCHAR(16),
                created_at TIMESTAMP,
                admit_date DATE
            )
            """
        )
        cur.execute("DELETE FROM claims")
        cur.execute(
            """
            INSERT INTO claims VALUES
            (1, 'CLM-001', 100.50, 'APPROVED', 'JB', '2026-01-01 10:00:00', '2026-01-01'),
            (2, 'clm-002', NULL, 'PENDING', 'JT', '2026-01-02 11:30:00', '2026-01-02'),
            (3, 'CLM-003', 250.00, NULL, 'DKI', '2026-01-03 14:00:00', '2026-01-03'),
            (4, 'A', 50.25, 'APPROVED', 'JB', '2026-01-04 09:00:00', '2026-01-04'),
            (5, 'b', 75.00, 'REJECTED', 'JT', '2026-01-05 16:00:00', '2026-01-05'),
            (9223372036854775807, 'MAX', 999.99, 'APPROVED', 'DKI', '2026-01-06 12:00:00', '2026-01-06'),
            (-9223372036854775807, 'MIN', 0.00, 'PENDING', 'JB', '2026-01-07 08:00:00', '2026-01-07')
            """
        )
        if hasattr(conn, "commit"):
            try:
                conn.commit()
            except Exception:
                pass
    finally:
        cur.close()


def default_cases() -> list[DiffCase]:
    """Return differential test cases covering documented StarRocks-vs-Postgres pitfalls."""
    return [
        # 1. NULL ordering
        DiffCase(
            name="null_ordering",
            sql="SELECT claim_id, amount FROM claims ORDER BY amount ASC NULLS LAST, claim_id ASC",
            ordered=True,
        ),
        # 2. Text range and ORDER BY collation
        DiffCase(
            name="text_range_and_collation",
            sql="SELECT claim_id, claim_no FROM claims WHERE claim_no >= 'A' AND claim_no <= 'Z' ORDER BY claim_id",
            ordered=True,
        ),
        # 3. Typed literal casts (numeric(10,2), varchar(n))
        DiffCase(
            name="typed_literal_casts",
            sql="SELECT claim_id, CAST(amount AS NUMERIC(10,2)), CAST(claim_no AS VARCHAR(10)) FROM claims ORDER BY claim_id",
            ordered=True,
        ),
        # 4. Date and timestamp time-zone handling
        DiffCase(
            name="date_timestamp_tz",
            sql="SELECT claim_id, created_at, admit_date FROM claims ORDER BY claim_id",
            ordered=True,
        ),
        # 5. Alias case
        DiffCase(
            name="alias_case",
            sql="SELECT claim_id AS ClaimId, amount AS TotalAmount FROM claims ORDER BY claim_id",
            ordered=True,
        ),
        # 6. Table-qualified ORDER BY vs output alias
        DiffCase(
            name="table_qualified_order_by",
            sql="SELECT claims.claim_id AS id, amount FROM claims ORDER BY claims.claim_id",
            ordered=True,
        ),
        # 7. SELECT * column order
        DiffCase(
            name="select_star_column_order",
            sql="SELECT * FROM claims ORDER BY claim_id",
            ordered=True,
        ),
        # 8. int64 boundary values
        DiffCase(
            name="int64_boundary_values",
            sql="SELECT claim_id, claim_no FROM claims WHERE claim_id > 9000000000000000000 OR claim_id < -9000000000000000000 ORDER BY claim_id",
            ordered=True,
        ),
    ]
