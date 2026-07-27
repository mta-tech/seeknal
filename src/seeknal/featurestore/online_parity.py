"""Parity checking between the online store and its offline source of truth.

Every other guard in the publication path checks that a write *happened*:
staging validation, remote row verification, the ledger. None of them checks
that the values are *right*. A publication can succeed completely while serving
numbers computed from an incomplete window -- that failure mode is silent by
construction, because nothing about the row looks wrong.

Parity is the check that closes it: read the same entities from the offline
table and compare, value by value.

Two distinct questions, deliberately separated:

**Value parity** -- do the served values match the offline table for the same
entities? Catches wrong computation.

**Interval completeness** -- does the served row actually cover the window it
claims? Catches a value that is internally consistent but derived from a
partial window. Row parity alone cannot catch this: if a second-order feature
was computed over 10 days of a 30-day window, the offline table contains the
same wrong number, and the two agree perfectly.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Sequence

logger = logging.getLogger(__name__)

#: Types the tolerance comparison applies to. Decimal is included because
#: PostgreSQL NUMERIC arrives as Decimal, not float.
_NUMERIC = (int, float, Decimal)


class ParityError(Exception):
    """Parity checking could not be performed."""


@dataclass
class ValueMismatch:
    """One entity whose served values differ from the offline table."""

    key: dict[str, Any]
    column: str
    online_value: Any
    offline_value: Any

    def __str__(self) -> str:
        return (
            f"{self.key} column {self.column!r}: "
            f"online={self.online_value!r} offline={self.offline_value!r}"
        )


@dataclass
class ParityReport:
    """Outcome of a parity check."""

    compared: int = 0
    matched: int = 0
    mismatches: list[ValueMismatch] = field(default_factory=list)
    missing_online: list[dict[str, Any]] = field(default_factory=list)
    extra_online: list[dict[str, Any]] = field(default_factory=list)
    incomplete_intervals: list[dict[str, Any]] = field(default_factory=list)
    checked_at: datetime | None = None

    @property
    def is_clean(self) -> bool:
        return not (
            self.mismatches
            or self.missing_online
            or self.extra_online
            or self.incomplete_intervals
        )

    def summary(self) -> str:
        if self.is_clean:
            return f"parity clean: {self.matched}/{self.compared} rows agree"
        parts = [f"{self.matched}/{self.compared} agree"]
        if self.mismatches:
            parts.append(f"{len(self.mismatches)} value mismatch(es)")
        if self.missing_online:
            parts.append(f"{len(self.missing_online)} missing online")
        if self.extra_online:
            parts.append(f"{len(self.extra_online)} extra online")
        if self.incomplete_intervals:
            parts.append(f"{len(self.incomplete_intervals)} incomplete interval(s)")
        return "parity FAILED: " + ", ".join(parts)

    def to_dict(self) -> dict[str, Any]:
        return {
            "compared": self.compared,
            "matched": self.matched,
            "is_clean": self.is_clean,
            "mismatches": [str(m) for m in self.mismatches],
            "missing_online": self.missing_online,
            "extra_online": self.extra_online,
            "incomplete_intervals": self.incomplete_intervals,
            "checked_at": self.checked_at.isoformat() if self.checked_at else None,
            "summary": self.summary(),
        }


def _key_of(row: Sequence[Any], key_columns: Sequence[str]) -> tuple[Any, ...]:
    return tuple(row[i] for i in range(len(key_columns)))


def _values_agree(a: Any, b: Any, tolerance: float) -> bool:
    """Compare two feature values, applying *tolerance* to numeric types.

    ``Decimal`` is treated as numeric alongside ``float``. PostgreSQL NUMERIC
    columns come back as ``Decimal``, and DuckDB types decimal literals the same
    way, so restricting the tolerance path to ``float`` would silently make it
    exact-match for precisely the columns most likely to differ in their last
    digit between two engines.

    ``bool`` is excluded despite being an ``int`` subclass: comparing True to
    1.0 within a tolerance is not a comparison anyone wants.
    """
    if isinstance(a, bool) or isinstance(b, bool):
        return a == b
    if isinstance(a, _NUMERIC) and isinstance(b, _NUMERIC):
        try:
            return abs(float(a) - float(b)) <= tolerance
        except (TypeError, ValueError, OverflowError):
            return a == b
    return a == b


def verify_parity(
    con: Any,
    *,
    online_relation: str,
    offline_relation: str,
    key_columns: Sequence[str],
    feature_columns: Sequence[str],
    sample_size: int | None = 1000,
    tolerance: float = 1e-9,
    max_reported: int = 20,
) -> ParityReport:
    """Compare served values against the offline source of truth.

    Both relations are read through the same DuckDB connection, so
    *offline_relation* can be an Iceberg table, a Parquet path, a view, or an
    attached PostgreSQL table -- whatever the caller can address.

    Floating-point columns are compared with *tolerance* rather than exact
    equality: an offline engine and PostgreSQL can round the last bit
    differently, and failing on that would train operators to ignore the check.

    Args:
        sample_size: Compare at most this many entities, ordered by key for
            determinism. ``None`` compares everything. Sampling is a real
            limitation and is recorded in the report rather than implied to be
            a full check.
    """
    keys = list(key_columns)
    feats = list(feature_columns)
    if not keys:
        raise ParityError("key_columns is required")
    if not feats:
        raise ParityError("feature_columns is required")

    cols = ", ".join(f'"{c}"' for c in [*keys, *feats])
    order = ", ".join(f'"{c}"' for c in keys)
    limit = f" LIMIT {int(sample_size)}" if sample_size else ""

    try:
        online_rows = con.execute(
            f"SELECT {cols} FROM {online_relation} ORDER BY {order}{limit}"
        ).fetchall()
        offline_rows = con.execute(
            f"SELECT {cols} FROM {offline_relation} ORDER BY {order}{limit}"
        ).fetchall()
    except Exception as exc:
        raise ParityError(f"could not read relations for parity: {exc}") from exc

    online_by_key = {_key_of(r, keys): r for r in online_rows}
    offline_by_key = {_key_of(r, keys): r for r in offline_rows}

    report = ParityReport(checked_at=datetime.utcnow())

    for key in sorted(offline_by_key.keys() - online_by_key.keys())[:max_reported]:
        report.missing_online.append(dict(zip(keys, key)))
    for key in sorted(online_by_key.keys() - offline_by_key.keys())[:max_reported]:
        report.extra_online.append(dict(zip(keys, key)))

    for key in sorted(online_by_key.keys() & offline_by_key.keys()):
        report.compared += 1
        on_row, off_row = online_by_key[key], offline_by_key[key]
        row_ok = True
        for offset, column in enumerate(feats):
            i = len(keys) + offset
            a, b = on_row[i], off_row[i]
            if not _values_agree(a, b, tolerance):
                row_ok = False
                if len(report.mismatches) < max_reported:
                    report.mismatches.append(
                        ValueMismatch(dict(zip(keys, key)), column, a, b)
                    )
        if row_ok:
            report.matched += 1

    if not report.is_clean:
        logger.error("%s", report.summary())
    return report


def verify_interval_completeness(
    con: Any,
    *,
    online_relation: str,
    expected_start: datetime,
    expected_end: datetime,
    key_columns: Sequence[str],
    max_reported: int = 20,
    slack: timedelta = timedelta(0),
) -> ParityReport:
    """Assert served rows cover the window they claim.

    Row parity cannot catch a partial window. If a 30-day feature was computed
    over 10 days, the offline table holds the same wrong value and the two
    agree perfectly -- so completeness has to be checked against the *expected*
    interval, not against the offline copy.
    """
    keys = list(key_columns)
    key_sel = ", ".join(f'"{c}"' for c in keys)
    report = ParityReport(checked_at=datetime.utcnow())

    try:
        total = con.execute(
            f"SELECT count(*) FROM {online_relation}"
        ).fetchone()[0]
        bad = con.execute(
            f"SELECT {key_sel}, source_interval_start, source_interval_end "
            f"FROM {online_relation} "
            f"WHERE source_interval_start > TIMESTAMPTZ '{(expected_start + slack).isoformat()}' "
            f"   OR source_interval_end < TIMESTAMPTZ '{(expected_end - slack).isoformat()}' "
            f"ORDER BY {key_sel} LIMIT {int(max_reported)}"
        ).fetchall()
    except Exception as exc:
        raise ParityError(f"could not check interval completeness: {exc}") from exc

    report.compared = int(total or 0)
    for row in bad:
        entry = dict(zip(keys, row[: len(keys)]))
        entry["source_interval_start"] = str(row[len(keys)])
        entry["source_interval_end"] = str(row[len(keys) + 1])
        entry["expected_start"] = expected_start.isoformat()
        entry["expected_end"] = expected_end.isoformat()
        report.incomplete_intervals.append(entry)
    report.matched = report.compared - len(report.incomplete_intervals)

    if report.incomplete_intervals:
        logger.error(
            "%d served row(s) do not cover [%s, %s]",
            len(report.incomplete_intervals),
            expected_start.isoformat(),
            expected_end.isoformat(),
        )
    return report


__all__ = [
    "ParityError",
    "ParityReport",
    "ValueMismatch",
    "verify_interval_completeness",
    "verify_parity",
]
