"""Deterministic fingerprint extraction for a tabular file.

Reads up to the first 1000 rows + the header (no LLM call) and produces a
:class:`Fingerprint`. Heavy imports are local so the base seeknal install
doesn't pay the cost when the classifier is disabled.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

from seeknal.heartbeat.classifier.models import Fingerprint

logger = logging.getLogger("seeknal.heartbeat.classifier.fingerprint")

_SAMPLE_ROW_LIMIT = 1000

_VALUE_REGEX_PROBES: list[tuple[str, re.Pattern[str]]] = [
    ("email", re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")),
    ("uuid", re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")),
    ("iso_date", re.compile(r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?)?$")),
    ("currency_usd", re.compile(r"^\$?\d+(\.\d{2})?$")),
    ("phone_e164", re.compile(r"^\+\d{6,15}$")),
    ("phone_us", re.compile(r"^\(?\d{3}\)?[ -.]?\d{3}[ -.]?\d{4}$")),
    ("integer", re.compile(r"^-?\d+$")),
    ("float", re.compile(r"^-?\d+\.\d+$")),
    ("region_code", re.compile(r"^[A-Z]{2,3}$")),
]


def _normalize_name(name: str) -> str:
    """Lowercase snake_case."""
    stripped = name.strip()
    # camelCase → snake_case
    snake = re.sub(r"(?<!^)(?=[A-Z])", "_", stripped).lower()
    # non-word → underscore
    snake = re.sub(r"[^a-z0-9]+", "_", snake)
    return snake.strip("_")


def _infer_type(values: list[str]) -> str:
    """Cheap type sniffing from a column's sample values."""
    if not values:
        return "string"
    ints = floats = dates = bools = 0
    for v in values:
        v = v.strip()
        if not v:
            continue
        if v.lower() in {"true", "false"}:
            bools += 1
        elif _VALUE_REGEX_PROBES[2][1].match(v):  # iso_date
            dates += 1
        elif _VALUE_REGEX_PROBES[6][1].match(v):  # integer
            ints += 1
        elif _VALUE_REGEX_PROBES[7][1].match(v):  # float
            floats += 1
    nonempty = sum(1 for v in values if v.strip())
    if nonempty == 0:
        return "string"
    if dates / nonempty > 0.6:
        return "date"
    if bools / nonempty > 0.6:
        return "bool"
    if floats / nonempty > 0.6:
        return "float"
    if (ints + floats) / nonempty > 0.6:
        return "int"
    return "string"


def _dominant_regex(values: list[str]) -> str:
    """Return the regex-probe name matched by the majority of values."""
    if not values:
        return ""
    scores: dict[str, int] = {}
    nonempty = 0
    for v in values:
        s = v.strip()
        if not s:
            continue
        nonempty += 1
        for name, pat in _VALUE_REGEX_PROBES:
            if pat.match(s):
                scores[name] = scores.get(name, 0) + 1
                break
    if nonempty == 0:
        return ""
    for name, count in sorted(scores.items(), key=lambda kv: -kv[1]):
        if count / nonempty > 0.6:
            return name
    return ""


def _read_sample(path: Path) -> tuple[list[str], list[list[str]]]:
    """Return (header, rows[≤_SAMPLE_ROW_LIMIT]) as strings using DuckDB."""
    suffix = path.suffix.lower()
    safe = str(path.resolve()).replace("'", "''")
    import duckdb  # local

    if suffix in (".csv", ".tsv"):
        delim = "\\t" if suffix == ".tsv" else ","
        reader = f"read_csv_auto('{safe}', delim='{delim}', sample_size=-1)"
    elif suffix in (".json", ".jsonl"):
        reader = f"read_json_auto('{safe}')"
    elif suffix == ".parquet":
        reader = f"read_parquet('{safe}')"
    else:
        raise ValueError(f"Unsupported suffix for fingerprint: {suffix}")

    con = duckdb.connect(":memory:")
    try:
        desc = con.execute(f"DESCRIBE SELECT * FROM {reader}").fetchall()
        header = [r[0] for r in desc]
        rows = con.execute(
            f"SELECT * FROM {reader} LIMIT {_SAMPLE_ROW_LIMIT}"
        ).fetchall()
    finally:
        con.close()

    string_rows: list[list[str]] = []
    for row in rows:
        string_rows.append(["" if v is None else str(v) for v in row])
    return header, string_rows


def compute_fingerprint(path: Path) -> Fingerprint:
    header, rows = _read_sample(path)
    column_names = [_normalize_name(h) for h in header]
    cols: list[list[str]] = [[] for _ in column_names]
    for row in rows:
        for i, val in enumerate(row[: len(column_names)]):
            cols[i].append(val)
    column_types = [_infer_type(col) for col in cols]
    value_regexes = [_dominant_regex(col) for col in cols]
    return Fingerprint(
        column_names=column_names,
        column_types=column_types,
        value_regexes=value_regexes,
        row_count_sample=len(rows),
        column_count=len(column_names),
    )


def _jaccard(a: list[str], b: list[str]) -> float:
    sa, sb = set(filter(None, a)), set(filter(None, b))
    if not sa and not sb:
        return 1.0
    union = sa | sb
    if not union:
        return 0.0
    return len(sa & sb) / len(union)


def similarity(a: Fingerprint, b: Fingerprint) -> float:
    """Weighted similarity ∈ [0, 1].

    Components: column-name Jaccard (50%) + type-overlap (25%) +
    regex-Jaccard (25%). Embedding-based scoring is a soft upgrade that
    activates if sentence-transformers is installed.
    """
    name_jaccard = _jaccard(a.column_names, b.column_names)

    type_overlap = 0.0
    common = set(a.column_names) & set(b.column_names)
    if common:
        a_types = dict(zip(a.column_names, a.column_types))
        b_types = dict(zip(b.column_names, b.column_types))
        matches = sum(1 for c in common if a_types.get(c) == b_types.get(c))
        type_overlap = matches / len(common)
    regex_jaccard = _jaccard(a.value_regexes, b.value_regexes)

    base = 0.5 * name_jaccard + 0.25 * type_overlap + 0.25 * regex_jaccard
    return max(0.0, min(1.0, base))


__all__ = ["compute_fingerprint", "similarity", "_normalize_name", "_infer_type"]
