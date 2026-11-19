"""Configuration and sizing settings for Seeknal Serving DuckDB acceleration."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Union

try:
    from seeknal.serving.buildinfo import (  # type: ignore[import-not-found]
        EXPECTED_DUCKDB_VERSION as DEFAULT_EXPECTED_DUCKDB_VERSION,
    )
except ImportError:
    DEFAULT_EXPECTED_DUCKDB_VERSION = "2.0.0"

DEFAULT_MEMORY_LIMIT = "2GB"
DEFAULT_THREADS = 4
DEFAULT_KEEP = 3
DEFAULT_MAX_LAG_SECONDS = 93600
DEFAULT_CACHE_DIR = "/data/serving"
DEFAULT_PII_MODE = "null"
VALID_PII_MODES = ("null", "exclude")
VALID_SOURCE_TYPES = ("postgres", "iceberg")
LOADER_VERSION = "1.0.0"

MEMORY_LIMIT_RE = re.compile(
    r"^\d+(?:\.\d+)?\s*(?:B|KB|MB|GB|TB|PB|K|M|G|T|P)$",
    re.IGNORECASE,
)

# Standard libpq connection environment variables
LIBPQ_ENV_VARS = (
    "PGHOST",
    "PGPORT",
    "PGDATABASE",
    "PGUSER",
    "PGPASSWORD",
    "PGSSLMODE",
    "PGSSLROOTCERT",
)


def validate_memory_limit(val: str) -> str:
    """Validate memory limit string against approved unit format."""
    cleaned = str(val).strip()
    if not MEMORY_LIMIT_RE.fullmatch(cleaned):
        raise ValueError(
            f"Invalid memory_limit {val!r}; must be integer or float followed by unit (e.g. 2GB, 512MB)"
        )
    return cleaned


def validate_threads(val: Any) -> int:
    """Validate threads value is a positive integer between 1 and 256."""
    if isinstance(val, bool):
        raise ValueError(f"Invalid threads value {val!r}; must be an integer")
    try:
        thr = int(val)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Invalid threads value {val!r}; must be an integer") from exc
    if not 1 <= thr <= 256:
        raise ValueError(f"Threads value must be between 1 and 256, got {thr}")
    return thr


def validate_pii_mode(mode: Optional[str]) -> str:
    """Validate PII handling mode ('null' or 'exclude')."""
    if mode is None:
        return DEFAULT_PII_MODE
    if not isinstance(mode, str):
        raise ValueError(f"Invalid pii_mode {mode!r}; expected 'null' or 'exclude'")
    cleaned = mode.strip().lower()
    if cleaned not in VALID_PII_MODES:
        raise ValueError(
            f"Invalid pii_mode {mode!r}; expected one of {VALID_PII_MODES}"
        )
    return cleaned


@dataclass(frozen=True)
class AccelerationSettings:
    memory_limit: str = DEFAULT_MEMORY_LIMIT
    threads: int = DEFAULT_THREADS
    keep: int = DEFAULT_KEEP
    max_lag_seconds: int = DEFAULT_MAX_LAG_SECONDS
    cache_dir: str = DEFAULT_CACHE_DIR
    expected_duckdb_version: str = DEFAULT_EXPECTED_DUCKDB_VERSION


def get_memory_limit(override: Optional[str] = None) -> str:
    raw = (
        override
        or os.environ.get("ACCEL_MEMORY_LIMIT")
        or os.environ.get("DUCKDB_MEMORY_LIMIT")
        or DEFAULT_MEMORY_LIMIT
    )
    return validate_memory_limit(raw)


def get_threads(override: Optional[Any] = None) -> int:
    if override is not None:
        return validate_threads(override)
    raw = os.environ.get("ACCEL_THREADS") or os.environ.get("DUCKDB_THREADS")
    if raw:
        return validate_threads(raw)
    return DEFAULT_THREADS


def get_keep(override: Optional[int] = None) -> int:
    if override is not None:
        return override
    val = os.environ.get("ACCEL_KEEP")
    if val:
        try:
            return int(val)
        except ValueError:
            pass
    return DEFAULT_KEEP


def validate_extension_dir(path: Optional[Union[str, Path]]) -> Optional[str]:
    """Validate extension_dir is an existing absolute directory path without quote characters."""
    if path is None:
        return None
    raw = str(path).strip()
    if not raw:
        return None
    if any(c in raw for c in ("'", '"', ";", "\0", "\n", "\r")):
        raise ValueError(
            f"Invalid extension_dir {raw!r}; quote or delimiter characters not permitted"
        )
    p = Path(raw)
    if not p.is_absolute():
        raise ValueError(f"Invalid extension_dir {raw!r}; must be an absolute path")
    if not p.is_dir():
        raise ValueError(f"Invalid extension_dir {raw!r}; directory does not exist")
    return str(p)


def get_expected_duckdb_version(override: Optional[str] = None) -> str:
    return (
        override
        or os.environ.get("ACCEL_DUCKDB_VERSION")
        or DEFAULT_EXPECTED_DUCKDB_VERSION
    )


def get_extension_dir(override: Optional[Union[str, Path]] = None) -> Optional[str]:
    raw = override or os.environ.get("ACCEL_EXTENSION_DIR") or None
    if raw is not None:
        cleaned = str(raw).strip()
        return cleaned if cleaned else None
    return None


def get_settings() -> AccelerationSettings:
    return AccelerationSettings(
        memory_limit=get_memory_limit(),
        threads=get_threads(),
        keep=get_keep(),
        expected_duckdb_version=get_expected_duckdb_version(),
    )
