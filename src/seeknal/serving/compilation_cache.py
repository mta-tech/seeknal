"""Bounded thread-safe LRU compilation cache for raw parsed SQLGlot AST templates.

Invariants:
- Stores ONLY raw parsed logical `sqlglot.exp.Expression` ASTs before default-catalog
  canonicalization, routing, policy enforcement, table rewrites, or parameter binding.
- Never stores grants, subjects, workspaces, policies, parameter values, routing decisions,
  physical SQL, or query results.
- Clones the AST via `.copy()` on both insertion and retrieval so downstream AST rewrites
  never mutate cached templates.
- Keyed by `(sql_sha256, sql_dialect, default_catalog, catalog_generation)` with no literal
  normalization.
- `invalidate()` atomically advances `catalog_generation` and clears cached entries, fencing
  concurrent in-flight parses from an older generation so they cannot repopulate the cache.
- `info()` exposes only exact numeric counters `{hits, misses, evictions, size, max_entries}`.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

from sqlglot import exp

from seeknal.serving.enforce import EnforcementError

DEFAULT_COMPILATION_CACHE_MAX_ENTRIES = 256
MAX_COMPILATION_CACHE_MAX_ENTRIES = 1024
MAX_CACHEABLE_SQL_BYTES = 32_768


def validate_compilation_cache_max_entries(
    value: Any,
    *,
    error_cls: type[Exception] = EnforcementError,
) -> int:
    """Validate `compilation_cache_max_entries` (int in 0..1024, rejecting bool/float/other)."""
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < 0
        or value > MAX_COMPILATION_CACHE_MAX_ENTRIES
    ):
        raise error_cls(
            f"'compilation_cache_max_entries' must be an integer between 0 and {MAX_COMPILATION_CACHE_MAX_ENTRIES}"
        )
    return value


@dataclass(frozen=True)
class CompilationCacheKey:
    """Grant-independent cache key for a raw parsed SQL template."""

    sql_sha256: str
    sql_dialect: str
    default_catalog: str | None
    catalog_generation: int


class CompilationCache:
    """Thread-safe bounded LRU cache of raw parsed SQLGlot ASTs."""

    def __init__(
        self,
        max_entries: int = DEFAULT_COMPILATION_CACHE_MAX_ENTRIES,
    ) -> None:
        self._max_entries = validate_compilation_cache_max_entries(
            max_entries, error_cls=EnforcementError
        )
        self._lock = threading.Lock()
        self._entries: OrderedDict[CompilationCacheKey, exp.Expression] = OrderedDict()
        self._generation: int = 0
        self._hits: int = 0
        self._misses: int = 0
        self._evictions: int = 0

    @property
    def max_entries(self) -> int:
        return self._max_entries

    @property
    def generation(self) -> int:
        with self._lock:
            return self._generation

    def get(self, key: CompilationCacheKey) -> exp.Expression | None:
        """Look up a raw AST by key, returning a defensive copy on hit or None on miss."""
        if not isinstance(key, CompilationCacheKey):
            raise EnforcementError("cache key must be a CompilationCacheKey")

        with self._lock:
            if self._max_entries == 0 or key.catalog_generation != self._generation:
                self._misses += 1
                return None

            cached = self._entries.get(key)
            if cached is None:
                self._misses += 1
                return None

            self._entries.move_to_end(key)
            self._hits += 1
            return cached.copy()

    def put(self, key: CompilationCacheKey, statement: exp.Expression) -> bool:
        """Store a defensive copy of a raw parsed AST if enabled and generation is current."""
        if not isinstance(key, CompilationCacheKey):
            raise EnforcementError("cache key must be a CompilationCacheKey")
        if not isinstance(statement, exp.Expression):
            raise EnforcementError("cached statement must be a sqlglot Expression")

        if self._max_entries == 0:
            return False
        with self._lock:
            if key.catalog_generation != self._generation:
                return False

        cloned = statement.copy()
        with self._lock:
            if self._max_entries == 0 or key.catalog_generation != self._generation:
                return False

            if key in self._entries:
                self._entries[key] = cloned
                self._entries.move_to_end(key)
                return True

            while len(self._entries) >= self._max_entries:
                self._entries.popitem(last=False)
                self._evictions += 1

            self._entries[key] = cloned
            return True

    def invalidate(self) -> int:
        """Atomically increment catalog generation and clear all cached AST templates."""
        with self._lock:
            self._generation += 1
            self._entries.clear()
            return self._generation

    def info(self) -> dict[str, int]:
        """Return read-only numeric cache counters."""
        with self._lock:
            return {
                "hits": self._hits,
                "misses": self._misses,
                "evictions": self._evictions,
                "size": len(self._entries),
                "max_entries": self._max_entries,
            }


__all__ = [
    "CompilationCache",
    "CompilationCacheKey",
    "DEFAULT_COMPILATION_CACHE_MAX_ENTRIES",
    "MAX_CACHEABLE_SQL_BYTES",
    "MAX_COMPILATION_CACHE_MAX_ENTRIES",
    "validate_compilation_cache_max_entries",
]
