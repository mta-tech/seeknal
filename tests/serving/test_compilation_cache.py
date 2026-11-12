"""Tests for bounded thread-safe SQLGlot AST compilation cache (`seeknal.serving.compilation_cache`)."""

from __future__ import annotations

import hashlib
import threading

import pytest
import sqlglot
from sqlglot import exp

from seeknal.serving.compilation_cache import (
    DEFAULT_COMPILATION_CACHE_MAX_ENTRIES,
    MAX_COMPILATION_CACHE_MAX_ENTRIES,
    CompilationCache,
    CompilationCacheKey,
    validate_compilation_cache_max_entries,
)
from seeknal.serving.enforce import EnforcementError


def _make_key(
    sql: str,
    *,
    dialect: str = "postgres",
    default_catalog: str | None = "lakekeeper",
    generation: int = 0,
) -> CompilationCacheKey:
    return CompilationCacheKey(
        sql_sha256=hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        sql_dialect=dialect,
        default_catalog=default_catalog,
        catalog_generation=generation,
    )


def test_capacity_validation():
    """Default is 256, 0 disables, 1024 is max; bool/float/negative/oversize are rejected."""
    default_cache = CompilationCache()
    assert default_cache.max_entries == DEFAULT_COMPILATION_CACHE_MAX_ENTRIES == 256
    assert default_cache.info() == {
        "hits": 0,
        "misses": 0,
        "evictions": 0,
        "size": 0,
        "max_entries": 256,
    }

    disabled_cache = CompilationCache(max_entries=0)
    assert disabled_cache.max_entries == 0

    max_cache = CompilationCache(max_entries=MAX_COMPILATION_CACHE_MAX_ENTRIES)
    assert max_cache.max_entries == 1024

    for invalid in (True, False, -1, -100, 1025, 2048, 1.5, 0.0, "256", None, [256]):
        with pytest.raises(EnforcementError):
            CompilationCache(max_entries=invalid)  # type: ignore[arg-type]
        with pytest.raises(EnforcementError):
            validate_compilation_cache_max_entries(invalid)


def test_lru_hits_misses_evictions_and_exact_info_contract():
    """Verify LRU ordering, eviction counts, and exact numeric info() keys."""
    cache = CompilationCache(max_entries=2)
    sql1 = "SELECT id FROM lakekeeper.analytics.orders WHERE id = $1 LIMIT 1"
    sql2 = "SELECT region, SUM(total) FROM lakekeeper.analytics.orders WHERE order_month = $1 GROUP BY region LIMIT 100"
    sql3 = "SELECT id, total FROM lakekeeper.analytics.orders WHERE id = $1 LIMIT 1"

    k1 = _make_key(sql1)
    k2 = _make_key(sql2)
    k3 = _make_key(sql3)

    assert cache.get(k1) is None
    assert cache.put(k1, sqlglot.parse_one(sql1, read="postgres")) is True
    assert cache.get(k2) is None
    assert cache.put(k2, sqlglot.parse_one(sql2, read="postgres")) is True

    # Hit k1 so k1 becomes MRU and k2 becomes LRU
    hit1 = cache.get(k1)
    assert hit1 is not None

    # Insert k3 -> k2 must be evicted
    assert cache.put(k3, sqlglot.parse_one(sql3, read="postgres")) is True
    assert cache.get(k2) is None  # miss (evicted)
    assert cache.get(k1) is not None  # hit (retained)
    assert cache.get(k3) is not None  # hit (retained)

    info = cache.info()
    assert set(info.keys()) == {"hits", "misses", "evictions", "size", "max_entries"}
    for val in info.values():
        assert type(val) is int
    assert info == {
        "hits": 3,
        "misses": 3,
        "evictions": 1,
        "size": 2,
        "max_entries": 2,
    }


def test_disabled_mode_zero_capacity():
    """Capacity 0 disables caching completely while preserving exact numeric info()."""
    cache = CompilationCache(max_entries=0)
    sql = "SELECT id FROM lakekeeper.analytics.orders WHERE id = $1 LIMIT 1"
    k = _make_key(sql)
    ast = sqlglot.parse_one(sql, read="postgres")

    assert cache.get(k) is None
    assert cache.put(k, ast) is False
    assert cache.get(k) is None

    assert cache.info() == {
        "hits": 0,
        "misses": 2,
        "evictions": 0,
        "size": 0,
        "max_entries": 0,
    }


def test_defensive_clone_prevents_ast_mutation_pollution():
    """Mutating AST before/after put or after get never pollutes the cached template."""
    cache = CompilationCache(max_entries=16)
    sql = "SELECT id, total FROM analytics.orders WHERE id = $1 LIMIT 1"
    key = _make_key(sql)

    raw_ast = sqlglot.parse_one(sql, read="postgres")
    baseline_sql = raw_ast.sql(dialect="postgres")

    assert cache.put(key, raw_ast) is True

    # Mutate original raw_ast in place after put()
    for tbl in raw_ast.find_all(exp.Table):
        tbl.set("catalog", exp.to_identifier("mutated_catalog_before"))

    # First cache hit: mutate returned AST heavily (table rewrite, WHERE filter, placeholder replacement)
    hit_a = cache.get(key)
    assert hit_a is not None
    assert hit_a.sql(dialect="postgres") == baseline_sql
    for tbl in hit_a.find_all(exp.Table):
        tbl.replace(exp.to_table("secret_physical_schema.secret_table"))
    for param in hit_a.find_all((exp.Parameter, exp.Placeholder)):
        param.replace(exp.var("__SEEKNAL_PARAM_0__"))
    hit_a.set("limit", exp.Limit(expression=exp.Literal.number(999)))

    # Second cache hit must still return a clean, unmodified raw AST
    hit_b = cache.get(key)
    assert hit_b is not None
    assert hit_b is not hit_a
    assert hit_b.sql(dialect="postgres") == baseline_sql
    assert "secret_physical_schema" not in hit_b.sql(dialect="postgres")
    assert "__SEEKNAL_PARAM_0__" not in hit_b.sql(dialect="postgres")
    assert "mutated_catalog_before" not in hit_b.sql(dialect="postgres")


def test_key_dimensions_separate_entries():
    """Different SQL hash, dialect, default_catalog, or catalog_generation do not share entries."""
    cache = CompilationCache(max_entries=16)
    sql = "SELECT id FROM lakekeeper.analytics.orders WHERE id = $1 LIMIT 1"
    ast = sqlglot.parse_one(sql, read="postgres")

    k_base = _make_key(sql, dialect="postgres", default_catalog="lakekeeper", generation=0)
    k_dialect = _make_key(sql, dialect="duckdb", default_catalog="lakekeeper", generation=0)
    k_catalog = _make_key(sql, dialect="postgres", default_catalog="other_cat", generation=0)
    k_none_cat = _make_key(sql, dialect="postgres", default_catalog=None, generation=0)

    assert cache.put(k_base, ast) is True
    assert cache.get(k_base) is not None
    assert cache.get(k_dialect) is None
    assert cache.get(k_catalog) is None
    assert cache.get(k_none_cat) is None


def test_invalidate_clears_cache_and_fences_stale_generation_put():
    """invalidate() advances generation, clears entries, and fences stale-generation put/get."""
    cache = CompilationCache(max_entries=16)
    sql = "SELECT id FROM lakekeeper.analytics.orders WHERE id = $1 LIMIT 1"
    ast = sqlglot.parse_one(sql, read="postgres")

    gen0 = cache.generation
    assert gen0 == 0
    k_gen0 = _make_key(sql, generation=gen0)
    assert cache.put(k_gen0, ast) is True
    assert cache.info()["size"] == 1

    # Atomically invalidate (simulating update_catalog)
    gen1 = cache.invalidate()
    assert gen1 == 1
    assert cache.generation == 1
    assert cache.info()["size"] == 0

    # Stale generation lookup misses, and stale generation put is fenced (rejected)
    assert cache.get(k_gen0) is None
    assert cache.put(k_gen0, ast) is False
    assert cache.info()["size"] == 0

    # New generation put and get succeed
    k_gen1 = _make_key(sql, generation=gen1)
    assert cache.put(k_gen1, ast) is True
    assert cache.info()["size"] == 1
    assert cache.get(k_gen1) is not None


def test_thread_safety_under_concurrent_access_and_invalidation():
    """Concurrent get/put/invalidate across multiple threads maintains invariants without errors."""
    cache = CompilationCache(max_entries=8)
    templates = [
        f"SELECT id FROM lakekeeper.analytics.orders WHERE id = $1 AND col_{i} = $2 LIMIT 1"
        for i in range(16)
    ]
    parsed = [sqlglot.parse_one(s, read="postgres") for s in templates]
    errors: list[Exception] = []

    def worker(tid: int) -> None:
        try:
            for step in range(40):
                idx = (tid + step) % len(templates)
                gen = cache.generation
                k = _make_key(templates[idx], generation=gen)
                got = cache.get(k)
                if got is None:
                    cache.put(k, parsed[idx])
                else:
                    # Mutate local copy to verify thread isolation
                    got.set("limit", exp.Limit(expression=exp.Literal.number(step + 1)))
                if step % 15 == 0 and tid == 0:
                    cache.invalidate()
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    info = cache.info()
    assert 0 <= info["size"] <= 8
    assert info["max_entries"] == 8
