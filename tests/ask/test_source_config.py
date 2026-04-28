"""Tests for source registry configuration and context sync."""

from pathlib import Path
import textwrap

import pytest

from seeknal.sources.config import (
    SourceConfigError,
    SourceRegistry,
    load_source_registry,
    read_sync_state,
    write_source_context,
)


def test_load_explicit_connected_read_only_source(tmp_path: Path):
    (tmp_path / "seeknal_agent.yml").write_text(
        textwrap.dedent(
            """\
            mode:
              default: analyst
            sources:
              warehouse:
                source_kind: connected
                source_type: database
                connector: postgresql
                namespace: wh
                access: read_only
                role: business_source_of_truth
                priority: 100
                description: Analytics warehouse
                include: [mart.*, analytics.*]
                context_sync:
                  enabled: true
                  refresh_policy: manual
                  stale_after_hours: 24
                  templates: [overview, columns]
            """
        )
    )

    registry = load_source_registry(tmp_path)

    assert registry.explicit is True
    assert registry.default_mode == "analyst"
    assert list(registry.sources) == ["warehouse"]
    source = registry.sources["warehouse"]
    assert source.namespace == "wh"
    assert source.connector == "postgresql"
    assert source.is_read_only is True
    assert source.context_sync.enabled is True
    assert source.context_sync.stale_after_hours == 24


def test_auto_mode_is_preserved_for_agent_routing():
    registry = SourceRegistry.from_agent_config({"mode": {"default": "auto"}})
    assert registry.default_mode == "auto"


def test_duplicate_namespaces_are_rejected():
    config = {
        "sources": {
            "a": {
                "source_kind": "connected",
                "source_type": "database",
                "connector": "postgresql",
                "namespace": "same",
                "access": "read_only",
                "role": "business_source_of_truth",
            },
            "b": {
                "source_kind": "connected",
                "source_type": "database",
                "connector": "mysql",
                "namespace": "same",
                "access": "read_only",
                "role": "business_source_of_truth",
            },
        }
    }

    with pytest.raises(SourceConfigError, match="duplicates"):
        SourceRegistry.from_agent_config(config)


def test_other_role_requires_description():
    config = {
        "sources": {
            "scratch": {
                "source_kind": "connected",
                "source_type": "database",
                "connector": "duckdb",
                "namespace": "scratch",
                "access": "read_only",
                "role": "other",
            }
        }
    }

    with pytest.raises(SourceConfigError, match="description"):
        SourceRegistry.from_agent_config(config)


def test_write_source_context_creates_catalog_files(tmp_path: Path):
    config = {
        "sources": {
            "warehouse": {
                "source_kind": "connected",
                "source_type": "database",
                "connector": "postgresql",
                "namespace": "wh",
                "access": "read_only",
                "role": "business_source_of_truth",
                "description": "Analytics warehouse",
                "context_sync": {"enabled": True},
            }
        }
    }
    registry = SourceRegistry.from_agent_config(config, project_path=tmp_path)

    results = write_source_context(registry, tmp_path)

    assert results[0]["status"] == "metadata_only"
    source_md = tmp_path / ".seeknal/context/sources/wh/SOURCE.md"
    assert source_md.exists()
    source_text = source_md.read_text()
    assert "Analytics warehouse" in source_text
    assert "Sync status: `metadata_only`" in source_text
    assert (tmp_path / ".seeknal/catalog/tables.jsonl").exists()
    assert (tmp_path / ".seeknal/catalog/columns.jsonl").exists()
    state = read_sync_state(tmp_path)
    assert state["sources"]["warehouse"]["namespace"] == "wh"

class _ContextSyncRepl:
    attached = {"wh"}

    def execute_oneshot(self, sql: str, limit=None):
        normalized = " ".join(sql.split()).lower()
        if '"wh".information_schema.tables' in normalized:
            return ["table_schema", "table_name", "table_type"], [
                ("analytics", "orders", "BASE TABLE"),
                ("analytics", "customers", "BASE TABLE"),
                ("scratch", "tmp", "BASE TABLE"),
            ]
        if '"wh".information_schema.columns' in normalized and "orders" in normalized:
            return ["column_name", "data_type"], [
                ("order_id", "INTEGER"),
                ("customer_id", "INTEGER"),
                ("amount", "DOUBLE"),
            ]
        if '"wh".information_schema.columns' in normalized and "customers" in normalized:
            return ["column_name", "data_type"], [
                ("customer_id", "INTEGER"),
                ("name", "VARCHAR"),
            ]
        if "select * from wh.analytics.orders" in normalized:
            return ["order_id", "customer_id", "amount"], [(1, 10, 42.5)]
        if "select * from wh.analytics.customers" in normalized:
            return ["customer_id", "name"], [(10, "Ada")]
        if "_seek_profile" in normalized:
            return ["scanned_rows", "null_count", "distinct_count"], [(10, 0, 3)]
        raise AssertionError(f"unexpected SQL: {sql}")


def test_write_source_context_syncs_attached_database_context(tmp_path: Path):
    config = {
        "sources": {
            "warehouse": {
                "source_kind": "connected",
                "source_type": "database",
                "connector": "postgresql",
                "namespace": "wh",
                "access": "read_only",
                "role": "business_source_of_truth",
                "include": ["analytics.*"],
                "exclude": ["*.tmp"],
                "context_sync": {
                    "enabled": True,
                    "templates": ["overview", "columns", "preview", "profiling", "relationships"],
                    "sample": {"rows": 1},
                },
            }
        }
    }
    registry = SourceRegistry.from_agent_config(config, project_path=tmp_path)

    results = write_source_context(registry, tmp_path, repl=_ContextSyncRepl())

    assert results[0]["status"] == "synced"
    assert results[0]["tables"] == 2
    assert results[0]["columns"] == 5
    table_dir = tmp_path / ".seeknal/context/sources/wh/tables/analytics.orders"
    source_text = (tmp_path / ".seeknal/context/sources/wh/SOURCE.md").read_text()
    assert "Sync status: `synced`" in source_text
    assert "Discovered tables: `2`" in source_text
    assert "metadata-only" not in source_text
    assert (table_dir / "overview.md").exists()
    assert "customer_id" in (table_dir / "columns.md").read_text()
    assert "42.5" in (table_dir / "preview.md").read_text()
    assert "distinct=1" in (table_dir / "profiling.md").read_text()
    relationships = tmp_path / ".seeknal/context/sources/wh/relationships.md"
    assert "customer_id" in relationships.read_text()
    assert "scratch.tmp" not in (tmp_path / ".seeknal/catalog/tables.jsonl").read_text()
    assert "analytics.orders" in (tmp_path / ".seeknal/catalog/tables.jsonl").read_text()
    state = read_sync_state(tmp_path)
    assert state["sources"]["warehouse"]["status"] == "synced"
    assert "_catalog_tables" not in state["sources"]["warehouse"]
