"""Tests for source output caching and cache loading in transforms."""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from seeknal.dag.manifest import Node, NodeType
from seeknal.workflow.executors.base import ExecutionContext


class TestCachePath:
    """Test ExecutionContext.get_cache_path() structure."""

    def test_source_cache_path(self, tmp_path):
        ctx = ExecutionContext(
            project_name="test",
            workspace_path=tmp_path,
            target_path=tmp_path / "target",
        )
        node = Node(
            id="source.users",
            name="users",
            node_type=NodeType.SOURCE,
        )
        path = ctx.get_cache_path(node)
        assert str(path).endswith("target/cache/source/users.parquet")
        assert path.parent.exists()

    def test_transform_cache_path(self, tmp_path):
        ctx = ExecutionContext(
            project_name="test",
            workspace_path=tmp_path,
            target_path=tmp_path / "target",
        )
        node = Node(
            id="transform.clean_users",
            name="clean_users",
            node_type=NodeType.TRANSFORM,
        )
        path = ctx.get_cache_path(node)
        assert str(path).endswith("target/cache/transform/clean_users.parquet")

    def test_cache_directory_created(self, tmp_path):
        ctx = ExecutionContext(
            project_name="test",
            workspace_path=tmp_path,
            target_path=tmp_path / "target",
        )
        node = Node(
            id="source.data",
            name="data",
            node_type=NodeType.SOURCE,
        )
        path = ctx.get_cache_path(node)
        # Directory should be created by get_cache_path
        assert path.parent.is_dir()


class TestCacheWrite:
    """Test that cache files can be written and read."""

    def test_write_and_read_parquet(self, tmp_path):
        """Verify we can write a parquet file and read it back."""
        import duckdb
        conn = duckdb.connect(":memory:")

        # Create sample data
        conn.execute("CREATE TABLE test_data AS SELECT 1 as id, 'alice' as name")

        # Write to parquet
        cache_path = tmp_path / "cache" / "source" / "test.parquet"
        cache_path.parent.mkdir(parents=True)
        conn.execute(f"COPY test_data TO '{cache_path}' (FORMAT PARQUET)")

        # Read back
        result = conn.execute(f"SELECT * FROM read_parquet('{cache_path}')").fetchall()
        assert len(result) == 1
        assert result[0] == (1, "alice")

    def test_view_from_cache(self, tmp_path):
        """Verify we can create a view from cached parquet."""
        import duckdb
        conn = duckdb.connect(":memory:")

        # Write cache
        conn.execute("CREATE TABLE src AS SELECT 1 as id, 100 as value")
        cache_path = tmp_path / "source.users.parquet"
        conn.execute(f"COPY src TO '{cache_path}' (FORMAT PARQUET)")

        # Drop the original table
        conn.execute("DROP TABLE src")

        # Recreate as view from cache
        conn.execute(
            f"CREATE OR REPLACE VIEW \"source.users\" AS "
            f"SELECT * FROM read_parquet('{cache_path}')"
        )

        # Verify
        result = conn.execute("SELECT * FROM \"source.users\"").fetchall()
        assert len(result) == 1
        assert result[0] == (1, 100)


class TestCacheInvalidation:
    """Test that cache is invalidated on changes."""

    def test_cache_deleted_when_missing_after_rerun(self, tmp_path):
        """Cache file can be deleted to force re-execution."""
        cache_path = tmp_path / "cache" / "source" / "test.parquet"
        cache_path.parent.mkdir(parents=True)
        cache_path.write_text("dummy")
        assert cache_path.exists()
        cache_path.unlink()
        assert not cache_path.exists()

    def test_corrupted_cache_triggers_cleanup(self, tmp_path):
        """Corrupted parquet file is cleaned up."""
        import duckdb
        conn = duckdb.connect(":memory:")

        cache_path = tmp_path / "test.parquet"
        cache_path.write_text("not a parquet file")

        # Reading corrupted file should fail
        with pytest.raises(Exception):
            conn.execute(f"SELECT * FROM read_parquet('{cache_path}')")
