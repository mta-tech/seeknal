"""Tests for search_project_files tool."""

import os
from pathlib import Path

import pytest

from seeknal.ask.agents.tools.search_project_files import _do_search


@pytest.fixture
def project_dir(tmp_path):
    """Create a temporary project directory with test files."""
    # Create some test files
    (tmp_path / "seeknal").mkdir()
    (tmp_path / "seeknal" / "pipeline.yml").write_text(
        "kind: transform\nname: revenue_calc\nsql: |\n  SELECT amount * quantity AS revenue\n"
    )
    (tmp_path / "seeknal" / "custom_transform.py").write_text(
        "def transform(ctx):\n    revenue = ctx.amount * ctx.quantity\n    return revenue\n"
    )
    (tmp_path / "config.toml").write_text('[project]\nname = "test"\n')
    (tmp_path / "README.md").write_text("# Test Project\nThis has revenue calculations.\n")

    # Create excluded dirs
    (tmp_path / ".git").mkdir()
    (tmp_path / ".git" / "config").write_text("git config")
    (tmp_path / "target").mkdir()
    (tmp_path / "target" / "output.parquet").write_bytes(b"\x00\x01\x02")
    (tmp_path / "__pycache__").mkdir()
    (tmp_path / "__pycache__" / "module.cpython-311.pyc").write_bytes(b"\x00")

    # Create sensitive files
    (tmp_path / ".env").write_text("SECRET_KEY=abc123")
    (tmp_path / "profiles.yml").write_text("password: secret")

    return tmp_path


class TestDoSearch:
    def test_finds_matches(self, project_dir):
        result = _do_search("revenue", project_dir)
        assert "revenue" in result.lower()
        assert "Found" in result

    def test_file_pattern_glob(self, project_dir):
        result = _do_search("revenue", project_dir, file_pattern="*.yml")
        assert "pipeline.yml" in result
        assert "custom_transform.py" not in result

    def test_max_results_limit(self, project_dir):
        # Create many files with matches
        for i in range(10):
            (project_dir / f"file_{i}.txt").write_text(f"revenue line {i}\n")
        result = _do_search("revenue", project_dir, max_results=3)
        assert result.count("- `") == 3

    def test_excludes_git_dir(self, project_dir):
        result = _do_search("git config", project_dir)
        assert "No matches found" in result

    def test_excludes_target_dir(self, project_dir):
        (project_dir / "target" / "log.txt").write_text("some output")
        result = _do_search("some output", project_dir)
        assert "No matches found" in result

    def test_excludes_pycache(self, project_dir):
        result = _do_search("module", project_dir, file_pattern="*.pyc")
        assert "No matches found" in result

    def test_blocked_files_skipped(self, project_dir):
        result = _do_search("SECRET_KEY", project_dir)
        assert "No matches found" in result

    def test_blocked_profiles_skipped(self, project_dir):
        result = _do_search("password", project_dir)
        assert "No matches found" in result

    def test_binary_file_skipped(self, project_dir):
        (project_dir / "binary.bin").write_bytes(b"\x00\x01\x02\xff\xfe")
        result = _do_search("binary", project_dir)
        # Should not crash, binary file is skipped
        assert "binary.bin" not in result

    def test_empty_results(self, project_dir):
        result = _do_search("nonexistent_xyz_pattern_12345", project_dir)
        assert "No matches found" in result

    def test_invalid_regex(self, project_dir):
        result = _do_search("[invalid regex", project_dir)
        assert "Invalid regex pattern" in result

    def test_long_lines_truncated(self, project_dir):
        long_line = "revenue " + "x" * 300
        (project_dir / "long.txt").write_text(long_line)
        result = _do_search("revenue", project_dir)
        assert "..." in result

    def test_large_file_skipped(self, project_dir):
        # Create a file larger than 1MB
        (project_dir / "large.txt").write_text("revenue\n" * 200_000)
        result = _do_search("revenue", project_dir, file_pattern="large.txt")
        assert "No matches found" in result

    def test_path_stays_within_project(self, project_dir):
        # Ensure results show relative paths
        result = _do_search("revenue", project_dir)
        assert str(project_dir) not in result  # Should show relative paths
