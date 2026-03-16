"""Tests for write security module."""

from pathlib import Path

import pytest

from seeknal.ask.agents.tools._write_security import (
    validate_write_path,
    validate_draft_path,
    WRITABLE_DIRS,
    BLOCKED_WRITE_FILES,
)


@pytest.fixture
def project_dir(tmp_path):
    """Create a temporary project directory."""
    (tmp_path / "seeknal" / "sources").mkdir(parents=True)
    (tmp_path / "seeknal" / "transforms").mkdir(parents=True)
    (tmp_path / "data").mkdir()
    (tmp_path / ".env").write_text("SECRET=abc")
    (tmp_path / "profiles.yml").write_text("password: secret")
    (tmp_path / "seeknal_project.yml").write_text("name: test")
    return tmp_path


class TestValidateWritePath:
    def test_allows_draft_in_project_root(self, project_dir):
        result = validate_write_path("draft_source_customers.yml", project_dir)
        assert result == (project_dir / "draft_source_customers.yml").resolve()

    def test_allows_seeknal_directory(self, project_dir):
        result = validate_write_path("seeknal/sources/test.yml", project_dir)
        assert result.is_relative_to(project_dir.resolve())

    def test_allows_data_directory(self, project_dir):
        result = validate_write_path("data/test.csv", project_dir)
        assert result.is_relative_to(project_dir.resolve())

    def test_blocks_path_traversal(self, project_dir):
        with pytest.raises(ValueError, match="not allowed"):
            validate_write_path("../../etc/passwd", project_dir)

    def test_blocks_env_file(self, project_dir):
        with pytest.raises(ValueError, match="protected"):
            validate_write_path(".env", project_dir)

    def test_blocks_profiles_yml(self, project_dir):
        with pytest.raises(ValueError, match="protected"):
            validate_write_path("profiles.yml", project_dir)

    def test_blocks_seeknal_project_yml(self, project_dir):
        with pytest.raises(ValueError, match="protected"):
            validate_write_path("seeknal_project.yml", project_dir)

    def test_blocks_non_writable_directory(self, project_dir):
        with pytest.raises(ValueError, match="not allowed"):
            validate_write_path("target/intermediate/test.parquet", project_dir)

    def test_blocks_hidden_directory(self, project_dir):
        with pytest.raises(ValueError, match="not allowed"):
            validate_write_path(".git/config", project_dir)


class TestValidateDraftPath:
    def test_valid_yml_draft(self, project_dir):
        result = validate_draft_path("draft_source_customers.yml", project_dir)
        assert result.name == "draft_source_customers.yml"

    def test_valid_py_draft(self, project_dir):
        result = validate_draft_path("draft_transform_clean.py", project_dir)
        assert result.name == "draft_transform_clean.py"

    def test_invalid_filename_pattern(self, project_dir):
        with pytest.raises(ValueError, match="Invalid draft filename"):
            validate_draft_path("not_a_draft.yml", project_dir)

    def test_invalid_extension(self, project_dir):
        with pytest.raises(ValueError, match="Invalid draft filename"):
            validate_draft_path("draft_source_test.json", project_dir)

    def test_strips_directory_component(self, project_dir):
        result = validate_draft_path("some/path/draft_source_test.yml", project_dir)
        assert result.name == "draft_source_test.yml"
