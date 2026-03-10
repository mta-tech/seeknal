"""Tests for read_project_file tool."""

from pathlib import Path

import pytest

from seeknal.ask.agents.tools.read_project_file import _do_read


@pytest.fixture
def project_dir(tmp_path):
    """Create a temporary project directory with test files."""
    (tmp_path / "seeknal").mkdir()
    (tmp_path / "seeknal" / "pipeline.yml").write_text(
        "kind: transform\nname: test\nsql: |\n  SELECT 1\n"
    )
    (tmp_path / "script.py").write_text(
        "#!/usr/bin/env python\ndef main():\n    print('hello')\n\nif __name__ == '__main__':\n    main()\n"
    )
    (tmp_path / "config.toml").write_text('[project]\nname = "test"\n')

    # Sensitive files
    (tmp_path / ".env").write_text("SECRET=abc")
    (tmp_path / "profiles.yml").write_text("password: secret")

    return tmp_path


class TestDoRead:
    def test_reads_file_with_line_numbers(self, project_dir):
        result = _do_read("script.py", project_dir)
        assert "```python" in result
        assert "1 |" in result
        assert "def main():" in result

    def test_yaml_syntax_hint(self, project_dir):
        result = _do_read("seeknal/pipeline.yml", project_dir)
        assert "```yaml" in result

    def test_start_line_pagination(self, project_dir):
        result = _do_read("script.py", project_dir, start_line=3)
        assert "3 |" in result
        assert "1 |" not in result

    def test_max_lines_limit(self, project_dir):
        # Create file with many lines
        lines = "\n".join(f"line {i}" for i in range(50))
        (project_dir / "many.txt").write_text(lines)
        result = _do_read("many.txt", project_dir, max_lines=5)
        assert "more lines" in result

    def test_path_traversal_blocked(self, project_dir):
        result = _do_read("../../etc/passwd", project_dir)
        # Path traversal sequences are sanitized, resulting in file not found
        assert "not found" in result.lower() or "Error" in result

    def test_blocked_env_file(self, project_dir):
        result = _do_read(".env", project_dir)
        assert "Error" in result
        assert "blocked" in result.lower()

    def test_blocked_profiles(self, project_dir):
        result = _do_read("profiles.yml", project_dir)
        assert "Error" in result
        assert "blocked" in result.lower()

    def test_file_not_found(self, project_dir):
        result = _do_read("nonexistent.txt", project_dir)
        assert "not found" in result.lower()

    def test_binary_file_blocked(self, project_dir):
        (project_dir / "binary.bin").write_bytes(b"\x00\x01\x02\xff\xfe")
        result = _do_read("binary.bin", project_dir)
        assert "binary" in result.lower()

    def test_file_size_limit(self, project_dir):
        # Create file larger than 500KB
        (project_dir / "large.txt").write_text("x" * 600_000)
        result = _do_read("large.txt", project_dir)
        assert "too large" in result.lower()

    def test_backslash_normalized(self, project_dir):
        # Backslash paths should work
        result = _do_read("seeknal\\pipeline.yml", project_dir)
        assert "kind: transform" in result

    def test_directory_not_file(self, project_dir):
        result = _do_read("seeknal", project_dir)
        assert "Not a file" in result
