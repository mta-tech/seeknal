"""Tests for document CLI (seeknal docs generate)."""

import json

import pytest

from seeknal.ask.project import find_project_path
from seeknal.cli.document import (
    _find_project_path,
    _gather_pipeline_context,
    _parse_llm_response,
    _update_yaml_file,
)


class TestFindProjectPath:
    def test_explicit_path(self, tmp_path):
        (tmp_path / "seeknal").mkdir()
        result = _find_project_path(str(tmp_path))
        assert result == tmp_path

    def test_nonexistent_path(self):
        import typer

        with pytest.raises(typer.BadParameter, match="not found"):
            _find_project_path("/nonexistent/path/xyz")

    def test_shared_find_project_path_explicit(self, tmp_path):
        result = find_project_path(str(tmp_path))
        assert result == tmp_path

    def test_shared_find_project_path_nonexistent(self):
        with pytest.raises(FileNotFoundError, match="not found"):
            find_project_path("/nonexistent/path/xyz")


class TestGatherPipelineContext:
    def test_basic_context(self, tmp_path):
        (tmp_path / "seeknal").mkdir()
        code = "kind: transform\nname: test\nsql: SELECT 1\n"
        (tmp_path / "seeknal" / "test.yml").write_text(code)

        pipeline = {
            "name": "test",
            "kind": "transform",
            "file_path": "seeknal/test.yml",
        }
        result = _gather_pipeline_context(pipeline, tmp_path)
        assert result["kind"] == "transform"
        assert result["name"] == "test"
        assert "SELECT 1" in result["code"]
        assert result["schema_section"] == ""
        assert result["sample_section"] == ""

    def test_missing_file(self, tmp_path):
        pipeline = {
            "name": "missing",
            "kind": "transform",
            "file_path": "seeknal/missing.yml",
        }
        result = _gather_pipeline_context(pipeline, tmp_path)
        assert "could not read file" in result["code"]


class TestParseLlmResponse:
    def test_valid_json(self):
        response = json.dumps({
            "description": "Pipeline for revenue calc",
            "column_descriptions": {"revenue": "Total revenue"},
        })
        result = _parse_llm_response(response)
        assert result is not None
        assert result["description"] == "Pipeline for revenue calc"
        assert result["column_descriptions"]["revenue"] == "Total revenue"

    def test_json_in_code_fence(self):
        response = '```json\n{"description": "test", "column_descriptions": {}}\n```'
        result = _parse_llm_response(response)
        assert result is not None
        assert result["description"] == "test"

    def test_json_in_plain_code_fence(self):
        response = '```\n{"description": "test", "column_descriptions": {}}\n```'
        result = _parse_llm_response(response)
        assert result is not None

    def test_invalid_json(self):
        result = _parse_llm_response("This is not JSON at all")
        assert result is None

    def test_json_missing_description(self):
        result = _parse_llm_response('{"foo": "bar"}')
        assert result is None

    def test_empty_response(self):
        result = _parse_llm_response("")
        assert result is None


class TestUpdateYamlFile:
    def test_adds_description(self, tmp_path):
        yaml_file = tmp_path / "test.yml"
        yaml_file.write_text("kind: transform\nname: test\nsql: SELECT 1\n")

        changed, message = _update_yaml_file(
            yaml_file, "A test pipeline", {}, force=False, dry_run=False
        )
        assert changed is True
        content = yaml_file.read_text()
        assert "A test pipeline" in content

    def test_adds_column_descriptions(self, tmp_path):
        yaml_file = tmp_path / "test.yml"
        yaml_file.write_text("kind: transform\nname: test\nsql: SELECT 1\n")

        changed, message = _update_yaml_file(
            yaml_file, "", {"revenue": "Total revenue"}, force=False, dry_run=False
        )
        assert changed is True
        content = yaml_file.read_text()
        assert "revenue" in content

    def test_skips_existing_description(self, tmp_path):
        yaml_file = tmp_path / "test.yml"
        yaml_file.write_text(
            "kind: transform\nname: test\ndescription: Existing docs\nsql: SELECT 1\n"
        )

        changed, message = _update_yaml_file(
            yaml_file, "New docs", {}, force=False, dry_run=False
        )
        assert changed is False
        assert "Already documented" in message

    def test_force_overwrites(self, tmp_path):
        yaml_file = tmp_path / "test.yml"
        yaml_file.write_text(
            "kind: transform\nname: test\ndescription: Old\nsql: SELECT 1\n"
        )

        changed, message = _update_yaml_file(
            yaml_file, "New docs", {}, force=True, dry_run=False
        )
        assert changed is True
        content = yaml_file.read_text()
        assert "New docs" in content

    def test_dry_run_no_write(self, tmp_path):
        yaml_file = tmp_path / "test.yml"
        yaml_file.write_text("kind: transform\nname: test\nsql: SELECT 1\n")

        changed, message = _update_yaml_file(
            yaml_file, "A test pipeline", {}, force=False, dry_run=True
        )
        assert changed is True
        # File should NOT be modified
        content = yaml_file.read_text()
        assert "A test pipeline" not in content

    def test_non_yaml_root(self, tmp_path):
        yaml_file = tmp_path / "test.yml"
        yaml_file.write_text("- item1\n- item2\n")

        changed, message = _update_yaml_file(
            yaml_file, "desc", {}, force=False, dry_run=False
        )
        assert changed is False
        assert "not a mapping" in message

    def test_ruamel_not_installed(self, tmp_path, monkeypatch):
        yaml_file = tmp_path / "test.yml"
        yaml_file.write_text("kind: transform\nname: test\n")

        # Simulate ruamel.yaml not being installed
        import builtins
        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "ruamel.yaml":
                raise ImportError("No module named 'ruamel.yaml'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", mock_import)
        changed, message = _update_yaml_file(
            yaml_file, "desc", {}, force=False, dry_run=False
        )
        assert changed is False
        assert "ruamel.yaml" in message
