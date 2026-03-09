"""Tests for seeknal.ask.report.builder."""

import os
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestCheckNode:
    """Test Node.js availability checking."""

    def test_no_npm_returns_error(self):
        from seeknal.ask.report.builder import _check_node

        with patch("shutil.which", return_value=None):
            result = _check_node()
            assert "Node.js is required" in result

    def test_npm_available_no_node_returns_error(self):
        from seeknal.ask.report.builder import _check_node

        def mock_which(name):
            return "/usr/bin/npm" if name == "npm" else None

        with patch("shutil.which", side_effect=mock_which):
            result = _check_node()
            assert "Node.js is required" in result

    def test_old_node_version_returns_error(self):
        from seeknal.ask.report.builder import _check_node

        with patch("shutil.which", return_value="/usr/bin/node"):
            mock_result = MagicMock()
            mock_result.stdout = "v16.20.0\n"
            with patch("subprocess.run", return_value=mock_result):
                result = _check_node()
                assert "18+ is required" in result

    def test_new_node_version_returns_empty(self):
        from seeknal.ask.report.builder import _check_node

        with patch("shutil.which", return_value="/usr/bin/node"):
            mock_result = MagicMock()
            mock_result.stdout = "v20.11.0\n"
            with patch("subprocess.run", return_value=mock_result):
                result = _check_node()
                assert result == ""


class TestBuildEnv:
    """Test environment variable stripping."""

    def test_strips_api_keys(self):
        from seeknal.ask.report.builder import _get_build_env

        with patch.dict(os.environ, {
            "PATH": "/usr/bin",
            "GOOGLE_API_KEY": "secret123",
            "HOME": "/home/user",
            "AWS_SECRET_ACCESS_KEY": "aws-secret",
        }):
            env = _get_build_env()
            assert "PATH" in env
            assert "HOME" in env
            assert "GOOGLE_API_KEY" not in env
            assert "AWS_SECRET_ACCESS_KEY" not in env

    def test_strips_tokens(self):
        from seeknal.ask.report.builder import _get_build_env

        with patch.dict(os.environ, {
            "PATH": "/usr/bin",
            "GITHUB_TOKEN": "ghp_xxx",
            "AUTH_PASSWORD": "pass123",
        }):
            env = _get_build_env()
            assert "GITHUB_TOKEN" not in env
            assert "AUTH_PASSWORD" not in env


class TestBuildReport:
    """Test the build_report function."""

    def test_no_node_returns_error(self, tmp_path):
        from seeknal.ask.report.builder import build_report

        with patch("shutil.which", return_value=None):
            result = build_report(tmp_path)
            assert "Node.js is required" in result

    def test_build_timeout(self, tmp_path):
        from seeknal.ask.report.builder import build_report
        import subprocess

        (tmp_path / "node_modules").mkdir()

        with patch("shutil.which", return_value="/usr/bin/node"):
            mock_result = MagicMock()
            mock_result.stdout = "v20.0.0\n"

            def mock_run(*args, **kwargs):
                if args[0] == ["/usr/bin/node", "--version"]:
                    return mock_result
                raise subprocess.TimeoutExpired(args[0], kwargs.get("timeout", 120))

            with patch("subprocess.run", side_effect=mock_run):
                result = build_report(tmp_path, timeout=1)
                assert "timed out" in result

    def test_build_success(self, tmp_path):
        from seeknal.ask.report.builder import build_report

        (tmp_path / "node_modules").mkdir()
        build_dir = tmp_path / "build"
        build_dir.mkdir()
        (build_dir / "index.html").write_text("<html></html>")

        with patch("shutil.which", return_value="/usr/bin/node"):
            mock_version = MagicMock()
            mock_version.stdout = "v20.0.0\n"
            mock_build = MagicMock()
            mock_build.returncode = 0

            call_count = [0]

            def mock_run(*args, **kwargs):
                call_count[0] += 1
                if args[0][0] == "/usr/bin/node":
                    return mock_version
                return mock_build

            with patch("subprocess.run", side_effect=mock_run):
                result = build_report(tmp_path)
                assert result.endswith("index.html")

    def test_build_failure_returns_error(self, tmp_path):
        from seeknal.ask.report.builder import build_report

        (tmp_path / "node_modules").mkdir()

        with patch("shutil.which", return_value="/usr/bin/node"):
            mock_version = MagicMock()
            mock_version.stdout = "v20.0.0\n"
            mock_build = MagicMock()
            mock_build.returncode = 1
            mock_build.stderr = "SyntaxError: Unexpected token"

            def mock_run(*args, **kwargs):
                if args[0][0] == "/usr/bin/node":
                    return mock_version
                return mock_build

            with patch("subprocess.run", side_effect=mock_run):
                result = build_report(tmp_path)
                assert "Evidence build failed" in result
                assert "SyntaxError" in result


@pytest.mark.skipif(
    not shutil.which("npm"), reason="Node.js required for integration test"
)
class TestBuildIntegration:
    """Integration tests that require Node.js."""

    pass  # Placeholder for actual integration tests
