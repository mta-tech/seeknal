"""Tests for seeknal.ask.agents.tools.open_in_browser — _do_open helper."""

import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from seeknal.ask.agents.tools.open_in_browser import _do_open


def _make_report(tmp_path: Path, slug: str = "test-report", with_launcher: bool = True):
    """Helper: scaffold a minimal report directory under target/reports/."""
    report_dir = tmp_path / "target" / "reports" / slug
    build_dir = report_dir / "build"
    build_dir.mkdir(parents=True)
    (build_dir / "index.html").write_text("<html></html>")
    if with_launcher:
        (report_dir / "open_report.py").write_text("# launcher stub")
    return report_dir


# ---------------------------------------------------------------------------
# Path validation
# ---------------------------------------------------------------------------

def test_path_traversal_blocked(tmp_path: Path):
    _make_report(tmp_path)
    result = _do_open("../../etc/passwd", tmp_path)
    assert "Error" in result


def test_path_outside_reports_blocked(tmp_path: Path):
    # Create a file in target/ but outside reports/
    (tmp_path / "target" / "intermediate").mkdir(parents=True)
    (tmp_path / "target" / "intermediate" / "data.html").write_text("<html></html>")
    result = _do_open("target/intermediate/data.html", tmp_path)
    assert "Error" in result
    assert "target/reports/" in result


def test_nonexistent_report(tmp_path: Path):
    (tmp_path / "target" / "reports").mkdir(parents=True)
    result = _do_open("target/reports/nonexistent", tmp_path)
    assert "Error" in result
    assert "not found" in result


# ---------------------------------------------------------------------------
# Launcher path (Evidence reports)
# ---------------------------------------------------------------------------

@patch.dict(os.environ, {}, clear=False)
@patch("seeknal.ask.agents.tools.open_in_browser.subprocess.Popen")
def test_directory_with_launcher(mock_popen, tmp_path: Path):
    """Report directory with open_report.py -> launches it."""
    os.environ.pop("SSH_CONNECTION", None)
    report_dir = _make_report(tmp_path, with_launcher=True)
    mock_popen.return_value = MagicMock()

    result = _do_open("target/reports/test-report", tmp_path)

    mock_popen.assert_called_once()
    call_args = mock_popen.call_args
    assert str(report_dir / "open_report.py") in call_args[0][0][1]
    assert call_args[1]["start_new_session"] is True
    assert "server starting" in result


@patch.dict(os.environ, {}, clear=False)
@patch("seeknal.ask.agents.tools.open_in_browser.subprocess.Popen")
def test_html_file_with_launcher_in_parent(mock_popen, tmp_path: Path):
    """HTML file inside build/ -> finds launcher in report root."""
    os.environ.pop("SSH_CONNECTION", None)
    _make_report(tmp_path, with_launcher=True)
    mock_popen.return_value = MagicMock()

    result = _do_open("target/reports/test-report/build/index.html", tmp_path)

    mock_popen.assert_called_once()
    assert "server starting" in result


# ---------------------------------------------------------------------------
# Fallback: webbrowser.open
# ---------------------------------------------------------------------------

@patch.dict(os.environ, {}, clear=False)
@patch("seeknal.ask.agents.tools.open_in_browser.webbrowser.open")
def test_html_without_launcher_falls_back_to_webbrowser(mock_wb, tmp_path: Path):
    """No launcher -> direct file:// open."""
    os.environ.pop("SSH_CONNECTION", None)
    report_dir = _make_report(tmp_path, with_launcher=False)

    result = _do_open("target/reports/test-report/build/index.html", tmp_path)

    mock_wb.assert_called_once()
    url = mock_wb.call_args[0][0]
    assert url.startswith("file://")
    assert "index.html" in url
    assert "Opened" in result


# ---------------------------------------------------------------------------
# SSH detection
# ---------------------------------------------------------------------------

@patch.dict(os.environ, {"SSH_CONNECTION": "192.168.1.1 22 10.0.0.1 22"})
def test_ssh_session_skips_browser(tmp_path: Path):
    _make_report(tmp_path, with_launcher=True)

    result = _do_open("target/reports/test-report", tmp_path)

    assert "SSH session detected" in result
    assert "manually" in result


# ---------------------------------------------------------------------------
# validate_report_path
# ---------------------------------------------------------------------------

def test_validate_report_path_rejects_traversal(tmp_path: Path):
    from seeknal.ask.agents.tools._security import validate_report_path

    with pytest.raises(ValueError, match="target/reports/"):
        validate_report_path("../../../etc/passwd", tmp_path)


def test_validate_report_path_accepts_valid(tmp_path: Path):
    from seeknal.ask.agents.tools._security import validate_report_path

    reports = tmp_path / "target" / "reports" / "my-report"
    reports.mkdir(parents=True)
    result = validate_report_path("target/reports/my-report", tmp_path)
    assert result == reports.resolve()
