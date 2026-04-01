"""Tests for seeknal.ui.progress module."""

from io import StringIO

from rich.console import Console
from rich.panel import Panel

from seeknal.ui.theme import DARK_THEME


def _capture_console() -> Console:
    """Return a Console that writes to a StringIO buffer."""
    return Console(file=StringIO(), theme=DARK_THEME, highlight=False)


# ---- render_summary --------------------------------------------------------


def test_render_summary_all_fields():
    from seeknal.ui.progress import render_summary

    panel = render_summary(
        {
            "total": 5,
            "executed": 4,
            "cached": 1,
            "failed": 0,
            "duration": 3.245,
            "log_path": "logs/run_42.log",
        }
    )
    assert isinstance(panel, Panel)


def test_render_summary_partial_fields():
    from seeknal.ui.progress import render_summary

    panel = render_summary({"total": 3, "duration": 1.5})
    assert isinstance(panel, Panel)


def test_render_summary_failed_zero_when_missing():
    from seeknal.ui.progress import render_summary

    panel = render_summary({"total": 2})
    # Render to string and check "0" appears for failed
    con = _capture_console()
    con.print(panel)
    output = con.file.getvalue()
    assert "0" in output


def test_render_summary_contains_stats():
    from seeknal.ui.progress import render_summary

    panel = render_summary(
        {
            "total": 10,
            "executed": 8,
            "cached": 2,
            "failed": 1,
            "duration": 45.3,
        }
    )
    con = _capture_console()
    con.print(panel)
    output = con.file.getvalue()
    assert "10" in output
    assert "8" in output
    assert "2" in output
    assert "1" in output


def test_render_summary_shows_log_path():
    from seeknal.ui.progress import render_summary

    panel = render_summary({"log_path": "/tmp/seeknal.log"})
    con = _capture_console()
    con.print(panel)
    output = con.file.getvalue()
    assert "/tmp/seeknal.log" in output


def test_render_summary_duration_formatted():
    from seeknal.ui.progress import render_summary

    panel = render_summary({"duration": 125.7})
    con = _capture_console()
    con.print(panel)
    output = con.file.getvalue()
    # 125.7s -> "2m 5.7s"
    assert "2m" in output


# ---- ExecutionProgress -----------------------------------------------------


def test_execution_progress_creates():
    from seeknal.ui.progress import ExecutionProgress

    ep = ExecutionProgress(total_nodes=3)
    assert ep is not None


def test_execution_progress_context_manager_non_tty():
    """Non-TTY path (StringIO console) should work without crashing."""
    from seeknal.ui.progress import ExecutionProgress

    con = _capture_console()
    ep = ExecutionProgress(total_nodes=2, console=con)
    with ep:
        pass


def test_start_complete_fail_non_tty(monkeypatch):
    """start_node / complete_node / fail_node don't crash on non-TTY."""
    from seeknal.ui.progress import ExecutionProgress

    con = _capture_console()
    monkeypatch.setattr("seeknal.ui.progress.echo_info", lambda msg: None)

    ep = ExecutionProgress(total_nodes=3, console=con)
    with ep:
        ep.start_node("node_a")
        ep.complete_node("node_a", rows=100, duration=0.5)
        ep.start_node("node_b")
        ep.fail_node("node_b", error="timeout")
        ep.start_node("node_c")
        ep.complete_node("node_c")


def test_render_summary_empty_dict():
    """render_summary({}) should still produce a valid panel."""
    from seeknal.ui.progress import render_summary

    panel = render_summary({})
    assert isinstance(panel, Panel)
    con = _capture_console()
    con.print(panel)
    output = con.file.getvalue()
    assert "0" in output
