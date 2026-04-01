"""Tests for seeknal.ui.output, tables, and panels modules."""

from io import StringIO

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from seeknal.ui.theme import DARK_THEME


def _capture_console() -> Console:
    """Return a Console that writes to a StringIO buffer."""
    return Console(file=StringIO(), theme=DARK_THEME, highlight=False)


# ---- output.py -----------------------------------------------------------


def test_echo_success_prints(monkeypatch):
    con = _capture_console()
    monkeypatch.setattr("seeknal.ui.output.get_console", lambda: con)

    from seeknal.ui.output import echo_success

    echo_success("done")
    output = con.file.getvalue()
    assert "done" in output


def test_echo_error_prints(monkeypatch):
    con = _capture_console()
    monkeypatch.setattr("seeknal.ui.output.get_console", lambda: con)

    from seeknal.ui.output import echo_error

    echo_error("fail")
    output = con.file.getvalue()
    assert "fail" in output


def test_echo_warning_prints(monkeypatch):
    con = _capture_console()
    monkeypatch.setattr("seeknal.ui.output.get_console", lambda: con)

    from seeknal.ui.output import echo_warning

    echo_warning("deprecated")
    output = con.file.getvalue()
    assert "deprecated" in output


def test_echo_info_prints(monkeypatch):
    con = _capture_console()
    monkeypatch.setattr("seeknal.ui.output.get_console", lambda: con)

    from seeknal.ui.output import echo_info

    echo_info("processing")
    output = con.file.getvalue()
    assert "processing" in output


# ---- tables.py -----------------------------------------------------------


def test_make_table_returns_table():
    from seeknal.ui.tables import make_table

    table = make_table(["Name"], [["Alice"]])
    assert isinstance(table, Table)


def test_make_table_with_title():
    from seeknal.ui.tables import make_table

    table = make_table(["Name", "Age"], [["Alice", "30"]], title="Users")
    assert isinstance(table, Table)
    assert table.title == "Users"


def test_print_table_runs(monkeypatch):
    con = _capture_console()
    monkeypatch.setattr("seeknal.ui.tables.get_console", lambda: con)

    from seeknal.ui.tables import print_table

    print_table(["Name"], [["Alice"]])
    output = con.file.getvalue()
    assert "Alice" in output


# ---- panels.py -----------------------------------------------------------


def test_code_panel_returns_panel():
    from seeknal.ui.panels import code_panel

    panel = code_panel("SELECT 1", "sql")
    assert isinstance(panel, Panel)


def test_info_panel_returns_panel():
    from seeknal.ui.panels import info_panel

    panel = info_panel("hello")
    assert isinstance(panel, Panel)


def test_success_panel_returns_panel():
    from seeknal.ui.panels import success_panel

    panel = success_panel("ok")
    assert isinstance(panel, Panel)


def test_error_panel_returns_panel():
    from seeknal.ui.panels import error_panel

    panel = error_panel("err")
    assert isinstance(panel, Panel)


def test_warning_panel_returns_panel():
    from seeknal.ui.panels import warning_panel

    panel = warning_panel("warn")
    assert isinstance(panel, Panel)


def test_summary_panel_returns_panel():
    from seeknal.ui.panels import summary_panel

    panel = summary_panel("stats")
    assert isinstance(panel, Panel)


def test_summary_panel_default_title():
    from seeknal.ui.panels import summary_panel

    panel = summary_panel("stats")
    assert panel.title == "Summary"


def test_code_panel_renders(monkeypatch):
    con = _capture_console()
    from seeknal.ui.panels import code_panel

    panel = code_panel("SELECT * FROM users", "sql", title="Query")
    con.print(panel)
    output = con.file.getvalue()
    assert "SELECT" in output
    assert "Query" in output
