"""Visual tests for streaming output helpers — verify panels, syntax, tables."""

from seeknal.ask.streaming import (
    _show_answer,
    _show_reasoning,
    _show_sql,
    _show_sql_result_table,
    _show_tool_start,
)
from tests.ui.pyte_helpers import (
    SUCCESS,
    assert_text_present,
    console_to_screen,
    get_fg_colors_for_text,
    make_capturing_console,
)


def _render_helper(fn, *args, width=80, height=20):
    """Call a streaming helper with a capturing console and return a pyte Screen."""
    console = make_capturing_console(width=width)
    fn(console, *args)
    return console_to_screen(console, width=width, height=height)


class TestReasoningPanel:
    def test_content_rendered(self):
        screen = _render_helper(_show_reasoning, "thinking about data")
        assert_text_present(screen, "thinking about data")

    def test_content_present(self):
        screen = _render_helper(_show_reasoning, "analyzing the schema")
        assert_text_present(screen, "analyzing the schema")


class TestAnswerPanel:
    def test_title_present(self):
        screen = _render_helper(_show_answer, "The answer is 42")
        assert_text_present(screen, "Answer")

    def test_border_is_green(self):
        screen = _render_helper(_show_answer, "The answer is 42")
        from tests.ui.pyte_helpers import find_char

        positions = find_char(screen, "\u256d")  # ╭
        assert positions, "No panel border found"
        row, col = positions[0]
        assert screen.buffer[row][col].fg == SUCCESS

    def test_content_present(self):
        screen = _render_helper(_show_answer, "The data has 10 rows.")
        assert_text_present(screen, "10 rows")


class TestToolStart:
    def test_tool_name_appears(self):
        screen = _render_helper(_show_tool_start, "execute_sql", {"sql": "SELECT 1"})
        assert_text_present(screen, "execute_sql")

    def test_tool_name_is_bold(self):
        screen = _render_helper(_show_tool_start, "execute_sql", {"sql": "SELECT 1"})
        from tests.ui.pyte_helpers import find_text

        positions = find_text(screen, "execute_sql")
        assert positions
        row, col = positions[0]
        assert screen.buffer[row][col].bold is True


class TestSqlHighlighting:
    def test_sql_keyword_has_color(self):
        screen = _render_helper(_show_sql, "SELECT * FROM users WHERE id = 1")
        assert_text_present(screen, "SELECT")
        colors = get_fg_colors_for_text(screen, "SELECT")
        assert colors != {"default"}, f"Expected syntax highlighting, got {colors}"

    def test_sql_content_present(self):
        screen = _render_helper(_show_sql, "SELECT name FROM users")
        assert_text_present(screen, "users")


class TestSqlResultTable:
    def test_table_data_renders(self):
        md_table = "| name  | age |\n|-------|-----|\n| Alice | 30  |\n| Bob   | 25  |"
        screen = _render_helper(_show_sql_result_table, md_table, height=15)
        assert_text_present(screen, "Alice")
        assert_text_present(screen, "Bob")

    def test_table_headers_render(self):
        md_table = "| name  | age |\n|-------|-----|\n| Alice | 30  |"
        screen = _render_helper(_show_sql_result_table, md_table, height=15)
        assert_text_present(screen, "name")
        assert_text_present(screen, "age")
