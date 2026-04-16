"""Tests for seeknal ask streaming module.

Tests rendering helpers (no LLM needed) and basic function contracts.
Streaming integration tests require pydantic-ai's agent.iter() which
needs a real or mocked Agent — those are covered in QA/E2E tests.
"""

from io import StringIO
from pathlib import Path


from seeknal.ask.streaming import (
    _extract_report_paths,
    _format_chat_loop_error,
    _save_chat_error_log,
    _tool_spinner_message,
    _sanitize_output,
    _sanitize_user_input,
    _show_reasoning,
    _show_tool_start,
    _show_tool_end,
    _show_answer,
    _show_retry,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_console():
    """Create a Rich Console that captures output to a string."""
    from rich.console import Console
    from seeknal.ui.theme import DARK_THEME

    buf = StringIO()
    return Console(file=buf, force_terminal=False, width=120, theme=DARK_THEME), buf


# ---------------------------------------------------------------------------
# _sanitize_output tests
# ---------------------------------------------------------------------------


class TestSanitizeOutput:
    def test_strips_ansi_escape_codes(self):
        text = "hello \x1b[31mred\x1b[0m world"
        assert _sanitize_output(text) == "hello red world"

    def test_strips_screen_clear(self):
        text = "\x1b[2J\x1b[Hdangerous"
        assert _sanitize_output(text) == "dangerous"

    def test_preserves_clean_text(self):
        text = "clean text with no escapes"
        assert _sanitize_output(text) == text


class TestSanitizeUserInput:
    """Guard against terminal escape sequences leaking into chat questions.

    The production bug: IDE shortcuts (e.g. VS Code's 'Developer: Reload
    Window') and bracketed-paste wrapping got into ``input("You: ")`` as
    literal characters, so the agent saw 'giDeveloper: Reload Window'
    instead of 'gimana bisnis saya'. Sanitizer must drop all CSI sequences.
    """

    def test_strips_bracketed_paste_markers(self):
        text = "\x1b[200~hello world\x1b[201~"
        assert _sanitize_user_input(text) == "hello world"

    def test_strips_function_key_sequences(self):
        text = "gi\x1b[15~Developer: Reload Window"
        assert _sanitize_user_input(text) == "giDeveloper: Reload Window"

    def test_strips_sgr_color_codes(self):
        text = "hello \x1b[31mred\x1b[0m world"
        assert _sanitize_user_input(text) == "hello red world"

    def test_preserves_clean_text(self):
        text = "how many customers?"
        assert _sanitize_user_input(text) == text

    def test_handles_empty_string(self):
        assert _sanitize_user_input("") == ""

    def test_strips_cursor_movement(self):
        # \x1b[2A = move cursor up 2 lines
        text = "foo\x1b[2Abar"
        assert _sanitize_user_input(text) == "foobar"


class TestPublishToSeeknalReportActionRespectsProjectPath:
    """Regression: post-build publish action used Path.cwd() instead of
    ctx.project_path, so ``seeknal ask chat --project <elsewhere>`` looked
    up the build dir under the shell's cwd and always failed.
    """

    def test_uses_tool_context_project_path(self, tmp_path, monkeypatch):
        from unittest.mock import MagicMock
        from seeknal.ask.streaming import _publish_to_seeknal_report_action
        from seeknal.ask.agents.tools._context import ToolContext, set_tool_context

        # Point cwd at a DIFFERENT dir — the bug was that the action
        # resolved the build dir from cwd, so without the fix the test
        # would look at tmp_path/other instead of project_path.
        other_dir = tmp_path / "other"
        project_path = tmp_path / "project"
        build_dir = project_path / "target" / "reports" / "my-report" / "build"
        build_dir.mkdir(parents=True)
        (build_dir / "index.html").write_text("<html></html>")
        other_dir.mkdir()

        monkeypatch.chdir(other_dir)

        # Seed the tool context with the real project path
        ctx = ToolContext(
            repl=MagicMock(),
            artifact_discovery=MagicMock(),
            project_path=project_path,
        )
        set_tool_context(ctx)

        # Force "no server configured" so the action exits before the
        # network call, but only AFTER the build_dir resolution which
        # is what we want to test.
        monkeypatch.delenv("SEEKNAL_PUBLISH_SERVER", raising=False)
        monkeypatch.delenv("SEEKNAL_PUBLISH_TOKEN", raising=False)

        buf_console, out = make_console()
        _publish_to_seeknal_report_action(buf_console, "my-report")
        rendered = out.getvalue()

        # Should NOT print "Build directory not found" — i.e. the fix
        # resolved the path under project_path, not cwd.
        assert "Build directory not found" not in rendered, (
            f"Still using cwd instead of ctx.project_path:\n{rendered}"
        )


# ---------------------------------------------------------------------------
# Rendering helper tests
# ---------------------------------------------------------------------------


class TestRenderingHelpers:
    def test_show_reasoning(self):
        console, buf = make_console()
        _show_reasoning(console, "I need to find the table")
        output = buf.getvalue()
        assert "I need to find the table" in output

    def test_show_tool_start_generic(self):
        console, buf = make_console()
        _show_tool_start(console, "list_tables", {})
        output = buf.getvalue()
        assert "list_tables" in output

    def test_show_tool_start_execute_sql(self):
        console, buf = make_console()
        _show_tool_start(console, "execute_sql", {"sql": "SELECT 1"})
        output = buf.getvalue()
        assert "execute_sql" in output
        assert "SELECT" in output

    def test_show_tool_start_execute_python(self):
        console, buf = make_console()
        _show_tool_start(console, "execute_python", {"code": "print(1)"})
        output = buf.getvalue()
        assert "execute_python" in output
        assert "print" in output

    def test_show_tool_start_generate_report(self):
        console, buf = make_console()
        _show_tool_start(console, "generate_report", {"title": "My Report"})
        output = buf.getvalue()
        assert "generate_report" in output
        assert "My Report" in output

    def test_show_tool_end_generic(self):
        console, buf = make_console()
        _show_tool_end(console, "list_tables", "Available tables:\n- customers\n- orders")
        output = buf.getvalue()
        assert "Done" in output

    def test_show_tool_end_truncates_long_output(self):
        console, buf = make_console()
        long_output = "x" * 500
        _show_tool_end(console, "list_tables", long_output)
        output = buf.getvalue()
        assert "..." in output

    def test_show_tool_end_sanitizes_ansi(self):
        console, buf = make_console()
        _show_tool_end(console, "list_tables", "result \x1b[31mred\x1b[0m text")
        output = buf.getvalue()
        assert "\x1b[31m" not in output

    def test_show_answer(self):
        console, buf = make_console()
        _show_answer(console, "There are 5 customers.")
        output = buf.getvalue()
        assert "Answer" in output
        assert "5 customers" in output

    def test_show_retry(self):
        console, buf = make_console()
        _show_retry(console, 1, 3)
        output = buf.getvalue()
        assert "attempt 1/3" in output

    def test_show_tool_end_sql_result_table(self):
        """execute_sql output with markdown table gets rendered as Rich Table."""
        console, buf = make_console()
        md_table = (
            "| city | count |\n"
            "| --- | --- |\n"
            "| Jakarta | 14 |\n"
            "| Bandung | 12 |\n"
            "\n(2 rows)"
        )
        _show_tool_end(console, "execute_sql", md_table)
        output = buf.getvalue()
        assert "Jakarta" in output
        assert "Bandung" in output

    def test_show_tool_end_python_output(self):
        console, buf = make_console()
        _show_tool_end(console, "execute_python", "42")
        output = buf.getvalue()
        assert "42" in output

    def test_show_tool_end_python_plot(self):
        console, buf = make_console()
        _show_tool_end(console, "execute_python", "Plots saved: /tmp/plot.png")
        output = buf.getvalue()
        assert "Plots saved" in output

    def test_show_tool_end_report_success(self):
        console, buf = make_console()
        _show_tool_end(console, "generate_report", "Report built successfully: /tmp/report.html")
        output = buf.getvalue()
        assert "Report built" in output

    def test_show_tool_end_report_error(self):
        console, buf = make_console()
        _show_tool_end(console, "generate_report", "Error: npm not found")
        output = buf.getvalue()
        assert "Report Build Error" in output

    def test_extract_report_paths(self):
        output = (
            "Report built successfully!\n\n"
            "Open: /tmp/report/build/index.html\n"
            "Open in browser: /tmp/report/open_report.sh\n"
            "Fallback: python3 /tmp/report/open_report.py"
        )
        html_path, browser_path = _extract_report_paths(output)
        assert str(html_path) == "/tmp/report/build/index.html"
        assert str(browser_path) == "/tmp/report/open_report.sh"


    def test_tool_spinner_message_for_generate_report(self):
        assert _tool_spinner_message(
            "generate_report",
            {"title": "VIP Retention Strategy - Feb 2025"},
        ) == "Generating report: VIP Retention Strategy - Feb 2025"
        assert _tool_spinner_message("execute_sql", {}) is None

    def test_show_report_output_offers_open_action(self, monkeypatch):
        from seeknal.ask import streaming as streaming_module

        console, buf = make_console()
        launcher = Path('/tmp/open_report.sh')

        class FakeMenu:
            def __init__(self, *args, **kwargs):
                pass

            def run(self):
                return "Open in browser"

        popen_calls = []

        class FakePopen:
            def __init__(self, cmd, **kwargs):
                popen_calls.append((cmd, kwargs))

        monkeypatch.setattr(streaming_module.sys.stdout, 'isatty', lambda: True)
        monkeypatch.setattr(streaming_module.Path, 'exists', lambda self: str(self) == str(launcher), raising=False)
        monkeypatch.setattr('seeknal.ui.interactive_menu.InteractiveMenu', FakeMenu)
        monkeypatch.setattr(streaming_module.subprocess, 'Popen', FakePopen)

        streaming_module._show_report_output(
            console,
            "Report built successfully!\n\n"
            "Open: /tmp/report/build/index.html\n"
            f"Open in browser: {launcher}\n"
            "Fallback: python3 /tmp/report/open_report.py",
        )

        output = buf.getvalue()
        assert "Report built successfully" in output
        assert popen_calls
        assert popen_calls[0][0] == [str(launcher)]
        assert "Opened in browser via" in output


class TestFormatChatLoopError:
    """Regression tests for the chat-loop error formatter.

    Live verification caught an opaque `Error: [Errno 8] nodename nor servname
    provided, or not known` dump with no actionable context. The formatter
    now classifies network/DNS/timeout errors and emits structured hints.
    """

    def test_dns_error_macos_message_format(self):
        """The macOS gaierror string is the exact pattern from the bug
        report: '[Errno 8] nodename nor servname provided, or not known'."""
        import socket
        e = socket.gaierror(8, "nodename nor servname provided, or not known")
        msg = _format_chat_loop_error(e)
        assert "Network/DNS error" in msg
        assert "LLM provider" in msg
        assert "PROOF_BASE_URL" in msg or "SEEKNAL_PUBLISH_SERVER" in msg
        assert "gaierror" in msg

    def test_dns_error_linux_message_format(self):
        """Linux variant — 'Name or service not known'."""
        e = OSError("[Errno -2] Name or service not known")
        msg = _format_chat_loop_error(e)
        assert "Network/DNS error" in msg
        assert "Name or service not known" in msg

    def test_dns_error_via_string_match(self):
        """Even if the exception class is generic, string-match the
        platform-specific wording."""
        e = RuntimeError("[Errno 8] nodename nor servname provided, or not known")
        msg = _format_chat_loop_error(e)
        assert "Network/DNS error" in msg

    def test_connection_error_no_dns_match(self):
        """Plain ConnectionError that isn't a name-resolution failure."""
        e = ConnectionRefusedError("Connection refused")
        msg = _format_chat_loop_error(e)
        assert "Connection error" in msg
        assert "Network/DNS error" not in msg
        assert "host unreachable" in msg or "port closed" in msg

    def test_unknown_error_falls_back_to_raw(self):
        """Errors that don't match any classifier fall back to the raw
        string with the standard `Error:` prefix."""
        e = ValueError("Some unrelated value error from the agent")
        msg = _format_chat_loop_error(e)
        assert "Error: Some unrelated value error" in msg
        assert "Network/DNS error" not in msg
        assert "Connection error" not in msg

    def test_includes_log_path_when_provided(self):
        """If the caller passes a log path, the message points the user
        at it for the full traceback."""
        e = RuntimeError("boom")
        msg = _format_chat_loop_error(e, log_path=Path("/tmp/test-error.log"))
        assert "/tmp/test-error.log" in msg
        assert "Full traceback saved to" in msg

    def test_omits_log_path_when_none(self):
        e = RuntimeError("boom")
        msg = _format_chat_loop_error(e, log_path=None)
        assert "Full traceback" not in msg

    def test_dns_error_shows_raw_for_diagnosis(self):
        """Even when classified, the raw error text MUST appear so the
        user can copy-paste it for debugging."""
        import socket
        e = socket.gaierror(8, "nodename nor servname provided, or not known")
        msg = _format_chat_loop_error(e)
        assert "[Errno 8]" in msg or "nodename" in msg


class TestSaveChatErrorLog:
    """Regression tests for the chat error-log saver."""

    def test_writes_traceback_to_file(self, tmp_path, monkeypatch):
        """The saver writes type, str, and full traceback to the log."""
        monkeypatch.setenv("HOME", str(tmp_path))

        try:
            raise RuntimeError("test boom")
        except RuntimeError as e:
            log_path = _save_chat_error_log(e)

        assert log_path is not None
        assert log_path.exists()
        content = log_path.read_text()
        assert "RuntimeError: test boom" in content
        assert "test_writes_traceback_to_file" in content  # frame name
        assert "Traceback" in content

    def test_appends_multiple_errors_in_one_session(self, tmp_path, monkeypatch):
        """Multiple errors in a single session all get captured (append, not overwrite)."""
        monkeypatch.setenv("HOME", str(tmp_path))

        try:
            raise ValueError("first error")
        except ValueError as e:
            _save_chat_error_log(e)
        try:
            raise KeyError("second error")
        except KeyError as e:
            log_path = _save_chat_error_log(e)

        content = log_path.read_text()
        assert "ValueError: first error" in content
        assert "KeyError: 'second error'" in content

    def test_returns_none_silently_on_write_failure(self, tmp_path, monkeypatch):
        """If the log dir can't be created (e.g. read-only HOME), the saver
        returns None instead of crashing the chat loop."""
        # Point HOME at a path we can't write to.
        bad_home = tmp_path / "nope" / "deeper"
        bad_home.parent.mkdir()
        bad_home.parent.chmod(0o500)  # read+execute, no write
        monkeypatch.setenv("HOME", str(bad_home))

        try:
            raise RuntimeError("can't log this")
        except RuntimeError as e:
            log_path = _save_chat_error_log(e)

        # Best-effort: returns None on failure, no exception raised.
        # On some platforms the read-only chmod won't actually block the
        # write (e.g. running as root in CI), so we accept either outcome.
        assert log_path is None or log_path.exists()
        # Restore perms so pytest can clean up
        bad_home.parent.chmod(0o700)


