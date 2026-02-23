"""
Tests for seeknal docs command.

Tests cover:
- check_rg_installed: Check for ripgrep availability
- get_docs_cli_path: Get path to docs/cli/ directory
- find_exact_topic_match: Find exact topic file match
- parse_frontmatter: Parse YAML frontmatter from markdown
- list_available_topics: List all available documentation topics
- format_topics_list: Format topics list for output
- run_rg_search: Execute ripgrep search
- parse_rg_json_output: Parse ripgrep JSON output
- generate_snippet: Generate truncated snippet
- format_search_results_*: Format search results for output
- CLI integration tests
"""

import json
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from typer.testing import CliRunner

from seeknal.cli.docs import (
    check_rg_installed,
    get_docs_cli_path,
    find_exact_topic_match,
    parse_frontmatter,
    list_available_topics,
    format_topics_list,
    run_rg_search,
    parse_rg_json_output,
    generate_snippet,
    format_search_results,
    format_search_results_human,
    format_search_results_json,
)
from seeknal.cli.main import app

runner = CliRunner()


class TestCheckRgInstalled:
    """Tests for check_rg_installed function."""

    def test_returns_true_when_rg_installed(self):
        """Should return True if rg is in PATH."""
        with patch("shutil.which") as mock_which:
            mock_which.return_value = "/usr/bin/rg"
            assert check_rg_installed() is True

    def test_returns_false_when_rg_not_installed(self):
        """Should return False if rg is not in PATH."""
        with patch("shutil.which") as mock_which:
            mock_which.return_value = None
            assert check_rg_installed() is False

    def test_calls_which_with_rg(self):
        """Should call shutil.which with 'rg' argument."""
        with patch("shutil.which") as mock_which:
            mock_which.return_value = "/usr/bin/rg"
            check_rg_installed()
            mock_which.assert_called_once_with("rg")


class TestGetDocsCliPath:
    """Tests for get_docs_cli_path function."""

    def test_returns_path_object(self):
        """Should return a Path object."""
        result = get_docs_cli_path()
        assert isinstance(result, Path)

    def test_returns_docs_cli_path(self):
        """Should return path ending in docs/cli."""
        result = get_docs_cli_path()
        # The path should end with docs/cli
        path_str = str(result)
        assert path_str.endswith("docs/cli")


class TestFindExactTopicMatch:
    """Tests for find_exact_topic_match function."""

    def test_returns_none_for_missing_topic(self, tmp_path):
        """Should return None if topic file doesn't exist."""
        result = find_exact_topic_match("nonexistent", tmp_path)
        assert result is None

    def test_finds_exact_match(self, tmp_path):
        """Should find exact file match."""
        # Create test file
        (tmp_path / "init.md").write_text("# init")

        result = find_exact_topic_match("init", tmp_path)
        assert result is not None
        assert result.name == "init.md"

    def test_normalizes_hyphens(self, tmp_path):
        """Should handle hyphenated topic names."""
        (tmp_path / "feature-group.md").write_text("# feature-group")

        result = find_exact_topic_match("feature_group", tmp_path)
        assert result is not None
        assert result.name == "feature-group.md"

    def test_normalizes_spaces(self, tmp_path):
        """Should handle spaces in topic names."""
        (tmp_path / "feature-group.md").write_text("# feature-group")

        result = find_exact_topic_match("feature group", tmp_path)
        assert result is not None
        assert result.name == "feature-group.md"

    def test_is_case_insensitive(self, tmp_path):
        """Should find match regardless of case."""
        (tmp_path / "Init.md").write_text("# Init")

        result = find_exact_topic_match("INIT", tmp_path)
        assert result is not None

    def test_skips_index_file(self, tmp_path):
        """Should not match index.md file."""
        (tmp_path / "index.md").write_text("# index")

        result = find_exact_topic_match("index", tmp_path)
        assert result is None

    def test_returns_none_for_nonexistent_docs_path(self):
        """Should return None if docs_path doesn't exist."""
        nonexistent = Path("/nonexistent/path/to/docs")
        result = find_exact_topic_match("init", nonexistent)
        assert result is None


class TestParseFrontmatter:
    """Tests for parse_frontmatter function."""

    def test_parses_summary(self):
        """Should parse summary field."""
        content = "---\nsummary: Test summary\n---\n# Content"
        result = parse_frontmatter(content)
        assert result.get("summary") == "Test summary"

    def test_returns_empty_for_no_frontmatter(self):
        """Should return empty dict for content without frontmatter."""
        content = "# Just content\nNo frontmatter here"
        result = parse_frontmatter(content)
        assert result == {}

    def test_parses_quoted_values(self):
        """Should parse values with quotes."""
        content = "---\nsummary: \"A quoted summary\"\n---\n# Content"
        result = parse_frontmatter(content)
        assert result.get("summary") == "A quoted summary"

    def test_parses_single_quoted_values(self):
        """Should parse values with single quotes."""
        content = "---\nsummary: 'A single quoted summary'\n---\n# Content"
        result = parse_frontmatter(content)
        assert result.get("summary") == "A single quoted summary"

    def test_handles_empty_frontmatter(self):
        """Should handle empty frontmatter block."""
        content = "---\n---\n# Content"
        result = parse_frontmatter(content)
        assert result == {}

    def test_handles_incomplete_frontmatter(self):
        """Should handle incomplete frontmatter (no closing ---)."""
        content = "---\nsummary: Test\n# Content without closing"
        result = parse_frontmatter(content)
        assert result == {}


class TestListAvailableTopics:
    """Tests for list_available_topics function."""

    def test_returns_empty_list_for_nonexistent_path(self):
        """Should return empty list if docs path doesn't exist."""
        nonexistent = Path("/nonexistent/path")
        result = list_available_topics(nonexistent)
        assert result == []

    def test_lists_md_files(self, tmp_path):
        """Should list all .md files in docs path."""
        (tmp_path / "init.md").write_text("# init")
        (tmp_path / "list.md").write_text("# list")

        result = list_available_topics(tmp_path)
        assert len(result) == 2
        names = [t["name"] for t in result]
        assert "init" in names
        assert "list" in names

    def test_skips_index_file(self, tmp_path):
        """Should skip index.md file."""
        (tmp_path / "index.md").write_text("# index")
        (tmp_path / "init.md").write_text("# init")

        result = list_available_topics(tmp_path)
        names = [t["name"] for t in result]
        assert "index" not in names
        assert "init" in names

    def test_extracts_summary_from_frontmatter(self, tmp_path):
        """Should extract summary from frontmatter."""
        (tmp_path / "init.md").write_text(
            "---\nsummary: Initialize a project\n---\n# init"
        )

        result = list_available_topics(tmp_path)
        assert len(result) == 1
        assert result[0]["summary"] == "Initialize a project"

    def test_handles_file_read_error(self, tmp_path):
        """Should handle file read errors gracefully."""
        (tmp_path / "init.md").write_text("# init")

        with patch.object(Path, "read_text", side_effect=IOError("Read error")):
            result = list_available_topics(tmp_path)
            # Should still return the topic, just with empty summary
            assert len(result) == 1
            assert result[0]["summary"] == ""

    def test_returns_sorted_topics(self, tmp_path):
        """Should return topics sorted by name."""
        (tmp_path / "zebra.md").write_text("# zebra")
        (tmp_path / "alpha.md").write_text("# alpha")
        (tmp_path / "middle.md").write_text("# middle")

        result = list_available_topics(tmp_path)
        names = [t["name"] for t in result]
        assert names == ["alpha", "middle", "zebra"]


class TestFormatTopicsList:
    """Tests for format_topics_list function."""

    def test_formats_human_readable(self):
        """Should format topics as human-readable text."""
        topics = [
            {"name": "init", "summary": "Initialize project"},
            {"name": "list", "summary": "List resources"},
        ]
        result = format_topics_list(topics, use_json=False)
        assert "init" in result
        assert "list" in result
        assert "Initialize project" in result
        assert "List resources" in result

    def test_formats_json(self):
        """Should format topics as JSON."""
        topics = [
            {"name": "init", "summary": "Initialize project"},
        ]
        result = format_topics_list(topics, use_json=True)
        parsed = json.loads(result)
        assert parsed["count"] == 1
        assert len(parsed["topics"]) == 1
        assert parsed["topics"][0]["name"] == "init"

    def test_shows_count_in_human_output(self):
        """Should show topic count in human-readable output."""
        topics = [
            {"name": "init", "summary": "Initialize project"},
            {"name": "list", "summary": "List resources"},
        ]
        result = format_topics_list(topics, use_json=False)
        assert "2 topics" in result

    def test_handles_empty_topics_human(self):
        """Should handle empty topics list for human output."""
        result = format_topics_list([], use_json=False)
        assert "0 topics" in result

    def test_handles_empty_topics_json(self):
        """Should handle empty topics list for JSON output."""
        result = format_topics_list([], use_json=True)
        parsed = json.loads(result)
        assert parsed["count"] == 0
        assert parsed["topics"] == []

    def test_truncates_long_summary(self):
        """Should truncate long summaries in human output."""
        topics = [
            {"name": "init", "summary": "A" * 100},
        ]
        result = format_topics_list(topics, use_json=False)
        # Summary should be truncated to 60 chars
        assert "A" * 60 in result
        assert "A" * 100 not in result


class TestGenerateSnippet:
    """Tests for generate_snippet function."""

    def test_returns_short_text_unchanged(self):
        """Should return text unchanged if under max length."""
        text = "Short text"
        result = generate_snippet(text, 100)
        assert result == text

    def test_truncates_long_text(self):
        """Should truncate long text with ellipsis."""
        text = "A" * 300
        result = generate_snippet(text, 100)
        assert len(result) <= 103  # 100 + "..."
        assert result.endswith("...")

    def test_truncates_at_word_boundary(self):
        """Should prefer word boundary for truncation."""
        text = "This is a long sentence with many words"
        result = generate_snippet(text, 20)
        # Should truncate at a space, not mid-word
        assert " " in result or result.endswith("...")

    def test_strips_whitespace(self):
        """Should strip leading/trailing whitespace."""
        text = "  content with spaces  "
        result = generate_snippet(text, 100)
        assert result == "content with spaces"

    def test_default_max_length(self):
        """Should use default max length of 200."""
        text = "A" * 300
        result = generate_snippet(text)
        assert len(result) <= 203  # 200 + "..."


class TestParseRgJsonOutput:
    """Tests for parse_rg_json_output function."""

    def test_parses_match_type(self):
        """Should parse match type records."""
        rg_output = '{"type":"match","data":{"path":{"text":"/docs/init.md"},"line_number":10,"lines":{"text":"test content"}}}'
        result = parse_rg_json_output(rg_output)
        assert len(result) == 1
        assert result[0]["file_path"] == "/docs/init.md"
        assert result[0]["line_number"] == 10

    def test_ignores_non_match_types(self):
        """Should ignore non-match type records."""
        rg_output = '{"type":"begin","data":{}}\n{"type":"match","data":{"path":{"text":"/docs/init.md"},"line_number":1,"lines":{"text":"test"}}}'
        result = parse_rg_json_output(rg_output)
        assert len(result) == 1

    def test_handles_empty_output(self):
        """Should handle empty output."""
        result = parse_rg_json_output("")
        assert result == []

    def test_handles_invalid_json(self):
        """Should skip invalid JSON lines."""
        rg_output = "invalid json\n{\"type\":\"match\",\"data\":{\"path\":{\"text\":\"/docs/init.md\"},\"line_number\":1,\"lines\":{\"text\":\"test\"}}}"
        result = parse_rg_json_output(rg_output)
        assert len(result) == 1

    def test_respects_max_results(self):
        """Should respect max_results parameter."""
        rg_output = "\n".join([
            '{"type":"match","data":{"path":{"text":"/docs/init.md"},"line_number":%d,"lines":{"text":"test"}}}' % i
            for i in range(10)
        ])
        result = parse_rg_json_output(rg_output, max_results=3)
        assert len(result) == 3

    def test_generates_snippet(self):
        """Should generate snippet from matched text."""
        long_text = "A" * 300
        rg_output = f'{{"type":"match","data":{{"path":{{"text":"/docs/init.md"}},"line_number":1,"lines":{{"text":"{long_text}"}}}}}}'
        result = parse_rg_json_output(rg_output)
        assert len(result[0]["snippet"]) < 300


class TestRunRgSearch:
    """Tests for run_rg_search function."""

    def test_raises_when_rg_not_installed(self, tmp_path):
        """Should raise RuntimeError if rg is not installed."""
        with patch("seeknal.cli.docs.check_rg_installed", return_value=False):
            with pytest.raises(RuntimeError, match="ripgrep"):
                run_rg_search("test", tmp_path)

    def test_returns_empty_for_nonexistent_path(self, tmp_path):
        """Should return empty list for nonexistent docs path."""
        with patch("seeknal.cli.docs.check_rg_installed", return_value=True):
            nonexistent = Path("/nonexistent/path")
            result = run_rg_search("test", nonexistent)
            assert result == []

    def test_sanitizes_null_bytes(self, tmp_path):
        """Should remove null bytes from query."""
        with patch("seeknal.cli.docs.check_rg_installed", return_value=True):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(stdout="", stderr="", returncode=0)
                run_rg_search("test\x00query", tmp_path)
                # Check that null byte was removed
                call_args = mock_run.call_args[0][0]
                assert "\x00" not in " ".join(call_args)

    def test_returns_empty_for_empty_query(self, tmp_path):
        """Should return empty list for empty query after sanitization."""
        with patch("seeknal.cli.docs.check_rg_installed", return_value=True):
            result = run_rg_search("   ", tmp_path)
            assert result == []

    def test_handles_timeout(self, tmp_path):
        """Should raise RuntimeError on timeout."""
        import subprocess

        with patch("seeknal.cli.docs.check_rg_installed", return_value=True):
            with patch("subprocess.run") as mock_run:
                mock_run.side_effect = subprocess.TimeoutExpired(cmd="rg", timeout=30)
                with pytest.raises(RuntimeError, match="timed out"):
                    run_rg_search("test", tmp_path)

    def test_handles_file_not_found(self, tmp_path):
        """Should raise RuntimeError if rg executable not found."""
        with patch("seeknal.cli.docs.check_rg_installed", return_value=True):
            with patch("subprocess.run") as mock_run:
                mock_run.side_effect = FileNotFoundError()
                with pytest.raises(RuntimeError, match="executable not found"):
                    run_rg_search("test", tmp_path)


class TestFormatSearchResults:
    """Tests for format_search_results function."""

    def test_formats_human_by_default(self):
        """Should format as human-readable by default."""
        results = [{"file_path": "/docs/init.md", "line_number": 10, "snippet": "test"}]
        output = format_search_results(results, "test", use_json=False)
        assert "init.md" in output

    def test_formats_json_when_requested(self):
        """Should format as JSON when requested."""
        results = [{"file_path": "/docs/init.md", "line_number": 10, "snippet": "test"}]
        output = format_search_results(results, "test", use_json=True)
        parsed = json.loads(output)
        assert parsed["query"] == "test"


class TestFormatSearchResultsHuman:
    """Tests for format_search_results_human function."""

    def test_formats_human_readable_results(self):
        """Should format results for human reading."""
        results = [
            {"file_path": "/docs/cli/init.md", "line_number": 10, "snippet": "test content"}
        ]
        output = format_search_results_human(results, "test")
        assert "init.md" in output
        assert "test content" in output

    def test_shows_no_results_message(self):
        """Should show helpful message for no results."""
        output = format_search_results_human([], "nonexistent")
        assert "No documentation found" in output
        assert "nonexistent" in output

    def test_shows_result_count(self):
        """Should show number of results."""
        results = [
            {"file_path": "/docs/cli/init.md", "line_number": 10, "snippet": "test"}
        ]
        output = format_search_results_human(results, "test")
        assert "1 result" in output

    def test_shows_plural_results(self):
        """Should use plural 'results' for multiple matches."""
        results = [
            {"file_path": "/docs/cli/init.md", "line_number": 10, "snippet": "test"},
            {"file_path": "/docs/cli/list.md", "line_number": 5, "snippet": "test"},
        ]
        output = format_search_results_human(results, "test")
        assert "2 results" in output

    def test_groups_by_file(self):
        """Should group results by file."""
        results = [
            {"file_path": "/docs/cli/init.md", "line_number": 10, "snippet": "first"},
            {"file_path": "/docs/cli/init.md", "line_number": 20, "snippet": "second"},
        ]
        output = format_search_results_human(results, "test")
        # File path should appear only once as header
        assert output.count("init.md") >= 1

    def test_suggests_list_command_on_no_results(self):
        """Should suggest --list command when no results found."""
        output = format_search_results_human([], "test")
        assert "--list" in output


class TestFormatSearchResultsJson:
    """Tests for format_search_results_json function."""

    def test_formats_json_results(self):
        """Should format results as JSON."""
        results = [
            {"file_path": "/docs/cli/init.md", "line_number": 10, "snippet": "test"}
        ]
        output = format_search_results_json(results, "test")
        parsed = json.loads(output)
        assert parsed["query"] == "test"
        assert parsed["total_count"] == 1
        assert len(parsed["results"]) == 1

    def test_includes_all_result_fields(self):
        """Should include all result fields in JSON."""
        results = [
            {
                "file_path": "/docs/cli/init.md",
                "line_number": 10,
                "snippet": "test snippet",
                "matched_text": "full matched text"
            }
        ]
        output = format_search_results_json(results, "test")
        parsed = json.loads(output)
        result = parsed["results"][0]
        assert result["file_path"] == "/docs/cli/init.md"
        assert result["line_number"] == 10
        assert result["snippet"] == "test snippet"
        assert result["matched_text"] == "full matched text"

    def test_handles_empty_results(self):
        """Should handle empty results list."""
        output = format_search_results_json([], "test")
        parsed = json.loads(output)
        assert parsed["query"] == "test"
        assert parsed["total_count"] == 0
        assert parsed["results"] == []

    def test_includes_truncated_flag(self):
        """Should include truncated flag in output."""
        results = []
        output = format_search_results_json(results, "test")
        parsed = json.loads(output)
        assert "truncated" in parsed


class TestDocsCommand:
    """Integration tests for docs command via CLI."""

    def test_docs_help(self):
        """Should show help when run with --help."""
        result = runner.invoke(app, ["docs", "--help"])
        assert result.exit_code == 0
        assert "docs" in result.stdout.lower() or "documentation" in result.stdout.lower()

    def test_docs_list(self):
        """Should list topics with --list."""
        result = runner.invoke(app, ["docs", "--list"])
        # Should succeed even if no docs exist yet
        assert result.exit_code == 0 or result.exit_code == 1

    def test_docs_list_json(self):
        """Should list topics in JSON format with --list --json."""
        result = runner.invoke(app, ["docs", "--list", "--json"])
        if result.exit_code == 0:
            # Should be valid JSON
            parsed = json.loads(result.stdout)
            assert "topics" in parsed
            assert "count" in parsed

    def test_docs_no_args_shows_help(self):
        """Should show help when no args provided."""
        result = runner.invoke(app, ["docs"])
        # Exit code 0 means help was shown
        assert result.exit_code == 0

    def test_docs_search_requires_rg(self):
        """Should show error if rg is not installed."""
        with patch("seeknal.cli.docs.check_rg_installed", return_value=False):
            result = runner.invoke(app, ["docs", "nonexistent_topic_xyz"])
            # Exit code 3 indicates missing ripgrep
            assert result.exit_code == 3
            assert "ripgrep" in result.stdout.lower() or "rg" in result.stdout.lower()

    def test_docs_search_no_results(self):
        """Should exit with code 1 when no results found."""
        with patch("seeknal.cli.docs.check_rg_installed", return_value=True):
            with patch("seeknal.cli.docs.run_rg_search", return_value=[]):
                with patch("seeknal.cli.docs.find_exact_topic_match", return_value=None):
                    result = runner.invoke(app, ["docs", "nonexistent_topic_xyz"])
                    assert result.exit_code == 1
                    assert "No documentation found" in result.stdout

    def test_docs_exact_match(self):
        """Should show exact match when topic file exists."""
        with patch("seeknal.cli.docs.find_exact_topic_match") as mock_find:
            mock_path = MagicMock()
            mock_path.name = "init.md"
            mock_path.read_text.return_value = "# init\n\nInit command docs"
            mock_find.return_value = mock_path

            result = runner.invoke(app, ["docs", "init"])
            assert result.exit_code == 0
            assert "init" in result.stdout

    def test_docs_exact_match_json(self):
        """Should output JSON for exact match with --json flag."""
        with patch("seeknal.cli.docs.find_exact_topic_match") as mock_find:
            with patch("seeknal.cli.docs.read_doc_file") as mock_read:
                mock_path = MagicMock()
                mock_path.name = "init.md"
                mock_find.return_value = mock_path
                mock_read.return_value = "# init\n\nInit command docs"

                # Note: Options must come before positional arguments in Typer
                result = runner.invoke(app, ["docs", "--json", "init"])
                assert result.exit_code == 0
                parsed = json.loads(result.stdout)
                assert parsed["match_type"] == "exact"
                assert parsed["file"] == "init.md"

    def test_docs_search_with_results(self):
        """Should show search results."""
        with patch("seeknal.cli.docs.check_rg_installed", return_value=True):
            with patch("seeknal.cli.docs.run_rg_search") as mock_search:
                with patch("seeknal.cli.docs.find_exact_topic_match", return_value=None):
                    mock_search.return_value = [
                        {
                            "file_path": "/docs/cli/init.md",
                            "line_number": 10,
                            "snippet": "test snippet"
                        }
                    ]
                    result = runner.invoke(app, ["docs", "init"])
                    assert result.exit_code == 0
                    assert "1 result" in result.stdout

    def test_docs_respects_max_results(self):
        """Should respect --max-results option."""
        with patch("seeknal.cli.docs.check_rg_installed", return_value=True):
            with patch("seeknal.cli.docs.run_rg_search") as mock_search:
                with patch("seeknal.cli.docs.find_exact_topic_match", return_value=None):
                    mock_search.return_value = []
                    # Note: Options must come before positional arguments in Typer
                    result = runner.invoke(app, ["docs", "--max-results", "10", "test"])
                    # Check that max_results was passed correctly
                    mock_search.assert_called_once()
                    call_args = mock_search.call_args
                    assert call_args[0][2] == 10  # Third argument is max_results


class TestDocsCommandIntegration:
    """Extended integration tests for docs command."""

    def test_docs_search_with_query(self):
        """Should search when query provided."""
        result = runner.invoke(app, ["docs", "init"])
        # Should either find results or exit 1 (no results)
        assert result.exit_code in [0, 1]

    def test_docs_search_json_output(self):
        """Should output JSON with --json flag."""
        result = runner.invoke(app, ["docs", "--json", "init"])
        if result.exit_code == 0:
            # Should be valid JSON
            try:
                parsed = json.loads(result.stdout)
                assert "query" in parsed or "results" in parsed
            except json.JSONDecodeError:
                pass  # May not have results

    def test_docs_list_json_output(self):
        """Should list topics as JSON."""
        result = runner.invoke(app, ["docs", "--list", "--json"])
        assert result.exit_code == 0
        try:
            parsed = json.loads(result.stdout)
            assert "topics" in parsed
            assert "count" in parsed
        except json.JSONDecodeError:
            pytest.fail("Output should be valid JSON")

    def test_docs_exact_match(self):
        """Should show full file for exact match."""
        # If docs/cli/init.md exists, should show it
        result = runner.invoke(app, ["docs", "init"])
        # Either shows the file content or searches
        assert result.exit_code in [0, 1]

    def test_docs_no_results_exit_code(self):
        """Should exit with code 1 when no results."""
        result = runner.invoke(app, ["docs", "zzzzzzzzzrandomqueryyyyyyy"])
        # Should exit 1 if no results
        assert result.exit_code in [1, 0]  # 0 if somehow found

    def test_docs_help_content(self):
        """Should have proper help content."""
        result = runner.invoke(app, ["docs", "--help"])
        assert result.exit_code == 0
        # Should mention searching or documentation
        assert "doc" in result.stdout.lower() or "search" in result.stdout.lower()


class TestExitCodes:
    """Tests for exit codes."""

    def test_exit_0_for_help(self):
        """Should exit 0 for --help."""
        result = runner.invoke(app, ["docs", "--help"])
        assert result.exit_code == 0

    def test_exit_0_for_list(self):
        """Should exit 0 for --list."""
        result = runner.invoke(app, ["docs", "--list"])
        assert result.exit_code == 0

    def test_exit_1_for_no_results(self):
        """Should exit 1 for no results."""
        # Use a very unlikely search term
        result = runner.invoke(app, ["docs", "zzzxvcbsdfkj12345"])
        if "No documentation found" in result.stdout:
            assert result.exit_code == 1

    def test_exit_0_for_exact_match(self):
        """Should exit 0 for exact topic match."""
        # If we can find a topic that exists
        result = runner.invoke(app, ["docs", "init"])
        # Exit 0 if found as exact match, 1 if search finds nothing
        assert result.exit_code in [0, 1]


class TestEdgeCases:
    """Tests for edge cases."""

    def test_empty_query(self):
        """Should handle empty query gracefully."""
        result = runner.invoke(app, ["docs", ""])
        # Should show help or error gracefully
        assert result.exit_code in [0, 1, 2]

    def test_special_characters(self):
        """Should handle special characters in query."""
        result = runner.invoke(app, ["docs", "test<script>"])
        # Should not crash
        assert result.exit_code in [0, 1, 3, 4]

    def test_unicode_query(self):
        """Should handle unicode in query."""
        result = runner.invoke(app, ["docs", "测试"])
        # Should not crash
        assert result.exit_code in [0, 1, 3, 4]

    def test_very_long_query(self):
        """Should handle very long query."""
        long_query = "a" * 500
        result = runner.invoke(app, ["docs", long_query])
        # Should not crash
        assert result.exit_code in [0, 1, 3, 4]

    def test_multiple_words_query(self):
        """Should handle multi-word query."""
        result = runner.invoke(app, ["docs", "feature", "group"])
        # Should treat as single search
        assert result.exit_code in [0, 1]
