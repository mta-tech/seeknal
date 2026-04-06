"""Tests for multi-tier history compaction processors."""

import pytest

from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)

from seeknal.ask.processors import (
    MicrocompactProcessor,
    SqlResultCompactor,
    _CLEARED_PLACEHOLDER,
    _count_user_turns,
    _extract_table_digest,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _user_msg(text: str) -> ModelRequest:
    return ModelRequest(parts=[UserPromptPart(content=text)])


def _tool_return(
    content: str,
    tool_name: str = "execute_sql",
    call_id: str = "call_1",
) -> ModelRequest:
    return ModelRequest(parts=[ToolReturnPart(
        tool_name=tool_name,
        content=content,
        tool_call_id=call_id,
    )])


def _model_response(text: str = "ok") -> ModelResponse:
    return ModelResponse(parts=[TextPart(content=text)])


def _build_conversation(num_turns: int, tool_content: str = "some result") -> list[ModelMessage]:
    """Build a multi-turn conversation with tool calls each turn."""
    messages: list[ModelMessage] = []
    for i in range(num_turns):
        messages.append(_user_msg(f"Question {i}"))
        messages.append(_model_response(f"Let me check turn {i}"))
        messages.append(_tool_return(tool_content, call_id=f"call_{i}"))
        messages.append(_model_response(f"Answer for turn {i}"))
    return messages


SMALL_TABLE = """\
| city   | count |
|--------|-------|
| NYC    | 10    |
| LA     | 5     |
"""

LARGE_TABLE = "| city | count | revenue | growth | margin |\n|------|-------|---------|--------|--------|\n"
for i in range(30):
    LARGE_TABLE += f"| city_{i} | {i * 10} | {i * 100.5} | {i * 0.1:.1f} | {i * 0.05:.2f} |\n"
LARGE_TABLE += "\n(30 rows)"


# ---------------------------------------------------------------------------
# MicrocompactProcessor Tests
# ---------------------------------------------------------------------------


class TestCountUserTurns:
    def test_empty_messages(self):
        assert _count_user_turns([]) == 0

    def test_single_turn(self):
        msgs = [_user_msg("hello"), _model_response()]
        assert _count_user_turns(msgs) == 1

    def test_multiple_turns(self):
        msgs = _build_conversation(5)
        assert _count_user_turns(msgs) == 5

    def test_tool_returns_not_counted(self):
        msgs = [
            _user_msg("q1"),
            _tool_return("result"),
            _model_response(),
        ]
        assert _count_user_turns(msgs) == 1


class TestMicrocompactProcessor:
    def test_preserves_recent_turns(self):
        msgs = _build_conversation(3)
        processor = MicrocompactProcessor(keep_recent_turns=3)
        result = processor(msgs)
        # All 3 turns are recent — nothing should be cleared.
        for msg in result:
            if isinstance(msg, ModelRequest):
                for part in msg.parts:
                    if isinstance(part, ToolReturnPart):
                        assert part.content != _CLEARED_PLACEHOLDER

    def test_clears_old_tool_results(self):
        msgs = _build_conversation(5)
        processor = MicrocompactProcessor(keep_recent_turns=3)
        result = processor(msgs)

        # Turns 0 and 1 should be cleared (5 - 3 = cutoff at turn 2)
        cleared_ids = set()
        for msg in result:
            if isinstance(msg, ModelRequest):
                for part in msg.parts:
                    if isinstance(part, ToolReturnPart) and part.content == _CLEARED_PLACEHOLDER:
                        cleared_ids.add(part.tool_call_id)

        assert "call_0" in cleared_ids
        assert "call_1" in cleared_ids

    def test_preserves_recent_tool_results(self):
        msgs = _build_conversation(5)
        processor = MicrocompactProcessor(keep_recent_turns=3)
        result = processor(msgs)

        # Turns 2, 3, 4 should be preserved
        preserved_ids = set()
        for msg in result:
            if isinstance(msg, ModelRequest):
                for part in msg.parts:
                    if isinstance(part, ToolReturnPart) and part.content != _CLEARED_PLACEHOLDER:
                        preserved_ids.add(part.tool_call_id)

        assert "call_2" in preserved_ids
        assert "call_3" in preserved_ids
        assert "call_4" in preserved_ids

    def test_preserves_model_responses(self):
        msgs = _build_conversation(5)
        processor = MicrocompactProcessor(keep_recent_turns=3)
        result = processor(msgs)

        response_count = sum(1 for m in result if isinstance(m, ModelResponse))
        original_response_count = sum(1 for m in msgs if isinstance(m, ModelResponse))
        assert response_count == original_response_count

    def test_preserves_user_prompt_parts(self):
        msgs = _build_conversation(5)
        processor = MicrocompactProcessor(keep_recent_turns=3)
        result = processor(msgs)

        user_parts = []
        for msg in result:
            if isinstance(msg, ModelRequest):
                for part in msg.parts:
                    if isinstance(part, UserPromptPart):
                        user_parts.append(part.content)

        assert len(user_parts) == 5

    def test_no_mutation_of_originals(self):
        msgs = _build_conversation(5)
        # Capture original content
        original_contents = []
        for msg in msgs:
            if isinstance(msg, ModelRequest):
                for part in msg.parts:
                    if isinstance(part, ToolReturnPart):
                        original_contents.append(part.content)

        processor = MicrocompactProcessor(keep_recent_turns=3)
        processor(msgs)

        # Verify originals are untouched
        idx = 0
        for msg in msgs:
            if isinstance(msg, ModelRequest):
                for part in msg.parts:
                    if isinstance(part, ToolReturnPart):
                        assert part.content == original_contents[idx]
                        idx += 1

    def test_already_cleared_not_re_cleared(self):
        """Already-cleared placeholders should not be modified."""
        msgs = [
            _user_msg("q1"),
            ModelRequest(parts=[ToolReturnPart(
                tool_name="execute_sql",
                content=_CLEARED_PLACEHOLDER,
                tool_call_id="call_0",
            )]),
            _model_response(),
            _user_msg("q2"),
            _model_response(),
            _user_msg("q3"),
            _model_response(),
            _user_msg("q4"),
            _model_response(),
            _user_msg("q5"),
            _model_response(),
        ]
        processor = MicrocompactProcessor(keep_recent_turns=3)
        result = processor(msgs)
        # Should still be the placeholder, not double-processed
        for msg in result:
            if isinstance(msg, ModelRequest):
                for part in msg.parts:
                    if isinstance(part, ToolReturnPart):
                        assert part.content == _CLEARED_PLACEHOLDER

    def test_non_sql_tools_also_cleared(self):
        """MicrocompactProcessor clears ALL old tool results, not just SQL."""
        msgs = [
            _user_msg("q1"),
            _tool_return("file content here", tool_name="read_project_file", call_id="call_file_0"),
            _model_response(),
            _user_msg("q2"),
            _model_response(),
            _user_msg("q3"),
            _model_response(),
            _user_msg("q4"),
            _model_response(),
        ]
        processor = MicrocompactProcessor(keep_recent_turns=3)
        result = processor(msgs)

        for msg in result:
            if isinstance(msg, ModelRequest):
                for part in msg.parts:
                    if isinstance(part, ToolReturnPart) and part.tool_call_id == "call_file_0":
                        assert part.content == _CLEARED_PLACEHOLDER

    def test_custom_keep_recent(self):
        msgs = _build_conversation(5)
        processor = MicrocompactProcessor(keep_recent_turns=1)
        result = processor(msgs)

        # Only the last turn (turn 4) should be preserved
        cleared = []
        preserved = []
        for msg in result:
            if isinstance(msg, ModelRequest):
                for part in msg.parts:
                    if isinstance(part, ToolReturnPart):
                        if part.content == _CLEARED_PLACEHOLDER:
                            cleared.append(part.tool_call_id)
                        else:
                            preserved.append(part.tool_call_id)

        assert len(cleared) == 4
        assert preserved == ["call_4"]


# ---------------------------------------------------------------------------
# SQL Digest Extraction Tests
# ---------------------------------------------------------------------------


class TestExtractTableDigest:
    def test_basic_table(self):
        table = "| city | count |\n|------|-------|\n| NYC | 10 |\n| LA | 5 |\n"
        digest = _extract_table_digest(table)
        assert digest is not None
        assert "Columns: city, count" in digest
        assert "Rows: 2" in digest

    def test_with_row_count_footer(self):
        table = "| city | count |\n|------|-------|\n| NYC | 10 |\n\n(25 rows)"
        digest = _extract_table_digest(table)
        assert digest is not None
        assert "Rows: 25" in digest

    def test_no_table(self):
        text = "This is just plain text with no table."
        assert _extract_table_digest(text) is None

    def test_no_separator(self):
        text = "| city | count |\n| NYC | 10 |\n"
        assert _extract_table_digest(text) is None

    def test_digest_format(self):
        table = "| a | b | c |\n|---|---|---|\n| 1 | 2 | 3 |\n"
        digest = _extract_table_digest(table)
        assert digest is not None
        assert digest.startswith("[SQL Result Digest]")
        assert "Columns: a, b, c" in digest
        assert "Rows: 1" in digest


# ---------------------------------------------------------------------------
# SqlResultCompactor Tests
# ---------------------------------------------------------------------------


class TestSqlResultCompactor:
    def test_preserves_small_results(self):
        msgs = [
            _user_msg("q1"),
            _tool_return(SMALL_TABLE),
            _model_response(),
        ]
        processor = SqlResultCompactor(min_chars=500)
        result = processor(msgs)

        # Small table should be preserved
        for msg in result:
            if isinstance(msg, ModelRequest):
                for part in msg.parts:
                    if isinstance(part, ToolReturnPart):
                        assert part.content == SMALL_TABLE

    def test_compacts_large_tables(self):
        msgs = [
            _user_msg("q1"),
            _tool_return(LARGE_TABLE),
            _model_response(),
        ]
        processor = SqlResultCompactor(min_chars=500)
        result = processor(msgs)

        for msg in result:
            if isinstance(msg, ModelRequest):
                for part in msg.parts:
                    if isinstance(part, ToolReturnPart):
                        assert "[SQL Result Digest]" in part.content
                        assert "Columns: city, count, revenue, growth, margin" in part.content
                        assert "Rows: 30" in part.content

    def test_preserves_surrounding_text(self):
        content = f"Here is the query result:\n\n{LARGE_TABLE}\n\nThe data shows growth."
        msgs = [
            _user_msg("q1"),
            _tool_return(content),
            _model_response(),
        ]
        processor = SqlResultCompactor(min_chars=500)
        result = processor(msgs)

        for msg in result:
            if isinstance(msg, ModelRequest):
                for part in msg.parts:
                    if isinstance(part, ToolReturnPart):
                        assert "Here is the query result:" in part.content
                        assert "The data shows growth." in part.content
                        assert "[SQL Result Digest]" in part.content

    def test_skips_cleared_placeholders(self):
        msgs = [
            _user_msg("q1"),
            ModelRequest(parts=[ToolReturnPart(
                tool_name="execute_sql",
                content=_CLEARED_PLACEHOLDER,
                tool_call_id="call_0",
            )]),
            _model_response(),
        ]
        processor = SqlResultCompactor()
        result = processor(msgs)

        for msg in result:
            if isinstance(msg, ModelRequest):
                for part in msg.parts:
                    if isinstance(part, ToolReturnPart):
                        assert part.content == _CLEARED_PLACEHOLDER

    def test_skips_non_table_large_content(self):
        large_text = "x" * 1000  # Large but no table
        msgs = [
            _user_msg("q1"),
            _tool_return(large_text),
            _model_response(),
        ]
        processor = SqlResultCompactor(min_chars=500)
        result = processor(msgs)

        for msg in result:
            if isinstance(msg, ModelRequest):
                for part in msg.parts:
                    if isinstance(part, ToolReturnPart):
                        assert part.content == large_text

    def test_no_mutation_of_originals(self):
        original_content = LARGE_TABLE
        msgs = [
            _user_msg("q1"),
            _tool_return(original_content),
            _model_response(),
        ]
        processor = SqlResultCompactor(min_chars=500)
        processor(msgs)

        # Original message should be untouched
        for msg in msgs:
            if isinstance(msg, ModelRequest):
                for part in msg.parts:
                    if isinstance(part, ToolReturnPart):
                        assert part.content == original_content


# ---------------------------------------------------------------------------
# Combined Pipeline Tests
# ---------------------------------------------------------------------------


class TestCombinedProcessors:
    def test_microcompact_then_sql_snip(self):
        """Tier 1 clears old results, Tier 2 snips recent large results."""
        msgs = []
        # Turn 0: old, large SQL result
        msgs.append(_user_msg("q0"))
        msgs.append(_tool_return(LARGE_TABLE, call_id="old_call"))
        msgs.append(_model_response())
        # Turns 1-3: recent, with a large table on turn 3
        for i in range(1, 4):
            msgs.append(_user_msg(f"q{i}"))
            if i == 3:
                msgs.append(_tool_return(LARGE_TABLE, call_id="recent_large"))
            else:
                msgs.append(_tool_return("small result", call_id=f"recent_{i}"))
            msgs.append(_model_response())

        # Run Tier 1 then Tier 2
        tier1 = MicrocompactProcessor(keep_recent_turns=3)
        tier2 = SqlResultCompactor(min_chars=500)

        result = tier1(msgs)
        result = tier2(result)

        # Check: old_call should be cleared (Tier 1)
        # recent_large should be digested (Tier 2)
        # recent_1, recent_2 should be preserved
        for msg in result:
            if isinstance(msg, ModelRequest):
                for part in msg.parts:
                    if isinstance(part, ToolReturnPart):
                        if part.tool_call_id == "old_call":
                            assert part.content == _CLEARED_PLACEHOLDER
                        elif part.tool_call_id == "recent_large":
                            assert "[SQL Result Digest]" in part.content
                        elif part.tool_call_id in ("recent_1", "recent_2"):
                            assert part.content == "small result"

    def test_empty_messages(self):
        tier1 = MicrocompactProcessor()
        tier2 = SqlResultCompactor()
        assert tier1([]) == []
        assert tier2([]) == []
