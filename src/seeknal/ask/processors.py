"""Multi-tier history compaction processors for Seeknal Ask.

Tier 1 — MicrocompactProcessor: Zero-cost, age-based clearing of old tool results.
Tier 2 — SqlResultCompactor: Zero-cost, domain-aware digests for large SQL tables.
Tier 3 — pydantic-deep's built-in context_manager (LLM summarization fallback).

Both processors implement the HistoryProcessor protocol:
    (messages: list[ModelMessage]) -> list[ModelMessage]
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass

from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    ToolReturnPart,
    UserPromptPart,
)

_CLEARED_PLACEHOLDER = "[Old tool result cleared — re-run tool if needed]"


def _count_user_turns(messages: list[ModelMessage]) -> int:
    """Count the number of user turns (ModelRequests containing UserPromptPart)."""
    count = 0
    for msg in messages:
        if isinstance(msg, ModelRequest):
            for part in msg.parts:
                if isinstance(part, UserPromptPart):
                    count += 1
                    break  # one UserPromptPart per message is enough
    return count


def _get_turn_index(messages: list[ModelMessage], msg_idx: int) -> int:
    """Return the user-turn index that a message at msg_idx belongs to.

    A "turn" starts at each ModelRequest with a UserPromptPart. Messages
    between two user prompts (e.g. tool results, model responses) belong
    to the preceding turn.
    """
    current_turn = -1
    for i, msg in enumerate(messages):
        if isinstance(msg, ModelRequest):
            for part in msg.parts:
                if isinstance(part, UserPromptPart):
                    current_turn += 1
                    break
        if i == msg_idx:
            return max(current_turn, 0)
    return max(current_turn, 0)


@dataclass
class MicrocompactProcessor:
    """Tier 1: Replace old tool results with a short placeholder.

    Scans messages for ToolReturnPart elements. For those belonging to
    turns older than the last ``keep_recent_turns`` user turns, replaces
    content with a cleared placeholder.

    This is zero-cost (no LLM calls) and prevents old, large tool outputs
    from consuming context budget.
    """

    keep_recent_turns: int = 3

    def __call__(self, messages: list[ModelMessage]) -> list[ModelMessage]:
        total_turns = _count_user_turns(messages)
        cutoff_turn = total_turns - self.keep_recent_turns

        if cutoff_turn <= 0:
            # All messages are recent enough — nothing to compact.
            return messages

        result: list[ModelMessage] = []

        for idx, msg in enumerate(messages):
            if not isinstance(msg, ModelRequest):
                result.append(msg)
                continue

            turn = _get_turn_index(messages, idx)
            if turn >= cutoff_turn:
                # Recent turn — keep as-is.
                result.append(msg)
                continue

            # Old turn — check for ToolReturnParts to clear.
            new_parts = []
            modified = False
            for part in msg.parts:
                if isinstance(part, ToolReturnPart):
                    content_str = part.content if isinstance(part.content, str) else str(part.content)
                    if content_str != _CLEARED_PLACEHOLDER:
                        new_parts.append(
                            ToolReturnPart(
                                tool_name=part.tool_name,
                                content=_CLEARED_PLACEHOLDER,
                                tool_call_id=part.tool_call_id,
                                timestamp=part.timestamp,
                            )
                        )
                        modified = True
                    else:
                        new_parts.append(part)
                else:
                    new_parts.append(part)

            if modified:
                result.append(
                    ModelRequest(
                        parts=new_parts,
                        timestamp=msg.timestamp,
                        instructions=msg.instructions,
                    )
                )
            else:
                result.append(msg)

        return result


# ---------------------------------------------------------------------------
# Tier 2: SQL Result Compactor
# ---------------------------------------------------------------------------

# Pattern: markdown table row (starts with |, contains at least one more |)
_TABLE_ROW_RE = re.compile(r"^\|.+\|", re.MULTILINE)
# Pattern: separator row like |---|---|
_SEPARATOR_RE = re.compile(r"^\|[\s\-:]+\|", re.MULTILINE)
# Pattern: row count footer like "(25 rows)"
_ROW_COUNT_FOOTER_RE = re.compile(r"\((\d+)\s+rows?\)")


def _extract_table_digest(table_text: str) -> str | None:
    """Extract a compact digest from a markdown table.

    Returns None if the text doesn't look like a markdown table.
    """
    lines = table_text.strip().splitlines()

    # Find table start: look for header row followed by separator
    table_start = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if _TABLE_ROW_RE.match(stripped) and i + 1 < len(lines) and _SEPARATOR_RE.match(lines[i + 1].strip()):
            table_start = i
            break

    if table_start is None:
        return None

    # Extract column names from header
    header_line = lines[table_start].strip()
    columns = [col.strip() for col in header_line.split("|") if col.strip()]

    if not columns:
        return None

    # Count data rows (lines after separator that match table row pattern)
    data_rows = 0
    for line in lines[table_start + 2:]:
        stripped = line.strip()
        if _TABLE_ROW_RE.match(stripped):
            data_rows += 1
        elif not stripped:
            continue
        else:
            break

    # Check for row count footer
    footer_match = _ROW_COUNT_FOOTER_RE.search(table_text)
    if footer_match:
        row_count = int(footer_match.group(1))
    else:
        row_count = data_rows

    return (
        "[SQL Result Digest]\n"
        f"Columns: {', '.join(columns)}\n"
        f"Rows: {row_count}"
    )


def _compact_sql_content(content: str) -> str:
    """Replace markdown tables in content with digests, preserving surrounding text."""
    # Find contiguous blocks of table lines
    lines = content.splitlines(keepends=True)
    result_parts: list[str] = []
    table_block: list[str] = []
    in_table = False

    for line in lines:
        stripped = line.strip()
        is_table_line = bool(_TABLE_ROW_RE.match(stripped)) or bool(_SEPARATOR_RE.match(stripped))
        is_empty = not stripped

        if is_table_line:
            if not in_table:
                in_table = True
            table_block.append(line)
        elif in_table and is_empty:
            # Empty line might be within a table block — accumulate but
            # don't commit yet.
            table_block.append(line)
        else:
            if in_table:
                # End of table block — try to digest it.
                block_text = "".join(table_block)
                digest = _extract_table_digest(block_text)
                if digest is not None:
                    result_parts.append(digest + "\n")
                else:
                    result_parts.append(block_text)
                table_block = []
                in_table = False
            result_parts.append(line)

    # Flush any remaining table block
    if table_block:
        block_text = "".join(table_block)
        digest = _extract_table_digest(block_text)
        if digest is not None:
            result_parts.append(digest + "\n")
        else:
            result_parts.append(block_text)

    return "".join(result_parts)


@dataclass
class SqlResultCompactor:
    """Tier 2: Replace large SQL markdown tables with compact digests.

    Scans ToolReturnPart elements for markdown table patterns. Only processes
    parts that are still in the recent window (not cleared by Tier 1) and
    whose content exceeds ``min_chars``.

    Preserves any non-table text (e.g. agent interpretation, SQL query text)
    before/after the table.
    """

    min_chars: int = int(os.environ.get("SEEKNAL_SQL_RESULT_COMPACT_MIN_CHARS", "10000"))

    def __call__(self, messages: list[ModelMessage]) -> list[ModelMessage]:
        result: list[ModelMessage] = []

        for msg in messages:
            if not isinstance(msg, ModelRequest):
                result.append(msg)
                continue

            new_parts = []
            modified = False

            for part in msg.parts:
                if not isinstance(part, ToolReturnPart):
                    new_parts.append(part)
                    continue

                content_str = part.content if isinstance(part.content, str) else str(part.content)

                # Skip already-cleared parts
                if content_str == _CLEARED_PLACEHOLDER:
                    new_parts.append(part)
                    continue

                # Skip small results
                if len(content_str) < self.min_chars:
                    new_parts.append(part)
                    continue

                # Check if content has a markdown table
                if not _TABLE_ROW_RE.search(content_str):
                    new_parts.append(part)
                    continue

                compacted = _compact_sql_content(content_str)

                # Only replace if compaction actually reduced size
                if len(compacted) < len(content_str):
                    new_parts.append(
                        ToolReturnPart(
                            tool_name=part.tool_name,
                            content=compacted,
                            tool_call_id=part.tool_call_id,
                            timestamp=part.timestamp,
                        )
                    )
                    modified = True
                else:
                    new_parts.append(part)

            if modified:
                result.append(
                    ModelRequest(
                        parts=new_parts,
                        timestamp=msg.timestamp,
                        instructions=msg.instructions,
                    )
                )
            else:
                result.append(msg)

        return result
