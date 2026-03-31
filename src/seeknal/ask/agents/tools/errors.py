"""Structured error taxonomy for seeknal ask agent tools.

Defines error categories and helpers to produce JSON-structured error
strings that the agent can parse for retry decisions and self-correction.
"""

import json
import re

# ---------------------------------------------------------------------------
# Error categories
# ---------------------------------------------------------------------------

RETRYABLE_SYNTAX = "retryable_syntax"
RETRYABLE_MISSING_REF = "retryable_missing_ref"
RETRYABLE_TYPE = "retryable_type"
TERMINAL_SECURITY = "terminal_security"
TERMINAL_CRASH = "terminal_crash"
TERMINAL_TIMEOUT = "terminal_timeout"

_RETRYABLE_CATEGORIES = frozenset({
    RETRYABLE_SYNTAX,
    RETRYABLE_MISSING_REF,
    RETRYABLE_TYPE,
})


def format_tool_error(
    category: str,
    message: str,
    hint: str | None = None,
) -> str:
    """Return a JSON string with structured error metadata.

    Args:
        category: One of the error category constants.
        message: The raw error message.
        hint: Optional hint for the agent on how to fix the issue.

    Returns:
        A JSON string like ``{"category": "...", "retryable": true, ...}``.
    """
    payload = {
        "category": category,
        "retryable": category in _RETRYABLE_CATEGORIES,
        "message": message,
        "hint": hint,
    }
    return json.dumps(payload)


# ---------------------------------------------------------------------------
# DuckDB error classification
# ---------------------------------------------------------------------------

# Patterns checked in order; first match wins.
_DUCKDB_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"Catalog Error", re.IGNORECASE), RETRYABLE_MISSING_REF),
    (re.compile(r"Binder Error.*column", re.IGNORECASE | re.DOTALL), RETRYABLE_MISSING_REF),
    (re.compile(r"Binder Error.*type", re.IGNORECASE | re.DOTALL), RETRYABLE_TYPE),
    (re.compile(r"Binder Error", re.IGNORECASE), RETRYABLE_MISSING_REF),
    (re.compile(r"Parser Error", re.IGNORECASE), RETRYABLE_SYNTAX),
    (re.compile(r"IO Error", re.IGNORECASE), TERMINAL_CRASH),
    (re.compile(r"OutOfMemoryError", re.IGNORECASE), TERMINAL_CRASH),
]


def classify_duckdb_error(error_msg: str) -> str:
    """Map a DuckDB error message to an error category.

    Uses regex matching against known DuckDB error prefixes.
    Falls back to ``RETRYABLE_SYNTAX`` (conservative — assume
    the agent can fix it).
    """
    for pattern, category in _DUCKDB_PATTERNS:
        if pattern.search(error_msg):
            return category
    return RETRYABLE_SYNTAX
