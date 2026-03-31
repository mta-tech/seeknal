"""Answer quality gate for seeknal ask.

Validates that the agent's final answer contains specific data (numbers,
percentages, dollar amounts) or is a valid explanation answer. This is
output validation — like a linter on the answer — not intent routing.
"""

import re

# Matches numbers in data context: "50 customers", "$1,234", "42%", "3.14"
_DATA_PATTERN = re.compile(
    r'\b\d[\d,]*\.?\d*\s*[%$]?'
    r'|\$\s*[\d,]+\.?\d*'
    r'|\b\d+\.\d+\b'
)

# Explanation context keywords -- answers about lineage/methodology don't need numbers
_EXPLANATION_KEYWORDS = {
    "pipeline", "transform", "lineage", "defined", "calculated",
    "produced", "flow",
}


def check_answer_quality(answer: str) -> tuple[bool, str]:
    """Check if answer contains specific data or is an explanation.

    Returns:
        (passes, reason) where passes is True if the answer meets quality
        standards, and reason explains why it failed (empty string on pass).
    """
    if not answer or len(answer) < 50:
        return False, "Answer is too short to be useful"

    # Check for numeric data
    if _DATA_PATTERN.search(answer):
        return True, ""

    # Check for explanation context
    answer_lower = answer.lower()
    if any(kw in answer_lower for kw in _EXPLANATION_KEYWORDS):
        return True, ""

    return False, "Answer lacks specific data — cite numbers from your query results"
