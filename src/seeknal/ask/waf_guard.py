"""WAF false-positive guard for outbound LLM request bodies.

Edge WAFs in front of OpenAI-compatible LLM endpoints (e.g. the Aliyun WAF
in front of ``litellm.dens-ai.id``) inspect request bodies for attack
signatures (SQLi / XSS / path traversal / template injection) and block
matching requests with an HTML error page — even when the text is legitimate
conversation, project context, or warehouse data. A single matching string
anywhere in the body (question, session history, tool results) rejects the
whole request before it reaches the model.

This module detects the signature shapes we empirically confirmed and
*neutralizes* them by inserting an invisible zero-width space (U+200B)
inside the contiguous token that the WAF rule matches. The model sees
semantically identical text; the WAF no longer sees the contiguous
signature.

Empirically validated live (2026-10-12, 14/14 payloads):

    blocked form                      neutralized form            result
    ------------------------------    --------------------------  ------
    ' or 1=1 / ' OR 1=1 --            ' o\\u200br 1=1             200 OK
    1 UNION SELECT 1,2,3              1 UN\\u200bION SELECT 1,2,3 200 OK
    1' UNION SELECT ... --            1' UN\\u200bION SELECT ...  200 OK
    UNION ALL SELECT 'x' (quoted)     UN\\u200bION ALL SELECT 'x'  200 OK
    ORDER BY 1 (SQLi enum)            ORDER B\\u200bY 1           200 OK
    etc/passwd                        et\\u200bc/passwd           200 OK
    ../../../../                      ..\\u200b/..\\u200b/..\\u200b/    200 OK
    ../../etc (2+ levels)             ../../e\\u200btc            200 OK
    <img src=x onerror=alert(1)>      one\\u200brror=alert(1)     200 OK
    <script>alert('xss')</script>     <script>al\\u200bert(...)   200 OK
    javascript:alert(1)               javas\\u200bcript:alert(1)  200 OK
    {{7*7}} / ${7*7}                  {\\u200b{7*7}} / $\\u200b{7*7}   200 OK

Benign text is a no-op (nothing changes when no pattern matches). This is an
availability mitigation for false positives on our own paid endpoint — it is
not a substitute for fixing the WAF rule set (request a whitelist for the
LiteLLM API paths). See ``finding/SPEC-WAF-GUARD-2026-10-12.md``.

Opt-in per project: disabled by default so deploying this module changes no
existing behavior. Enable in the project's ``seeknal_agent.yml``::

    guard:
      waf:
        enabled: true

The setting is read once per project via ``configure_from_project`` (called
by the gateway and the SQL tool), cached per project path.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

ZWSP = "\u200b"


def _break_mid(token: str) -> str:
    """Insert an invisible ZWSP inside *token* (after its midpoint)."""
    if len(token) < 2:
        return token
    i = max(1, len(token) // 2)
    return token[:i] + ZWSP + token[i:]


_STATE: dict[str, Any] = {"enabled": False}
_CACHE: dict[str, bool] = {}


def configure_from_project(project_path: str | Path | None) -> bool:
    """Load ``guard.waf.enabled`` from the project's ``seeknal_agent.yml``.

    Caches the answer per resolved project path and makes it the process-wide
    state consulted by :func:`neutralize_text` / :func:`neutralize_messages`.
    A missing file, missing key, or unresolvable path means disabled
    (default off — no behavior change until a project opts in).
    """
    if project_path is None:
        _STATE["enabled"] = False
        return False
    try:
        key = str(Path(project_path).resolve())
    except (TypeError, OSError, ValueError):
        _STATE["enabled"] = False
        return False
    if key not in _CACHE:
        # Lazy import keeps this module dependency-light and cycle-free.
        from seeknal.ask.config import get_waf_guard_enabled, load_agent_config

        config = load_agent_config(Path(key))
        _CACHE[key] = get_waf_guard_enabled(config)
    _STATE["enabled"] = _CACHE[key]
    return _STATE["enabled"]


def is_enabled() -> bool:
    """Return the currently configured guard state (set by the callers)."""
    return bool(_STATE["enabled"])


# ---------------------------------------------------------------------------
# Patterns — conservative shapes derived from the live block matrix.
# Each entry: (name, compiled regex, neutralizer(match) -> str).
# ---------------------------------------------------------------------------

_QUOTE_TAUTOLOGY = re.compile(
    r"""(['"]\s*)(or|and)(\s+['"\d(])""",
    re.IGNORECASE,
)

_UNION_QUOTE_COMMENT = re.compile(
    r"""(['"]\s*)(union)(\b[^\n]{0,120}?--)""",
    re.IGNORECASE,
)

# NOTE: these patterns intentionally avoid \b word-boundary anchors.
# Streamed tool-call args are stored as raw JSON text where newlines are the
# two characters backslash+n ("n" is a word char), so "\border" silently
# fails on "\nORDER BY 1" — the exact form the model's SQL arrives in.
# Over-matching is harmless (invisible U+200B only).
_UNION_SELECT_FN = re.compile(
    r"""(union)(\s+(?:all\s+)?select\s+[\d'"(])""",
    re.IGNORECASE,
)

_ORDER_BY_NUMBER = re.compile(
    r"""(order\s+)(by)(\s+\d)""",
    re.IGNORECASE,
)

_ETC_PASSWD = re.compile(
    r"""(etc)(/)(passwd|shadow)""",
    re.IGNORECASE,
)

_TRAVERSAL = re.compile(r"(?:\.\.[/\\]){3,}")

_TRAVERSAL_ETC = re.compile(
    r"""((?:\.\.[/\\]){2,}\s*)(etc)""",
    re.IGNORECASE,
)

_SCRIPT_FN = re.compile(
    r"""(<script\b[^>]*>[^<]{0,200}?)(alert|prompt|confirm|eval)(\s*\()""",
    re.IGNORECASE | re.DOTALL,
)

_TAG_EVENT_HANDLER = re.compile(
    r"""(<[a-zA-Z][^>\n]*?)(on[a-zA-Z]+)(\s*=\s*[^>\n]*\()""",
    re.IGNORECASE,
)

_JAVASCRIPT_URI = re.compile(
    r"""(javascript)(\s*:\s*(?:alert|prompt|confirm|eval)\s*\()""",
    re.IGNORECASE,
)

_MUSTACHE_MATH = re.compile(
    r"""(\{\{)([^}\n]{0,40}\d[^}\n]{0,40}\}\})"""
)

_SSTI_DOLLAR = re.compile(
    r"""(\$)(\{[^}\n]{0,40}\d[^}\n]{0,40}\})"""
)


def _n_quote_tautology(m: re.Match) -> str:
    return m.group(1) + _break_mid(m.group(2)) + m.group(3)


def _n_union_quote_comment(m: re.Match) -> str:
    return m.group(1) + _break_mid(m.group(2)) + m.group(3)


def _n_union_select_fn(m: re.Match) -> str:
    return _break_mid(m.group(1)) + m.group(2)


def _n_order_by_number(m: re.Match) -> str:
    return m.group(1) + _break_mid(m.group(2)) + m.group(3)


def _n_etc_passwd(m: re.Match) -> str:
    return _break_mid(m.group(1)) + m.group(2) + m.group(3)


def _n_traversal(m: re.Match) -> str:
    return re.sub(r"([/\\])", lambda mm: ZWSP + mm.group(1), m.group(0))


def _n_traversal_etc(m: re.Match) -> str:
    return m.group(1) + _break_mid(m.group(2))


def _n_script_fn(m: re.Match) -> str:
    return m.group(1) + _break_mid(m.group(2)) + m.group(3)


def _n_tag_event_handler(m: re.Match) -> str:
    return m.group(1) + _break_mid(m.group(2)) + m.group(3)


def _n_javascript_uri(m: re.Match) -> str:
    return _break_mid(m.group(1)) + m.group(2)


def _n_mustache_math(m: re.Match) -> str:
    return m.group(1)[0] + ZWSP + m.group(1)[1:] + m.group(2)


def _n_ssti_dollar(m: re.Match) -> str:
    return m.group(1) + ZWSP + m.group(2)


_PATTERNS: list[tuple[str, re.Pattern[str], object]] = [
    ("union_quote_comment", _UNION_QUOTE_COMMENT, _n_union_quote_comment),
    ("quote_tautology", _QUOTE_TAUTOLOGY, _n_quote_tautology),
    ("union_select_fn", _UNION_SELECT_FN, _n_union_select_fn),
    ("order_by_number", _ORDER_BY_NUMBER, _n_order_by_number),
    ("etc_passwd", _ETC_PASSWD, _n_etc_passwd),
    ("traversal_repeated", _TRAVERSAL, _n_traversal),
    ("traversal_etc", _TRAVERSAL_ETC, _n_traversal_etc),
    ("script_fn", _SCRIPT_FN, _n_script_fn),
    ("tag_event_handler", _TAG_EVENT_HANDLER, _n_tag_event_handler),
    ("javascript_uri", _JAVASCRIPT_URI, _n_javascript_uri),
    ("mustache_math", _MUSTACHE_MATH, _n_mustache_math),
    ("ssti_dollar", _SSTI_DOLLAR, _n_ssti_dollar),
]


def scan_text(text: str | None) -> list[str]:
    """Return the names of WAF-signature patterns present in *text*."""
    if not text:
        return []
    return [name for name, pattern, _ in _PATTERNS if pattern.search(text)]


def neutralize_text(text: str | None) -> tuple[str | None, list[str]]:
    """Neutralize WAF-triggering signatures in *text*.

    Returns ``(new_text, applied_pattern_names)``. When the guard is disabled
    or nothing matched, ``new_text`` is the original object.
    """
    if not text or not is_enabled():
        return text, []
    return _neutralize_text_impl(text)


def _neutralize_text_impl(text: str) -> tuple[str, list[str]]:
    """Gate-free text neutralization (callers decide enablement)."""
    current = text
    applied: list[str] = []
    for name, pattern, repl in _PATTERNS:
        new_text, count = pattern.subn(repl, current)  # type: ignore[arg-type]
        if count:
            applied.append(name)
            current = new_text
    return current, applied


def neutralize_str(text: str) -> str:
    """``neutralize_text`` variant that always returns ``str`` (never None)."""
    new_text, _ = neutralize_text(text)
    return text if new_text is None else new_text


def neutralize_messages(messages: list) -> tuple[int, list[str]]:
    """Neutralize text in pydantic-ai messages in place (gated by enablement).

    Walks ``ModelRequest``/``ModelResponse``-style objects (anything with a
    ``parts`` iterable) and neutralizes every string ``content``/``args``
    value — including string leaves inside nested dicts/lists (structured
    tool returns). Non-string leaves are left untouched.

    Returns ``(changed_part_count, applied_pattern_names)``.
    """
    if not messages or not is_enabled():
        return 0, []
    return _neutralize_messages_impl(messages)


def _neutralize_obj(obj):
    """Recursively neutralize string leaves in str/dict/list/tuple values.

    Returns ``(new_obj, applied_pattern_names)``. The original object is
    returned unchanged (same identity) when nothing matched, so callers can
    cheaply detect a no-op.
    """
    if isinstance(obj, str):
        new, names = _neutralize_text_impl(obj)
        return new, set(names)
    if isinstance(obj, dict):
        collected: set[str] = set()
        rebuilt: dict = {}
        for key, value in obj.items():
            new_value, names = _neutralize_obj(value)
            rebuilt[key] = new_value
            collected.update(names)
        return (rebuilt, collected) if collected else (obj, set())
    if isinstance(obj, (list, tuple)):
        collected: set[str] = set()
        rebuilt_items = []
        for value in obj:
            new_value, names = _neutralize_obj(value)
            rebuilt_items.append(new_value)
            collected.update(names)
        if not collected:
            return obj, set()
        if isinstance(obj, tuple):
            return tuple(rebuilt_items), collected
        return rebuilt_items, collected
    return obj, set()


def _neutralize_messages_impl(messages: list) -> tuple[int, list[str]]:
    """Gate-free message walk (callers decide enablement)."""
    changed = 0
    applied: set[str] = set()
    for message in messages:
        for part in getattr(message, "parts", None) or []:
            for attr in ("content", "args"):
                value = getattr(part, attr, None)
                if not isinstance(value, (str, dict, list, tuple)):
                    continue
                new_value, names = _neutralize_obj(value)
                if names and new_value != value:
                    try:
                        setattr(part, attr, new_value)
                    except Exception:  # noqa: BLE001 — frozen model: skip
                        continue
                    changed += 1
                    applied.update(names)
    return changed, sorted(applied)


_PROCESSOR_LOGGED = False


def build_history_processor():
    """Return a pydantic-ai history processor that neutralizes every request.

    Attached by ``create_agent`` when ``guard.waf.enabled`` is true. Unlike
    the pre-run pass, pydantic-ai invokes history processors before *each*
    model request inside the run, so this also covers content a pre-run pass
    cannot see: tool results from every tool, generated text, and tool-call
    arguments.

    The processor mutates the message objects in place and returns the same
    list, matching the behaviour of seeknal's other history processors. It is
    gate-free on purpose: attaching it *is* the enablement decision.

    Version note: on pydantic-ai >= 1.106 the ``history_processors=`` kwarg is
    deprecated but functional (wrapped into a ``ProcessHistory`` capability =
    ``before_model_request`` lifecycle hook, which runs before each model
    request). Before pydantic-ai v2.0, migrate to
    ``capabilities=[ProcessHistory(fn)]``.
    """
    global _PROCESSOR_LOGGED
    if not _PROCESSOR_LOGGED:
        import logging

        logging.getLogger(__name__).info(
            "waf_guard: history processor attached (guard.waf.enabled=true)"
        )
        _PROCESSOR_LOGGED = True

    def _waf_history_processor(messages: list) -> list:
        changed, applied = _neutralize_messages_impl(messages)
        if changed:
            import logging

            logging.getLogger(__name__).info(
                "[waf_guard] neutralized %d message part(s) patterns=%s",
                changed,
                applied,
            )
        return messages

    return _waf_history_processor


__all__ = [
    "ZWSP",
    "build_history_processor",
    "configure_from_project",
    "is_enabled",
    "neutralize_messages",
    "neutralize_str",
    "neutralize_text",
    "scan_text",
]
