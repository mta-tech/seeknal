"""Tool context — shared state passed to all agent tools.

The ToolContext holds the REPL instance and artifact discovery service.
It's set once per session at agent initialization and accessed by tools
via get_tool_context().

Uses ``contextvars.ContextVar`` for async-safe per-session isolation so
multiple concurrent ask sessions don't cross-contaminate each other's
DuckDB connections or project artifacts.
"""

from __future__ import annotations

import contextvars
import hashlib
import json
import threading
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from seeknal.ask.background import BackgroundRegistry
    from seeknal.ask.modules.artifact_discovery.service import ArtifactDiscovery
    from seeknal.cli.repl import REPL

# Per-task (async-safe) context variable instead of a module-level global.
# Each asyncio task / OS process gets its own ToolContext.
_tool_context_var: contextvars.ContextVar[ToolContext] = contextvars.ContextVar(
    "seeknal_tool_context"
)


@dataclass
class ToolContext:
    """Shared context for all agent tools within one session.

    Includes a threading lock to protect the DuckDB connection from
    concurrent access — pydantic-ai may invoke tools in parallel threads.
    """

    repl: REPL
    artifact_discovery: ArtifactDiscovery
    project_path: Path
    session_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    db_lock: threading.Lock = field(default_factory=threading.Lock)
    fs_lock: threading.Lock = field(default_factory=threading.Lock)
    request_limit: int = 100
    background_threshold: int = 60
    require_report_approval: bool = True
    report_approval_granted: bool = False
    require_proof_publish_approval: bool = True
    proof_publish_approval_granted: bool = False
    require_proof_edit_approval: bool = True
    proof_edit_approval_granted: bool = False
    require_seeknal_report_publish_approval: bool = True
    seeknal_report_publish_approval_granted: bool = False
    background_registry: BackgroundRegistry = field(default_factory=lambda: _make_registry())
    console: Any = None
    plan_steps: list[str] = field(default_factory=list)
    plan_step_index: int = 0
    last_read_staging_path: str | None = None
    disable_quality_gate: bool = False
    unavailable_python_modules: set[str] = field(default_factory=set)
    current_question: str | None = None
    tool_call_limit: int | None = None
    tool_calls_this_turn: int = 0
    successful_sql_results_this_turn: int = 0
    evidence_snippets_this_turn: list[str] = field(default_factory=list)
    terminal_tool_errors_this_turn: list[str] = field(default_factory=list)
    failed_tool_signatures: dict[str, str] = field(default_factory=dict)
    successful_sql_cache: dict[str, str] = field(default_factory=dict)
    loaded_sql_pairs_this_turn: dict[str, str] = field(default_factory=dict)
    sql_pairs_checked_this_turn: bool = False
    authoritative_sql_pair_result_this_turn: dict[str, str] | None = None
    sql_timeout_seconds: int = 60
    discovery_cache_ttl_seconds: int = 300
    discovery_cache: dict[str, tuple[float, Any]] = field(default_factory=dict)
    timing_events_this_turn: list[dict[str, Any]] = field(default_factory=list)
    # SQL-pair guard behavior — "authoritative" keeps the legacy terminal
    # stop after an authoritative SQL-pair result; "advisory" treats pairs as
    # cheatsheets and never escalates Guard 3 to TERMINAL.
    sql_pair_mode: str = "authoritative"
    # Number of times the agent tried to run drifting SQL after an
    # authoritative pair landed this turn. The first attempt is now a
    # RETRYABLE nudge so the agent can override with allow_sql_pair_drift;
    # only a second attempt without the override escalates to TERMINAL.
    authoritative_drift_attempts_this_turn: int = 0


def _make_registry():
    """Lazy factory to avoid circular import at module load."""
    from seeknal.ask.background import BackgroundRegistry
    return BackgroundRegistry()


def set_tool_context(ctx: ToolContext) -> None:
    """Set the tool context for the current async task / thread."""
    _tool_context_var.set(ctx)


def get_tool_context() -> ToolContext:
    """Get the current session's tool context.

    Raises:
        RuntimeError: If context hasn't been initialized for this task.
    """
    try:
        return _tool_context_var.get()
    except LookupError:
        raise RuntimeError(
            "Tool context not initialized. "
            "Call set_tool_context() before using agent tools."
        )


def get_successful_sql_cache(ctx: ToolContext | None = None) -> dict[str, str]:
    """Return session SQL-result cache attached to the durable REPL object.

    Some agent runtimes may invoke tools through copied contextvars. The REPL
    instance is the durable per-session object, so mirror cache state there to
    keep successful-query reuse reliable across tool-call threads.
    """
    ctx = ctx or get_tool_context()
    cache = getattr(ctx.repl, "_seeknal_successful_sql_cache", None)
    if not isinstance(cache, dict):
        cache = {}
        setattr(ctx.repl, "_seeknal_successful_sql_cache", cache)
    ctx.successful_sql_cache = cache
    return cache


def get_loaded_sql_pairs(ctx: ToolContext | None = None) -> dict[str, str]:
    """Return same-turn loaded SQL-pair memory attached to the REPL object."""
    ctx = ctx or get_tool_context()
    pairs = getattr(ctx.repl, "_seeknal_loaded_sql_pairs_this_turn", None)
    if not isinstance(pairs, dict):
        pairs = {}
        setattr(ctx.repl, "_seeknal_loaded_sql_pairs_this_turn", pairs)
    ctx.loaded_sql_pairs_this_turn = pairs
    return pairs


def mark_sql_pairs_checked(ctx: ToolContext | None = None) -> None:
    """Record that SQL-pair context was consulted this turn."""
    ctx = ctx or get_tool_context()
    ctx.sql_pairs_checked_this_turn = True
    setattr(ctx.repl, "_seeknal_sql_pairs_checked_this_turn", True)


def sql_pairs_checked(ctx: ToolContext | None = None) -> bool:
    """Return whether SQL-pair context was consulted this turn."""
    ctx = ctx or get_tool_context()
    checked = bool(getattr(ctx.repl, "_seeknal_sql_pairs_checked_this_turn", False))
    ctx.sql_pairs_checked_this_turn = checked
    return checked


def get_authoritative_sql_pair_result(
    ctx: ToolContext | None = None,
) -> dict[str, str] | None:
    """Return the successful SQL-pair result for the current user turn."""
    ctx = ctx or get_tool_context()
    result = getattr(ctx.repl, "_seeknal_authoritative_sql_pair_result_this_turn", None)
    if not isinstance(result, dict):
        result = None
    ctx.authoritative_sql_pair_result_this_turn = result
    return result


def record_authoritative_sql_pair_result(
    *,
    name: str,
    path: str,
    result: str,
    ctx: ToolContext | None = None,
) -> None:
    """Record that a project-owned SQL pair produced usable evidence."""
    ctx = ctx or get_tool_context()
    payload = {"name": name, "path": path, "result": result}
    ctx.authoritative_sql_pair_result_this_turn = payload
    setattr(ctx.repl, "_seeknal_authoritative_sql_pair_result_this_turn", payload)

    ctx.evidence_snippets_this_turn.append(
        _compact_evidence(
            f"execute_sql_pair:{name}",
            f"# Executed SQL pair: {path}\n\n{result}",
        )
    )
    if len(ctx.evidence_snippets_this_turn) > 8:
        del ctx.evidence_snippets_this_turn[:-8]


def should_synthesize_after_authoritative_sql_pair(
    ctx: ToolContext | None = None,
) -> bool:
    """Return True when the harness should stop tool use and answer.

    A matched SQL pair is an authoritative project memory item for ordinary
    business questions. Advanced analysis requests (forecasting, modeling,
    correlations, charts, etc.) may legitimately need Python or follow-up
    queries after the pair result, so those remain agent-directed.

    When the project opts into ``sql_pair_mode: advisory`` (see
    ``seeknal_agent.yml``), SQL pairs are treated as cheatsheets and this
    helper never escalates — Guard 3's terminal branch becomes dormant.
    """
    ctx = ctx or get_tool_context()
    if get_authoritative_sql_pair_result(ctx) is None:
        return False
    if getattr(ctx, "sql_pair_mode", "authoritative") == "advisory":
        return False
    return not _question_requests_post_sql_analysis(ctx.current_question)


def authoritative_drift_attempts(ctx: ToolContext | None = None) -> int:
    """Return how many drifting-SQL attempts the agent has made this turn."""
    ctx = ctx or get_tool_context()
    return int(getattr(ctx, "authoritative_drift_attempts_this_turn", 0) or 0)


def bump_authoritative_drift_attempt(ctx: ToolContext | None = None) -> int:
    """Record one drifting-SQL attempt after an authoritative pair landed.

    Returns the post-increment count so callers can decide between RETRYABLE
    and TERMINAL responses without keeping their own per-turn state.
    """
    ctx = ctx or get_tool_context()
    current = authoritative_drift_attempts(ctx) + 1
    ctx.authoritative_drift_attempts_this_turn = current
    return current


def _normalize_question(text: str | None) -> str:
    """Normalize a user question for verbatim re-ask comparison.

    Lowercases, collapses whitespace to single spaces, strips leading/
    trailing whitespace, and trims the common trailing punctuation
    (``.``, ``?``, ``!``). Deliberately does NOT strip diacritics,
    stopwords, or stem — exact meaning must be preserved so non-English
    questions (Indonesian, Malay) are comparable verbatim.
    """
    if not text:
        return ""
    collapsed = " ".join(text.split())
    lowered = collapsed.lower().strip()
    return lowered.rstrip(".?!").rstrip()


def record_prior_turn_answer(
    *,
    question: str,
    answer: str,
    ctx: ToolContext | None = None,
) -> None:
    """Store the (normalized question, final answer) pair on the durable REPL.

    Only the last turn's pair is kept. Skipped when the answer is empty/
    whitespace, or when it is a tool-error JSON payload — we never want to
    lock the user into a degraded answer on the next identical re-ask.
    """
    ctx = ctx or get_tool_context()
    if not answer or not answer.strip():
        return
    if _parse_tool_error(answer) is not None:
        return
    normalized = _normalize_question(question)
    if not normalized:
        return
    setattr(ctx.repl, "_seeknal_prior_turn_question", normalized)
    setattr(ctx.repl, "_seeknal_prior_turn_answer", answer)


def lookup_prior_turn_answer(
    current_question: str | None,
    ctx: ToolContext | None = None,
) -> str | None:
    """Return the prior turn's final answer when this is a verbatim re-ask.

    Returns the stored answer only when:
      * ``current_question`` normalizes to a non-empty string, AND
      * the stored normalized question matches it exactly, AND
      * the stored answer is non-empty / non-whitespace.

    Otherwise returns ``None`` and the caller must continue normally.
    """
    ctx = ctx or get_tool_context()
    normalized_current = _normalize_question(current_question)
    if not normalized_current:
        return None
    stored_question = getattr(ctx.repl, "_seeknal_prior_turn_question", None)
    stored_answer = getattr(ctx.repl, "_seeknal_prior_turn_answer", None)
    if not isinstance(stored_question, str) or not isinstance(stored_answer, str):
        return None
    if normalized_current != stored_question:
        return None
    if not stored_answer.strip():
        return None
    return stored_answer


def build_verbatim_restate_response(prior_answer: str) -> str:
    """Return the prior-turn answer for a verbatim re-ask, unchanged.

    A verbatim re-ask returns the previous answer exactly — no banner,
    no meta-commentary, no "this is the same question" notice. The gate
    behaves as a silent cache: identical question in, identical answer
    out. The function is kept as the single named seam the gateway and
    streaming entry points call, so the restate policy lives in one
    place and is trivial to evolve.
    """
    return prior_answer


def seed_prior_turn_from_history(message_history: Any) -> None:
    """Re-seed the durable prior-turn pair from a persisted message_history.

    The gateway rebuilds the agent's REPL on every turn (``create_agent``),
    so the in-memory prior-turn pair written by ``record_prior_turn_answer``
    does not survive between turns — and the verbatim re-ask gate, which
    reads that pair off ``ctx.repl``, never fires on gateway/Telegram
    sessions. ``message_history`` IS persisted per session (via the
    session store), so it is the authoritative cross-turn source.

    Call this after ``create_agent`` (so the ToolContext + fresh REPL
    exist) and before the verbatim gate. Walks ``message_history``
    backwards for the most recent (user question, assistant answer) pair
    and stores it via ``record_prior_turn_answer``.

    Triple-ask safe by construction: a verbatim restate returns the
    prior answer unchanged (see ``build_verbatim_restate_response``), so
    the stored answer is always canonical — re-seeding from it can never
    compound. Best-effort and non-fatal — any failure leaves the gate
    dormant rather than crashing the turn.
    """
    if not message_history:
        return
    try:
        from pydantic_ai.messages import (
            ModelRequest,
            ModelResponse,
            TextPart,
            UserPromptPart,
        )
    except Exception:  # noqa: BLE001 - pydantic_ai optional at import time
        return

    prior_answer: str | None = None
    prior_question: str | None = None
    for msg in reversed(message_history):
        if prior_answer is None and isinstance(msg, ModelResponse):
            for part in msg.parts:
                if (
                    isinstance(part, TextPart)
                    and isinstance(part.content, str)
                    and part.content.strip()
                ):
                    prior_answer = part.content
                    break
        elif (
            prior_answer is not None
            and prior_question is None
            and isinstance(msg, ModelRequest)
        ):
            for part in msg.parts:
                if isinstance(part, UserPromptPart) and isinstance(
                    part.content, str
                ) and part.content.strip():
                    prior_question = part.content
                    break
        if prior_answer is not None and prior_question is not None:
            break

    if prior_answer is None or prior_question is None:
        return

    if not prior_answer.strip():
        return
    record_prior_turn_answer(question=prior_question, answer=prior_answer)


def reset_turn_governor(question: str | None = None) -> None:
    """Reset per-user-turn harness state.

    The governor is intentionally core behavior, not project configuration:
    it tracks evidence, tool budget, and repeated failures so read-only
    analyst sessions stop after enough useful work instead of looping.
    """
    ctx = get_tool_context()
    ctx.current_question = question
    ctx.tool_calls_this_turn = 0
    ctx.successful_sql_results_this_turn = 0
    ctx.evidence_snippets_this_turn.clear()
    ctx.terminal_tool_errors_this_turn.clear()
    ctx.loaded_sql_pairs_this_turn.clear()
    get_loaded_sql_pairs(ctx).clear()
    # NOTE: ``sql_pairs_checked`` is intentionally NOT reset here. Once the
    # agent has seen the project's SQL pairs in this session (via
    # list_sql_pairs/read_sql_pair/execute_sql_pair), Guard 1 should stay
    # dormant for subsequent turns. Re-firing Guard 1 every new user turn
    # forced the agent to re-list pairs even on simple follow-up questions
    # ("dari 2024 dan seterusnya?") and burned tool budget on recovery.
    # The agent can still choose to re-list pairs when the question shifts
    # to a clearly new domain.
    ctx.authoritative_sql_pair_result_this_turn = None
    setattr(ctx.repl, "_seeknal_authoritative_sql_pair_result_this_turn", None)
    ctx.authoritative_drift_attempts_this_turn = 0
    ctx.timing_events_this_turn.clear()


def get_discovery_cache_value(key: str) -> Any | None:
    """Return a non-expired per-session discovery cache value."""
    import time

    ctx = get_tool_context()
    ttl = int(getattr(ctx, "discovery_cache_ttl_seconds", 0) or 0)
    if ttl <= 0:
        return None
    cached = ctx.discovery_cache.get(key)
    if cached is None:
        return None
    created_at, value = cached
    if time.monotonic() - created_at > ttl:
        ctx.discovery_cache.pop(key, None)
        return None
    return value


def set_discovery_cache_value(key: str, value: Any) -> None:
    """Store a per-session discovery cache value."""
    import time

    ctx = get_tool_context()
    ttl = int(getattr(ctx, "discovery_cache_ttl_seconds", 0) or 0)
    if ttl <= 0:
        return
    ctx.discovery_cache[key] = (time.monotonic(), value)


def record_timing_event(name: str, elapsed_ms: int, **extra: Any) -> None:
    """Record bounded timing data for diagnostics and gateway events."""
    ctx = get_tool_context()
    payload = {"name": name, "elapsed_ms": int(elapsed_ms), **extra}
    ctx.timing_events_this_turn.append(payload)
    if len(ctx.timing_events_this_turn) > 20:
        del ctx.timing_events_this_turn[:-20]


def make_tool_signature(tool_name: str, args: dict[str, Any] | None = None) -> str:
    """Return a stable, low-cardinality signature for a tool call."""
    args = args or {}
    normalized: dict[str, Any] = {}
    for key, value in sorted(args.items()):
        text = str(value)
        if key in {"sql", "query", "code"}:
            text = " ".join(text.split())
            if len(text) > 500:
                text = text[:500]
        normalized[key] = text
    payload = json.dumps(normalized, sort_keys=True, default=str)
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]
    return f"{tool_name}:{digest}"


def repeated_failure_message(tool_name: str, args: dict[str, Any] | None = None) -> str | None:
    """Return a prior failure explanation if this exact tool path failed."""
    ctx = get_tool_context()
    return ctx.failed_tool_signatures.get(make_tool_signature(tool_name, args))


def record_tool_result(
    tool_name: str,
    output: str,
    *,
    args: dict[str, Any] | None = None,
) -> None:
    """Record success/failure evidence for harness-level stopping behavior."""
    ctx = get_tool_context()
    ctx.tool_calls_this_turn += 1
    text = output or ""
    error_payload = _parse_tool_error(text)
    if error_payload is not None:
        category = str(error_payload.get("category", "tool_error"))
        retryable = bool(error_payload.get("retryable", False))
        message = str(error_payload.get("message", ""))[:500]
        signature = make_tool_signature(tool_name, args)
        ctx.failed_tool_signatures[signature] = (
            f"{tool_name} already failed with {category}: {message}"
        )
        if not retryable or category.startswith("terminal_"):
            ctx.terminal_tool_errors_this_turn.append(
                f"{tool_name}: {category} — {message}"
            )
        return

    if tool_name == "execute_sql" and "|" in text and " rows" in text:
        ctx.successful_sql_results_this_turn += 1
        ctx.evidence_snippets_this_turn.append(_compact_evidence(tool_name, text))
    elif tool_name == "execute_python" and text.strip() and not text.startswith("Error"):
        ctx.evidence_snippets_this_turn.append(_compact_evidence(tool_name, text))

    # Keep memory bounded even in long sessions.
    if len(ctx.evidence_snippets_this_turn) > 8:
        del ctx.evidence_snippets_this_turn[:-8]


def has_sufficient_evidence() -> bool:
    """Return True once a business answer can be grounded in collected data."""
    ctx = get_tool_context()
    if get_authoritative_sql_pair_result(ctx) is not None and ctx.evidence_snippets_this_turn:
        return True
    if ctx.successful_sql_results_this_turn < 1 or not ctx.evidence_snippets_this_turn:
        return False
    joined = "\n".join(ctx.evidence_snippets_this_turn).lower()
    metric_markers = (
        "jumlah",
        "total",
        "count",
        "revenue",
        "orders",
        "rate",
        "avg",
        "sum",
    )
    grain_markers = (
        "tahun",
        "year",
        "bulan",
        "month",
        "tanggal",
        "date",
        "skala",
        "segment",
        "category",
        "kategori",
    )
    return any(marker in joined for marker in metric_markers) and any(
        marker in joined for marker in grain_markers
    )


def synthesize_evidence_fallback(reason: str) -> str:
    """Create a deterministic final answer from collected evidence snippets."""
    ctx = get_tool_context()
    snippets = ctx.evidence_snippets_this_turn[-4:]
    errors = ctx.terminal_tool_errors_this_turn[-2:]
    lines = [
        "I am stopping further tool use to keep this answer bounded.",
        f"Stop reason: {reason}",
        "",
    ]
    if snippets:
        lines.append("Evidence collected:")
        lines.extend(f"\n{snippet}" for snippet in snippets)
    if errors:
        lines.append("\nLimitations / failed paths:")
        lines.extend(f"- {error}" for error in errors)
    lines.append(
        "\nUse these results as the grounded answer, with the noted caveats. "
        "If you want a polished report or chart, ask for that explicitly."
    )
    return "\n".join(lines).strip()


def build_evidence_synthesis_prompt(reason: str) -> str:
    """Build a no-tool final-answer prompt from the turn governor evidence.

    This is a safety valve for bounded read-only analysis sessions. If the
    model keeps exploring until the harness budget is reached, we give it the
    already-collected evidence and explicitly prohibit more tools so the user
    gets a natural-language answer instead of raw harness diagnostics.
    """
    ctx = get_tool_context()
    question = (ctx.current_question or "").strip()
    snippets = "\n\n".join(ctx.evidence_snippets_this_turn[-6:]).strip()
    errors = "\n".join(f"- {error}" for error in ctx.terminal_tool_errors_this_turn[-3:])
    authoritative_pair = get_authoritative_sql_pair_result(ctx)
    if not snippets:
        return synthesize_evidence_fallback(reason)

    parts = [
        "You are writing the final answer for a read-only data-analysis turn.",
        "Do not call any tools. Do not ask for more context.",
        "Use only the evidence below; if it is incomplete, say what is missing as a caveat.",
        "Do not infer labels for coded dimensions unless the evidence includes the mapping.",
        "If a label mapping is missing, keep the raw code and state that the label was not found.",
        "A mapping is valid only when it belongs to the exact field/category being analyzed.",
        "Respond in the user's language and include concrete values from the evidence.",
        "",
        f"Original user question: {question or '(not available)'}",
        f"Stop reason: {reason}",
    ]
    if authoritative_pair is not None:
        parts.extend(
            [
                "",
                "Authoritative SQL pair executed:",
                f"- name: {authoritative_pair.get('name', '')}",
                f"- path: {authoritative_pair.get('path', '')}",
                "Treat that SQL-pair result as the preferred evidence. Do not "
                "replace its logic with an alternate query.",
            ]
        )
    parts.extend(["", "Evidence:", snippets])
    if errors:
        parts.extend(["", "Failed/limited paths:", errors])
    parts.extend([
        "",
        "Write a concise, polished business answer with:",
        "1. direct answer / key trend",
        "2. supporting numbers",
        "3. caveats about evidence coverage",
        "Do not mention internal tool-call budgets unless needed as a caveat.",
    ])
    return "\n".join(parts).strip()


def visualization_requested(question: str | None) -> bool:
    """Return whether the user explicitly asked for visual output."""
    if not question:
        return False
    lowered = question.lower()
    triggers = (
        "visual",
        "visualisasi",
        "chart",
        "grafik",
        "plot",
        "diagram",
        "dashboard",
        "report",
        "laporan",
        "gambar",
    )
    return any(trigger in lowered for trigger in triggers)


def _question_requests_post_sql_analysis(question: str | None) -> bool:
    """Detect generic requests that need work after a matched SQL pair.

    This is deliberately domain-agnostic. It avoids hardcoding table or
    business concepts while preserving Seeknal's ability to take initiative
    for modeling, forecasting, charts, deeper comparisons, and similar
    analytical work.
    """
    if not question:
        return False
    lowered = question.lower()
    triggers = (
        "forecast",
        "predict",
        "prediction",
        "projection",
        "machine learning",
        "model",
        "regression",
        "correlation",
        "korelasi",
        "cluster",
        "segmentasi",
        "anomaly",
        "anomal",
        "root cause",
        "why",
        "kenapa",
        "mengapa",
        "compare",
        "comparison",
        "bandingkan",
        "visual",
        "visualisasi",
        "chart",
        "grafik",
        "plot",
        "dashboard",
        "report",
        "laporan",
        "python",
        "statistical",
        "statistik",
        # Filter-tweak intents — the user is asking for the same metric with
        # a different scope. Treat as legitimate post-SQL work so Guard 3 does
        # not lock the agent to a pair that lacks the requested exclusion.
        # Keep these explicit (verbs / negations) and avoid common prepositions
        # like " per " or " by " — those naturally show up in same-grain
        # questions an authoritative pair already covers.
        "exclud",
        "without",
        "tanpa",
        "kecuali",
        "selain",
        "filter",
        "breakdown",
        # Common phrasing for asking the same number on a slightly different
        # population — these should not trigger the lock-in either.
        "saja",
        "khusus",
    )
    return any(trigger in lowered for trigger in triggers)


def _parse_tool_error(text: str) -> dict[str, Any] | None:
    stripped = text.strip()
    if not stripped.startswith("{"):
        return None
    try:
        data = json.loads(stripped)
    except json.JSONDecodeError:
        return None
    if isinstance(data, dict) and "category" in data:
        return data
    return None


def _compact_evidence(tool_name: str, text: str) -> str:
    stripped = text.strip()
    if len(stripped) > 1800:
        stripped = stripped[:1800] + "\n... (truncated)"
    return f"[{tool_name}]\n{stripped}"


# Unique discriminator labels — each approval is keyed off a single unique
# option label. The agent must present a menu with the discriminator as one
# of the options AND the user must explicitly select it. We deliberately do
# NOT require a strict superset of 4 exact labels, because the model
# frequently paraphrases one of them (e.g. "Just done" vs "Done for now"),
# which silently dropped approval even when the user clearly picked the
# discriminator. See the last ask session bug report.
_REPORT_APPROVAL_DISCRIMINATOR = "generate report now"
_PROOF_PUBLISH_APPROVAL_DISCRIMINATOR = "publish memo to proof"
_PROOF_EDIT_APPROVAL_DISCRIMINATOR = "apply edit to proof"
_SEEKNAL_REPORT_PUBLISH_DISCRIMINATOR = "publish to seeknal report server"


def reset_report_approval() -> None:
    """Reset per-turn approval state for report AND proof tools.

    The agent must re-ask the user every turn before generating reports, or
    publishing/editing documents on Proof — no approval carries across turns.
    """
    ctx = get_tool_context()
    ctx.require_report_approval = True
    ctx.report_approval_granted = False
    ctx.require_proof_publish_approval = True
    ctx.proof_publish_approval_granted = False
    ctx.require_proof_edit_approval = True
    ctx.proof_edit_approval_granted = False
    ctx.require_seeknal_report_publish_approval = True
    ctx.seeknal_report_publish_approval_granted = False


def record_ask_user_response(options: list[dict[str, Any]], answer: str) -> None:
    """Persist explicit report or proof-publish approval when the user grants it.

    Each approval is keyed off a single unique discriminator label:
      - `generate report now`  → grants report generation
      - `publish memo to proof` → grants publishing to Proof / memokami
      - `apply edit to proof`  → grants editing an existing Proof doc

    Grant conditions (all must hold):
      1. The discriminator label appears as one of the options shown.
      2. The user's selected answer equals that discriminator (case/whitespace
         insensitive).
      3. The corresponding `require_*_approval` flag is still enabled.

    Previously this also required a strict 4-element subset of exact labels,
    which silently dropped approval whenever the LLM paraphrased even one of
    them. That strictness was removed after a repeated session-level lockout
    where the user selected `Publish memo to Proof` three times and the tool
    kept returning "Approval required". The discriminator is unique enough
    that a false grant would require the agent to deliberately reuse the
    same discriminator label for an unrelated menu.
    """
    ctx = get_tool_context()

    labels = {
        str(option.get("label", "")).strip().lower()
        for option in options
        if option.get("label")
    }
    normalized_answer = answer.strip().lower()

    if (
        getattr(ctx, "require_report_approval", False)
        and _REPORT_APPROVAL_DISCRIMINATOR in labels
    ):
        ctx.report_approval_granted = (
            normalized_answer == _REPORT_APPROVAL_DISCRIMINATOR
        )

    if (
        getattr(ctx, "require_proof_publish_approval", False)
        and _PROOF_PUBLISH_APPROVAL_DISCRIMINATOR in labels
    ):
        ctx.proof_publish_approval_granted = (
            normalized_answer == _PROOF_PUBLISH_APPROVAL_DISCRIMINATOR
        )

    if (
        getattr(ctx, "require_proof_edit_approval", False)
        and _PROOF_EDIT_APPROVAL_DISCRIMINATOR in labels
    ):
        ctx.proof_edit_approval_granted = (
            normalized_answer == _PROOF_EDIT_APPROVAL_DISCRIMINATOR
        )

    if (
        getattr(ctx, "require_seeknal_report_publish_approval", False)
        and _SEEKNAL_REPORT_PUBLISH_DISCRIMINATOR in labels
    ):
        ctx.seeknal_report_publish_approval_granted = (
            normalized_answer == _SEEKNAL_REPORT_PUBLISH_DISCRIMINATOR
        )


def require_report_approval(tool_name: str) -> str | None:
    """Return an actionable error when a report tool is used without approval."""
    ctx = get_tool_context()
    if not ctx.require_report_approval or ctx.report_approval_granted:
        return None
    return (
        f"Approval required before {tool_name}. Load the 'report-generation' skill (or 'save-report-exposure' for codification) via load_skill() for the full workflow. Then summarize the current findings and proposed next step, then use ask_user with exactly these options: 'Continue analysis', 'Generate report now', 'Done for now', and 'Type your own'. Do not print those choices as plain text, bullets, or numbered lists. Only call {tool_name} after the user explicitly chooses 'Generate report now'."
    )


def require_proof_publish_approval(tool_name: str) -> str | None:
    """Return an actionable error when publish_to_proof is used without approval."""
    ctx = get_tool_context()
    if (
        not getattr(ctx, "require_proof_publish_approval", True)
        or getattr(ctx, "proof_publish_approval_granted", False)
    ):
        return None
    return (
        f"Approval required before {tool_name}. Load the 'publish-memo-to-proof' skill via "
        "load_skill() for the full workflow. Then summarize the memo content and its intended "
        "audience, then use ask_user with exactly these options: 'Continue analysis', 'Publish "
        "memo to Proof', 'Done for now', and 'Type your own'. Do not print those choices as plain "
        f"text, bullets, or numbered lists. Only call {tool_name} after the user explicitly "
        "chooses 'Publish memo to Proof'."
    )


def require_seeknal_report_publish_approval(tool_name: str) -> str | None:
    """Return an actionable error when publish_to_seeknal_report is used without approval."""
    ctx = get_tool_context()
    if (
        not getattr(ctx, "require_seeknal_report_publish_approval", True)
        or getattr(ctx, "seeknal_report_publish_approval_granted", False)
    ):
        return None
    return (
        f"Approval required before {tool_name}. Load the 'publish-to-seeknal-report' skill via "
        "load_skill() for the full workflow. Then summarize what will be published and to which "
        "server, then use ask_user with exactly these options: 'Continue analysis', 'Publish to "
        "Seeknal Report Server', 'Publish memo to Proof', 'Done for now', and 'Type your own'. "
        "Do not print those choices as plain text, bullets, or numbered lists. Only call "
        f"{tool_name} after the user explicitly chooses 'Publish to Seeknal Report Server'."
    )


def require_proof_edit_approval(tool_name: str) -> str | None:
    """Return an actionable error when edit_proof_document is used without approval."""
    ctx = get_tool_context()
    if (
        not getattr(ctx, "require_proof_edit_approval", True)
        or getattr(ctx, "proof_edit_approval_granted", False)
    ):
        return None
    return (
        f"Approval required before {tool_name}. Load the 'edit-proof-document' skill via "
        "load_skill() for the full workflow (read → diff → ask → edit). Then summarize the "
        "proposed edit (which document, what changes) and use ask_user with exactly these "
        "options: 'Continue analysis', 'Apply edit to Proof', 'Done for now', and 'Type your "
        "own'. Do not print those choices as plain text, bullets, or numbered lists. Only call "
        f"{tool_name} after the user explicitly chooses 'Apply edit to Proof'."
    )
