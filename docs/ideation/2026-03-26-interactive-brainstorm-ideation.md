---
date: 2026-03-26
topic: interactive-brainstorm-ask-user
focus: Add brainstorm steps in seeknal ask chat where agent asks questions with selectable options
---

# Ideation: Interactive Brainstorm Steps in Seeknal Ask

## Codebase Context

**Current Architecture:**
- Single LLM agent (Gemini 2.5 Flash) with 10-21 tools across 3 profiles (analysis/build/full)
- Built on LangGraph with deepagents backend (SubAgentMiddleware, task() tool)
- One-way flow: user asks -> agent works -> returns answer. Agent cannot ask clarifying questions.
- `core.j2` prompt says "ask design questions ONE at a time" but there's NO mechanism for this
- `streaming.py` renders tool calls, SQL, reasoning in real-time via Rich Console
- Chat mode (`chat_session`) loops: prompt -> stream_ask -> prompt again
- One-shot mode: single question -> agent -> answer (98% QA pass rate)
- `confirmed=True` gate pattern exists on `generate_report`, `run_pipeline`, `apply_draft`

**Key Constraints:**
- NO keyword-based intent detection (let LLM decide autonomously)
- Must not regress the one-shot happy path (98% QA baseline)
- LangGraph + asyncio + Rich Console stack
- `chat_session` already uses `asyncio.to_thread(input, ...)` for non-blocking stdin

**Inspiration Sources:**
- ce-brainstorm: one question at a time, single-select, platform blocking question tool
- Claude Code AskUserQuestion: numbered options, user picks or types free text

## Ranked Ideas

### 1. `ask_user` Blocking Tool (The Core Primitive)
**Description:** A new LangChain tool the agent calls mid-execution to pause, render numbered options + free-text fallback in Rich Console, block on `asyncio.to_thread(input)`, and return the user's choice as a tool result. The agent decides autonomously when to call it. In one-shot mode, returns sentinel so agent gracefully skips.
**Rationale:** Every other idea depends on this. Infrastructure is ~80% there. ~30 lines of new code unlocks the entire interactive layer.
**Downsides:** Must handle one-shot vs chat mode. Risk of over-calling without budget.
**Confidence:** 95%
**Complexity:** Low
**Status:** Explored

### 2. Schema-Seeded Options + Option-First UX
**Description:** `ask_user` accepts `options: list[str]` populated from live schema data. Agent proposes default and presents alternatives from real column names. User picks from real data, not from memory.
**Rationale:** Misidentified join keys/time columns are #1 dry-run failure cause. Grounding in live schema eliminates "wrong column name" errors.
**Downsides:** Requires prior `describe_table` call to populate options.
**Confidence:** 85%
**Complexity:** Low
**Status:** Explored

### 3. Dry-Run Failure Recovery Questions
**Description:** On dry_run_draft failure, agent calls `ask_user` with fix options instead of guessing. In one-shot mode, agent picks "try automatically".
**Rationale:** Ralph Loop's biggest token burn is self-repair after dry-run failures. One human answer replaces 2-3 LLM retry iterations.
**Downsides:** Only relevant in build mode with dry-run failures.
**Confidence:** 80%
**Complexity:** Low
**Status:** Explored

### 4. Opinionated Defaults in seeknal_agent.yml
**Description:** `defaults:` section in config pre-answers common questions. Loaded into system prompt. Agent checks defaults before asking.
**Rationale:** Zero harness code. Zero regression risk. Makes ask_user smarter.
**Downsides:** Static — doesn't learn from answers (v2).
**Confidence:** 85%
**Complexity:** Low
**Status:** Explored

### 5. Question Budget (Defensive Safeguard)
**Description:** `max_questions` counter on ToolContext (default 5, configurable). When exhausted, ask_user returns fallback and agent proceeds autonomously.
**Rationale:** Prevents confused agent from looping ask_user indefinitely.
**Downsides:** Arbitrary default.
**Confidence:** 90%
**Complexity:** Low
**Status:** Explored

### 6. Pre-Apply Draft Preview with Confirm
**Description:** Before apply_draft writes files, agent calls ask_user showing draft content with accept/edit/discard options. Rich Syntax highlighting.
**Rationale:** File writes are hard to undo. Users want to review generated YAML/Python.
**Downsides:** Adds pause to every apply. Must skip in one-shot.
**Confidence:** 70%
**Complexity:** Low-Medium
**Status:** Explored

### 7. Post-Inspect Refinement Question (Optional)
**Description:** After inspect_output shows sample rows, agent asks "Does this look right?" with structured options. Skippable via config.
**Rationale:** Closes feedback loop at verification step. Users can say "this is wrong" without new session.
**Downsides:** Adds friction after every inspect. Must be optional.
**Confidence:** 65%
**Complexity:** Low
**Status:** Explored

## Rejection Summary

| # | Idea | Reason Rejected |
|---|------|-----------------|
| 1 | Pipeline design interview (5 questions) | Belongs in prompt, not harness |
| 2 | Mid-run checkpoint | `confirmed=True` already exists |
| 3 | Session-start intent menu | Keyword detection in disguise |
| 4 | "What next?" after analysis | Agent behavior, not harness mechanism |
| 5 | DAG confirmation gate | Regresses one-shot mode |
| 6 | Multi-step scoping wizard + DesignState | Fragile state machine |
| 7 | $EDITOR integration | Async/TTY complexity |
| 8 | Checkpoint resumption | Separate system, out of scope |
| 9 | Branching alternative designs | 3x context cost |
| 10 | Per-step Y/n nudges | Redundant with confirmed pattern |
| 11 | Clarify-before-execute gate | Mandatory overhead on 98% happy path |
| 12 | Intent summary before first tool | Harness surgery for prompt behavior |
| 13 | Pre-run clarification subgraph | Doubles architecture |
| 14 | User-driven build mode | Different product |
| 15 | Replay-and-branch | Requires checkpoint persistence |
| 16 | Memory-backed preference learning | Scope creep for v1 |
| 17 | Multi-source join negotiation | Already handled by profile_data + dry_run |
| 18 | Progressive Build Contract bundle | Bundles a rejected idea |

## Session Log
- 2026-03-26: Initial ideation — 39 raw ideas across 5 frames, 24 unique after dedup + 3 cross-cutting syntheses, 7 survived adversarial filtering. All 7 explored via brainstorm.
