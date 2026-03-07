---
title: "Seeknal Ask Step-by-Step Execution Visibility"
date: 2026-03-06
status: active
tags: [seeknal-ask, cli, ux, streaming, langgraph, rich]
---

# Seeknal Ask: Step-by-Step Execution Visibility

## What We're Building

Add KAI-style step-by-step visibility to `seeknal ask` CLI so users can see what the agent is doing as it works: reasoning panels, tool calls with arguments, SQL queries, intermediate result tables, and the final answer -- all rendered progressively in the terminal using Rich.

Currently, users see a static "Thinking..." spinner for 10-30 seconds with zero feedback. After this change, they'll see each step as it happens.

### Target Experience (CLI, no emojis per user preference)

```
You: How many customers per city?

[Reasoning] ----------------------------------------
  I need to find the customers table and group by
  city to count customers in each location.
--------------------------------------------------------

> list_tables
  Done: 8 tables found

> execute_sql
  SQL: SELECT city, COUNT(*) AS count
       FROM source_raw_customers
       GROUP BY city ORDER BY count DESC
  Done: 5 rows returned

  city      | count
  ----------|------
  Jakarta   |    14
  Bandung   |    12
  Surabaya  |    10
  Medan     |     8
  Semarang  |     6

[Answer] -------------------------------------------
  There are 50 customers across 5 cities. Jakarta
  leads with 14 customers, followed by Bandung (12)
  and Surabaya (10)...
--------------------------------------------------------
```

## Why This Approach

### Problem
- Users have no visibility into what the agent is doing
- 10-30 second blind wait with just "Thinking..." spinner
- When things go wrong (SQL errors, retries), user has no idea why it's slow
- Ralph Loop retries are invisible -- user just waits longer

### Reference: KAI's Architecture
Studied the KAI codebase at `/Volumes/Hyperdisk/project/self/KAI`. KAI uses:
- `astream_events()` from LangGraph for fine-grained event streaming
- SSE (Server-Sent Events) for web frontend
- Rich `Live` panels + `Console.print()` for CLI
- Event types: `token`, `tool_start`, `tool_end`, `thinking`, `sql_result`, `done`, `error`
- `TagStreamParser` to separate `<thinking>` from `<answer>` content
- Collapsible tool call blocks showing name, input args, output (truncated)

### Why `astream_events()` (async)
User chose the async fine-grained event API over synchronous `stream()` for maximum granularity. This gives us:
- `on_tool_start` / `on_tool_end` events with tool name + args + results
- `on_chat_model_stream` for token-level LLM output
- `on_chain_start` / `on_chain_end` for agent reasoning boundaries
- Closest match to KAI's proven architecture

## Key Decisions

1. **CLI-only scope** -- No web UI, no SSE API endpoint. Rich terminal rendering only.
2. **Async `astream_events()`** -- Use LangGraph's async event API wrapped with `asyncio.run()` in CLI entry points. Gives token-level and tool-level granularity.
3. **KAI-style full visibility** -- Show reasoning panels, tool name + args on call, tool results (truncated), SQL highlighted, data tables for query results, final answer.
4. **LLM text between tool calls = reasoning** -- No XML tag parsing needed. Any AI text generated before a tool call is treated as reasoning and displayed in a panel.
5. **Ralph Loop iterations visible** -- All retry passes show their tool calls too, so users see what's happening during retries.
6. **No emojis** -- Use text markers like `>`, `Done:`, `[Reasoning]`, `[Answer]` instead.
7. **Rich library components** -- `Console.print()`, `Panel`, `Table`, `Syntax` (for SQL), `Markdown` (for answer). Use sequential printing (not `Live`) since we stream events incrementally.

## Scope

### In Scope
- Streaming event loop replacing `agent.invoke()` with `astream_events()`
- Rich rendering of: reasoning, tool calls, SQL queries, result tables, final answer
- Both one-shot (`seeknal ask "question"`) and chat (`seeknal ask chat`) modes
- Ralph Loop retries with visibility
- Fallback to current behavior if Rich is not installed

### Out of Scope
- Web UI / SSE API endpoints
- Token-by-token streaming of the final answer (show answer as complete block)
- Chart/visualization recommendations
- Subagent nesting visualization
- Todo list panels (KAI feature not applicable to seeknal ask)

## Architecture Sketch

```
CLI (ask.py)
  |
  v
ask_streaming() -- new function, replaces ask()
  |
  v
asyncio.run(_astream_ask(agent, config, question, renderer))
  |
  v
async for event in agent.astream_events(input, config, version="v2"):
  |
  +-- on_tool_start  --> renderer.show_tool_start(name, args)
  +-- on_tool_end    --> renderer.show_tool_end(name, result)
  +-- on_chat_model_stream --> renderer.buffer_token(token)
  +-- (AI message before tool) --> renderer.show_reasoning(text)
  +-- (final AI message) --> renderer.show_answer(text)
  |
  v
StepRenderer (new class, wraps Rich Console)
  - show_reasoning(text) --> Panel with "Reasoning" title
  - show_tool_start(name, args) --> "> {name}" with dim args
  - show_tool_end(name, result) --> "  Done: {summary}" + optional Table
  - show_sql(sql) --> Syntax highlighted SQL block
  - show_answer(text) --> Panel with "Answer" title, Markdown rendered
```

## Open Questions

*None -- all resolved during brainstorm dialogue.*

## Resolved Questions

1. **Scope?** CLI-only with Rich. No web UI.
2. **Detail level?** KAI-style full visibility (reasoning, tools, SQL, tables, answer).
3. **Ralph Loop?** Show all iterations with step-by-step visibility.
4. **Reasoning trigger?** LLM text between tool calls = reasoning. No XML tags.
5. **Streaming API?** `astream_events()` async (user's choice over sync `stream()`).
