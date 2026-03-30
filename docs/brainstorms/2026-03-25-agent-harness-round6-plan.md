# Agent Harness Improvements: Round 6

## Context

Round 5 eliminated the #1 waste pattern (post-profile stalls): 0 nudges across 4 runs vs 8 in Round 4. QA score is 52/53 (98%) with the single failure being a DATE_DIFF argument direction bug in the agent's generated SQL. An additional infra failure (Gemini 503) revealed the harness has no API error resilience.

### Round 5 Scorecard

| Test | R4 | R5 | Nudges R4→R5 | Tool Calls |
|------|----|----|--------------|------------|
| tui-e2e | 11/11 (280s) | 11/11 (402s) | 2→0 | 44 |
| analytics-clv-rfm | 14/14 (400s) | 14/14 (470s) | 3+sync→0 | 85 |
| ml-feature-engineering | 15/15 (320s) | 14/15 (596s) | 2→0 | 45 |
| multi-source-joins | 13/13 (298s) | 13/13 (392s) | 1→0 | 40 |

**Key metrics:**
- Nudges: 8 → 0 (eliminated)
- Sync fallbacks: 1 → 0 (eliminated)
- Test failures: 0 → 1 (DATE_DIFF direction bug)
- Edit churn: analytics 23 edits for 14 nodes, ML 12 for 10

## Changes

### 1. API error resilience in streaming (P0)

**Problem:** Gemini 503 "Service Unavailable" crashes the entire run with exit code 1 and an unhandled `ServerError` exception. The analytics-clv-rfm run had built 5/6 transforms successfully before the crash — all that work was lost.

**Fix:** Wrap the streaming loop in a retry with exponential backoff for transient API errors (503, 429, timeout). If all retries fail, fall back to sync mode.

**File:** `src/seeknal/ask/streaming.py` — wrap `_stream_one_pass()` call

```python
# Inside stream_ask(), wrap the _stream_one_pass call:
try:
    answer, had_tool_calls, tools_called = await _stream_one_pass(
        agent, config, current_question, console
    )
except Exception as api_err:
    err_str = str(api_err).lower()
    is_transient = any(code in err_str for code in ["503", "429", "unavailable", "timed out", "rate limit"])
    if is_transient and attempt < _MAX_RALPH_RETRIES:
        wait = 2 ** (attempt + 1)  # 2, 4, 8 seconds
        console.print(f"\n[yellow]API error: {type(api_err).__name__}. Retrying in {wait}s...[/yellow]\n")
        await asyncio.sleep(wait)
        continue
    raise  # Non-transient or retries exhausted
```

**File:** `src/seeknal/cli/ask.py` — wrap `_run_oneshot()` to catch remaining errors gracefully

```python
# In _run_oneshot, wrap the asyncio.run with a fallback:
try:
    answer = asyncio.run(stream_ask(...))
except Exception as e:
    if "503" in str(e) or "429" in str(e) or "Unavailable" in str(e):
        console.print(f"\n[yellow]API temporarily unavailable. Retrying with sync mode...[/yellow]\n")
        answer = sync_ask(agent, config, question)
    else:
        raise
```

### 2. DATE_DIFF direction guard in safety.j2 (P0)

**Problem:** Agent wrote `DATE_DIFF('day', MAX(session_date), '2024-01-01')` instead of using `CURRENT_DATE`. This produces negative "days since" values when the max date is after the hardcoded date. This is the only test failure in Round 5.

**Root cause:** The agent hardcodes dates from training data instead of using `CURRENT_DATE`. The existing safety.j2 has DATE_DIFF guidance but doesn't address the "days since" pattern specifically.

**Fix:** Add explicit guidance to `safety.j2` DuckDB SQL Rules section:

```
- For "days since" calculations: `DATE_DIFF('day', CAST(event_date AS DATE), CURRENT_DATE)` — ALWAYS use CURRENT_DATE as the second argument, NEVER hardcode a date
- DATE_DIFF argument order: `DATE_DIFF(part, start_date, end_date)` — start first, end second. For "days since X", X is start and CURRENT_DATE is end.
```

**File:** `src/seeknal/ask/prompts/safety.j2` (after line 28, in DuckDB SQL Gotchas)

### 3. Reduce edit churn with write-once model template (P1)

**Problem:** analytics-clv-rfm had 23 edit_file calls for 14 nodes (1.6x multiplier). ML had 12 for 10 (1.2x). Extra edits are mostly fixing: wrong column names, missing CAST, wrong ref() names, missing imports. Each edit is a full LLM round-trip (~10s).

**Root cause:** The `draft_node` tool creates a generic template that the agent must then fully rewrite via `edit_file`. For complex transforms, the agent sometimes gets the SQL wrong on the first edit, then needs to read the error and fix it.

**Fix — two-pronged:**

**a) Allow `draft_node` to accept initial content:**

Add an optional `content` parameter to `draft_node` so the agent can write the full YAML in one shot instead of draft→edit:

```python
@tool
def draft_node(node_type: str, name: str, python: bool = False, content: str = "") -> str:
    """Draft a new pipeline node.
    ...
    Args:
        content: Optional initial YAML/Python content. If provided, replaces
                 the template content entirely. Saves one edit_file round-trip.
    """
```

If `content` is provided, write it directly to the draft file instead of the template. This eliminates the mandatory edit_file call for every node.

**b) Update build.j2 to prefer single-shot drafts:**

```
When you know the full YAML/Python content, pass it via `draft_node(content=...)` to skip the edit step.
Only use separate `edit_file` when you need to modify an existing draft or fix validation errors.
```

**File:** `src/seeknal/ask/agents/tools/draft_node.py`
**File:** `src/seeknal/ask/prompts/build.j2`

**Expected impact:** Reduces tool calls by ~20-30% for complex pipelines.

### 4. Parallel source creation (P1)

**Problem:** The agent creates sources one at a time: draft→apply for each. With 7 sources (multi-source-joins), that's 14 sequential tool calls. Sources are independent and could be created in parallel.

**Observation:** In tui-e2e, the agent actually batched 4 `draft_node` calls (lines 19-28), but then went back to edit+apply each sequentially. Multi-source-joins did draft→apply one at a time (7 pairs = 14 calls).

**Fix:** Update the build prompt to encourage batching source creation:

```
For sources, you can batch: call draft_node for ALL sources first, then apply_draft for ALL.
Sources are simple CSV mappings — they don't need edit_file or dry_run_draft.
```

**File:** `src/seeknal/ask/prompts/build.j2`

### 5. Warn on hardcoded dates in dry_run_draft (P1)

**Problem:** The DATE_DIFF direction bug in #2 passed `dry_run_draft` validation because the SQL is syntactically valid. The ref/input consistency check from Round 5 catches structural errors but not semantic ones.

**Fix:** Add a date hardcode warning to `_check_yaml_ref_consistency()` (rename to `_check_yaml_warnings()`):

```python
def _check_yaml_warnings(draft_path) -> str:
    """Check for common issues in transform YAML."""
    # ... existing ref consistency check ...

    # Check for hardcoded dates in "days since" patterns
    if transform_sql:
        # Warn if DATE_DIFF uses a literal date instead of CURRENT_DATE
        date_diff_pattern = re.findall(
            r"DATE_DIFF\s*\([^)]*'(\d{4}-\d{2}-\d{2})'[^)]*\)",
            transform_sql, re.IGNORECASE
        )
        if date_diff_pattern:
            warnings.append(
                f"WARNING: DATE_DIFF uses hardcoded date(s): {date_diff_pattern}. "
                f"Consider using CURRENT_DATE instead for 'days since' calculations."
            )

    return "\n".join(warnings)
```

**File:** `src/seeknal/ask/agents/tools/dry_run_draft.py`

### 6. Feature group `FROM source` validation in dry_run (P2)

**Problem:** Feature groups must use `FROM source` (not `ref()`). The existing dry_run catches this at runtime, but a pre-validation in the tool would be faster and more helpful.

**Fix:** In `_check_yaml_warnings()`, add:

```python
# Check feature_group uses FROM source, not ref()
kind = data.get("kind", "")
if kind == "feature_group" and transform_sql:
    if "ref(" in transform_sql:
        warnings.append(
            "WARNING: Feature group transforms must use 'FROM source', not ref(). "
            "The upstream data is automatically loaded as the 'source' table."
        )
```

**File:** `src/seeknal/ask/agents/tools/dry_run_draft.py`

## Files to Modify

1. `src/seeknal/ask/streaming.py` — API error resilience (#1)
2. `src/seeknal/cli/ask.py` — Fallback on API crash (#1)
3. `src/seeknal/ask/prompts/safety.j2` — DATE_DIFF direction guide (#2)
4. `src/seeknal/ask/agents/tools/draft_node.py` — `content` parameter (#3a)
5. `src/seeknal/ask/prompts/build.j2` — Single-shot drafts, batch sources (#3b, #4)
6. `src/seeknal/ask/agents/tools/dry_run_draft.py` — Date hardcode + feature_group warnings (#5, #6)

## Verification

```bash
# 1. Import checks
uv run python -c "from seeknal.ask.streaming import stream_ask; print('OK')"
uv run python -c "from seeknal.ask.agents.tools.dry_run_draft import _check_yaml_ref_consistency; print('OK')"

# 2. Test date hardcode warning
uv run python -c "
import tempfile, yaml
from pathlib import Path
data = {
    'kind': 'transform', 'name': 'test',
    'inputs': [{'ref': 'source.sessions'}],
    'transform': \"SELECT DATE_DIFF('day', MAX(date), CAST('2024-01-01' AS DATE)) FROM ref('source.sessions')\"
}
p = Path(tempfile.mktemp(suffix='.yml'))
p.write_text(yaml.dump(data))
from seeknal.ask.agents.tools.dry_run_draft import _check_yaml_ref_consistency
result = _check_yaml_ref_consistency(p)
assert '2024-01-01' in result, f'Expected date warning, got: {result}'
p.unlink()
print('Date hardcode warning OK')
"

# 3. Run all 4 QA tests
uv run python qa/runs/tui-e2e/run_tui_test.py
uv run python qa/runs/analytics-clv-rfm/run_analytics_clv_rfm_test.py
uv run python qa/runs/ml-feature-engineering/run_ml_feature_engineering_test.py
uv run python qa/runs/multi-source-joins/run_multi_source_joins_test.py
```

## Expected Impact

| Improvement | Est. Impact | Confidence |
|---|---|---|
| #1 API error resilience | Prevents 100% data loss on transient errors | High |
| #2 DATE_DIFF direction | Fixes 1/53 test failure | High |
| #3 Write-once drafts | -20-30% tool calls on complex pipelines | Medium |
| #4 Batch source creation | -5-10s per source (minor) | Medium |
| #5 Date hardcode warning | Catches semantic bugs at draft time | Medium |
| #6 Feature group validation | Prevents runtime errors | High |

## Cumulative Progress

| Round | Score | Nudges | Key Win |
|-------|-------|--------|---------|
| R1 | Baseline | N/A | Initial harness |
| R2 | 44/53 | High | Feature group FROM source fix |
| R3 | 48/53 | Moderate | dry_run, ref consistency |
| R4 | 53/53 (100%) | 8 | Full pass, but stall waste |
| R5 | 52/53 (98%) | 0 | Nudge elimination, 1 DATE_DIFF bug |
| R6 (target) | 53/53 | 0 | API resilience, semantic validation |
