# Seeknal Ask Agent Harness Rebuild — OpenClaw-Inspired Architecture

**Date:** 2026-03-23
**Status:** brainstorm
**Author:** Principal Engineer
**Research:** OpenClaw source code analysis + Anthropic harness engineering + Claude Code best practices

## What We're Building

A full harness rebuild for the seeknal Ask agent, inspired by OpenClaw's modular architecture. The goal is an agent that is easily configurable, token-efficient, and specialized for data engineering, ML development, and analytics.

## Why This Matters

**Current problems (validated in QA testing):**

1. **Monolithic 11.7KB system prompt** — causes Gemini empty responses, wastes tokens on irrelevant sections (report generation instructions shown during pipeline building)
2. **All 22 tools always loaded** — analysis queries get 8 write tools they can't use; the prompt says "don't use these" instead of not providing them
3. **No skills-on-demand** — pipeline design instructions, Evidence.dev syntax, DuckDB rules all embedded upfront even when not needed
4. **No progress persistence** — long pipeline builds lose context on retry; no session state between chat turns beyond LangGraph checkpointer
5. **Intent detection is prompt-only** — relies on LLM reading "ANALYSIS mode: NO pipeline changes" but tools are still available, leading to violations

**OpenClaw patterns that solve these:**

| OpenClaw Pattern | seeknal Gap | Impact |
|-----------------|------------|--------|
| Composable prompt sections with `promptMode` | One 11.7KB blob | ~60% prompt reduction for analysis mode |
| Tool profiles (minimal/coding/messaging/full) | All 22 tools always | Enforce read-only in analysis mode |
| Skills-on-demand (list → lazy read) | All instructions embedded | ~8KB saved per request |
| Bootstrap files (AGENTS.md, SOUL.md) | Only ArtifactDiscovery context | No user customization of agent behavior |
| Sub-agent minimal prompts | Full prompt for everything | 30-40% cost reduction for sub-tasks |

## Key Decisions

1. **Full harness rebuild** — not incremental patches. The current monolithic prompt has hit its limits.

2. **Single unified persona** — one "principal-level data, ML, and analytics engineer" identity. No role switching. Users who want different behavior customize via project-level config files.

3. **Lazy-load skills from project** — system prompt lists available skills with one-line descriptions. Agent reads the full SKILL.md only when the user's request matches. Saves ~8KB of prompt tokens on average.

4. **3 tool profiles**: `analysis`, `build`, `full`
   - **analysis** = read-only (profile_data, execute_sql, inspect_output, describe_table, list_tables, execute_python, generate_report, save_report_exposure, get_entities, get_entity_schema)
   - **build** = analysis + write tools (draft_node, dry_run_draft, apply_draft, edit_node, plan_pipeline, show_lineage, run_pipeline)
   - **full** = everything (build + search_pipelines, search_project_files, read_pipeline, read_project_file)

5. **Jinja2 template files for prompt sections** — split into separate `.j2` files, rendered and concatenated based on profile + intent. Easier to edit without touching Python code.

## Design: Modular Prompt Architecture

### Prompt Sections (Jinja2 templates)

```
src/seeknal/ask/prompts/
├── core.j2              # Identity, intent detection, critical rules (~2KB)
├── analysis.j2          # Data analysis workflow, SQL rules, DuckDB patterns (~2KB)
├── build.j2             # Pipeline building workflow, medallion architecture (~3KB)
├── report.j2            # Evidence.dev report generation, chart types (~4KB)
├── skills_catalog.j2    # Available skills listing for lazy loading (~0.5KB)
├── context.j2           # Dynamic project context from ArtifactDiscovery (~variable)
└── safety.j2            # Security rules, blocked functions (~0.5KB)
```

### Prompt Assembly by Profile

| Profile | Sections Loaded | Estimated Size |
|---------|----------------|----------------|
| analysis | core + analysis + context + safety | ~5KB |
| build | core + analysis + build + context + safety | ~8KB |
| full | core + analysis + build + report + skills_catalog + context + safety | ~12KB |

### Profile Selection Logic

```python
def select_profile(question: str, has_tables: bool) -> str:
    build_keywords = {"build", "create", "add", "design", "pipeline", "draft", "deploy"}
    report_keywords = {"report", "dashboard", "visualization", "chart"}

    words = set(question.lower().split())
    if words & build_keywords:
        return "build"
    if words & report_keywords:
        return "full"
    return "analysis"
```

Profile is selected per-request in one-shot mode, or at session start in chat mode.

### Skills Catalog (Lazy Loading)

The `skills_catalog.j2` template lists available skills without full content:

```
## Available Skills

Before acting, scan these skills. If one matches the user's request,
use `read_project_file` to load the full instructions from the skill path.

| Skill | Description | Path |
|-------|------------|------|
| pipeline | Build data pipelines with draft → dry-run → apply | .claude/skills/seeknal-pipeline/SKILL.md |
| pipeline-design | Interactive pipeline design brainstorm (chat mode) | .claude/skills/seeknal-pipeline-design/SKILL.md |
| analytics | Semantic models, metrics, exposures | .claude/skills/seeknal-analytics/SKILL.md |
| feature-group | Entity-based features for ML | .claude/skills/seeknal-feature-group/SKILL.md |
| model | ML model training and serving | .claude/skills/seeknal-model/SKILL.md |
| deploy | Materialization + environments | .claude/skills/seeknal-deploy/SKILL.md |
| debug | Troubleshooting pipeline failures | .claude/skills/seeknal-debug/SKILL.md |
```

Agent reads the full SKILL.md only when needed — saves ~8KB per request.

### Tool Profile Implementation

```python
TOOL_PROFILES = {
    "analysis": [
        profile_data, list_tables, describe_table,
        execute_sql, inspect_output,
        get_entities, get_entity_schema,
        execute_python, generate_report, save_report_exposure,
    ],
    "build": [
        # analysis tools +
        draft_node, dry_run_draft, apply_draft,
        edit_node, plan_pipeline, show_lineage, run_pipeline,
    ],
    "full": [
        # build tools +
        read_pipeline, search_pipelines,
        search_project_files, read_project_file,
    ],
}
```

In `create_agent()`, tools are selected based on profile:
```python
tools = TOOL_PROFILES[profile]
```

This means in analysis mode, the agent literally cannot call `draft_node` or `run_pipeline` — enforcement is structural, not just prompt-based.

## Design: Harness Patterns from Anthropic

### Progress Tracking (from Anthropic's harness research)

For long pipeline builds in chat mode:
- After each successful node creation, update a structured progress artifact
- On session restart, agent reads progress before acting
- Format: JSON (less likely to be overwritten by LLM than markdown)

```json
{
  "pipeline_design": { "nodes": 12, "edges": 11, "status": "confirmed" },
  "built_nodes": ["source.customers", "source.orders", "transform.enriched_orders"],
  "pending_nodes": ["transform.customer_360", "model.customer_segments"],
  "last_action": "Applied transform.enriched_orders",
  "errors": []
}
```

### Context Firewalls (Sub-Agent Optimization)

For complex pipelines, spawn sub-agents with minimal prompts:
- **Build sub-agent**: Gets `build` profile + specific node to create
- **Verify sub-agent**: Gets `analysis` profile + specific output to validate
- Parent orchestrates, sub-agents execute isolated tasks

## Comparison: Before vs After

| Aspect | Current | After Rebuild |
|--------|---------|---------------|
| Prompt size (analysis) | 11.7KB | ~5KB (57% reduction) |
| Prompt size (build) | 11.7KB | ~8KB (32% reduction) |
| Tool count (analysis) | 22 | 10 (no write tools) |
| Tool count (build) | 22 | 17 (analysis + build) |
| Intent enforcement | Prompt-only (soft) | Tool profile (hard) |
| Skills loading | All upfront | On-demand (~8KB saved) |
| Report instructions | Always loaded | Only when needed |
| Gemini empty response rate | ~30% first call | Expected ~10% (smaller prompt) |
| Progress tracking | None | JSON artifact in target/ |
| User customization | Edit source code | Edit .j2 templates or config |

## Resolved Questions

1. **Profile scope** → Auto-detected per message based on intent keywords. Most flexible — user can ask analytics questions and build nodes in the same chat session.
2. **Config file** → Yes, `seeknal_agent.yml` in project root with model, temperature, default_profile, disabled_tools, custom_prompt_sections. Like OpenClaw's config.json but YAML.
3. **Progress artifact** → Stored in `target/.ask_progress.json`, separate from `run_state.json` (which tracks pipeline execution, not agent state).

## Sources

### OpenClaw Source Code (local)
- System prompt builder: `/Volumes/Hyperdisk/project/self/openclaw/src/agents/system-prompt.ts`
- Tool catalog with profiles: `/Volumes/Hyperdisk/project/self/openclaw/src/agents/tool-catalog.ts`
- Skills loading: `/Volumes/Hyperdisk/project/self/openclaw/src/agents/skills/workspace.ts`
- Sub-agent spawn: `/Volumes/Hyperdisk/project/self/openclaw/src/agents/subagent-spawn.ts`

### External Research
- [OpenClaw System Prompt Docs](https://docs.openclaw.ai/concepts/system-prompt)
- [Anthropic: Effective Harnesses for Long-Running Agents](https://www.anthropic.com/engineering/effective-harnesses-for-long-running-agents)
- [HumanLayer: Harness Engineering for Coding Agents](https://www.humanlayer.dev/blog/skill-issue-harness-engineering-for-coding-agents)
- [Claude Code Best Practices](https://code.claude.com/docs/en/best-practices)
