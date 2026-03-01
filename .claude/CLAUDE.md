# Multi-Agent Development Workflow - Agent Guidelines

Guidelines for AI agents working in this multi-agent orchestration system.

## Overview

This project uses a multi-agent orchestration workflow inspired by IndyDevDan and Every's Compound Engineering. The focus is on orchestration over implementation - becoming a conductor rather than a typist.

## Architecture Patterns

### Orchestration First

**Principle:** Use agents to execute work, don't implement directly.

- Use Task tool to deploy agents
- Use TaskList and TaskGet to track progress
- Use TaskOutput to monitor agent execution
- Resume agents with preserved context for follow-up work

**Correct:**
```typescript
// Deploy agent to do the work
const agent = Task({
  description: "Build authentication system",
  prompt: "Create JWT auth endpoints...",
  subagent_type: "general-purpose",
  run_in_background: true
})

// Later, resume same agent with preserved context
Task({
  description: "Continue authentication",
  prompt: "Now add refresh tokens...",
  subagent_type: "general-purpose",
  resume: agent.agentId  // Critical: preserves context
})
```

**Incorrect:**
```
// Writing code directly without agents
// Misses the point of the orchestration system
```

---

### Multi-Agent Coordination

**Principle:** Multiple agents can work in parallel when tasks are independent.

- Use `run_in_background: true` for parallel execution
- Use TaskOutput with `block: false` for non-blocking checks
- Wait for dependent agents with `block: true`

**Example:**
```typescript
// Launch frontend and backend agents in parallel
const frontendAgent = Task({
  description: "Build UI",
  subagent_type: "frontend-agent",
  run_in_background: true
})

const backendAgent = Task({
  description: "Build API",
  subagent_type: "backend-agent",
  run_in_background: true
})

// Both work in parallel...
// Wait for frontend
TaskOutput({ task_id: frontendAgent.agentId, block: true })

// Wait for backend
TaskOutput({ task_id: backendAgent.agentId, block: true })

// Integration test depends on both
Task({
  description: "Integration test",
  subagent_type: "test-agent",
  run_in_background: false  // Sequential
})
```

---

## Documentation Patterns

### Compounding Knowledge

**Principle:** Each unit of work should make subsequent units easier.

After completing builds, run `/compound` to document:
- Architecture decisions (ADRs in docs/adr/)
- Mistakes and solutions (docs/solutions/)
- Deployment learnings (docs/deployment.md)
- Reusable patterns (CLAUDE.md)

**When to Compound:**
- After significant features are built
- After solving difficult problems
- After making architectural decisions
- Before forgetting what was learned

---

### Knowledge-Aware Planning

**Principle:** Plans should leverage accumulated knowledge from past builds.

The compound engineering loop has two sides:
- **Write side**: `/compound` extracts learnings → `docs/adr/`, `docs/solutions/`, `docs/planning-patterns.md`
- **Read side**: `/plan_w_team` reads these during "Review Past Learnings" before designing

When applying past learnings, always cite the source (e.g., "Based on ADR-003").

---

### Plan-First Development

**Principle:** Always create a plan before building.

1. Run `/plan` or `/plan_w_team` to create a spec
2. Review the plan with the team
3. Run `/build` to execute the plan with multi-agent coordination
4. Run `/compound` to document learnings

**Never skip the planning step** - it ensures alignment and saves time.

---

## Implementation Patterns

### Command Structure

Follow the established pattern from `.claude/commands/`:

```yaml
---
name: command-name
description: Brief description
argument-hint: [optional-argument]
model: opus
allowed-tools: Task, TaskOutput, Bash, Glob, Grep, Read, Edit, Write, AskUserQuestion, Skill
---

# Command Name

## Variables
- `VAR`: Description

## Instructions
...

## Workflow
...

## Report
```

### Agent Structure

Follow the established pattern from `.claude/agents/`:

```yaml
---
name: agent-name
description: Brief description
tools: Task, TaskOutput, Bash, Glob, Grep, Read, Edit, Write, AskUserQuestion, Skill
model: opus
permissionMode: default
skills:
  - workflow:work
color: purple
---

# Agent Name

## Purpose
<what this agent does>

## Instructions
<step-by-step instructions>

## Workflow
<process flow>

## Report
<output format>
```

---

### Content-Based Feature Detection

**Principle:** Detect capabilities from content, not from metadata or headings alone.

When determining what features a document supports, scan for actual content patterns rather than structural headings. Headings may exist as empty templates.

**Example:** Team mode detection scans for `####` subheadings, `- **Name:**` fields, and `- **Agent Type:**` fields under `### Team Members` — not just the heading itself.

---

## Quality Standards

### Code Quality

- Follow existing code patterns in the codebase
- Match naming conventions exactly
- Write tests for new functionality
- Run tests after changes

### Documentation Quality

- Update plan document checkboxes as tasks complete
- Keep documentation synchronized with code
- Use clear, concise language
- Include examples for complex concepts

### Commit Quality

- Use incremental commits for logical units
- Write conventional commit messages
- Don't create "WIP" commits
- Group related changes into single commits

---

## Common Patterns

### Reading Codebases

**For exploration:**
- Use Task tool with Explore subagent for codebase exploration
- Use Glob to find files by pattern
- Use Grep to search for patterns
- Use Read to understand specific files

**For implementation:**
- Read existing patterns before creating new code
- Match the style and structure of existing code
- Reuse existing components where possible
- Don't reinvent patterns

---

### Error Handling

**When an agent fails:**
1. Read the error message from TaskOutput
2. Assess the cause (code error, missing dependency, unclear requirements?)
3. Choose action: Retry, Adjust prompt, or Skip
4. Document the resolution

**Recovery pattern:**
```typescript
// First attempt
Task({
  description: "Build API",
  prompt: "Create REST API endpoints...",
  subagent_type: "backend-agent"
})
// Returns with error about missing dependency

// Resume with correction
Task({
  description: "Build API - with dependency fix",
  prompt: "Add the missing dependency first, then create endpoints...",
  subagent_type: "backend-agent",
  resume: previousAgentId  // Preserve context
})
```

---

## Plugin Development Patterns

### State Schema Extension

When extending `scripts/state-file.js` to add new state fields, follow the established extension pattern.

- Add a new options parameter to `createInitialState` after existing optional params
- Use conditional initialization: only set the new field if the caller passes the option
- Add backward compatibility in `readStateFile`: default missing fields to `null`
- Keep new utility functions in a separate module (e.g., `scripts/ralph-loop.js`), not in `state-file.js`

**Correct:**
```javascript
// state-file.js — extend createInitialState signature
function createInitialState(specPath, specName, options, ralphOptions) {
  const state = { /* existing fields */ };
  if (ralphOptions) {
    state.ralphLoop = createRalphLoopState(ralphOptions);
  }
  return state;
}

// readStateFile — backward-compatible default
function readStateFile(stateDir) {
  const state = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  if (!state.ralphLoop) state.ralphLoop = null;  // safe default
  return state;
}
```

**Incorrect:**
```javascript
// WRONG — always initializing new field even when feature is unused
function createInitialState(specPath, specName, options) {
  return {
    ralphLoop: createRalphLoopState(),  // breaks callers that don't use ralph
    /* ... */
  };
}

// WRONG — putting ralph-loop helpers directly in state-file.js
function advanceRalphIteration(state) { /* ... */ }  // belongs in ralph-loop.js
```

**Rationale:** Conditional initialization keeps the state schema lean for callers that do not use the new feature, backward-compatible defaults prevent crashes when reading state files created before the extension, and separate modules keep `state-file.js` focused on generic state I/O.

---

### Stop Hook Implementation

For implementing Claude Code Stop hooks (bash scripts invoked before Claude exits).

- The script receives JSON on stdin containing `transcript_path`
- To block the exit: output JSON `{ "decision": "block", "reason": "...", "systemMessage": "..." }`
- To allow the exit: exit 0 with no stdout
- Use `node -e` (or heredoc `node << 'SCRIPT'`) for JSON operations instead of depending on `jq`
- Always exit 0 on errors -- safe default is to allow exit rather than trapping the user
- Use `$CLAUDE_PLUGIN_ROOT` for resolving script paths

**Correct:**
```bash
#!/bin/bash
set -euo pipefail

STDIN_JSON=$(cat)
TRANSCRIPT=$(echo "$STDIN_JSON" | node -e "
  let d=''; process.stdin.on('data',c=>d+=c);
  process.stdin.on('end',()=>console.log(JSON.parse(d).transcript_path));
")

# Evaluate whether to block
SHOULD_BLOCK=$(node "$CLAUDE_PLUGIN_ROOT/scripts/ralph-loop.js" check "$TRANSCRIPT" 2>/dev/null) || true

if [ "$SHOULD_BLOCK" = "block" ]; then
  echo '{"decision":"block","reason":"Ralph loop iteration incomplete","systemMessage":"..."}'
  exit 0
fi

# Allow exit — no stdout
exit 0
```

**Incorrect:**
```bash
# WRONG — depends on jq being installed
TRANSCRIPT=$(echo "$STDIN_JSON" | jq -r '.transcript_path')

# WRONG — exits non-zero on error, which may confuse Claude Code
if [ ! -f "$TRANSCRIPT" ]; then
  exit 1  # should exit 0 and allow exit instead
fi
```

**Rationale:** Stop hooks must be resilient. A failing hook should never trap the user inside a session. Using `node -e` avoids adding `jq` as a system dependency since Node.js is already guaranteed to be available.

---

### Shell Escaping in Node Validation

When writing inline `node -e` one-liners inside bash/zsh scripts, watch for shell interpretation conflicts.

- Avoid the `!==` operator in `node -e '...'` strings -- zsh interprets `!` as history expansion even inside single quotes in some contexts
- Prefer heredoc syntax (`node << 'SCRIPT' ... SCRIPT`) for multi-line or complex scripts
- Alternatively, rewrite logic using equality checks with inverted control flow

**Correct:**
```bash
# Option A: heredoc (preferred for anything non-trivial)
node << 'SCRIPT'
const data = JSON.parse(require("fs").readFileSync("/dev/stdin","utf8"));
if (data.status === "complete") {
  process.exit(0);
}
process.exit(1);
SCRIPT

# Option B: invert logic to avoid !==
node -e 'const v = JSON.parse(process.argv[1]).status; if (v === "complete") process.exit(0); process.exit(1);' "$JSON_VAR"
```

**Incorrect:**
```bash
# WRONG — !== triggers zsh history expansion, may silently corrupt the script
node -e 'if (JSON.parse(process.argv[1]).status !== "complete") process.exit(1);' "$JSON_VAR"
```

**Rationale:** Zsh history expansion (`!` followed by characters) can cause silent script corruption or unexpected errors that are extremely difficult to debug. Heredocs with single-quoted delimiters (`'SCRIPT'`) suppress all shell interpretation.

---

### Command File Modification

When adding new flags or modes to existing command files (`build.md`, `plan-w-team.md`, etc.), follow an additive-only approach.

- Update the `argument-hint` line in the frontmatter to include the new flag
- Add flag parsing logic in the Mode Detection / Variables section
- Add conditional behavior in the relevant workflow sections
- Add usage examples at the end of the file
- Preserve all existing content -- additions only, never remove or rewrite existing behavior

**Correct:**
```yaml
---
name: build
description: Multi-agent build from spec
argument-hint: <spec-name> [--ralph-loop]   # <-- flag added here
---

## Variables
- `SPEC_NAME`: The spec to build from
- `RALPH_LOOP`: If `--ralph-loop` flag is present, enable iterative loop mode  # <-- new

## Mode Detection
# ... existing detection logic preserved ...
RALPH_MODE=false
if echo "$ARGUMENTS" | grep -q -- '--ralph-loop'; then
  RALPH_MODE=true
fi

## Workflow
# ... existing steps preserved ...

### Step N: Ralph Loop (conditional)
If RALPH_MODE is true:
  - Initialize ralph loop state
  - Run iterative build cycle
```

**Incorrect:**
```yaml
# WRONG — rewriting the entire workflow section to add a flag
## Workflow
### Step 1: Ralph Loop Setup    # <-- existing steps were deleted/reordered
### Step 2: Build               # <-- original step 1 renumbered
```

**Rationale:** Command files are the user-facing API of the plugin. Rewriting existing content risks breaking established workflows. Additive changes preserve backward compatibility and make diffs reviewable.

---

## Growth Mindset

### Continuous Learning

- Document what you learn so others benefit
- Read documentation before implementing
- Ask questions when uncertain
- Share knowledge with the team

### Iteration Over Perfection

- Ship complete features, not perfect features
- Iterate based on feedback
- Focus on delivering value
- Improve incrementally

---

**Last Updated:** 2026-02-20
**Status:** Updated with plugin development patterns from ralph loop integration


<claude-mem-context>
# Recent Activity

<!-- This section is auto-generated by claude-mem. Edit content outside the tags. -->

*No recent activity*
</claude-mem-context>