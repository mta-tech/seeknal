---
name: compound
description: Extract and document learnings from completed builds. Use after /build completes to compound knowledge.
argument-hint: [path-to-spec]
model: opus
allowed-tools: Task, TaskOutput, Bash, Glob, Grep, Read, Edit, Write, AskUserQuestion, Skill
---

# /compound - Knowledge Compounding from Builds

## Purpose

Extract and document learnings from completed builds to fuel the compound engineering process. Each build makes future builds easier by capturing what was learned.

> "Each unit of engineering work should make subsequent units of work easier—not harder."

## When to Use

Run `/compound` after completing builds with `/build` to document:
- **Architecture choices** - decisions made during implementation
- **Deployment choices** - environment setup and configuration
- **Mistakes & solutions** - errors encountered and their fixes
- **CLAUDE.md updates** - project patterns and guidelines

## Usage

```bash
# Compound the most recent build
/compound

# Compound a specific spec
/compound specs/user-authentication.md

# Dry run (preview without writing)
/compound --dry-run

# Force re-compound (skip "already compounded" check)
/compound --force
```

## Variables

The command receives these variables:

```yaml
spec_path:
  description: Path to spec file to compound
  default: Auto-detect most recent spec in specs/
  required: false

dry_run:
  description: Preview changes without writing files
  default: false
  type: boolean

force:
  description: Skip "already compounded" check
  default: false
  type: boolean
```

## Instructions

### Discovery Phase

First, discover what to compound:

**Step 1: Find Spec File**

If `spec_path` is not provided:

```bash
# Find most recent spec
find specs/ -name "*.md" -type f -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1 | cut -d' ' -f2-
```

Or use glob:

```typescript
// Get all specs
Glob({ pattern: "specs/*.md" })
```

Use the most recently modified spec file.

**Step 2: Check If Already Compounded**

Read the spec file and check for:

```yaml
# At the end of spec file
## Compounded

- [ ] Last compounded: YYYY-MM-DD
- [ ] ADRs created: N
- [ ] Solutions documented: N
```

If `## Compounded` section exists with all checkboxes marked `[x]`:
- Return message: "This spec was already compounded on [date]. Use `/compound --force` to re-compound."
- Exit unless `force: true`

**Step 3: Validate Spec Has Completed Tasks**

```typescript
// Get all completed tasks
const tasks = await TaskList({})

// Filter for completed tasks
const completedTasks = tasks.filter(t => t.status === 'completed')

if (completedTasks.length === 0) {
  return {
    error: "no_completed_tasks",
    message: "No completed tasks found. Run /build first to complete the spec.",
    spec_path: specPath
  }
}
```

### Analysis Phase

Launch 7 parallel subagents to analyze the spec and tasks:

```typescript
// Launch all agents in parallel
const agents = [
  Task({
    subagent_type: "task-analyzer-agent",
    prompt: `Analyze spec ${specPath} and extract all architecture decisions, errors, deployment info, and patterns.

Output a structured JSON with:
- decisions[] (architecture choices made)
- errors[] (bugs encountered and fixed)
- deployment{} (environment and configuration)
- patterns[] (reusable patterns)

Save output to: task-analysis.json`,
    model: "opus",
    run_in_background: true
  }),

  Task({
    subagent_type: "architecture-writer-agent",
    prompt: `Create Architecture Decision Records (ADRs) for the build.

Input will be provided via task-analysis.json with decisions array.
For each decision, create: docs/adr/adr-XXX-title.md

Use ADR format:
---
adr_id: ADR-XXX
date: YYYY-MM-DD
status: accepted
title: Decision Title
---

# ADR-XXX: Decision Title

## Context
[What problem are we solving?]

## Decision
[What was chosen?]

**Rationale:**
- [Why this choice?]

**Consequences:**
- [ ] Positive consequence
- [ ] Negative consequence

## Alternatives Considered
- Alternative 1
- Alternative 2

## Related
- Spec: specs/feature.md
- Tasks: task-1, task-2

Reference .claude/adr-counter.txt for next ADR number.`,
    model: "opus",
    run_in_background: true
  }),

  Task({
    subagent_type: "deployment-writer-agent",
    prompt: `Update deployment documentation with changelog.

Input will be provided via task-analysis.json with deployment object.
Update: docs/deployment.md

Add new entry to ## Changelog section:
### YYYY-MM-DD - [Build Name]
- [Deployment change 1]
- [Deployment change 2]

**Lessons Learned:**
- [Lesson 1]
- [Lesson 2]

Keep the frontmatter updated:
---
last_updated: YYYY-MM-DD
environment: [environment]
platform: [platform]
---`,
    model: "opus",
    run_in_background: true
  }),

  Task({
    subagent_type: "mistake-extractor-agent",
    prompt: `Extract and document mistakes and solutions.

Input will be provided via task-analysis.json with errors array.
For each error, create: docs/solutions/[category]/[problem].md

Use solution format:
---
category: [category]
component: [component]
tags: [tag1, tag2]
date_resolved: YYYY-MM-DD
related_build: [build-name]
related_tasks: [task-ids]
---

# [Problem Title]

## Problem Symptom
[What went wrong?]

## Investigation Steps
1. [Step 1]
2. [Step 2]

## Root Cause
[Technical explanation]

## Working Solution
[Step-by-step fix with code examples]

**Result:** [Before → After metrics]

## Prevention Strategies
1. [Strategy 1]
2. [Strategy 2]

## Test Cases Added
[code examples]

## Cross-References
- Related: [links to related docs]`,
    model: "opus",
    run_in_background: true
  }),

  Task({
    subagent_type: "claude-updater-agent",
    prompt: `Update CLAUDE.md with new patterns.

Input will be provided via task-analysis.json with patterns array.
Update: CLAUDE.md

Add patterns to appropriate sections (Architecture, Deployment, Testing, etc.):
## [Category] Patterns

### [Pattern Name]
[Description]

**Correct:**
[code example]

**Incorrect:**
[code example]

**Rationale:** [Why this pattern matters]`,
    model: "opus",
    run_in_background: true
  }),

  Task({
    subagent_type: "planning-patterns-agent",
    prompt: `Analyze spec ${specPath} and extract reusable planning-level patterns.

Focus on decisions about:
- Team composition (who was on the team and why)
- Testing strategy (what testing approach was used)
- Task ordering and dependencies
- Scope decisions (what was included/excluded)

Read existing docs/planning-patterns.md to avoid duplicates.
Append new patterns under the appropriate category.

Report what patterns were extracted.`,
    model: "opus",
    run_in_background: true
  }),

  Task({
    subagent_type: "doc-assembler-agent",
    prompt: `Coordinate final assembly and validation.

Wait for all other agents to complete:
1. Read task-analysis.json
2. Read all created ADRs (docs/adr/*.md)
3. Read all solutions (docs/solutions/*/*.md)
4. Read updated docs/deployment.md
5. Read updated CLAUDE.md
6. Read updated docs/planning-patterns.md

Validate:
- [ ] All decisions have ADRs created
- [ ] All errors have solutions documented
- [ ] Deployment.md has new changelog entry
- [ ] CLAUDE.md has new patterns (if any)
- [ ] Planning patterns extracted (if any)
- [ ] All files are valid markdown
- [ ] All cross-references work

Generate final summary report with links to created docs.`,
    model: "opus",
    run_in_background: true
  })
]

// Wait for all agents to complete
const results = await Promise.all(agents.map(a => a.result))
```

**Wait for Doc Assembler:**

The doc-assembler-agent will produce the final report. Use TaskOutput to retrieve it:

```typescript
// Get doc assembler output
const report = await TaskOutput({
  task_id: docAssemblerTaskId,
  block: true,
  timeout: 300000 // 5 minutes
})
```

### Assembly Phase

After doc-assembler-agent completes:

**Step 1: Read Final Report**

The report contains:
- Total decisions documented
- Total errors/solutions documented
- Deployment changes captured
- Patterns added to CLAUDE.md
- Links to all created documents

**Step 2: Update Spec with Compounded Status**

Add to the end of the spec file:

```markdown
## Compounded

- [x] Last compounded: YYYY-MM-DD
- [x] ADRs created: N
- [x] Solutions documented: N
- [x] Deployment changes: N
- [x] Patterns added: N

**Generated Documents:**
- ADRs: [links]
- Solutions: [links]
- Deployment: docs/deployment.md
- Guidelines: CLAUDE.md
```

**Step 3: Update ADR Counter**

```bash
# Increment ADR counter
current=$(cat .claude/adr-counter.txt)
echo $((current + N)) > .claude/adr-counter.txt
```

Where N is the number of new ADRs created.

**Step 4: Present Final Report**

```
## Knowledge Compounded

Spec: specs/feature-name.md

Extracted Learnings:
- Architecture Decisions: 3
- Errors/Solutions: 2
- Deployment Changes: 1
- Reusable Patterns: 4

**Created Documents:**

Architecture Decisions (ADRs):
- docs/adr/adr-001-use-fiber-framework.md
- docs/adr/adr-002-gorm-for-orm.md
- docs/adr/adr-003-jwt-authentication.md

Mistakes & Solutions:
- docs/solutions/performance-issues/n-plus-one-employee-list.md
- docs/solutions/database-optimizations/sqlite-wal-mode.md

Deployment:
- docs/deployment.md (updated with changelog)

Agent Guidelines:
- CLAUDE.md (updated with new patterns)

Planning Patterns:
- docs/planning-patterns.md (N new patterns extracted)

---
Knowledge compounded! Future builds will now benefit from these learnings.
```

## Report Format

The final report should include:

```yaml
spec: specs/feature-name.md
spec_title: Feature Name
completed_tasks: 15
extraction_date: 2026-02-03T10:00:00Z

decisions_documented: 3
decisions:
  - title: "Use Fiber framework"
    adr: "docs/adr/adr-001-use-fiber-framework.md"

errors_documented: 2
errors:
  - category: "performance"
    problem: "N+1 Query in Employee List"
    solution: "docs/solutions/performance-issues/n-plus-one-employee-list.md"

deployment_changes: 1
deployment:
  environment: "production"
  platform: "linux-amd64"
  updated: "docs/deployment.md"

patterns_added: 4
patterns:
  - "Multi-tenant query filtering"
  - "GORM Preloading"
  - "Error response format"
  - "Service management with systemd"

planning_patterns_extracted: 2
planning_patterns:
  - category: "Testing Strategy"
    pattern: "Always include QA with real data tests across csv/iceberg/postgres"
  - category: "Team Composition"
    pattern: "Include dedicated docs agent for API-heavy features"

validation:
  all_adrs_created: true
  all_solutions_documented: true
  deployment_updated: true
  claude_md_updated: true
  all_cross_references_valid: true
  planning_patterns_updated: true
```

## Error Handling

### Spec Not Found

```json
{
  "error": "spec_not_found",
  "spec_path": "specs/non-existent.md",
  "available_specs": [
    "specs/user-authentication.md",
    "specs/user-profile.md"
  ],
  "suggestion": "Choose an available spec or create the spec file first"
}
```

**Action:** Present list of available specs and ask user to choose.

### No Completed Tasks Found

```json
{
  "error": "no_completed_tasks",
  "spec_path": "specs/feature.md",
  "message": "No completed tasks found for this spec.",
  "suggestions": [
    "Run /build to complete the spec first",
    "Check if tasks were tracked via Task tool",
    "Tasks may have been deleted"
  ]
}
```

**Action:** Suggest running `/build` first.

### Already Compounded

```json
{
  "warning": "already_compounded",
  "spec_path": "specs/feature.md",
  "last_compounded": "2026-02-01",
  "message": "This spec was already compounded on 2026-02-01.",
  "suggestion": "Use /compound --force to re-compound"
}
```

**Action:** Inform user and exit unless `--force` flag is set.

### Subagent Failures

If any subagent fails:

```json
{
  "error": "subagent_failure",
  "failed_agent": "architecture-writer-agent",
  "error_message": "[error details]",
  "suggestion": "Check agent logs and retry"
}
```

**Action:**
1. Log which agent failed
2. Present error message
3. Suggest manual investigation or retry

### Dry Run Mode

When `dry_run: true`:

```json
{
  "dry_run": true,
  "spec_path": "specs/feature.md",
  "would_create": [
    "docs/adr/adr-001-use-fiber-framework.md",
    "docs/adr/adr-002-gorm-for-orm.md",
    "docs/solutions/performance-issues/n-plus-one-query.md",
    "docs/deployment.md (updated)",
    "CLAUDE.md (updated)"
  ],
  "message": "Dry run complete. No files were written."
}
```

**Action:** Preview what would be created, but don't write any files.

## Dry Run Mode

Dry run previews what would be created without writing files:

**Workflow:**
1. Run task-analyzer-agent to extract information
2. Preview all documents that would be created
3. Show file paths and brief descriptions
4. Ask user to confirm
5. If confirmed, proceed with full compound
6. If cancelled, exit without changes

## Examples

### Example 1: Simple Feature

**Input:** `/compound specs/user-authentication.md`

**Output:**
```
## Knowledge Compounded

Spec: specs/user-authentication.md

Extracted Learnings:
- Architecture Decisions: 1
- Errors/Solutions: 0
- Deployment Changes: 0
- Reusable Patterns: 1

**Created Documents:**

Architecture Decisions (ADRs):
- docs/adr/adr-001-jwt-authentication.md

Mistakes & Solutions:
None

Deployment:
No changes

Agent Guidelines:
- CLAUDE.md (updated with JWT pattern)

---
Knowledge compounded! Future builds will now benefit from these learnings.
```

### Example 2: Feature with Errors

**Input:** `/compound specs/employee-list.md`

**Output:**
```
## Knowledge Compounded

Spec: specs/employee-list.md

Extracted Learnings:
- Architecture Decisions: 1
- Errors/Solutions: 1
- Deployment Changes: 1
- Reusable Patterns: 2

**Created Documents:**

Architecture Decisions (ADRs):
- docs/adr/adr-002-gorm-for-orm.md

Mistakes & Solutions:
- docs/solutions/performance-issues/n-plus-one-employee-list.md
  - Symptom: Page load 15s for 200 employees
  - Solution: Added .Preload('Department')
  - Result: 15s → 400ms

Deployment:
- docs/deployment.md (updated with systemd service config)

Agent Guidelines:
- CLAUDE.md (updated with GORM preloading pattern)

---
Knowledge compounded! Future builds will now benefit from these learnings.
```

## Tips

1. **Run after significant builds** - Don't compound every small change. Focus on meaningful features.
2. **Review generated docs** - The compound command creates drafts. Review and refine if needed.
3. **Link related docs** - Add cross-references between ADRs and solutions.
4. **Keep ADRs focused** - One decision per ADR. Split complex decisions into multiple ADRs.
5. **Categorize solutions well** - Use specific categories for easy lookup (performance-issues, security-vulnerabilities, etc.)
6. **Update CLAUDE.md patterns** - These guide future agents. Keep them clear and actionable.
7. **Use dry run first** - Preview what will be created before committing.
8. **Force re-compound sparingly** - Only use --force if you need to update documentation.

## Related Commands

- `/build` - Execute build plans (creates tasks to compound)
- `/plan` - Create implementation plans (inputs to /build)
- `/status` - Check build status (see if ready to compound)
- `/brainstorm` - Explore ideas before planning
- `/work` - Execute work plans directly

## References

- Compound Engineering Philosophy: `docs/plugin-docs/compound-engineering.md`
- ADR Format: https://adr.github.io/
- Task Analyzer Agent: `agents/task-analyzer-agent.md`
- Architecture Writer Agent: `agents/architecture-writer-agent.md`
- Deployment Writer Agent: `agents/deployment-writer-agent.md`
- Mistake Extractor Agent: `agents/mistake-extractor-agent.md`
- CLAUDE Updater Agent: `agents/claude-updater-agent.md`
- Doc Assembler Agent: `agents/doc-assembler-agent.md`
