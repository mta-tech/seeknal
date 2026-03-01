# Commands Reference

This document describes all available commands for the multi-agent development workflow.

## Planning Commands

### `/plan_w_team`

Create detailed implementation plans with team coordination. Supports three modes:

**Create Mode** (default) — Generate a new plan from a prompt:
```bash
/plan_w_team "Feature description"

# With orchestration guidance
/plan_w_team "Build real-time chat" "Use frontend-agent for UI, backend-agent for API. Run in parallel."
```

**Accept Mode** — Import existing plan document(s):
```bash
# Single document
/plan_w_team --accept docs/plans/my-plan.md
/plan_w_team -a docs/plans/my-plan.md

# Multiple documents (comma-separated) — merges into one spec
/plan_w_team --accept docs/plans/backend.md,docs/plans/frontend.md,docs/plans/api.md

# With orchestration to guide merge decisions
/plan_w_team --accept docs/plans/backend.md,docs/plans/frontend.md "Prioritize backend tasks"
```

**BMad Mode** — Convert BMad output to spec:
```bash
/plan_w_team --bmad ~/project/_bmad-output/planning-artifacts
```

**Output:** Creates `specs/<feature-name>.md` with:
- Task breakdown with dependencies
- Team member assignments
- Implementation phases
- Acceptance criteria
- Validation commands

**Multi-doc merge** reads all input files, applies AI-powered merge to combine tasks (renumbered), team members (deduplicated), acceptance criteria (combined), and relevant files (deduplicated) into a single unified spec. The merged output includes `merge_sources` in frontmatter for traceability.

**Ralph flags:**
- `--ralph` — Auto-start build with ralph loop after planning completes (skips handoff question).
- `--max-iterations N` — Set max iterations for the auto-started build.

```bash
/plan_w_team "Add authentication" --ralph
/plan_w_team --accept docs/plans/my-plan.md --ralph --max-iterations 10
```

### `/plan`

Quick planning mode for simpler features.

**Usage:**
```bash
/plan "Add user authentication"
```

### `/party`

Run a full product team through brainstorm → plan → build → validate using Agent Teams.

**Usage:**
```bash
# Start a party with a topic
/party "Build user authentication with OAuth"

# Resume an interrupted party (auto-detects from state)
/party "Build user authentication with OAuth"
```

**What it does:**
1. Spawns 8 specialist agents (PM, Architect, Backend, Frontend, QA, UX, Docs, DevOps)
2. **BRAINSTORM**: All agents research the topic from their expertise, lead synthesizes findings
3. **PLAN**: Agents contribute planning from their domain, lead assembles a `/build`-compatible spec
4. **BUILD**: Tasks created from spec, agents self-claim and implement, lead monitors progress
5. **VALIDATE**: QA validates, Docs writes docs, DevOps checks deployment readiness
6. User checkpoint between each phase (approve/refine/adjust/abort)

**Prerequisites:**
```bash
export CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1
```

**Comparison with `/build --team`:**

| Aspect | `/party` | `/build --team` |
|--------|----------|-----------------|
| Input | An idea/topic | A pre-written spec |
| Phases | 4 (brainstorm → plan → build → validate) | Build only |
| Team size | 8 specialists (fixed roster) | Defined in spec |
| Produces | Spec + Implementation + Docs | Implementation only |
| Best for | New features from scratch | Implementing existing plans |

**Team Roster:**

| Agent | Role | Primary Phases |
|-------|------|---------------|
| pm | Product Manager | Brainstorm, Plan |
| architect | System Architect | Brainstorm, Plan, Build |
| backend | Backend Developer | Plan, Build |
| frontend | Frontend Developer | Plan, Build |
| qa | QA Engineer | Plan, Build, Validate |
| ux | UX Designer | Brainstorm, Plan |
| docs | Tech Writer | Build, Validate |
| devops | DevOps Engineer | Build, Validate |

**Example output:**
```
Party Mode: "Build user authentication with OAuth"
Phase: 2/4 (PLAN)

Collecting planning contributions...
✅ PM: User stories with acceptance criteria
✅ Architect: Component architecture and tech stack
✅ Backend: API task breakdown
✅ Frontend: UI component hierarchy
✅ QA: Test strategy and validation commands
✅ UX: User flow descriptions
✅ Docs: Documentation plan
✅ DevOps: Deployment strategy

Assembling spec → specs/party-build-user-auth.md

Plan assembled. What next?
> Proceed to Build (recommended)
> Edit plan
> Revise
> Abort
```

## Execution Commands

### `/build`

Execute a plan with multi-agent coordination.

**Usage:**
```bash
# Subagent mode (default) - independent, focused tasks
/build specs/conversational-ui-revamp.md

# Agent Teams mode - teammates communicate directly, self-claim tasks
/build specs/user-auth.md --team
```

**Flags:**
- `--team` - Use Agent Teams instead of subagents (requires `CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1`)
- `--fresh` - Start over from beginning, ignoring previous state
- `--phase N` - Start from specific phase
- `--from-task NAME` - Continue from a specific task
- `--ralph` — Enable ralph loop mode. Iterates build→validate until all validation passes or max iterations reached.
- `--max-iterations N` — Set maximum ralph iterations (default: 5, max: 50). Safety limit to prevent infinite loops.
- `--self-heal` — Enable self-healing mode. Auto-retries failed tasks up to 3 times per task before marking as "stuck".
- `--completion-promise TEXT` — Exit ralph loop when `<promise>TEXT</promise>` is detected in output.

**What it does:**
1. Parses the plan document
2. Creates tasks in the shared task list
3. Sets up task dependencies
4. Deploys agents/teammates for each task
5. Monitors progress (polling for subagents, messages for teams)
6. Handles parallel/sequential execution
7. Validates results and persists state

**Subagent vs Agent Teams:**

| Aspect | Subagents (default) | Agent Teams (--team) |
|--------|-------------------|---------------------|
| Communication | One-way (worker to lead) | Multi-directional (any to any) |
| Task claiming | Lead assigns explicitly | Self-claiming + lead assignment |
| Token cost | Lower | Higher (each teammate = full session) |
| Best for | Independent, focused tasks | Cross-cutting work needing coordination |
| Resume | Agent resume via agentId | Fresh team for remaining tasks |

**Prerequisites for `--team`:**
```bash
export CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1
```

**Ralph loop examples:**
```bash
/build specs/feature.md --ralph
/build specs/feature.md --ralph --max-iterations 10
/build specs/feature.md --ralph --self-heal
/build specs/feature.md --team --ralph
```

**Example output:**
```
Build Progress

Phase 1: Foundation
Task 1: Setup database - Complete
Task 2: Create migrations - In progress

Phase 2: Core Implementation
Task 3: Build frontend (waiting)
Task 4: Build backend (waiting)
```

### `/continue`

Resume an agent with additional work (preserves context).

**Usage:**
```bash
/continue <agent-id> "Add input validation to endpoints"
```

**When to use:**
- Adding work to an in-progress task
- Refining what an agent has built
- Follow-up changes to completed work

**Example:**
```bash
/continue abc123 "Add error handling to all endpoints"
```

### `/retry`

Retry a failed task with corrected instructions.

**Usage:**
```bash
/retry <task-id> "The endpoint needs query parameters, not body params"
```

**When to use:**
- Agent failed with code errors
- Wrong approach was taken
- Requirements need clarification

### `/continue-spec`

Resume the entire build workflow from a spec file.

**Usage:**
```bash
/continue-spec specs/user-auth.md
/continue-spec specs/user-auth.md --dry-run
/continue-spec specs/user-auth.md --from-task 5
/continue-spec specs/user-auth.md --restart
```

**What it does:**
1. Reads state file to detect previous build progress
2. **Auto-detects build mode** (subagent or team) from state file
3. For subagent mode: resumes agents with preserved context when possible
4. For team mode: creates fresh team and assigns remaining tasks (Agent Teams do not support session resumption)
5. Validates spec hasn't changed (checksum detection)

**Flags:**
- `--dry-run` - Show what would be done without executing
- `--from-task N` - Start from specific task N
- `--restart` - Ignore completed tasks, start fresh
- `--max-iterations N` — Overrides the saved ralph max iterations value on resume

**Ralph awareness:** `/continue-spec` detects ralph loop state on resume:
- If max iterations exhausted: offers to restart, resume without ralph, or increase max
- If aborted: offers to resume ralph or continue manually

### /ralph-stop

Cancel an active ralph loop gracefully.

**Usage:** `/ralph-stop`

Scans for active ralph loops in `.claude/specs/*/state.json`, marks them as aborted, and creates a `.claude/ralph-abort` sentinel file. The Stop hook detects this sentinel and allows the session to exit normally.

After cancelling, use `/continue-spec` to resume work without ralph mode, or `/build --ralph` to start a fresh loop.

## Monitoring Commands

### `/status`

Show status of all tasks and running agents.

**Team mode awareness:** When a team-mode build is active, `/status` reads the team config and displays a teammate table with names, agent types, current tasks, and status.

**Ralph loop awareness:** Now displays ralph loop iteration status when a ralph loop is active, completed, failed, or aborted.

**Usage:**
```bash
/status          # Show all tasks
/status --pending # Show only pending tasks
/status --active # Show only in-progress tasks
```

**Example output:**
```
📋 Task Status

Pending Tasks:
- [5] Build UI components

In Progress:
- [2] Create migrations (assigned to: builder-db)
  Status: Creating tables...

Completed:
- [1] Setup database ✅

Blocked:
- [3] Build API (blocked by: 2)
```

### `/agents`

List all available team members.

**Usage:**
```bash
/agents
```

**Example output:**
```
🤖 Available Team Members

General Purpose:
- general-purpose - Default agent for general tasks

Specialized Agents:
- frontend-agent - Builds UI components and views
  Role: Frontend Implementation
  Type: frontend-agent

- backend-agent - Creates APIs and business logic
  Role: Backend Implementation
  Type: backend-agent
```

## Validation Commands

### `/validate`

Run validation commands and check acceptance criteria.

**Usage:**
```bash
/validate specs/conversational-ui-revamp.md
```

**What it does:**
1. Runs all validation commands from the plan
2. Checks acceptance criteria
3. Compiles results into a report

**Example output:**
```
🔍 Validation Report

Plan: specs/conversational-ui-revamp.md

Validation Commands:
✅ npm run test
✅ npm run lint
❌ npm run build - TypeScript errors found

Acceptance Criteria:
Functional Requirements:
✅ F1: User can start conversations
✅ F2: User can ask follow-up questions
⏳ F3: User can search conversations (manual verification)

Non-Functional Requirements:
✅ NF1: First insight < 3s (measured: 2.1s)
❌ NF2: Reconnect < 5s (measured: 7.3s, target: 5s)

Overall Status: PARTIAL

Issues:
- TypeScript errors in ChatContainer.tsx
- SSE reconnection too slow
```

## Workflow Example

Complete workflow for a feature:

```bash
# 1. Plan the feature
/plan_w_team "Add real-time notifications"

# 2. Review the plan
cat specs/real-time-notifications.md

# 3. Execute the plan
/build specs/real-time-notifications.md

# 4. Monitor progress (in another terminal)
/status

# 5. If an agent fails, retry with correction
/retry 7 "Use WebSocket instead of polling"

# 6. Add more work to in-progress task
/continue abc123 "Add notification sounds"

# 7. When complete, validate
/validate specs/real-time-notifications.md
```

## Parallel Execution Example

```bash
# Terminal 1: Start the build
/build specs/conversational-ui.md

# Terminal 2: Monitor status
/watch --interval 5 /status

# Terminal 3: Watch logs
tail -f logs/*.log
```

## Tips

1. **Always plan first** - Use `/plan_w_team` before starting implementation
2. **Monitor progress** - Use `/status` to track what's happening
3. **Resume when possible** - Use `/continue` to preserve agent context
4. **Validate at the end** - Use `/validate` to ensure quality
5. **Read the plan** - Review the generated plan before building
