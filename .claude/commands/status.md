---
name: status
description: Show the current status of all tasks and running agents. Use to monitor build progress.
argument-hint:
model: opus
allowed-tools: Task, TaskOutput, TaskList, TaskGet, Bash, Glob, Grep, Read
---

# Status

Show the current status of all tasks in the task list and any running agents.

## Variables

None

## Instructions

1. **Get Task List** - Use `TaskList` to retrieve all tasks
2. **Get Task Details** - Use `TaskGet` for tasks with status "in_progress"
3. **Check Running Agents** - If there are background agents, check their status
4. **Check Team Mode** - Read state file to detect if this is a team-mode build. If so, read team config for teammate info.

## Output Format

```
📋 Task Status

Pending Tasks (ready to start):
- [<ID>] <Task Name>

In Progress:
- [<ID>] <Task Name> (assigned to: <owner>)
  Status: <details>

Completed:
- [<ID>] <Task Name> ✅

Blocked (waiting for dependencies):
- [<ID>] <Task Name> (blocked by: <task IDs>)
```

If there are running background agents:

```
🤖 Running Agents

- Agent <ID>: <Task Name>
  Status: <running/completed/error>
  Output file: <path>
```

## Team Mode Output

If the build is running in Agent Teams mode (detected from state file `build.mode === "team"`):

Read the state file from `.claude/specs/<spec-name>/state.json` to detect team mode.
Read the team config from `~/.claude/teams/<teamName>/config.json` for teammate details.

```
Build Mode: Agent Teams
Team: <teamName>

Teammates:
| Name | Agent Type | Current Task | Status |
|------|-----------|-------------|--------|
| backend-api | backend-agent | [3] Build auth API | Working |
| frontend-ui | frontend-agent | [5] Create login | Working |
| test-runner | test-agent | (idle) | Waiting |

Task Progress:
Completed: X/Y tasks
Remaining: Z tasks

Recent Messages:
- backend-api: "Completed Task 2, claiming Task 4"
- frontend-ui: "Need API contract from backend-api"
```

## Party Mode Output

If the build is running in Party Mode (detected from state file `build.mode === "party"`):

Read the state file from `.claude/specs/<spec-name>/state.json` to detect party mode.
Read the team config from `~/.claude/teams/<teamName>/config.json` for teammate details.

```
Build Mode: Party Mode
Topic: "<party topic>"
Phase: <N>/4 (<phase-name>) <progress-bar>

Phase History:
| Phase | Status | Duration | Artifacts |
|-------|--------|----------|-----------|
| 1. Brainstorm | ✅ Complete | 3m 42s | brainstorm-summary.md |
| 2. Plan | ✅ Complete | 5m 18s | specs/party-<topic>.md |
| 3. Build | 🔄 In Progress | 8m 12s | 7/10 tasks done |
| 4. Validate | ⏳ Pending | - | - |

Active Agents:
| Name | Role | Current Task | Status |
|------|------|-------------|--------|
| pm | Product Manager | (idle) | Waiting |
| architect | System Architect | Task 8: API Gateway | Building |
| backend | Backend Developer | Task 9: Auth Service | Building |
| frontend | Frontend Developer | Task 7: Login UI | Building |
| qa | QA Engineer | (idle) | Waiting for build |
| ux | UX Designer | (idle) | Waiting |
| docs | Tech Writer | (idle) | Waiting for validate |
| devops | DevOps Engineer | (idle) | Waiting for validate |

Key Decisions:
- <decision 1>
- <decision 2>
```

## Ralph Loop Status

If the state file has a `ralph` field that is not null, display ralph loop information.

Read the state file from `.claude/specs/<spec-name>/state.json` and check for `ralph` field.

**When `ralph.active === true`:**

```
Ralph Loop: Active — iteration N/M
  Mode: <build-validate|self-heal|full-lifecycle>
  Self-heal: <enabled|disabled>
  <If self-heal enabled and taskRetryCounters has entries:>
    Retry counters: task X: A/3, task Y: B/3
  Last validation: <from latest history entry, e.g. "5/7 passed">
  Started: <ralph.startedAt formatted>
```

**When `ralph.status === 'completed'`:**

```
Ralph Loop: Completed — N iterations
  Mode: <mode>
  Result: All validation passed
  Duration: <calculated from startedAt to last history entry>
```

**When `ralph.status === 'failed'`:**

```
Ralph Loop: Failed — max iterations reached (N/M)
  Mode: <mode>
  Last validation: <from latest history entry>
  Report: .claude/specs/<name>/ralph-report.md
```

**When `ralph.status === 'aborted'`:**

```
Ralph Loop: Aborted at iteration N/M
  Use /continue-spec to resume
```

**When `ralph` is null:** Don't show any ralph section.

## Workflow

1. Call `TaskList({})` to get all tasks
2. Format output by status (pending, in_progress, completed)
3. Show dependencies for blocked tasks
4. If there are background agents, show their status
5. Read state file to detect build mode:
   - If team mode, read team config and show teammate table
   - If party mode, show phase progress, phase history, active agents, and key decisions
6. Check state file for `ralph` field — if present and not null, display Ralph Loop Status

## Report

Provide a concise status report showing:
- Total task count
- Completed count
- In progress count
- Pending count
- Blocked count
- Build mode (subagent/team)
- Teammate names and assignments (if team mode)
- Ralph loop status (if active or completed/failed/aborted)

## Examples

```bash
# Show all tasks
/status

# Show only pending tasks
/status --pending

# Show only in-progress tasks
/status --active
```
