---
name: build
description: Execute a plan document from specs/ directory with multi-agent coordination. Use after planning to implement the feature.
argument-hint: [path-to-plan] [--team] [--ralph [--max-iterations N] [--self-heal] [--completion-promise TEXT]]
model: opus
allowed-tools: Task, TaskOutput, Bash, Glob, Grep, Read, Edit, Write, NotebookEdit, WebFetch, WebSearch, AskUserQuestion, Skill, TodoWrite, TeamCreate, TeamDelete, SendMessage
---

# Build

Execute the implementation plan at `PATH_TO_PLAN` using multi-agent coordination. This command parses the plan, creates tasks, deploys agents, and manages the execution workflow.

## Variables

- `PATH_TO_PLAN`: $1 - Path to the plan file (e.g., `specs/conversational-ui-revamp.md`)
- `TEAM_MODE`: --team - Use Agent Teams instead of subagents for execution
- `TEAM_MEMBERS`: `agents/*.md` - Available team members
- `GENERAL_PURPOSE_AGENT`: `general-purpose` - Default agent type

## Instructions

### Prerequisites

- If no `PATH_TO_PLAN` is provided, STOP and ask the user to provide it
- The plan must exist and follow the plan format created by `plan_w_team`
- Read the plan document completely before starting execution

### Mode Detection

Parse arguments to determine execution mode:

```typescript
// Check if --team flag is present
const TEAM_MODE = arguments.includes('--team')

if (TEAM_MODE) {
  // Verify Agent Teams experimental flag is enabled
  const agentTeamsEnabled = process.env.CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS === '1'
  if (!agentTeamsEnabled) {
    console.error("Error: Agent Teams requires the experimental flag.")
    console.error("Enable it with:")
    console.error("  export CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1")
    console.error("Then restart Claude Code and retry.")
    return
  }
  console.log("Mode: Agent Teams (--team)")
} else {
  console.log("Mode: Subagents (default)")
}

// Ralph mode detection
const RALPH_MODE = arguments.includes('--ralph')
const MAX_ITERATIONS = (() => {
  const idx = arguments.indexOf('--max-iterations')
  if (idx !== -1 && arguments[idx + 1]) {
    const n = parseInt(arguments[idx + 1])
    return Math.min(Math.max(n, 1), 50) // Clamp between 1-50
  }
  return 5 // Default
})()
const SELF_HEAL = arguments.includes('--self-heal')
const COMPLETION_PROMISE = (() => {
  const idx = arguments.indexOf('--completion-promise')
  if (idx !== -1 && arguments[idx + 1]) {
    return arguments[idx + 1]
  }
  return null
})()

if (RALPH_MODE) {
  console.log("Ralph Mode: Enabled")
  console.log(`  Max iterations: ${MAX_ITERATIONS}`)
  console.log(`  Self-heal: ${SELF_HEAL ? 'enabled' : 'disabled'}`)
  console.log(`  Completion promise: ${COMPLETION_PROMISE || 'none'}`)
  if (MAX_ITERATIONS === 5) {
    console.log("  (Default max iterations. Use --max-iterations N to change.)")
  }
}
```

### Execution Strategy

You are the **Orchestrator** - your job is to coordinate multiple agents to execute the plan, NOT to write code directly.

#### Core Principles

1. **Parse the Plan** - Extract all tasks, dependencies, and team assignments
2. **Create Task List** - Use `TaskCreate` to create all tasks in the shared task list
3. **Set Dependencies** - Use `TaskUpdate` with `addBlockedBy` to establish task dependencies
4. **Deploy Agents** - Use `Task` tool to deploy agents for each assigned task
5. **Monitor Progress** - Use `TaskList` and `TaskOutput` to track progress
6. **Handle Failures** - If an agent fails, assess and either retry or adjust approach

#### DO NOT Write Code Directly

Your role is ORCHESTRATION, not IMPLEMENTATION. Use agents to do the actual work:
- `TaskCreate` - Create tasks in the shared list
- `TaskUpdate` - Update task status and dependencies
- `Task` - Deploy agents to execute work

## Workflow

### Phase 0: Resume Detection

Before starting, check for existing state from a previous build:

```typescript
// Clean up stale abort sentinel from previous session
if (RALPH_MODE) {
  const abortFile = '.claude/ralph-abort'
  if (fs.existsSync(abortFile)) {
    fs.unlinkSync(abortFile)
    console.log("Cleaned up stale ralph-abort sentinel from previous session")
  }
}

// Load state file helper functions
// Note: These functions are available in scripts/state-file.js

const existingState = readStateFile(PATH_TO_PLAN)

if (existingState) {
  const previousMode = existingState.build.mode || 'subagent'
  const currentMode = TEAM_MODE ? 'team' : 'subagent'
  const inProgressTasks = existingState.tasks.filter(t => t.status === 'in-progress')
  const completedTasks = existingState.tasks.filter(t => t.status === 'completed')

  // Mixed-mode detection: state was created with different mode than current invocation
  if (previousMode !== currentMode) {
    console.warn(`Previous build used ${previousMode} mode, but current invocation uses ${currentMode} mode.`)

    const action = await AskUserQuestion({
      question: `Mode mismatch: previous build was ${previousMode}, current is ${currentMode}. What should we do?`,
      options: [
        {
          label: "Continue with current mode",
          description: `Honor ${completedTasks.length} completed tasks, use ${currentMode} mode for remaining`
        },
        {
          label: "Start completely fresh",
          description: "Discard all previous progress and start over"
        },
        { label: "Abort", description: "Cancel and keep existing state" }
      ]
    })

    if (action === "Abort") return
    if (action === "Start completely fresh") {
      // Clean up team resources if previous was team mode
      if (previousMode === 'team' && existingState.build.teamName) {
        try { TeamDelete() } catch (e) { /* team may not exist */ }
      }
      deleteStateFile(PATH_TO_PLAN)
      // Proceed with fresh build
    }
    // "Continue with current mode" falls through to resume logic below
  }

  if (inProgressTasks.length > 0 || (completedTasks.length > 0 && completedTasks.length < existingState.build.totalTasks)) {
    console.log(`Previous build found (${previousMode} mode):`)
    console.log(`- Status: ${completedTasks.length}/${existingState.build.totalTasks} tasks completed`)
    console.log(`- Started: ${existingState.build.startedAt}`)

    const action = await AskUserQuestion({
      question: "What would you like to do?",
      options: [
        { label: "Resume", description: "Continue from where we left off" },
        { label: "Fresh", description: "Start over from beginning" }
      ]
    })

    if (action === "Fresh") {
      // Clean up team resources if previous was team mode
      if (previousMode === 'team' && existingState.build.teamName) {
        try { TeamDelete() } catch (e) { /* team may not exist */ }
      }
      deleteStateFile(PATH_TO_PLAN)
      console.log("Starting fresh build...")
    } else {
      console.log("Resuming previous build...")
      return resumeBuild(existingState)
    }
  } else if (completedTasks.length === existingState.build.totalTasks) {
    // All completed, auto-start fresh without asking
    console.log("Previous build completed. Starting fresh build...")
    if (previousMode === 'team' && existingState.build.teamName) {
      try { TeamDelete() } catch (e) { /* team may not exist */ }
    }
    deleteStateFile(PATH_TO_PLAN)
  }
}
```

### Phase 1: Parse & Prepare

1. **Read the Plan** - Read `PATH_TO_PLAN` completely
2. **Extract Tasks** - Parse all tasks from the "Step by Step Tasks" section
3. **Extract Team Members** - Parse the "Team Members" section
4. **Validate Plan** - Ensure plan has required sections

### Phase 2: Create Task List

Use `TaskCreate` to create all tasks in the shared task list:

```typescript
// Create initial task
TaskCreate({
  subject: "<Task Name>",
  description: "<Full task description from plan>",
  activeForm: "<Active form - what shows in spinner>"
})
// Returns: taskId (e.g., "1")

// Create all tasks before starting execution
```

Create ALL tasks first before deploying any agents. This gives full visibility into the work.

**IMPORTANT: Create State File After Task Creation**

After creating all tasks, immediately persist them to the state file:

```typescript
// After creating all tasks:
const tasks = []  // Array of task objects with their IDs

for (const taskDef of parsedTasks) {
  const result = await TaskCreate({
    subject: taskDef.subject,
    description: taskDef.description,
    activeForm: taskDef.activeForm
  })
  tasks.push({ ...taskDef, id: result.taskId })
}

// Create initial state file using createInitialState helper
const mode = TEAM_MODE ? 'team' : 'subagent'
const state = createInitialState(PATH_TO_PLAN, tasks, mode, null,
  RALPH_MODE ? {
    maxIterations: MAX_ITERATIONS,
    selfHeal: SELF_HEAL,
    completionPromise: COMPLETION_PROMISE,
    mode: SELF_HEAL ? 'self-heal' : 'build-validate'
  } : null
)

writeStateFile(PATH_TO_PLAN, state)
console.log(`✓ State saved (${mode} mode) to .claude/specs/${sanitizeSpecName(PATH_TO_PLAN)}/state.json`)
```

### Phase 3: Set Dependencies

Use `TaskUpdate` with `addBlockedBy` to establish dependencies:

```typescript
// Example: Task 2 depends on Task 1
TaskUpdate({
  taskId: "2",
  addBlockedBy: ["1"]
})

// Example: Task 3 depends on both Task 1 and Task 2
TaskUpdate({
  taskId: "3",
  addBlockedBy: ["1", "2"]
})
```

### Phase 4: Deploy Agents (Subagent Mode) / Spawn Teammates (Team Mode)

**If `TEAM_MODE` is enabled, skip to [Phase 4-T: Team Mode Execution](#phase-4-t-team-mode-execution) below.**

#### Phase 4-S: Subagent Mode (Default)

Use the `Task` tool to deploy agents for each task:

```typescript
// Deploy an agent to execute a task
Task({
  description: "<Task Name>",
  prompt: `<Detailed instructions for the agent>

Execute the following task:
- Task ID: <taskId>
- Task Name: <task name>
- Requirements: <from plan>
- Files: <from Relevant Files section>
- Acceptance Criteria: <from plan>

Work through the implementation step by step.
Report progress as you complete each subtask.`,

  subagent_type: "<agent type from plan>",
  model: "opus",
  run_in_background: <true for parallel, false for sequential>
})
```

#### Parallel vs Sequential

- **Parallel** (`run_in_background: true`): Task has no dependencies or can run independently
- **Sequential** (`run_in_background: false`): Task depends on other tasks completing first

#### Phase 4-T: Team Mode Execution

**This section only applies when `TEAM_MODE` is enabled. Skip if using subagent mode.**

##### Step 1: Create Team

```typescript
const teamName = sanitizeSpecName(PATH_TO_PLAN)

// Check for existing team (orphaned from previous build)
try {
  TeamCreate({
    team_name: teamName,
    description: `Build: ${specTitle}`
  })
} catch (error) {
  // Team already exists - ask user
  const action = await AskUserQuestion({
    question: `Team "${teamName}" already exists (possibly from an incomplete build). What should we do?`,
    options: [
      { label: "Delete and recreate", description: "Clean up existing team and start fresh" },
      { label: "Abort", description: "Cancel the build" }
    ]
  })
  if (action === "Abort") return
  TeamDelete()
  TeamCreate({ team_name: teamName, description: `Build: ${specTitle}` })
}
```

##### Step 2: Spawn Teammates

```typescript
// Parse team members from spec's "### Team Members" section
const teamMembers = parseTeamMembers(specContent)
// Each member has: name, role, agentType, model (optional), planApproval (optional)

// Cost confirmation
const action = await AskUserQuestion({
  question: `About to spawn ${teamMembers.length} teammates (each is a full Claude session). Proceed?`,
  options: [
    { label: "Proceed", description: `Spawn ${teamMembers.length} teammates and start building` },
    { label: "Abort", description: "Cancel and clean up team" }
  ]
})
if (action === "Abort") {
  TeamDelete()
  return
}

// Spawn each teammate
for (const member of teamMembers) {
  Task({
    team_name: teamName,
    name: member.name,
    subagent_type: member.agentType || 'general-purpose',
    mode: member.planApproval ? 'plan' : 'default',
    model: member.model || 'opus',
    prompt: `You are ${member.name}, a teammate on the "${teamName}" team.
Your role: ${member.role}

## Your Tasks
Check the shared task list with TaskList. Work on tasks assigned to you (owner: "${member.name}").
When your assigned tasks are done, claim unassigned unblocked tasks with:
  TaskUpdate({ taskId, owner: "${member.name}", status: "in_progress" })
Prefer tasks in ID order (lowest first).

## After Completing Each Task
1. Mark completed: TaskUpdate({ taskId, status: "completed" })
2. Send the lead a completion summary:
   SendMessage({ type: "message", recipient: "team-lead", content: "Completed task [ID]: [summary of what was done]", summary: "Task [ID] complete" })
3. Check TaskList for your next available task
4. If no more tasks available, send:
   SendMessage({ type: "message", recipient: "team-lead", content: "All my tasks are done.", summary: "All tasks done" })

## Coordination
- Message the lead for blockers or questions
- Message other teammates directly if you need to coordinate on shared interfaces
- Read ~/.claude/teams/${teamName}/config.json to find teammate names`
  })
}
```

##### Step 3: Assign Initial Tasks

```typescript
// Assign unblocked tasks to their designated teammates
const unblockedTasks = tasks.filter(t => !t.blockedBy || t.blockedBy.length === 0)

for (const task of unblockedTasks) {
  // Get assignee from spec's "Assigned To" field for this task
  const assignee = findAssigneeFromSpec(task, teamMembers)

  TaskUpdate({
    taskId: task.id,
    owner: assignee.name,
    status: "in_progress"
  })
  updateTaskInState(PATH_TO_PLAN, task.id, {
    status: "in-progress",
    teammateName: assignee.name,
    deployedAt: new Date().toISOString()
  })
}

// Tasks with dependencies are left unassigned - teammates will self-claim them
// as they become unblocked (dependencies complete)
```

##### Step 4: Monitor via Messages

```typescript
// In team mode, messages from teammates arrive AUTOMATICALLY
// No polling with TaskOutput needed

// The lead's monitoring loop:
// 1. PROCESS COMPLETION MESSAGES
//    When a teammate reports task completion:
//    a. Capture output from message content
//    b. Run validation hooks (Phase 5.5 logic) using message content as agentOutput
//    c. Update state: updateTaskInState(specPath, taskId, {
//         status: "completed",
//         lastOutput: messageContent.slice(0, 500)
//       })
//    d. Check if newly unblocked tasks exist and assign them

// 2. HANDLE PLAN APPROVAL REQUESTS (if teammate has Plan Approval: true)
//    When teammate sends plan_approval_request:
//    - Review the plan
//    - SendMessage({ type: "plan_approval_response", recipient: teammateName,
//        request_id: requestId, approve: true/false,
//        content: "feedback if rejecting" })

// 3. RESOLVE BLOCKERS
//    If a teammate reports a blocker:
//    - Assess the situation
//    - Resolve or escalate to user via AskUserQuestion
//    - Send resolution: SendMessage({ type: "message", recipient: teammateName,
//        content: "resolution details", summary: "Blocker resolved" })

// 4. DETECT STALLED TEAMMATES
//    If a teammate has been working with no messages for an extended period:
//    - Send check-in: SendMessage({ type: "message", recipient: teammateName,
//        content: "Status update on your current task?", summary: "Check-in" })
//    - If no response after check-in, ask user:
//      AskUserQuestion: "Teammate [name] is unresponsive."
//      Options: "Respawn replacement" / "Skip their tasks" / "Wait longer"
//    - If respawn: spawn a new teammate, assign the stalled task

// 5. TRACK OVERALL PROGRESS
//    Periodically check TaskList for overall status
//    When all tasks are completed → proceed to Phase 7 (Validation)
```

##### Step 5: Shutdown & Cleanup

#### Ralph Mode Team Persistence

When ralph mode is active in team mode, suppress team cleanup to preserve teammates across iterations:

```typescript
// Check if ralph mode is active before team cleanup
if (RALPH_MODE) {
  const state = readStateFile(PATH_TO_PLAN)
  if (state.ralph && state.ralph.active) {
    console.log("Ralph mode active — keeping team alive for next iteration")
    // Skip shutdown and TeamDelete
    // The Stop hook will handle iteration decisions
    // Team persists across ralph iterations

    // Only update state, don't tear down
    state.build.lastUpdated = new Date().toISOString()
    writeStateFile(PATH_TO_PLAN, state)

    // Skip to Phase 7 (Validation) — the stop hook decides whether to continue
    // Do NOT proceed with Step 5 shutdown below
    return // Exit team cleanup section
  }
}

// If ralph is NOT active (or ralph completed/failed/aborted), proceed with normal shutdown:
```

```typescript
// After all tasks complete (or on abort):

// 1. Request shutdown for each teammate
for (const member of teamMembers) {
  SendMessage({
    type: "shutdown_request",
    recipient: member.name,
    content: "Build complete. Shutting down."
  })
}

// 2. Wait briefly for shutdown acknowledgments
// Teammates respond with shutdown_response { approve: true }
// If no response within 30 seconds, proceed anyway

// 3. Clean up team resources
TeamDelete()

// 4. Update state file
state.build.completedAt = new Date().toISOString()
state.build.lastUpdated = new Date().toISOString()
writeStateFile(PATH_TO_PLAN, state)

// 5. Proceed to Phase 7 (Validation) and Phase 8 (State Updates)
```

**After Team Mode Execution completes, skip to [Phase 7: Validation](#phase-7-validation).**

---

### Phase 5: Monitor & Coordinate (Subagent Mode Only)

**This section applies to subagent mode. Team mode uses Phase 4-T Step 4 above.**

While agents are working:

1. **Check Progress** - Use `TaskList` to see task statuses
2. **Monitor Agents** - Use `TaskOutput` to check on running agents
3. **Handle Completions** - When agent finishes, mark task as completed
4. **Unblock Tasks** - Completed tasks unblock dependent tasks
5. **Deploy Next Agents** - Start agents for newly unblocked tasks

```typescript
// Check on a background agent
TaskOutput({
  task_id: "<agentId>",
  block: false,  // Non-blocking check
  timeout: 5000
})

// Wait for agent completion
TaskOutput({
  task_id: "<agentId>",
  block: true,  // Blocks until done
  timeout: 300000  // 5 minutes
})
```

### Phase 6: Resume Pattern

For follow-up work on the same task, resume the existing agent:

```typescript
// First deployment - returns agentId
Task({
  description: "Build user service",
  prompt: "Create the user service...",
  subagent_type: "general-purpose"
})
// Returns: agentId: "abc123"

// Later - resume SAME agent with preserved context
Task({
  description: "Continue user service",
  prompt: "Now add validation to what you created...",
  subagent_type: "general-purpose",
  resume: "abc123"  // Critical: preserves context
})
```

### Phase 5.5: Per-Task Validation Hooks

**Critical:** Before marking any task as "completed", run validation hooks defined in the spec.

**Team Mode Note:** In team mode, the teammate's completion message content is used as `agentOutput` for validation. The lead captures the message content and passes it to the hooks in the same way as subagent mode uses `TaskOutput` results.

```typescript
// After agent finishes, run validation hooks before marking complete
// In Phase 5: Monitor & Coordinate - after TaskOutput returns

const result = await TaskOutput({ task_id: agentId, block: true })

// Load validation hooks from spec
const hooks = extractTaskHooks(specContent, task.subject)

if (hooks && hooks.stop && hooks.stop.length > 0) {
  console.log(`Running ${hooks.stop.length} validation hook(s) for task ${taskId}...`)

  const validationResults = await runHooks(hooks.stop, {
    agentOutput: result.output || '',
    taskId: taskId,
    specPath: PATH_TO_PLAN,
    workingDir: process.cwd()
  })

  // Display validation results
  console.log(`Validation: ${validationResults.passed}/${validationResults.total} passed`)

  if (validationResults.failed > 0) {
    console.error(`❌ Validation failed:`)
    validationResults.errors.forEach(err => console.error(`  - ${err}`))

    // --- Self-Heal Mode (Ralph) ---
    // When RALPH_MODE and SELF_HEAL are both active, bypass user prompt and auto-retry
    if (RALPH_MODE && SELF_HEAL) {
      const state = readStateFile(PATH_TO_PLAN)
      const ralph = state.ralph
      const retryCount = ralph.taskRetryCounters[taskId] || 0

      if (retryCount < 3) {
        // Increment retry counter
        ralph.taskRetryCounters[taskId] = retryCount + 1
        writeStateFile(PATH_TO_PLAN, state)

        console.log(`Self-heal: retrying task ${taskId} (attempt ${retryCount + 1}/3)`)
        console.log(`  Failures: ${validationResults.errors.join(', ')}`)

        // Resume agent with error context
        const retryResult = await Task({
          description: `Retry: ${task.subject}`,
          prompt: `Previous attempt failed validation. Fix these issues and try again:\n${validationResults.errors.map(e => `- ${e}`).join('\n')}\n\nOriginal task: ${task.description}`,
          subagent_type: task.agentType,
          resume: result.agentId
        })

        // Re-run validation after retry
        // (loop back to validation check)
        continue // retry validation
      } else {
        // Max retries reached - mark as stuck
        console.log(`Task ${taskId} stuck after 3 retries. Moving to next task.`)
        await TaskUpdate({ taskId: taskId, status: "completed" }) // Mark to unblock dependents
        updateTaskInState(PATH_TO_PLAN, taskId, {
          status: "stuck",
          lastOutput: `Stuck after 3 retries. Last errors: ${validationResults.errors.join(', ')}`
        })
        continue // move to next task
      }
    }

    // Normal mode: ask user (existing AskUserQuestion block follows)
    const action = await AskUserQuestion({
      question: `Task "${task.subject}" validation failed. What should we do?`,
      options: [
        {
          label: "Retry task",
          description: "Run agent again with same prompt"
        },
        {
          label: "Mark complete anyway",
          description: "Ignore validation failures and mark as completed"
        },
        {
          label: "Skip task",
          description: "Mark as skipped and continue to next task"
        },
        {
          label: "Abort build",
          description: "Stop the entire build process"
        }
      ]
    })

    // Handle user choice
    switch (action) {
      case "Retry task":
        // Resume agent with same prompt
        console.log(`Retrying task ${taskId}...`)
        const retryResult = await Task({
          description: task.subject,
          prompt: buildPrompt(task),
          subagent_type: task.agentType,
          resume: result.agentId
        })
        // Re-run validation...
        break

      case "Mark complete anyway":
        // Proceed to mark as completed
        console.log(`Marking task ${taskId} as complete despite validation failures...`)
        break

      case "Skip task":
        await TaskUpdate({ taskId: taskId, status: "deleted" })
        updateTaskInState(PATH_TO_PLAN, taskId, { status: "skipped" })
        // Continue to next task without marking complete
        continue

      case "Abort build":
        console.log("Build aborted by user.")
        return
    }
  } else {
    console.log(`✓ All validation hooks passed`)
  }
}

// Only mark complete if validation passed (or user chose to mark complete anyway)
await TaskUpdate({ taskId: taskId, status: "completed" })
updateTaskInState(PATH_TO_PLAN, taskId, {
  status: "completed",
  lastOutput: result.output?.slice(0, 500) || "",
  validationResults: validationResults
})
```

#### Self-Heal Mode (Ralph)

When `RALPH_MODE` and `SELF_HEAL` are both active and a task fails validation, bypass the user prompt and auto-retry:

```typescript
if (RALPH_MODE && SELF_HEAL && validationResults.failed > 0) {
  // Self-heal: auto-retry instead of asking user
  const state = readStateFile(PATH_TO_PLAN)
  const ralph = state.ralph
  const retryCount = ralph.taskRetryCounters[taskId] || 0

  if (retryCount < 3) {
    // Increment retry counter
    ralph.taskRetryCounters[taskId] = retryCount + 1
    writeStateFile(PATH_TO_PLAN, state)

    console.log(`Self-heal: retrying task ${taskId} (attempt ${retryCount + 1}/3)`)
    console.log(`  Failures: ${validationResults.errors.join(', ')}`)

    // Resume agent with error context
    const retryResult = await Task({
      description: `Retry: ${task.subject}`,
      prompt: `Previous attempt failed validation. Fix these issues and try again:\n${validationResults.errors.map(e => `- ${e}`).join('\n')}\n\nOriginal task: ${task.description}`,
      subagent_type: task.agentType,
      resume: result.agentId
    })

    // Re-run validation after retry
    // (loop back to validation check)
    continue // retry validation
  } else {
    // Max retries reached - mark as stuck
    console.log(`Task ${taskId} stuck after 3 retries. Moving to next task.`)
    await TaskUpdate({ taskId: taskId, status: "completed" }) // Mark to unblock dependents
    updateTaskInState(PATH_TO_PLAN, taskId, {
      status: "stuck",
      lastOutput: `Stuck after 3 retries. Last errors: ${validationResults.errors.join(', ')}`
    })
    continue // move to next task
  }
}

// Normal mode: ask user (existing AskUserQuestion block follows)
```

Add this BEFORE the existing `AskUserQuestion` block for validation failures. The existing user-prompt behavior remains unchanged for non-ralph/non-self-heal mode.

**Note:** Per-task retry counters (`ralph.taskRetryCounters`) reset to 0 at the start of each new ralph iteration. This gives each task fresh retry attempts on each loop cycle.

**Hook Format in Spec:**

Tasks can define validation hooks using YAML:

```markdown
### Task 1: Create user model
- [ ] Task 1: Create user model

**Validation:**
```yaml
stop:
  - type: artifact
    path: app/models/user.rb
    exists: true
  - type: command
    command: rails runner "puts User.column_names"
    expect: exit_code_0
  - type: agent_output
    validate: success
  - type: acceptance_criteria
    criteria:
      - "User model created"
      - "Email field present"
    require: all
```
```

**Validation Types:**

| Type | Description | Example |
|------|-------------|---------|
| `agent_output` | Validates agent output for success/errors | `validate: success` |
| `command` | Runs shell command and checks result | `command: test -f file.rb` |
| `artifact` | Checks file existence/content | `path: app/model.rb`, `exists: true` |
| `acceptance_criteria` | Verifies spec criteria are checked | `criteria: ["Model created"]` |

**On Failure Behavior:**

Hooks can specify `on_failure`:
- `fail` (default): Stop running more hooks, prompt user
- `continue`: Run remaining hooks, report all failures at end
- `retry`: Automatically retry the task (up to N times)

### Phase 5.6: Ralph Inter-Iteration Status (Ralph Mode Only)

When ralph mode is active, display iteration progress after each build cycle completes. This information helps the user (and the continuation prompt) understand where things stand.

When the Stop hook sends a continuation prompt back, display the current iteration status before resuming work:

```
Ralph Loop — Iteration N/M
  Completed: X/Y tasks
  Failed: <list of failed task names>
  Stuck: <list of stuck task names, if self-heal>
  Validation: <summary from latest history entry>
  <if self-heal> Retry counters: task X: A/3, task Y: B/3
  Continuing...
```

This information is read from the state file's `ralph.history` array (latest entry) and `ralph.taskRetryCounters`.

On completion (ralph loop finished successfully):
```
Ralph Loop Complete!
  Iterations: N
  Duration: Xm Ys
  All validation passed
```

On failure (max iterations reached):
```
Ralph Loop Failed — max iterations reached (N/M)
  Report saved to: .claude/specs/<name>/ralph-report.md
  Review the report and consider:
    - Fixing failures manually, then /continue-spec
    - Running with --self-heal for auto-retries
    - Increasing --max-iterations
```

### Ralph Abort Mechanism

If the user says "stop ralph", "abort ralph", or "cancel ralph" during a build:

1. Create sentinel file `.claude/ralph-abort`
2. Print: "Ralph abort requested. Will stop after current operation completes."
3. Exit normally — the Stop hook detects the sentinel file and allows exit

The Stop hook's abort handling:
- Checks for `.claude/ralph-abort` at the start of each hook invocation
- If found: removes the sentinel, sets `ralph.status = 'aborted'` and `ralph.active = false`, exits 0 (allows session exit)
- The user can then run `/continue-spec` to resume without ralph, or `/ralph-stop` for explicit cancellation

### Phase 7: Validation

When all implementation tasks complete:

1. **Run Validation Commands** - Execute commands from "Validation Commands" section
2. **Verify Acceptance Criteria** - Check all criteria are met
3. **Update Task Status** - Mark validation task as completed
4. **Update State File** - Final state write with validation results

```typescript
// Update validation section
const state = readStateFile(PATH_TO_PLAN)
if (state) {
  state.validation.commandsRun = validationCommandsRun
  state.validation.acceptanceCriteria = acceptanceCriteriaStatus
  state.build.lastUpdated = new Date().toISOString()
  writeStateFile(PATH_TO_PLAN, state)
}
```

### Phase 8: State Updates During Execution

**Critical:** Keep the state file updated as tasks progress.

#### Subagent Mode State Updates

```typescript
// Mark task as in_progress
await TaskUpdate({ taskId: taskId, status: "in_progress" })
updateTaskInState(PATH_TO_PLAN, taskId, { status: "in_progress" })

// Deploy agent, store agentId
const result = await Task({
  description: task.subject,
  prompt: buildPrompt(task),
  subagent_type: task.agentType,
  run_in_background: false
})

updateTaskInState(PATH_TO_PLAN, taskId, {
  agentId: result.agentId,
  deployedAt: new Date().toISOString()
})

// On task completion
await TaskUpdate({ taskId: taskId, status: "completed" })
updateTaskInState(PATH_TO_PLAN, taskId, {
  status: "completed",
  lastOutput: result.output?.slice(0, 500) || ""
})

// Track artifacts (optional but recommended)
state.artifacts.push({
  taskId: taskId,
  action: "created", // or "modified"
  path: "path/to/file",
  timestamp: new Date().toISOString()
})
writeStateFile(PATH_TO_PLAN, state)
```

#### Team Mode State Updates

```typescript
// When assigning task to teammate
updateTaskInState(PATH_TO_PLAN, taskId, {
  status: "in-progress",
  teammateName: assignee.name,
  deployedAt: new Date().toISOString()
})

// When teammate completes a task (from completion message)
updateTaskInState(PATH_TO_PLAN, taskId, {
  status: "completed",
  lastOutput: completionMessage.content.slice(0, 500)
})

// On build completion (after shutdown)
state.build.completedAt = new Date().toISOString()
state.build.lastUpdated = new Date().toISOString()
writeStateFile(PATH_TO_PLAN, state)
```

**State Update Milestones:**
- After Phase 2 (all tasks created)
- After each task completion
- After each agent deployment (store agentId) or teammate assignment (store teammateName)
- On build completion
- On any interruption/error

### Helper Functions Reference

The following functions are available:

**State File Helpers** (see `scripts/state-file.js`):

```javascript
// Get state file path from spec path
getStateFilePath(specPath) // Returns: ".claude/specs/user-auth/state.json"

// Calculate spec checksum
calculateChecksum(specPath) // Returns: "sha256:abc123..."

// Read state file
readStateFile(specPath) // Returns: state object or null

// Write state file
writeStateFile(specPath, state) // Creates directory if needed

// Update specific task
updateTaskInState(specPath, taskId, updates) // Merges updates

// Delete state file
deleteStateFile(specPath) // Removes state directory

// Sanitize spec name
sanitizeSpecName(specPath) // "specs/user-auth.md" -> "user-auth"

// Create initial state with mode
createInitialState(specPath, tasks, mode) // mode: "subagent" or "team"

// Rebuild from TaskList
rebuildStateFromTaskList(tasks, specPath, mode) // Auto-repair
```

**Validation Hooks Helpers** (see `scripts/hooks.js`):

```javascript
// Extract hooks YAML from a task section in spec
extractTaskHooks(specContent, taskSubject) // Returns: { stop: [...] } or null

// Parse YAML hooks content into structured object
parseHooksYAML(yamlContent) // Returns: { stop: [hooks] }

// Run all validation hooks for a task
runHooks(hooks, context) // Returns: { passed, failed, errors, details }

// Run a single validation hook
runSingleHook(hook, context) // Returns: { passed, error }

// Validate agent output
validateAgentOutput(hook, output) // Checks for success/errors/patterns

// Validate command execution
validateCommand(hook, workingDir) // Runs command, checks exit/output

// Validate artifact existence and content
validateArtifact(hook, workingDir) // Checks file exists/content

// Validate acceptance criteria from spec
validateAcceptanceCriteria(hook, specPath) // Checks checkboxes
```

## Task Execution Example

### Sequential Execution

```typescript
// Task 1: Setup database (no dependencies)
TaskUpdate({ taskId: "1", status: "in_progress" })
Task({
  description: "Setup database",
  prompt: "Create database schema...",
  subagent_type: "general-purpose",
  run_in_background: false
})
// Wait for completion...
TaskUpdate({ taskId: "1", status: "completed" })

// Task 2: Build API (depends on Task 1)
TaskUpdate({ taskId: "2", status: "in_progress" })
Task({
  description: "Build API endpoints",
  prompt: "Implement REST API...",
  subagent_type: "general-purpose",
  run_in_background: false
})
// Wait for completion...
TaskUpdate({ taskId: "2", status: "completed" })
```

### Parallel Execution

```typescript
// Start Task 1 (Frontend)
TaskUpdate({ taskId: "1", status: "in_progress" })
Task({
  description: "Build frontend",
  prompt: "Create UI components...",
  subagent_type: "frontend-agent",
  run_in_background: true  // Non-blocking
})
// Returns: agentId1

// Start Task 2 (Backend) - can run in parallel
TaskUpdate({ taskId: "2", status: "in_progress" })
Task({
  description: "Build backend API",
  prompt: "Create API endpoints...",
  subagent_type: "backend-agent",
  run_in_background: true  // Non-blocking
})
// Returns: agentId2

// Both agents now working in parallel...

// Wait for Task 1
TaskOutput({ task_id: agentId1, block: true })
TaskUpdate({ taskId: "1", status: "completed" })

// Wait for Task 2
TaskOutput({ task_id: agentId2, block: true })
TaskUpdate({ taskId: "2", status: "completed" })

// Task 3 (Integration) depends on both 1 and 2
TaskUpdate({ taskId: "3", status: "in_progress" })
Task({
  description: "Integration testing",
  prompt: "Test frontend + backend...",
  subagent_type: "test-agent",
  run_in_background: false
})
```

## Error Handling

If an agent fails:

1. **Assess the Error** - Read the error message from `TaskOutput`
2. **Determine Cause** - Is it a code error, missing dependency, or unclear requirements?
3. **Choose Action**:
   - **Retry** - For transient errors, resume the agent with corrected instructions
   - **Adjust** - Update the prompt with clarification
   - **Skip** - If task is optional, mark as completed and note the issue
4. **Document** - Add note to the plan about the issue and resolution

## Progress Updates

Provide regular progress updates to the user:

```
🔄 Build Progress

Phase 1: Foundation
✅ Task 1: Setup database
✅ Task 2: Create migrations
🔄 Task 3: Configure API Gateway (in progress)

Phase 2: Core Implementation
⏳ Task 4: Build frontend (waiting for Task 3)
⏳ Task 5: Build backend (waiting for Task 3)

Agents Running:
- agent-abc123: Working on API Gateway configuration
```

## Completion Report

When all tasks are complete, provide:

```
Build Complete!

Plan: specs/<plan-name>.md
Mode: <Subagents | Agent Teams>
Duration: <time taken>
Tasks Completed: <N>/<N>

Summary:
- <Component 1>: Complete
- <Component 2>: Complete
- <Component 3>: Complete

Validation:
- All validation commands passed
- All acceptance criteria met

Files Modified:
- <list of files created/modified>

Document Learnings
Run /compound to capture learnings from this build:
  /compound specs/<plan-name>.md

State Saved
Build state persisted to: .claude/specs/<spec-name>/state.json
Run /continue-spec to resume across sessions:
  /continue-spec specs/<plan-name>.md
```

**Team Mode additions to report:**

```
Teammates:
- <teammate-1-name> (<agent-type>): <N> tasks completed
- <teammate-2-name> (<agent-type>): <N> tasks completed
Team resources cleaned up.
```

### Ralph Team Cleanup (Team Mode + Ralph Complete)

When the ralph loop has completed (status: 'completed', 'failed', or 'aborted') AND team mode was used, perform final cleanup:

```typescript
const state = readStateFile(PATH_TO_PLAN)
if (TEAM_MODE && state.ralph && !state.ralph.active) {
  console.log("Ralph loop complete — cleaning up team resources")

  // 1. Send shutdown_request to all teammates
  for (const member of teamMembers) {
    SendMessage({
      type: "shutdown_request",
      recipient: member.name,
      content: "Ralph loop complete. Shutting down."
    })
  }

  // 2. Wait briefly for acknowledgments (30s timeout)
  // Teammates respond with shutdown_response { approve: true }

  // 3. Clean up team resources
  TeamDelete()

  // 4. Update state file
  state.build.completedAt = new Date().toISOString()
  state.build.lastUpdated = new Date().toISOString()
  writeStateFile(PATH_TO_PLAN, state)

  console.log("Team resources cleaned up")
}
```

## Report Format

After completing the build, provide a concise report following the Completion Report format above.

## Examples

```bash
# Basic usage (subagent mode - default)
/build specs/conversational-ui-revamp.md

# Agent Teams mode - teammates communicate directly, self-claim tasks
/build specs/user-auth.md --team

# Resume from previous build (auto-detected)
/build specs/user-auth.md
# > Previous build found: Resume or Fresh?

# Force fresh start (ignore existing state)
/build specs/user-auth.md --fresh

# With specific phase
/build specs/conversational-ui-revamp.md --phase 2

# Continue from specific task
/build specs/conversational-ui-revamp.md --from-task setup-database
```

### Ralph Mode Examples

```bash
# Ralph mode — iterate build→validate until success
/build specs/user-auth.md --ralph

# Custom max iterations (default is 5)
/build specs/user-auth.md --ralph --max-iterations 10

# Self-healing — auto-retry failed tasks up to 3 times each
/build specs/user-auth.md --ralph --self-heal

# Ralph with team mode — team persists across iterations
/build specs/user-auth.md --team --ralph

# Completion promise — exit when specific text appears
/build specs/user-auth.md --ralph --completion-promise "all tests passing"

# Kitchen sink
/build specs/complex-feature.md --team --ralph --self-heal --max-iterations 15
```

## Tips

1. **Create ALL tasks first** - Before deploying any agents, create the full task list
2. **Use descriptive prompts** - Give agents clear, detailed instructions
3. **Monitor actively** - Check on agents regularly, don't just wait
4. **Resume when appropriate** - Use resume pattern for follow-up work on same task (subagent mode)
5. **Handle failures gracefully** - Assess and recover, don't just fail
6. **Communicate progress** - Keep user informed of what's happening
7. **Use --team for cross-cutting work** - When tasks need to coordinate (e.g., frontend/backend API contracts), use Agent Teams
8. **Use subagents for independent tasks** - When tasks are self-contained and don't need inter-agent communication, subagent mode is cheaper and simpler
9. **Avoid file conflicts in team mode** - Ensure spec assigns different files to different teammates
