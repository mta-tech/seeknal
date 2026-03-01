---
name: continue-spec
description: Resume work from a spec file by detecting completed/in-progress/pending tasks and continuing from where work stopped. Use after /build is interrupted.
argument-hint: <path-to-spec> [options] [--max-iterations N]
model: opus
allowed-tools: Task, TaskOutput, TaskCreate, TaskUpdate, TaskList, TaskGet, TeamCreate, TeamDelete, SendMessage, Bash, Glob, Grep, Read, Edit, Write, AskUserQuestion, Skill
---

# Continue Spec Resume

Resume the entire build workflow from a spec file by intelligently detecting which tasks are complete and which remain.

## What This Does

Unlike `/continue <agent-id>` (which resumes one specific agent), this command resumes the **entire build workflow** from a spec:
- **Loads from state file** (cross-session persistence)
- Falls back to TaskList if state file missing
- Detects spec modifications via checksum
- Resumes accessible agents with preserved context
- Deploys fresh agents when resume not possible
- Handles failures gracefully with user input
- Prompts for `/compound` on completion

## Variables

- `SPEC_PATH`: $1 - Path to the spec file (e.g., `specs/user-auth.md`)
- `DRY_RUN`: --dry-run - Show what would be done without executing
- `FROM_TASK`: --from-task N - Start execution from specific task N
- `RESTART`: --restart - Ignore completed tasks, start fresh
- `TEAM_MODE_AUTO`: Auto-detected from state file `build.mode` field (no flag needed)

## Instructions

### Prerequisites

1. **Validate Spec Path**
   ```typescript
   // Check if spec path provided
   if (!SPEC_PATH) {
     console.error("Error: Spec path required")
     console.error("Usage: /continue-spec <path-to-spec>")
     console.error("Example: /continue-spec specs/user-auth.md")
     return
   }

   // Check if spec exists
   if (!fs.existsSync(SPEC_PATH)) {
     console.error(`Error: Spec file not found: ${SPEC_PATH}`)
     return
   }
   ```

2. **Read Spec Completely**
   - Read the entire spec document before any processing
   - Extract task definitions, dependencies, team assignments

### Phase 1: Detection Phase

**Goal**: Determine current state from state file or TaskList

```typescript
// Try state file first (cross-session persistence)
const state = readStateFile(SPEC_PATH)

if (state) {
  console.log(`Loading state from .claude/specs/${sanitizeSpecName(SPEC_PATH)}/state.json`)

  // Validate spec hasn't changed
  const currentChecksum = calculateChecksum(SPEC_PATH)
  if (state.build.specChecksum !== currentChecksum) {
    console.warn("⚠️ Spec modified since last build!")
    console.warn(`  Modified: ${new Date(fs.statSync(SPEC_PATH).mtime).toISOString()}`)
    console.warn(`  Build started: ${state.build.startedAt}`)
    console.warn("Changes may not be reflected in task definitions.")

    const proceed = await AskUserQuestion({
      question: "Spec has been modified. Continue anyway?",
      options: [
        { label: "Continue", description: "Proceed with current task state" },
        { label: "Restart build", description: "Run /build to pick up new spec changes" }
      ]
    })

    if (proceed === "Restart build") {
      console.log(`Run: /build ${SPEC_PATH}`)
      return
    }
  }

  // Load tasks from state
  const allTasks = state.tasks
  const completed = allTasks.filter(t => t.status === 'completed')
  const inProgress = allTasks.filter(t => t.status === 'in-progress')
  const pending = allTasks.filter(t => t.status === 'pending')

  console.log("Previous Build Status (from state file):")
  console.log(`✅ Completed: ${completed.length} tasks`)
  console.log(`⚠️ In-Progress: ${inProgress.length} tasks`)
  console.log(`⏳ Pending: ${pending.length} tasks`)
} else {
  // Fall back to TaskList (for in-progress builds without state file)
  console.log("No state file found, checking TaskList...")

  const allTasks = await TaskList({})

  if (allTasks.length === 0) {
    console.error("Error: No previous build found")
    console.error("Run /build first to create task state")
    console.error(`Example: /build ${SPEC_PATH}`)
    return
  }

  const completed = allTasks.filter(t => t.status === 'completed')
  const inProgress = allTasks.filter(t => t.status === 'in_progress')
  const pending = allTasks.filter(t => t.status === 'pending')

  console.log("Previous Build Status (from TaskList):")
  console.log(`✅ Completed: ${completed.length} tasks`)
  console.log(`⚠️ In-Progress: ${inProgress.length} tasks`)
  console.log(`⏳ Pending: ${pending.length} tasks`)
}
```

**Why state file first:**
- State file persists across sessions (TaskList is session-scoped)
- State file includes agentId mappings for resume capability
- State file has checksum for spec modification detection
- TaskList fallback ensures compatibility with in-progress builds

### Ralph State Detection

After loading the state file, check for ralph loop state:

```typescript
if (state.ralph && state.ralph.active) {
  console.log(`Ralph loop detected: iteration ${state.ralph.currentIteration}/${state.ralph.maxIterations}`)
  console.log(`  Mode: ${state.ralph.mode}`)
  console.log(`  Status: ${state.ralph.status}`)
  console.log(`  Self-heal: ${state.ralph.selfHeal ? 'enabled' : 'disabled'}`)
}
```

**Handle ralph statuses:**

If `state.ralph` exists:

1. **Status: 'failed'** (max iterations exhausted):
   ```typescript
   if (state.ralph.status === 'failed') {
     const action = await AskUserQuestion({
       question: `Previous ralph loop exhausted max iterations (${state.ralph.currentIteration}/${state.ralph.maxIterations}). What would you like to do?`,
       options: [
         { label: "Start new ralph loop", description: "Reset iteration counter, keep completed task state" },
         { label: "Resume without ralph", description: "Continue in manual mode (no auto-iteration)" },
         { label: "Increase max iterations", description: "Set new max and continue ralph loop" }
       ]
     })

     switch (action) {
       case "Start new ralph loop":
         state.ralph.currentIteration = 0
         state.ralph.status = 'active'
         state.ralph.active = true
         state.ralph.history = []
         state.ralph.taskRetryCounters = {}
         writeStateFile(specPath, state)
         // Continue to build with ralph active
         break
       case "Resume without ralph":
         state.ralph.active = false
         state.ralph.status = 'disabled'
         writeStateFile(specPath, state)
         // Continue to build without ralph
         break
       case "Increase max iterations":
         // Could parse --max-iterations from command line, or ask user
         state.ralph.maxIterations = state.ralph.maxIterations + 5
         state.ralph.status = 'active'
         state.ralph.active = true
         writeStateFile(specPath, state)
         console.log(`Max iterations increased to ${state.ralph.maxIterations}`)
         break
     }
   }
   ```

2. **Status: 'active' or 'paused'**:
   ```typescript
   if (state.ralph.status === 'active') {
     console.log(`Resuming ralph loop from iteration ${state.ralph.currentIteration} (max: ${state.ralph.maxIterations})`)
     // Continue to build with ralph state preserved
   }
   ```

3. **Status: 'aborted'**:
   ```typescript
   if (state.ralph.status === 'aborted') {
     const action = await AskUserQuestion({
       question: `Previous ralph loop was aborted at iteration ${state.ralph.currentIteration}/${state.ralph.maxIterations}. What would you like to do?`,
       options: [
         { label: "Resume ralph loop", description: "Continue from where it stopped" },
         { label: "Resume without ralph", description: "Continue in manual mode" }
       ]
     })

     if (action === "Resume ralph loop") {
       state.ralph.status = 'active'
       state.ralph.active = true
       writeStateFile(specPath, state)
     } else {
       state.ralph.active = false
       state.ralph.status = 'disabled'
       writeStateFile(specPath, state)
     }
   }
   ```

**Parameter override:** If `--max-iterations N` is provided on the continue-spec command line, it overrides `state.ralph.maxIterations`.

### Phase 1.5: Team Mode Detection

After loading state, check the build mode and branch accordingly:

```typescript
// Auto-detect team mode from state file
if (state && state.build.mode === 'team') {
  console.log(`Build mode: Agent Teams (team: ${state.build.teamName})`)
  console.log("Agent Teams do not support session resumption.")
  console.log("Will create a fresh team for remaining tasks.\n")

  // Check for orphaned team resources from previous build
  // Team config at: ~/.claude/teams/{teamName}/config.json
  const teamConfigPath = `~/.claude/teams/${state.build.teamName}/config.json`

  // Clean up any orphaned team resources
  try {
    TeamDelete()  // Clean up if team exists from previous interrupted build
  } catch (e) {
    // Team doesn't exist - that's fine
  }

  // Filter to remaining tasks only
  const remainingTasks = state.tasks.filter(t => t.status !== 'completed')
  const completedCount = state.tasks.filter(t => t.status === 'completed').length

  if (remainingTasks.length === 0) {
    console.log("All tasks already completed!")
    // Prompt for /compound
    return
  }

  console.log(`Honoring ${completedCount} completed tasks.`)
  console.log(`Resuming ${remainingTasks.length} remaining tasks with fresh team.\n`)

  // Create fresh team
  TeamCreate({
    team_name: state.build.teamName,
    description: `Resume build: ${state.build.specPath}`
  })

  // Read spec to get team member definitions
  const specContent = Read(SPEC_PATH)
  const teamMembers = parseTeamMembers(specContent)

  // Spawn teammates
  for (const member of teamMembers) {
    Task({
      team_name: state.build.teamName,
      name: member.name,
      subagent_type: member.agentType || 'general-purpose',
      mode: member.planApproval ? 'plan' : 'default',
      model: member.model || 'opus',
      prompt: `You are ${member.name}, a teammate on the "${state.build.teamName}" team.
Your role: ${member.role}

This is a RESUMED build. Some tasks are already completed.
Check TaskList for your assigned tasks. Only work on pending/in-progress tasks.

After completing each task:
1. Mark completed: TaskUpdate({ taskId, status: "completed" })
2. Send the lead a completion summary via SendMessage
3. Check TaskList for next available task
4. If no more tasks, notify the lead you're done`
    })
  }

  // Assign remaining tasks to teammates
  for (const task of remainingTasks) {
    const assignee = findAssigneeFromSpec(task, teamMembers)
    TaskUpdate({
      taskId: task.id,
      owner: assignee.name,
      status: "in_progress"
    })
    updateTaskInState(SPEC_PATH, task.id, {
      status: "in-progress",
      teammateName: assignee.name,
      deployedAt: new Date().toISOString()
    })
  }

  // Monitor via messages (same as /build --team Phase 6)
  // Messages arrive automatically from teammates
  // Process completion messages, update state, run validation
  // When all tasks complete, proceed to Phase 4 (Completion)

} else if (state && state.build.mode === 'party') {
  const currentPhase = state.party.currentPhase
  const phaseNames = ["brainstorm", "plan", "build", "validate"]
  const phaseName = phaseNames[currentPhase - 1] || "unknown"

  console.log(`Build mode: Party Mode`)
  console.log(`Topic: "${state.party.topic}"`)
  console.log(`Current phase: ${currentPhase}/4 (${phaseName})`)
  console.log("Agent Teams do not support session resumption.")
  console.log("Will create a fresh team for remaining work.\n")

  // Show phase history
  for (const phase of state.party.phases) {
    const icon = phase.status === 'completed' ? '✅' :
                 phase.status === 'in-progress' ? '🔄' : '⏳'
    console.log(`  ${icon} Phase ${phase.phase}: ${phase.name} - ${phase.status}`)
  }

  // Clean up any orphaned team resources from previous interrupted build
  try {
    TeamDelete()
  } catch (e) {
    // Team doesn't exist from previous session - that's fine
  }

  // Create fresh team
  TeamCreate({
    team_name: state.build.teamName,
    description: `Party resume: ${state.party.topic}`
  })

  // Spawn agents with phase-appropriate instructions and cross-phase context
  const context = state.party.context || {}
  const contextSummary = [
    context.keyDecisions?.length ? `Key Decisions: ${context.keyDecisions.join('; ')}` : '',
    context.architectureChoices?.length ? `Architecture: ${context.architectureChoices.join('; ')}` : '',
    context.constraints?.length ? `Constraints: ${context.constraints.join('; ')}` : ''
  ].filter(Boolean).join('\n')

  for (const agent of state.party.roster) {
    let phaseInstructions = ''

    if (currentPhase <= 1) {
      // Brainstorm incomplete → restart brainstorm
      phaseInstructions = `Phase: BRAINSTORM (restarting)
Research this topic from your expertise domain.
Message the lead with your findings when ready.`
    } else if (currentPhase === 2) {
      // Plan incomplete → restart plan phase
      phaseInstructions = `Phase: PLAN (restarting)
Contribute your planning expertise based on the brainstorm context below.
Message the lead with your plan contribution when ready.`
    } else if (currentPhase === 3) {
      // Build incomplete → resume with pending tasks
      phaseInstructions = `Phase: BUILD (resuming)
Check TaskList for remaining tasks matching your expertise.
Claim available tasks and implement them.
Message the lead on completion of each task.`
    } else if (currentPhase === 4) {
      // Validate incomplete → restart validation
      phaseInstructions = `Phase: VALIDATE (restarting)
Run validation for your area of expertise.
Message the lead with your validation report.`
    }

    Task({
      team_name: state.build.teamName,
      name: agent.name,
      subagent_type: agent.agentType || 'general-purpose',
      model: agent.model || 'sonnet',
      prompt: `You are the ${agent.role} on a party-mode team (RESUMED session).
Topic: "${state.party.topic}"

Context from previous phases:
${contextSummary}

${state.party.brainstormSummary ? `Brainstorm summary available at: ${state.party.brainstormSummary}` : ''}
${state.party.planPath ? `Plan spec available at: ${state.party.planPath}` : ''}

${phaseInstructions}
`
    })
  }

  // For build phase, recreate TaskList from state and assign remaining tasks
  if (currentPhase === 3) {
    const remainingTasks = state.tasks.filter(t => t.status !== 'completed')
    const completedCount = state.tasks.filter(t => t.status === 'completed').length

    if (remainingTasks.length === 0) {
      console.log("\nAll build tasks already completed! Proceeding to validation.")
      // Lead should advance to Phase 4 (VALIDATE)
    } else {
      console.log(`\nHonoring ${completedCount} completed tasks.`)
      console.log(`Resuming ${remainingTasks.length} remaining build tasks with fresh team.`)

      // Recreate TaskList entries for remaining tasks
      for (const task of remainingTasks) {
        TaskCreate({
          subject: task.subject,
          description: task.description
        })
      }
    }
  }

  // Resume is handled - the lead (this session) continues the party workflow
  // from the current phase using the respawned team.
  // Proceed to the appropriate phase logic in the party command.
  console.log(`\nParty team respawned. Resume the party workflow with:`)
  console.log(`  /party "${state.party.topic}"`)
  console.log(`The party command will detect existing state and resume from Phase ${currentPhase}.`)

} else {
  // Subagent mode (existing behavior) - continue with Phase 2
  console.log("Build mode: Subagents (default)")
}
```

### Phase 2: In-Progress Handling

**Note:** Phase 2 only applies to subagent-mode builds. Team-mode builds handle remaining tasks in Phase 1.5 above.

**Goal**: Handle in-progress tasks with user choice

```typescript
if (inProgress.length > 0) {
  console.log(`Found ${inProgress.length} in-progress task(s)`)

  for (const task of inProgress) {
    // When loading from state file, task has agentId directly
    // When loading from TaskList, get agentId from metadata
    let agentId, taskDetails, lastOutput

    if (state) {
      // From state file
      agentId = task.agentId
      lastOutput = task.lastOutput
      taskDetails = task
    } else {
      // From TaskList
      taskDetails = await TaskGet({ taskId: task.id })
      agentId = taskDetails.metadata?.agentId
      if (agentId) {
        const output = await TaskOutput({
          task_id: agentId,
          block: false,
          timeout: 5000
        })
        lastOutput = output.slice(0, 500)
      }
    }

    if (agentId || lastOutput) {
      console.log(`\nTask: ${taskDetails.subject}`)
      console.log(`Status: ${taskDetails.status}`)
      if (lastOutput) {
        console.log(`Last output:\n${lastOutput}...`)
      }
    }

    // Ask user what to do
    const action = await AskUserQuestion({
      question: `Task "${taskDetails.subject}" is in-progress. What should we do?`,
      options: [
        {
          label: "Resume",
          description: "Continue from where agent left off (preserves context)"
        },
        {
          label: "Restart",
          description: "Start this task over from beginning (fresh agent)"
        },
        {
          label: "Skip",
          description: "Mark as completed and move on to next task"
        }
      ]
    })

    // Handle user choice
    switch (action) {
      case "Resume":
        // Continue with existing agent (will resume in Phase 3)
        break
      case "Restart":
        // Mark as pending for fresh deployment
        if (state) {
          updateTaskInState(SPEC_PATH, task.id, { status: "pending" })
        } else {
          await TaskUpdate({ taskId: task.id, status: "pending" })
        }
        break
      case "Skip":
        // Mark as completed
        if (state) {
          updateTaskInState(SPEC_PATH, task.id, { status: "completed" })
        } else {
          await TaskUpdate({ taskId: task.id, status: "completed" })
        }
        break
    }
  }
}
```

### Phase 3: Pending Task Execution

**Goal**: Execute remaining tasks with hybrid resume/fresh deploy approach

```typescript
// 1. Sort pending tasks by dependencies (blockedBy)
const sortedPending = topologicalSort(pendingTasks)

// 2. Execute each task
for (const task of sortedPending) {
  // Get task details and agentId based on source
  let taskDetails, agentId

  if (state) {
    // From state file - agentId is direct property
    taskDetails = task
    agentId = task.agentId
  } else {
    // From TaskList - agentId is in metadata
    taskDetails = await TaskGet({ taskId: task.id })
    agentId = taskDetails.metadata?.agentId
  }

  // Check if agent is accessible
  const agentAccessible = agentId ? await isAgentAccessible(agentId) : false

  if (agentId && agentAccessible) {
    // Resume existing agent (preserves context!)
    console.log(`Resuming task: ${taskDetails.subject} (agent: ${agentId})`)

    const result = await Task({
      description: taskDetails.subject,
      prompt: buildPromptFromSpec(taskDetails, specContent),
      subagent_type: taskDetails.agentType || 'general-purpose',
      resume: agentId
    })

    // Update state with new agentId if returned
    if (result.agentId) {
      if (state) {
        updateTaskInState(SPEC_PATH, task.id, { agentId: result.agentId })
      } else {
        await TaskUpdate({
          taskId: task.id,
          metadata: { agentId: result.agentId }
        })
      }
    }
  } else {
    // Fresh deployment
    console.log(`Deploying fresh agent for: ${taskDetails.subject}`)

    const result = await Task({
      description: taskDetails.subject,
      prompt: buildPromptFromSpec(taskDetails, specContent),
      subagent_type: taskDetails.agentType || 'general-purpose',
      run_in_background: false
    })

    // Store agentId for future resume
    if (state) {
      updateTaskInState(SPEC_PATH, task.id, {
        agentId: result.agentId,
        deployedAt: new Date().toISOString()
      })
    } else {
      await TaskUpdate({
        taskId: task.id,
        metadata: {
          agentId: result.agentId,
          deployedAt: new Date().toISOString()
        }
      })
    }
  }

  // Mark task as completed
  if (state) {
    updateTaskInState(SPEC_PATH, task.id, { status: "completed" })
  } else {
    await TaskUpdate({ taskId: task.id, status: "completed" })
  }

  // Update spec checkbox
  updateSpecCheckbox(SPEC_PATH, task.id)
}

function isAgentAccessible(agentId) {
  try {
    TaskOutput({
      task_id: agentId,
      block: false,
      timeout: 1000
    })
    return true
  } catch {
    return false
  }
}
```

### Phase 4: Completion

**Goal**: Show summary and prompt for /compound

```typescript
console.log("\nAll Tasks Complete!")
console.log(`Tasks: ${allTasks.length}/${allTasks.length} completed`)
console.log(`Duration: ${calculateDuration()}`)

// Prompt for /compound
const compound = await AskUserQuestion({
  question: "All tasks complete! Would you like to document learnings?",
  options: [
    {
      label: "Run /compound",
      description: "Capture learnings (ADRs, solutions, patterns)"
    },
    {
      label: "Skip for now",
      description: "I'll run /compound manually later"
    }
  ]
})

if (compound === "Run /compound") {
  console.log(`Run: /compound ${SPEC_PATH}`)
}
```

## Edge Case Handling

| Edge Case | Detection | Handling |
|-----------|-----------|----------|
| **Spec never built** | State file null AND TaskList returns empty tasks | Error: "No previous build found. Run `/build` first." |
| **No task state** | TaskList error or empty | Suggest `/build` or ask if spec was modified externally |
| **Spec changed** | Checksum mismatch between state file and current spec | Warning: "Spec modified since last build. Changes may not be reflected in ongoing tasks." |
| **Agents lost** | TaskOutput fails or timeout | Info: "Agent no longer accessible, starting fresh deployment" |
| **Corrupted state file** | JSON parse error | Warning: "State file corrupted, attempting repair from TaskList..." |
| **State file missing** | File doesn't exist (ENOENT) | Info: "No state file found, checking TaskList..." |
| **Team-mode build interrupted** | `state.build.mode === "team"` | Create fresh team with `TeamCreate`, spawn teammates, assign only remaining tasks. Agent Teams cannot resume sessions. |
| **Party-mode interrupted** | `state.build.mode === "party"` | Create fresh team, spawn agents with phase context, resume from `party.currentPhase`. Phases 1-2 restart, Phase 3 resumes pending tasks, Phase 4 restarts validation. |

### Spec Modification Detection

```typescript
// Checksum-based detection (already done in Phase 1)
const currentChecksum = calculateChecksum(SPEC_PATH)
if (state.build.specChecksum !== currentChecksum) {
  console.warn("⚠️ Spec modified since last build.")
  // ... prompt user
}
```

### Corrupted State File Recovery

```typescript
// In readStateFile helper - attempt auto-repair
try {
  return JSON.parse(fs.readFileSync(statePath, 'utf8'))
} catch (error) {
  if (error instanceof SyntaxError) {
    console.warn("State file is corrupted (invalid JSON)")
    console.warn("Attempting to rebuild from TaskList...")

    try {
      const tasks = await TaskList({})
      if (tasks.length > 0) {
        const rebuiltState = rebuildStateFromTaskList(tasks, SPEC_PATH)
        writeStateFile(SPEC_PATH, rebuiltState)
        console.log("✓ State file rebuilt from TaskList")
        return rebuiltState
      }
    } catch (repairError) {
      console.error("✗ Cannot recover state from TaskList")
    }

    throw new Error("State file corrupted and unrecoverable. Run /build to start fresh.")
  }
  throw error
}
```

    const proceed = await AskUserQuestion({
      question: "Spec has been modified. Continue anyway?",
      options: [
        { label: "Continue", description: "Proceed with current task state" },
        { label: "Restart build", description: "Run /build to pick up new spec changes" }
      ]
    })

    if (proceed === "Restart build") {
      console.log(`Run: /build ${SPEC_PATH}`)
      return
    }
  }
}
```

## Flags

### --dry-run

Show what would be done without executing:

```typescript
if (DRY_RUN) {
  console.log("Dry Run Mode - No changes will be made\n")
  console.log("Would resume:")
  console.log(`- Skip ${completed.length} completed tasks`)
  console.log("- Handle in-progress tasks with user input")
  console.log(`- Execute ${pending.length} pending tasks`)
  console.log("\nRun without --dry-run to execute.")
  return
}
```

### --from-task N

Start execution from specific task N:

```typescript
if (FROM_TASK) {
  console.log(`Starting from task ${FROM_TASK}`)
  // Filter tasks to only those >= FROM_TASK
  pendingTasks = pendingTasks.filter(t => t.id >= FROM_TASK)
}
```

### --restart

Ignore completed tasks, start fresh:

```typescript
if (RESTART) {
  console.log("Restart mode - ignoring completed tasks")
  // Mark all completed tasks as pending
  for (const task of completed) {
    await TaskUpdate({
      taskId: task.id,
      status: "pending"
    })
  }
  pendingTasks = [...completed, ...pending]
}
```

## Helper Functions

### buildPromptFromSpec

```typescript
function buildPromptFromSpec(taskDetails, specContent) {
  return `
Execute the following task from the spec:

Spec: ${SPEC_PATH}

Task ID: ${taskDetails.id}
Task Name: ${taskDetails.subject}

Requirements:
${taskDetails.description}

Acceptance Criteria:
- [List from spec]

Work through the implementation step by step.
Report progress as you complete each subtask.
  `.trim()
}
```

### topologicalSort

```typescript
function topologicalSort(tasks) {
  // Sort by dependencies (blockedBy)
  const sorted = []
  const visited = new Set()

  function visit(task) {
    if (visited.has(task.id)) return
    visited.add(task.id)

    // Visit dependencies first
    for (const depId of task.blockedBy || []) {
      const depTask = tasks.find(t => t.id === depId)
      if (depTask) visit(depTask)
    }

    sorted.push(task)
  }

  for (const task of tasks) {
    visit(task)
  }

  return sorted
}
```

### updateSpecCheckbox

```typescript
function updateSpecCheckbox(specPath, taskId) {
  // Read spec content
  const content = fs.readFileSync(specPath, 'utf8')

  // Find and update the checkbox for this task
  // This depends on how checkboxes are formatted in the spec
  // Example: - [ ] Task 1 → - [x] Task 1

  const updated = content.replace(
    new RegExp(`^- \\[ \\] .*Task ${taskId}`),
    `- [x] Task ${taskId}`
  )

  fs.writeFileSync(specPath, updated)
}
```

## Helper Functions Reference

The following functions are available (see `scripts/state-file.js`):

```javascript
// Get state file path from spec path
getStateFilePath(specPath) // Returns: ".claude/specs/user-auth/state.json"

// Calculate spec checksum (SHA256)
calculateChecksum(specPath) // Returns: "sha256:abc123..."

// Read state file (returns null if not found)
readStateFile(specPath) // Returns: state object or null

// Write state file (creates directory if needed)
writeStateFile(specPath, state) // Writes JSON with 2-space indent

// Update specific task in state
updateTaskInState(specPath, taskId, updates) // Merges updates

// Delete state file and directory
deleteStateFile(specPath) // Removes state directory if empty

// Sanitize spec name to directory name
sanitizeSpecName(specPath) // "specs/user-auth.md" -> "user-auth"

// Rebuild state from TaskList (for auto-repair)
rebuildStateFromTaskList(tasks, specPath) // Creates new state object
```

  // Find and update the checkbox for this task
  // This depends on how checkboxes are formatted in the spec
  // Example: - [ ] Task 1 → - [x] Task 1

  const updated = content.replace(
    new RegExp(`^- \\[ \\] .*Task ${taskId}`),
    `- [x] Task ${taskId}`
  )

  fs.writeFileSync(specPath, updated)
}
```

## Progress Updates

Provide regular progress updates during execution:

```
Continue Progress:

Completed: 3 tasks (1, 2, 3)
Pending: 7 tasks (4-10)

Working on:
- Task 4: Implement JWT endpoints (agent: abc123 - resuming)
```

## Completion Report

When all tasks complete, provide:

```
Continue Complete!

Spec: specs/<plan-name>.md
Tasks Resumed: 3
Tasks Fresh Deployed: 4
Duration: <time taken>

Summary:
- <Component 1>: Complete (resumed from agent abc123)
- <Component 2>: Complete (fresh deployment)
- <Component 3>: Complete

Files Modified:
- <list of files created/modified>

Document Learnings
Run /compound to capture learnings from this build:
  /compound specs/<plan-name>.md
```

## Examples

### Basic Usage

```bash
# Resume from spec
/continue-spec specs/user-auth.md

# Dry run
/continue-spec specs/user-auth.md --dry-run

# From specific task
/continue-spec specs/user-auth.md --from-task 5

# Restart (ignore completed)
/continue-spec specs/user-auth.md --restart
```

### Example Output

```bash
$ /continue-spec specs/user-auth.md

Previous Build Status:
- Completed: 3 tasks (1, 2, 3)
- In-Progress: 1 task (4 - failed)
- Pending: 6 tasks (5-10)

Task 4 "Implement JWT endpoints" is in-progress

Last output:
Error: Failed to import jwt library...

What should we do?
> Resume - Continue from where agent left off
> Restart - Start this task over from beginning
> Skip - Mark as completed and move on

[User selects: Restart]

Restarting task 4...
[Task 4 completes]

Continuing with remaining tasks...
[Tasks 5-10 complete]

All Tasks Complete!
Tasks: 10/10 completed
Duration: 12 minutes

Document Learnings
Run /compound to capture learnings from this build:
  /compound specs/user-auth.md
```

## Tips

1. **TaskList is source of truth** - Always use TaskList for state detection, not spec parsing
2. **Preserve agent context** - Resume agents when possible to avoid losing context
3. **Handle failures gracefully** - Ask user before retrying failed tasks
4. **Update spec checkboxes** - Keep spec in sync with actual progress
5. **Prompt for /compound** - Always offer to document learnings after completion
