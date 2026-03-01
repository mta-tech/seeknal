---
name: party
description: Run a full product team through brainstorm → plan → build → validate using Agent Teams. Start from an idea, end with implementation.
argument-hint: [topic-description]
model: opus
allowed-tools: Task, TaskOutput, Bash, Glob, Grep, Read, Edit, Write, NotebookEdit, WebFetch, WebSearch, AskUserQuestion, Skill, TodoWrite, TeamCreate, TeamDelete, SendMessage
---

# Party Mode

Run a full product team through brainstorm → plan → build → validate using Claude Agent Teams. Start from an idea, end with implementation.

## Variables

- `TOPIC`: $1 - The topic or feature description (e.g., "Build user authentication with OAuth")
- `TEAM_NAME`: Derived from topic, sanitized to kebab-case with "party-" prefix

## Instructions

### Prerequisites

- If no `TOPIC` is provided, STOP and ask the user to describe what they want to build
- Party Mode requires Agent Teams. Verify the experimental flag is enabled:

```typescript
const agentTeamsEnabled = process.env.CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS === '1'
if (!agentTeamsEnabled) {
  console.error("Error: Party Mode requires Agent Teams (experimental).")
  console.error("Enable it with:")
  console.error("  export CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1")
  console.error("Then restart Claude Code and retry.")
  return
}
```

### Execution Strategy

You are the **Party Lead** - your job is to orchestrate a team of 8 specialist agents through 4 phases. You coordinate, synthesize, and present to the user. You do NOT write code directly.

#### Core Principles

1. **Orchestrate, don't implement** - Use agents to do the actual work
2. **Synthesize perspectives** - Combine 8 viewpoints into actionable summaries
3. **Checkpoint with user** - Get approval before each phase transition
4. **Persist state** - Save progress after every significant event
5. **Keep the team alive** - Same team throughout all 4 phases

## Workflow

### Phase 0: Prerequisites & Resume Detection

```typescript
// 1. Sanitize topic to kebab-case
const topicSlug = TOPIC.toLowerCase()
  .replace(/[^a-z0-9]+/g, '-')
  .replace(/^-|-$/g, '')
  .slice(0, 50)
const TEAM_NAME = `party-${topicSlug}`
const SPEC_PATH = `specs/party-${topicSlug}.md`

// 2. Check for existing state
const existingState = readStateFile(SPEC_PATH)

if (existingState && existingState.build.mode === 'party') {
  const phase = getPartyPhase(existingState)
  const phaseName = ['brainstorm', 'plan', 'build', 'validate'][phase - 1]

  console.log(`Previous party session found:`)
  console.log(`- Topic: ${existingState.party.topic}`)
  console.log(`- Phase: ${phase}/4 (${phaseName})`)

  const action = await AskUserQuestion({
    question: `Resume from Phase ${phase} (${phaseName})?`,
    options: [
      { label: "Resume", description: `Continue from Phase ${phase}` },
      { label: "Start fresh", description: "Discard previous progress" },
      { label: "Abort", description: "Cancel" }
    ]
  })

  if (action === "Abort") return
  if (action === "Start fresh") {
    try { TeamDelete() } catch (e) { /* team may not exist */ }
    deleteStateFile(SPEC_PATH)
  } else {
    // Resume: jump to Phase 1 (team setup) which will spawn fresh team
    // Agent Teams don't support session resumption
    return resumeParty(existingState)
  }
} else if (existingState) {
  // Non-party state exists for this path
  const action = await AskUserQuestion({
    question: `A non-party build state exists for "${topicSlug}". Overwrite?`,
    options: [
      { label: "Overwrite", description: "Replace with party mode state" },
      { label: "Abort", description: "Cancel" }
    ]
  })
  if (action === "Abort") return
  deleteStateFile(SPEC_PATH)
}

// 3. Create initial state
const partyOptions = {
  topic: TOPIC,
  roster: [
    { role: "Product Manager", name: "pm", teammateName: "pm", agentType: "party-pm-agent", model: "sonnet" },
    { role: "System Architect", name: "architect", teammateName: "architect", agentType: "party-architect-agent", model: "sonnet" },
    { role: "Backend Developer", name: "backend", teammateName: "backend", agentType: "party-backend-agent", model: "sonnet" },
    { role: "Frontend Developer", name: "frontend", teammateName: "frontend", agentType: "party-frontend-agent", model: "sonnet" },
    { role: "QA Engineer", name: "qa", teammateName: "qa", agentType: "party-qa-agent", model: "sonnet" },
    { role: "UX Designer", name: "ux", teammateName: "ux", agentType: "party-ux-agent", model: "sonnet" },
    { role: "Tech Writer", name: "docs", teammateName: "docs", agentType: "party-docs-agent", model: "sonnet" },
    { role: "DevOps Engineer", name: "devops", teammateName: "devops", agentType: "party-devops-agent", model: "sonnet" }
  ]
}

const state = createInitialState(SPEC_PATH, [], 'party', partyOptions)
writeStateFile(SPEC_PATH, state)
```

### Phase 1: Team Setup

```typescript
// 1. Cost confirmation
const proceed = await AskUserQuestion({
  question: `Party Mode will spawn 8 specialist agents (PM, Architect, Backend, Frontend, QA, UX, Docs, DevOps). Each is a full Claude session using significantly more tokens. Proceed?`,
  options: [
    { label: "Proceed", description: "Spawn 8 agents and start brainstorming" },
    { label: "Abort", description: "Cancel party mode" }
  ]
})
if (proceed === "Abort") {
  deleteStateFile(SPEC_PATH)
  return
}

// 2. Create team
TeamCreate({
  team_name: TEAM_NAME,
  description: `Party: ${TOPIC}`
})

// 3. Spawn 8 teammates with brainstorm instructions
const roster = partyOptions.roster

for (const agent of roster) {
  Task({
    team_name: TEAM_NAME,
    name: agent.name,
    subagent_type: agent.agentType,
    model: agent.model,
    prompt: `You are the ${agent.role} on a Party Mode team.

Topic: "${TOPIC}"

## Current Phase: BRAINSTORM

Research this topic from your expertise domain. Use the tools available to you:
- Read existing code with Glob, Grep, Read
- Search the web with WebSearch, WebFetch if needed
- Analyze the codebase structure

When your research is complete, send your findings to the lead using:
SendMessage({
  type: "message",
  recipient: "team-lead",
  content: "<your structured findings>",
  summary: "${agent.role} brainstorm findings"
})

After sending findings, WAIT for the lead to give you Phase 2 instructions. Do not proceed on your own.`
  })
}

// 4. Update state
updatePartyPhase(SPEC_PATH, 1, {
  startedAt: new Date().toISOString(),
  status: 'in-progress'
})
```

### Phase 2: BRAINSTORM

```typescript
// 1. Wait for all 8 agents to send their research findings
// Messages arrive automatically via SendMessage
// Track which agents have reported:
const receivedFrom = new Set()
const findings = {}

// As each message arrives:
// receivedFrom.add(senderName)
// findings[senderName] = messageContent

// If an agent hasn't reported in 5 minutes, send check-in:
// SendMessage({
//   type: "message",
//   recipient: agentName,
//   content: "Status update on your brainstorm research?",
//   summary: "Check-in on research"
// })

// 2. Once all 8 agents have reported (or after check-ins):
// Synthesize findings into a round-table summary

// 3. Write brainstorm artifact
// Write to: .claude/specs/party-<topic>/brainstorm-summary.md
// Content:
// # Brainstorm Summary: <TOPIC>
//
// ## Key Themes
// [Themes that multiple agents identified]
//
// ## Points of Agreement
// [What all or most agents agree on]
//
// ## Trade-offs & Disagreements
// [Where agents have different perspectives]
//
// ## Recommended Decisions
// [Synthesized recommendations from all perspectives]
//
// ## Individual Findings
// ### PM Findings
// [PM's contribution]
// ### Architect Findings
// [Architect's contribution]
// ... (all 8 agents)

// 4. Save brainstorm summary path to state
// state.party.brainstormSummary = summaryPath
// state.party.context.keyDecisions = extractedDecisions
// writeStateFile(SPEC_PATH, state)

// 5. Present synthesis to user
console.log("=== BRAINSTORM SUMMARY ===")
console.log(synthesis)

// 6. User checkpoint
const brainstormAction = await AskUserQuestion({
  question: "Brainstorm complete. What would you like to do?",
  options: [
    { label: "Proceed to Planning", description: "Move to Phase 2: collaborative planning" },
    { label: "Dig deeper", description: "Tell agents to research specific areas more" },
    { label: "Adjust scope", description: "Provide new constraints or direction" },
    { label: "Abort", description: "Shut down team and exit" }
  ]
})

if (brainstormAction === "Abort") {
  // Shutdown and cleanup
  goto Phase6_Cleanup
}

if (brainstormAction === "Dig deeper" || brainstormAction === "Adjust scope") {
  // Get user feedback
  const feedback = await AskUserQuestion({
    question: "What should the team focus on?",
    options: [] // Free text via "Other"
  })
  // Message relevant agents with feedback
  // Loop back to waiting for findings
}

// 7. Mark brainstorm complete
updatePartyPhase(SPEC_PATH, 1, {
  completedAt: new Date().toISOString(),
  status: 'completed',
  artifacts: [summaryPath],
  userApproval: { approved: true, timestamp: new Date().toISOString() }
})
```

### Phase 3: PLAN

```typescript
// 1. Update state
updatePartyPhase(SPEC_PATH, 2, {
  startedAt: new Date().toISOString(),
  status: 'in-progress'
})

// 2. Message all 8 agents with planning instructions
const brainstormContext = readBrainstormSummary()
const keyDecisions = state.party.context.keyDecisions

for (const agent of roster) {
  SendMessage({
    type: "message",
    recipient: agent.name,
    content: `## Phase: PLAN

Context from brainstorm:
${brainstormContext}

Key decisions so far:
${keyDecisions.map(d => `- ${d}`).join('\n')}

Your planning task: Contribute your section to the implementation plan.
Use your phase-specific instructions for the PLAN phase.

Message the lead when your contribution is ready.`,
    summary: "Phase 2 PLAN instructions"
  })
}

// 3. Collect planning contributions from all agents
// Track: architect (architecture), pm (stories), backend (tasks), frontend (tasks),
//        qa (test strategy), ux (user flows), docs (doc plan), devops (deployment)

// 4. Assemble into /build-compatible spec format
// Write spec to: specs/party-<topic>.md
//
// The spec MUST follow the /build plan format:
//
// ---
// title: "<TOPIC>"
// type: feat
// date: YYYY-MM-DD
// ---
//
// # <TOPIC>
//
// ## Overview
// [From brainstorm synthesis]
//
// ## Architecture
// [From Architect's contribution]
//
// ## Team Members
// | Name | Role | Agent Type |
// |------|------|------------|
// | pm | Product Manager | party-pm-agent |
// | architect | System Architect | party-architect-agent |
// | backend | Backend Developer | party-backend-agent |
// | frontend | Frontend Developer | party-frontend-agent |
// | qa | QA Engineer | party-qa-agent |
// | ux | UX Designer | party-ux-agent |
// | docs | Tech Writer | party-docs-agent |
// | devops | DevOps Engineer | party-devops-agent |
//
// ## Step by Step Tasks
// [From PM stories + dev task breakdown + QA test tasks + docs tasks]
//
// ### Task 1: <title>
// - **Description**: <description>
// - **Assigned To**: <agent name>
// - **Agent Type**: <agent type>
// - **Files**: <files to create/modify>
// - **Dependencies**: <task dependencies>
// - **AC**: <acceptance criteria>
//
// ## Validation Commands
// [From QA's test strategy]
//
// ## Acceptance Criteria
// [From PM + QA combined]

// 5. Save spec path to state
// state.party.planPath = SPEC_PATH
// state.build.specPath = SPEC_PATH
// state.build.specChecksum = calculateChecksum(SPEC_PATH)
// writeStateFile(SPEC_PATH, state)

// 6. Present plan to user
console.log("=== IMPLEMENTATION PLAN ===")
console.log(specContent)

// 7. User checkpoint
const planAction = await AskUserQuestion({
  question: "Plan assembled. What would you like to do?",
  options: [
    { label: "Proceed to Build", description: "Start implementing the plan" },
    { label: "Edit plan", description: "Open spec for manual edits" },
    { label: "Revise", description: "Tell agents to adjust specific sections" },
    { label: "Abort", description: "Shut down team and exit" }
  ]
})

if (planAction === "Abort") goto Phase6_Cleanup
if (planAction === "Edit plan") {
  // Open file for editing, wait for user confirmation
  Bash({ command: `open ${SPEC_PATH}` })
  // After user confirms edits are done, re-read and continue
}

// 8. Mark plan complete
updatePartyPhase(SPEC_PATH, 2, {
  completedAt: new Date().toISOString(),
  status: 'completed',
  artifacts: [SPEC_PATH],
  userApproval: { approved: true, timestamp: new Date().toISOString() }
})
```

### Phase 4: BUILD

```typescript
// 1. Update state
updatePartyPhase(SPEC_PATH, 3, {
  startedAt: new Date().toISOString(),
  status: 'in-progress'
})

// 2. Parse tasks from the generated spec
// Same parsing logic as /build Phase 1
const specContent = Read({ file_path: SPEC_PATH })
const parsedTasks = parseTasksFromSpec(specContent)

// 3. Create TaskList
for (const task of parsedTasks) {
  const result = TaskCreate({
    subject: task.subject,
    description: task.description,
    activeForm: task.activeForm
  })
  task.id = result.taskId
}

// 4. Set dependencies
for (const task of parsedTasks) {
  if (task.dependencies && task.dependencies.length > 0) {
    TaskUpdate({
      taskId: task.id,
      addBlockedBy: task.dependencies.map(dep => findTaskId(dep))
    })
  }
}

// 5. Save state with tasks
state.build.totalTasks = parsedTasks.length
state.tasks = parsedTasks.map(t => ({
  id: t.id,
  subject: t.subject,
  description: t.description,
  status: 'pending',
  activeForm: t.activeForm,
  blockedBy: t.blockedBy || [],
  agentType: t.agentType || 'general-purpose',
  agentId: null,
  teammateName: null
}))
writeStateFile(SPEC_PATH, state)

// 6. Message agents with build instructions
for (const agent of roster) {
  SendMessage({
    type: "message",
    recipient: agent.name,
    content: `## Phase: BUILD

The implementation plan is ready. Check the shared TaskList for available tasks.

Instructions:
1. Call TaskList() to see all tasks
2. Claim tasks matching your expertise:
   TaskUpdate({ taskId: "<id>", owner: "${agent.name}", status: "in_progress" })
3. Implement each task following existing codebase patterns
4. After completing each task:
   - TaskUpdate({ taskId: "<id>", status: "completed" })
   - SendMessage to team-lead with completion summary
5. Check TaskList for next available unblocked task
6. If all your tasks are done, message the lead

Prefer tasks in ID order (lowest first). Only claim tasks matching your role.`,
    summary: "Phase 3 BUILD instructions"
  })
}

// 7. Assign initial unblocked tasks to matching agents
const unblockedTasks = parsedTasks.filter(t => !t.blockedBy || t.blockedBy.length === 0)
for (const task of unblockedTasks) {
  const assignee = matchAgentToTask(task, roster)
  TaskUpdate({
    taskId: task.id,
    owner: assignee.name,
    status: "in_progress"
  })
  updateTaskInState(SPEC_PATH, task.id, {
    status: 'in-progress',
    teammateName: assignee.name,
    deployedAt: new Date().toISOString()
  })
}

// 8. Monitor via automatic message delivery
// Process messages as they arrive:
//
// a. TASK COMPLETION: Update state, run validation hooks, unblock dependencies
//    updateTaskInState(SPEC_PATH, taskId, {
//      status: 'completed',
//      lastOutput: messageContent.slice(0, 500)
//    })
//
// b. BLOCKERS: Assess and resolve, or escalate to user
//    SendMessage({ type: "message", recipient: agentName,
//      content: "resolution", summary: "Blocker resolved" })
//
// c. STALLED AGENTS (5 min no message): Send check-in
//    SendMessage({ type: "message", recipient: agentName,
//      content: "Status update?", summary: "Check-in" })
//
// d. ALL TASKS COMPLETE: Move to Phase 5

// 9. When all tasks complete
updatePartyPhase(SPEC_PATH, 3, {
  completedAt: new Date().toISOString(),
  status: 'completed'
})
```

### Phase 5: VALIDATE

```typescript
// 1. Update state
updatePartyPhase(SPEC_PATH, 4, {
  startedAt: new Date().toISOString(),
  status: 'in-progress'
})

// 2. Message validation agents
SendMessage({
  type: "message",
  recipient: "qa",
  content: `## Phase: VALIDATE

Run the validation commands from the spec at ${SPEC_PATH}.
Verify all acceptance criteria are met.
Run the full test suite.
Send results to the lead.`,
  summary: "Phase 4 VALIDATE - run tests"
})

SendMessage({
  type: "message",
  recipient: "docs",
  content: `## Phase: VALIDATE

Review and finalize all documentation.
Verify code examples work.
Check documentation completeness.
Send report to the lead.`,
  summary: "Phase 4 VALIDATE - review docs"
})

SendMessage({
  type: "message",
  recipient: "devops",
  content: `## Phase: VALIDATE

Check deployment readiness:
- Verify configuration is correct
- Check CI/CD pipeline compatibility
- Validate environment setup
Send report to the lead.`,
  summary: "Phase 4 VALIDATE - check deployment"
})

// 3. Collect validation results from QA, Docs, DevOps

// 4. Compile completion report
const report = {
  topic: TOPIC,
  phases: state.party.phases.map(p => ({
    name: p.name,
    duration: calculateDuration(p.startedAt, p.completedAt),
    status: p.status
  })),
  tasksCompleted: `${completedCount}/${state.build.totalTasks}`,
  validationResults: qaResults,
  documentationStatus: docsResults,
  deploymentReadiness: devopsResults,
  filesModified: getModifiedFiles(),
  keyDecisions: state.party.context.keyDecisions
}

// 5. Present to user
console.log("=== PARTY MODE COMPLETE ===")
console.log(formatReport(report))

// 6. User checkpoint
const finalAction = await AskUserQuestion({
  question: "Party complete! What would you like to do?",
  options: [
    { label: "Accept and finish", description: "Close the team and save results" },
    { label: "Re-run validation", description: "Run validation again" },
    { label: "Fix issues", description: "Send agents back to fix specific problems" },
    { label: "Run /compound", description: "Document learnings from this build" }
  ]
})

if (finalAction === "Re-run validation") {
  // Loop back to step 2
}
if (finalAction === "Fix issues") {
  // Get user input on what to fix, message relevant agents, loop to build
}
if (finalAction === "Run /compound") {
  // Invoke /compound skill after cleanup
}

// 7. Mark validate complete
updatePartyPhase(SPEC_PATH, 4, {
  completedAt: new Date().toISOString(),
  status: 'completed'
})
```

### Phase 6: Cleanup

```typescript
// 1. Send shutdown requests to all 8 teammates
for (const agent of roster) {
  SendMessage({
    type: "shutdown_request",
    recipient: agent.name,
    content: "Party complete. Shutting down."
  })
}

// 2. Wait briefly for shutdown acknowledgments (30s timeout)
// Teammates respond with shutdown_response { approve: true }

// 3. Clean up team resources
TeamDelete()

// 4. Final state save
state.build.completedAt = new Date().toISOString()
state.build.lastUpdated = new Date().toISOString()
writeStateFile(SPEC_PATH, state)
```

### Resume Logic

When resuming from a previous party session (via Phase 0 or /continue-spec):

```typescript
function resumeParty(existingState) {
  const phase = getPartyPhase(existingState)
  const SPEC_PATH = existingState.build.specPath
  const TEAM_NAME = existingState.build.teamName
  const TOPIC = existingState.party.topic

  // Agent Teams don't support session resumption
  // Must create fresh team
  TeamCreate({
    team_name: TEAM_NAME,
    description: `Party resume: ${TOPIC}`
  })

  // Respawn all agents with context
  for (const agent of existingState.party.roster) {
    Task({
      team_name: TEAM_NAME,
      name: agent.name,
      subagent_type: agent.agentType,
      model: agent.model,
      prompt: `You are the ${agent.role} resuming a Party Mode session.

Topic: "${TOPIC}"
Current Phase: ${['brainstorm', 'plan', 'build', 'validate'][phase - 1]}

Context from previous phases:
- Key Decisions: ${existingState.party.context.keyDecisions.join(', ') || 'None yet'}
- Architecture Choices: ${existingState.party.context.architectureChoices.join(', ') || 'None yet'}

${phase === 3 ? 'Check TaskList for remaining tasks. Claim unassigned unblocked tasks.' : 'Wait for lead instructions.'}
`
    })
  }

  // Jump to the appropriate phase
  // Phase 1-2 incomplete: restart that phase from the beginning
  // Phase 3 incomplete: resume build, filter to pending tasks only
  // Phase 4 incomplete: restart validation

  if (phase <= 2) {
    // Restart current phase
    goto Phase${phase + 1} // Phase numbering: brainstorm=Phase2, plan=Phase3
  } else if (phase === 3) {
    // Resume build phase
    goto Phase4_BUILD // Only pending/in-progress tasks remain
  } else {
    // Restart validation
    goto Phase5_VALIDATE
  }
}
```

### Helper Functions Reference

**State File Helpers** (see `scripts/state-file.js`):

```javascript
createInitialState(specPath, tasks, mode, partyOptions) // mode: "party"
updatePartyPhase(specPath, phase, updates)              // Transition phases
getPartyPhase(state)                                    // Get current phase
readStateFile(specPath)                                 // Load state
writeStateFile(specPath, state)                         // Save state
updateTaskInState(specPath, taskId, updates)             // Update task
deleteStateFile(specPath)                               // Delete state
sanitizeSpecName(specPath)                              // Sanitize path
calculateChecksum(specPath)                             // SHA256 checksum
```

## Party Roster

| Name | Role | Agent Type | Color | Primary Phases |
|------|------|-----------|-------|----------------|
| pm | Product Manager | party-pm-agent | blue | Brainstorm, Plan |
| architect | System Architect | party-architect-agent | purple | Brainstorm, Plan, Build |
| backend | Backend Developer | party-backend-agent | green | Plan, Build |
| frontend | Frontend Developer | party-frontend-agent | yellow | Plan, Build |
| qa | QA Engineer | party-qa-agent | red | Plan, Build, Validate |
| ux | UX Designer | party-ux-agent | cyan | Brainstorm, Plan |
| docs | Tech Writer | party-docs-agent | orange | Build, Validate |
| devops | DevOps Engineer | party-devops-agent | orange | Build, Validate |

## Completion Report

```
Party Mode Complete!

Topic: <TOPIC>
Duration: <total time>

Phase Summary:
| Phase | Status | Duration |
|-------|--------|----------|
| 1. Brainstorm | ✅ | Xm Ys |
| 2. Plan | ✅ | Xm Ys |
| 3. Build | ✅ | Xm Ys |
| 4. Validate | ✅ | Xm Ys |

Tasks Completed: N/N
Validation: All passed / X failures

Teammates:
- pm (Product Manager): brainstorm + plan + validate
- architect (System Architect): brainstorm + plan + build
- backend (Backend Developer): plan + build
- frontend (Frontend Developer): plan + build
- qa (QA Engineer): plan + build + validate
- ux (UX Designer): brainstorm + plan
- docs (Tech Writer): build + validate
- devops (DevOps Engineer): build + validate

Key Decisions:
- [Decision 1]
- [Decision 2]

Files Modified:
- [list of files]

State Saved: .claude/specs/<topic>/state.json
Spec Produced: specs/party-<topic>.md

Document Learnings:
Run /compound to capture learnings from this build:
  /compound specs/party-<topic>.md
```

## Examples

```bash
# Basic usage - start from an idea
/party "Build user authentication with OAuth"

# Short topic
/party "Add dark mode support"

# Complex feature
/party "Create a real-time collaborative editing system with conflict resolution"

# Resume from previous session (auto-detected)
/party "Build user authentication with OAuth"
# > Previous party session found. Resume from Phase 3?
```

## Tips

1. **Be specific with the topic** - More detail helps agents research more effectively
2. **Review the brainstorm** - Phase 1 sets the direction; take time to review before proceeding
3. **Edit the plan** - The spec produced in Phase 2 can be manually edited before building
4. **Cost awareness** - 8 agents = 8x token usage. Use for substantial features, not small fixes
5. **File conflicts** - The plan phase assigns files to specific agents to avoid conflicts
6. **Checkpoints matter** - Use Refine/Adjust options to steer the team if direction is off
7. **Resume works** - If interrupted, `/party` or `/continue-spec` will detect previous state
