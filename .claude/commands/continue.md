---
name: continue
description: Resume work on a task using the same agent context. Use when adding work to an in-progress task.
argument-hint: [agent-id] [additional-instructions]
model: opus
allowed-tools: Task, TaskOutput, TaskUpdate
---

# Continue

Resume an agent with additional instructions, preserving the full context from previous work.

## Variables

- `AGENT_ID`: $1 - The ID of the agent to resume (from previous Task deployment)
- `ADDITIONAL_INSTRUCTIONS`: $2 - New instructions for the agent

## Instructions

### When to Use

Use `continue` when:
- You need to add more work to a task that an agent is already working on
- An agent completed initial work and you need follow-up changes
- You want to refine or extend what an agent has already built

### How It Works

The `continue` command uses the `resume` parameter of the `Task` tool, which preserves the agent's full context window including:
- Previous code written
- Conversations about the code
- Decisions made
- Patterns established

This is MUCH more efficient than starting a new agent, as the resume context maintains all the working knowledge.

## Workflow

1. **Verify Agent Exists** - Check that `AGENT_ID` is valid
2. **Get Agent Status** - Use `TaskOutput` to see current state
3. **Resume with New Instructions** - Deploy agent with `resume: AGENT_ID`

## Usage

```typescript
// Resume the agent with new instructions
Task({
  description: "<Brief description of additional work>",
  prompt: `<Additional instructions for the agent>

Based on your previous work, please:
<additional instructions>

Continue from where you left off.`,

  subagent_type: "<same agent type as original>",
  resume: AGENT_ID  // Critical - preserves context
})
```

## Finding Agent IDs

Agent IDs are returned when you first deploy an agent:

```typescript
// Initial deployment
Task({
  description: "Build user service",
  prompt: "Create user service...",
  subagent_type: "general-purpose",
  run_in_background: true
})
// Returns: agentId: "abc123"

// Use this ID later to continue
/continue abc123 "Add input validation to the endpoints"
```

You can also find running agent IDs with `/status`.

## Examples

```bash
# Continue an agent with additional work
/continue abc123 "Add error handling to all endpoints"

# Continue with refinement
/continue abc123 "Refactor the database layer to use transactions"

# Continue with follow-up feature
/continue abc123 "Add pagination to the user list endpoint"
```

## Report

After resuming the agent, provide:

```
🔄 Resumed Agent

Agent ID: <agent-id>
Previous Work: <brief summary>
New Instructions: <new instructions>
Status: <running|waiting>
```

## Notes

- Always use the same `subagent_type` as the original deployment
- The agent will have full context of previous work
- Don't use `continue` for completely unrelated tasks - start a new agent instead
