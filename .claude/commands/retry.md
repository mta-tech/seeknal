---
name: retry
description: Retry a failed task with corrected instructions. Use when an agent fails and needs to try again.
argument-hint: [task-id] [correction-instructions]
model: opus
allowed-tools: Task, TaskOutput, TaskGet, TaskUpdate
---

# Retry

Retry a failed task with corrected instructions or approach.

## Variables

- `TASK_ID`: $1 - The ID of the task to retry
- `CORRECTION_INSTRUCTIONS`: $2 - What went wrong and how to fix it

## Instructions

### When to Use

Use `retry` when:
- An agent failed to complete a task
- The approach needs adjustment
- New information clarifies the requirements
- An error occurred that can be corrected

### How It Works

1. **Get Task Details** - Read the task description and requirements
2. **Understand the Failure** - Parse the correction instructions
3. **Reset Task Status** - Mark task as `in_progress`
4. **Deploy New Agent** - Start fresh with corrected approach

## Workflow

1. **Get Task** - Use `TaskGet({ taskId: TASK_ID })` to read task details
2. **Update Status** - Use `TaskUpdate({ taskId: TASK_ID, status: "in_progress" })`
3. **Deploy Agent** - Use `Task` tool with corrected prompt:
   ```typescript
   Task({
     description: "<Task Name> (retry)",
     prompt: `<Corrected instructions>

   Previous attempt failed because: <correction instructions>

   Please try again with this approach:
   <corrected approach>

   Original task:
   <task description>`,
     subagent_type: "<agent type>",
     run_in_background: false
   })
   ```

4. **Monitor** - Wait for completion and update status

## Error Categories

### Code Errors
- **Syntax errors** - Fix the code syntax issue
- **Type errors** - Correct type mismatches
- **Runtime errors** - Fix the runtime problem

### Approach Errors
- **Wrong pattern** - Adjust the implementation approach
- **Missing context** - Provide more information
- **Misunderstanding** - Clarify requirements

### Environment Errors
- **Missing dependencies** - Install required packages
- **Configuration issues** - Fix configuration
- **Permission problems** - Adjust permissions

## Examples

```bash
# Retry with correction
/retry 5 "The API endpoint needs to accept query parameters, not just body params"

# Retry with different approach
/retry 12 "Use PostgreSQL instead of trying to use SQLite"

# Retry with clarification
/retry 8 "The component should be server-rendered, not client-side only"
```

## Report

After retrying, provide:

```
🔄 Retrying Task

Task ID: <task-id>
Task Name: <task name>
Previous Failure: <what went wrong>
Correction: <how we're fixing it>
Status: <retrying|waiting>
```

## Notes

- Don't use `resume` for retries - start fresh with new agent
- If task fails multiple times, consider breaking it into smaller subtasks
- Document the failure pattern in the plan for future reference
