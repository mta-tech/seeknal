---
name: workflow:work
description: Implement the plan created in planning mode. Use after planning is complete.
allowed-tools: Task, TaskOutput, Bash, Glob, Grep, Read, Edit, Write, NotebookEdit, WebFetch, TodoWrite, WebSearch, AskUserQuestion, Skill, ExitPlanMode
model: opus
context: fork
agent: general-purpose
user-invocable: true
---

# Work Mode Skill

## Purpose

Implement the detailed plan created during planning mode.

## Variables

PLAN_FILE: The plan file to implement (e.g., `.claude/plan-oauth-authentication.md`)

## Instructions

When invoked, implement the plan:

1. **Read the Plan:**
   - Load `PLAN_FILE`
   - Understand the task breakdown
   - Review dependencies
   - Check acceptance criteria

2. **Create Tasks:**
   - Use `TaskCreate` for each task
   - Set up dependencies (blocks/blockedBy)
   - Assign descriptive names
   - Add detailed descriptions

3. **Execute Tasks:**
   - Mark task as `in_progress` before starting
   - Implement the changes
   - Mark task as `completed` when done
   - Move to next task

## Workflow

```
1. Read plan file
2. Create tasks with TaskCreate
3. Execute tasks in dependency order
4. Update task status
5. Handle dependencies
6. Complete all tasks
```

## Task Management

Use these commands:
- `TaskCreate` - Create new task
- `TaskUpdate` - Update status to in_progress/completed
- `TaskList` - Show all tasks
- `TaskGet` - Get task details

## Dependencies

Tasks can depend on each other:
- `blocks` - Tasks waiting for this one
- `blockedBy` - Tasks that must complete first

## Report

After implementation, provide:
- List of completed tasks
- Files created/modified
- Tests added
- Documentation updated
