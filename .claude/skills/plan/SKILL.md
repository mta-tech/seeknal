---
name: workflow:plan
description: Enter planning mode to research and create detailed implementation plans. Use when starting a new feature or task.
allowed-tools: Task, TaskOutput, Bash, Glob, Grep, Read, Edit, Write, NotebookEdit, WebFetch, TodoWrite, WebSearch, AskUserQuestion, Skill, EnterPlanMode, ExitPlanMode
model: opus
context: fork
agent: general-purpose
user-invocable: true
---

# Planning Mode Skill

## Purpose

Decouple implementation from research by creating detailed plans before coding.

## Variables

PLAN_FILE: `.claude/plan-{feature_name}.md`
FEATURE_NAME: The name of the feature being planned

## Instructions

When invoked, enter plan mode and create a detailed implementation plan:

1. **Research Phase:**
   - Use WebSearch to find best practices
   - Analyze existing codebase patterns
   - Check for similar implementations
   - Identify dependencies and constraints

2. **Planning Phase:**
   - Break down feature into tasks
   - Identify task dependencies
   - Create implementation steps
   - Save plan to `PLAN_FILE`

3. **Documentation:**
   - Document architecture decisions
   - List files to be created/modified
   - Define acceptance criteria
   - Note potential risks

## Workflow

```
1. Enter plan mode (Shift+Tab or /plan)
2. Research best practices
3. Analyze codebase
4. Create detailed plan
5. Save to .claude/plan-*.md
6. Exit plan mode
7. Begin implementation
```

## Report

After planning, provide:
- Summary of research findings
- Detailed task breakdown
- Dependency graph
- Risk assessment
- Estimated effort

## Example

```bash
/plan "Add OAuth authentication"
```

Creates `.claude/plan-oauth-authentication.md` with:
- Research on OAuth providers
- Implementation steps
- Required dependencies
- Task list with dependencies
