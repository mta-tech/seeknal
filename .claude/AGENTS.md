# AGENTS.md

This file configures how agents should behave in this project.

## Project Overview

This project implements IndyDevDan's planning and team execution workflow using Claude Code, combined with the Compound Engineering methodology from Every.

## Agent Philosophy

> "One engineer with 10 agents ships faster than a room full of devs debating variable names and burrito orders."

The key is **orchestration over implementation**. You become a conductor, not a typist.

## Workflow Commands

Use these commands to implement features:

### Planning Phase

- `/workflows:plan` or `/plan` - Enter plan mode to research and create detailed implementation plans
- `/workflows:plan_w_team` - Plan with specialized agents

### Execution Phase

- `/workflows:work` or `/work` - Implement the plan
- `TaskCreate` - Create a new task
- `TaskUpdate` - Update task status (pending/in_progress/completed)
- `TaskList` - List all tasks
- `TaskGet` - Get task details

### Review Phase

- `/workflows:review` or `/review` - Run code review
- `/workflows:review:architecture` - Architecture review
- `/workflows:review:code-quality` - Code quality review
- `/workflows:review:security` - Security review

### Compound Phase

- `/workflows:compound` or `/compound` - Summarize learnings
- `/workflows:docs:add` - Add to documentation

## Available Agents

### Context Gatherer (`context-gatherer`)
Analyzes codebase patterns and finds relevant files.

### Task Planner (`task-planner`)
Creates detailed implementation plans and breaks down work.

### Frontend Agent (`frontend-agent`)
Builds UI components and views.

### Backend Agent (`backend-agent`)
Creates APIs and business logic.

### Test Agent (`test-agent`)
Writes test specifications and test cases.

### Docs Agent (`docs-agent`)
Updates documentation and guides.

### Code Reviewer (`code-reviewer`)
Reviews changes against best practices.

## Parallel Execution Pattern

```bash
# Terminal 1 - Frontend
claude --agent frontend

# Terminal 2 - Backend
claude --agent backend

# Terminal 3 - Tests
claude --agent test-writer

# Terminal 4 - Docs
claude --agent docs
```

## Best Practices

1. **Always plan first** - Never skip Plan Mode
2. **Fix problems at the lowest value stage** - Review architecture early
3. **Build prompts that build prompts** - Create reusable templates
4. **Run agents in parallel** - Maximize throughput
5. **Review at the right level** - Check architecture, not syntax

## Documentation

Learnings are documented in `.claude/docs/` for future reference.

## References

- [Claude Sub-Agents Workflow](https://www.youtube.com/watch?v=-zzbkh9B-5Q)
- [Compound Engineering Article](https://www.thisisuncharted.co/p/ai-agents-100x-engineers-every)
- [Every's Compound Engineering](https://lethain.com/everyinc-compound-engineering/)
- [Claude Code Docs - Common Workflows](https://code.claude.com/docs/en/common-workflows)
