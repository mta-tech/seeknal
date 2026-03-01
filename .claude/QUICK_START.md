# Quick Start Guide

This guide will help you get started with IndyDevDan's planning and team execution workflow.

## Installation

1. **Install Node.js 18+**
   ```bash
   node --version
   ```

2. **Install Claude Code**
   ```bash
   npm install -g @anthropic-ai/claude-code
   ```

3. **Install Claude Code Max ($200/month)**
   - Visit https://code.anthropic.com
   - Subscribe to Claude Code Max for Opus 4 access

4. **Clone this repository**
   ```bash
   git clone https://github.com/yourusername/analyse-yt-cc-plan-w-team.git
   cd analyse-yt-cc-plan-w-team
   ```

## Setup

1. **Copy agents and skills to your project**
   ```bash
   cp -r .claude /path/to/your/project/
   ```

2. **Update AGENTS.md**
   - Edit `.claude/AGENTS.md` to match your project

3. **Test the setup**
   ```bash
   cd /path/to/your/project
   claude
   ```

## Basic Usage

### Planning a Feature

```bash
# Enter plan mode
/plan "Add user authentication"

# Or plan with team
/plan_w_team "Add user authentication with OAuth"
```

### Implementing the Plan

```bash
# Start implementation
/work

# Create tasks manually
TaskCreate "Design database schema"
TaskUpdate "Design database schema" --status in_progress
```

### Running Parallel Agents

Open multiple terminals:

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

### Reviewing Changes

```bash
# Run code review
/review

# Architecture review
/review:architecture

# Security review
/review:security
```

### Compounding Learnings

```bash
# Summarize learnings
/compound "OAuth authentication implementation"
```

## Workflow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                     Start Feature Idea                       │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    Plan Mode (/plan)                         │
│  - Research best practices                                   │
│  - Analyze codebase                                          │
│  - Create detailed plan                                      │
│  - Save to .claude/plan-*.md                                 │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                     Work Mode (/work)                        │
│  - Create tasks with TaskCreate                              │
│  - Execute tasks in parallel                                 │
│  - Update status with TaskUpdate                             │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    Review Mode (/review)                     │
│  - Architecture review                                       │
│  - Code quality review                                       │
│  - Security review                                           │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   Compound Mode (/compound)                  │
│  - Document what worked                                      │
│  - Document what didn't work                                 │
│  - Save to .claude/docs/                                     │
│  - Feed back into Plan                                       │
└────────────────────────┬────────────────────────────────────┘
                         │
                         └───────────────────┐
                                             │
                        (The Compounding Effect)
                                             │
                                             ▼
                              ┌────────────────────────┐
                              │    Next Feature is     │
                              │    Faster & Better     │
                              └────────────────────────┘
```

## Key Commands Summary

| Command | Purpose |
|---------|---------|
| `/plan` | Enter plan mode |
| `/plan_w_team` | Plan with agents |
| `/work` | Implement plan |
| `/review` | Review changes |
| `/compound` | Summarize learnings |
| `TaskCreate` | Create task |
| `TaskUpdate` | Update task |
| `TaskList` | List tasks |
| `TaskGet` | Get task |

## References

- [Claude Code Docs - Common Workflows](https://code.claude.com/docs/en/common-workflows)
- [Claude Sub-Agents Workflow](https://www.youtube.com/watch?v=-zzbkh9B-5Q)
- [Compound Engineering Article](https://www.thisisuncharted.co/p/ai-agents-100x-engineers-every)
