---
name: workflow:compound
description: Summarize learnings from tasks to inform future planning. Use after completing a feature.
allowed-tools: Task, TaskOutput, Bash, Glob, Grep, Read, Edit, Write, NotebookEdit, WebFetch, TodoWrite, WebSearch, AskUserQuestion, Skill
model: opus
context: fork
agent: general-purpose
user-invocable: true
---

# Compound Mode Skill

## Purpose

Summarize learnings from completed tasks to inform future planning.

## Variables

DOCS_DIR: `.claude/docs/`
TOPIC: The topic or feature completed

## Instructions

When invoked, summarize learnings:

1. **What Worked:**
   - Successful approaches
   - Effective patterns
   - Useful tools/commands
   - Time-saving techniques

2. **What Didn't Work:**
   - Failed approaches
   - Common mistakes
   - Bottlenecks encountered
   - Tools/commands to avoid

3. **Lessons Learned:**
   - Architecture insights
   - Framework patterns
   - Best practices discovered
   - Team workflow improvements

4. **Future Recommendations:**
   - How to approach similar tasks
   - Tools to use
   - Patterns to follow
   - Pitfalls to avoid

## Workflow

```
1. Review completed task
2. Extract key learnings
3. Document what worked
4. Document what didn't work
5. Save to .claude/docs/
6. Update AGENTS.md if needed
```

## Documentation Format

Save as `.claude/docs/{topic}.md`:

```yaml
---
title: Learnings from {topic}
date: {current_date}
tags: [tags]
category: {category}
module: {module}
symptoms: [symptoms]
---

## What Worked

## What Didn't Work

## Lessons Learned

## Recommendations for Future
```

## Report

After compounding, provide:
- Summary of key learnings
- Documentation file created
- How this informs future planning
- Related patterns discovered

## Example

```bash
/compound "OAuth authentication implementation"
```

Creates `.claude/docs/oauth-authentication.md` with learnings.

## The Compounding Effect

The key insight is that **Compound** feeds back into **Plan**:

```
Plan -> Work -> Review -> Compound -> Plan -> ...
        ^                           |
        |---------------------------|
```

Each task makes the next one faster and more accurate.
