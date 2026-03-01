---
name: agents
description: List all available team members and their roles. Use to understand who can execute tasks.
argument-hint:
model: opus
allowed-tools: Glob, Grep, Read
---

# Agents

List all available team members (agents) and their roles. Agents are auto-discovered from the plugin's `agents/` directory.

## Variables

- `GENERAL_PURPOSE_AGENT`: `general-purpose`

## Instructions

1. **Find Agent Files** - Use `Glob` to find all agent `.md` files in the `agents/` directory
2. **Read Agent Definitions** - Read each agent file to extract:
   - Name (from frontmatter or title)
   - Description
   - Role/Focus
   - Agent Type
3. **Display List** - Format as a readable list

## Output Format

```
Available Team Members

General Purpose:
- general-purpose - Default agent for general tasks

Specialized Agents:
- <agent-name> - <description>
  Role: <role>
  Type: <agent-type>

- <agent-name> - <description>
  Role: <role>
  Type: <agent-type>
```

## Workflow

1. Use `Glob` to find `agents/*.md` files
2. Use `Read` to read each file
3. Extract name, description, role from each file
4. Format and display

## Examples

```bash
/agents
```

## Notes

If no specialized agents are defined, the system will use `general-purpose` for all tasks.
