---
adr_id: ADR-002
date: 2026-02-11
status: accepted
title: Warning-based parameter collision handling
---

# ADR-002: Warning-based parameter collision handling

## Context

User-provided parameters could silently override reserved system values (`run_id`, `run_date`, `project_id`, `workspace_path`). This created a confusing situation where users could accidentally override critical system parameters, leading to difficult-to-debug issues.

The problem occurred in the parameter resolution logic where user parameters from CLI, YAML, and environment variables were merged with system-generated values.

## Decision

Emit warnings instead of errors when parameter collisions are detected, while ensuring system values always take precedence over user parameters.

**Rationale:**
- Maintains backward compatibility with existing workflows that may have colliding parameter names
- Alerts users to potential issues without breaking their current workflows
- Allows users to identify and fix parameter naming issues incrementally
- System values taking precedence ensures predictable behavior and prevents accidental overrides
- Warnings are non-disruptive but provide clear guidance for fixing the issue

**Consequences:**
- [ ] System values always take precedence over user parameters
- [ ] Warnings are emitted when collisions are detected using Python's `warnings` module
- [ ] Existing workflows with colliding names continue to work but with warnings
- [ ] Users can identify and fix parameter naming issues incrementally
- [ ] Clear warning messages guide users to use different parameter names

## Alternatives Considered

- **Error on collision**: Breaking change for existing workflows - would require immediate fixes
- **Silent override**: Confusing behavior, difficult to debug when system values are unexpectedly replaced
- **Rename system parameters**: Large API change affecting multiple components

## Related

- Spec: specs/fix-integration-security-issues.md
- Files: src/seeknal/workflow/parameters/resolver.py
- Related ADRs: ADR-004 (Parameter name validation with regex)
