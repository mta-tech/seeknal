---
name: ralph-stop
description: Cancel an active ralph loop gracefully
argument-hint:
model: opus
allowed-tools: Bash, Glob, Grep, Read, Write
---

# Ralph Stop

Cancel an active ralph loop. Scans for active ralph state, marks it as aborted, and creates an abort sentinel file for the Stop hook.

## Instructions

1. **Find active ralph loops:**
   Scan `.claude/specs/*/state.json` for state files containing an active ralph loop (`ralph.active === true`).

2. **If active ralph loop found:**
   - Read the state file
   - Set `ralph.active = false`
   - Set `ralph.status = 'aborted'`
   - Set `ralph.finalResult = 'aborted-by-user'`
   - Write the updated state file
   - Create `.claude/ralph-abort` sentinel file (empty file — the Stop hook checks for its existence)
   - Display the cancellation report

3. **If no active ralph loop found:**
   - Print: "No active ralph loop found."
   - Check if any `.claude/specs/*/state.json` files exist with `ralph.status === 'aborted'` or `ralph.status === 'failed'`
   - If found, print: "Found previously stopped ralph loop for <spec-name>. Use /continue-spec to resume."

## Report

### When cancelled:
```
Ralph loop cancelled.

Spec: specs/<name>.md
Iteration: N/M
Mode: <build-validate|self-heal|full-lifecycle>
Tasks completed: X/Y

The ralph loop has been stopped. Options:
- /continue-spec specs/<name>.md — Resume without ralph (manual mode)
- /continue-spec specs/<name>.md — Resume with ralph (will ask for options)
- /build specs/<name>.md --ralph — Start a fresh ralph loop
```

### When no active loop:
```
No active ralph loop found.
```

## Examples

```bash
# Cancel the active ralph loop
/ralph-stop
```
