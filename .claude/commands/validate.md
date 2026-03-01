---
name: validate
description: Run validation commands and check acceptance criteria for a completed plan. Use after build completes.
argument-hint: [path-to-plan]
model: opus
allowed-tools: Task, TaskOutput, Bash, Read, Edit, Write
---

# Validate

Run validation commands and verify acceptance criteria for a completed implementation plan.

## Variables

- `PATH_TO_PLAN`: $1 - Path to the plan file to validate against

## Instructions

### Prerequisites

- If no `PATH_TO_PLAN` is provided, STOP and ask the user to provide it
- The build should be complete before running validation

### Validation Process

1. **Read the Plan** - Read `PATH_TO_PLAN` to extract:
   - Validation Commands from "Validation Commands" section
   - Acceptance Criteria from "Acceptance Criteria" section

2. **Run Validation Commands** - Execute each command in sequence:
   ```bash
   # For each validation command:
   <command>
   ```

3. **Check Results** - Verify each command succeeded (exit code 0)

4. **Verify Acceptance Criteria** - For each criterion:
   - Functional Requirements (F1, F2, etc.) - Verify feature works
   - Non-Functional Requirements (NF1, NF2, etc.) - Measure performance
   - Quality Gates - Check tests, linting, etc.

5. **Report Results** - Provide detailed validation report

## Output Format

```
🔍 Validation Report

Plan: specs/<plan-name>.md

Validation Commands:
✅ <command 1>
✅ <command 2>
❌ <command 3> - <error message>

Acceptance Criteria:
Functional Requirements:
✅ F1: <requirement>
✅ F2: <requirement>
⏳ F3: <requirement> (requires manual verification)

Non-Functional Requirements:
✅ NF1: <requirement> (measured: <value>)
❌ NF2: <requirement> (measured: <value>, target: <target>)

Quality Gates:
✅ Unit tests pass
✅ Integration tests pass
✅ No TypeScript errors
❌ Linting issues found

Overall Status: <PASSED|FAILED|PARTIAL>

Issues:
- <list of issues found>
```

## Workflow

1. Read the plan file
2. Extract validation commands
3. Extract acceptance criteria
4. Run each validation command
5. Check each acceptance criterion
6. Compile results into report

## Error Handling

If a validation command fails:
1. Capture the error output
2. Determine severity (critical vs warning)
3. Continue with remaining validations
4. Report all issues at the end

## Manual Verification

Some criteria may require manual verification (e.g., "UI looks good", "User can understand flow").

Mark these with ⏳ and provide instructions for manual verification:

```
⏳ F3: User can navigate intuitively
   Manual verification required:
   - Open the application
   - Try to complete the user flow
   - Confirm navigation is intuitive
```

## Examples

```bash
# Validate a completed build
/validate specs/conversational-ui-revamp.md

# Validate with detailed output
/validate specs/conversational-ui-revamp.md --verbose
```

## Notes

- Some validations may require the application to be running
- Performance tests may require specific load conditions
- Security scans may require additional tools
