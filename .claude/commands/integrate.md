---
description: Review, test, and integrate completed auto-claude worktree branches into main
allowed-tools: Bash, Read, Glob, Grep, Task, TodoWrite, AskUserQuestion
---

# Integrate Worktrees

Automate the review, testing, and integration of completed auto-claude worktree branches into main via an integration branch.

## Variables

FLAGS: $ARGUMENTS (optional: --push, --pr, --skip-review, --skip-tests, --local)

## Phase 1: Discovery

Scan `.worktrees/` for completed auto-claude work:

```bash
# List all worktrees and their status
for dir in .worktrees/*/; do
  if [ -f "${dir}.auto-claude-status" ]; then
    name=$(basename "$dir")
    status=$(cat "${dir}.auto-claude-status" | grep -o '"state": "[^"]*"' | cut -d'"' -f4)
    echo "${name}: ${status}"
  fi
done
```

For each worktree with `state: "complete"`:
1. Read `.auto-claude-status` to get subtask completion
2. Read spec from `.auto-claude/specs/<name>/spec.md` in the main repo
3. Run `git -C .worktrees/<name> diff main --stat` to get files changed
4. Collect: branch name, spec title, files changed count

If no complete worktrees found, report "No complete worktrees found" and exit.

## Phase 2: Review

For each complete worktree, display:

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📦 <worktree-name>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Branch: auto-claude/<worktree-name>
Status: complete (<completed>/<total> subtasks)

📋 Spec Summary:
<First 200 chars of spec overview section>

📁 Files Changed:
<git diff --stat output, limited to 10 files>

🔍 AI Code Review:
<Run code-reviewer agent unless --skip-review flag>

Include this branch? [y/n/skip-rest]
```

**AI Code Review (unless --skip-review):**
Use the Task tool with subagent_type='code-reviewer' to review the diff:
- Focus on bugs, security issues, and breaking changes
- Keep review concise (max 5 findings)

**Approval Options:**
- `y` - Include this branch in integration
- `n` - Exclude this branch
- `skip-rest` - Include all remaining complete worktrees without further review

## Phase 3: Test Execution

Unless `--skip-tests` flag is set:

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🧪 Running E2E Tests
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

For each approved worktree:
1. `cd .worktrees/<name>`
2. Activate venv if exists: `source .venv/bin/activate 2>/dev/null || true`
3. Run: `uv run pytest src/tests/ -v --tb=short -x` (stop on first failure for speed)
4. Capture result

Display:
```
[1/N] <worktree-name> ... ✅ PASS (X tests, Y.Zs)
```
or
```
[1/N] <worktree-name> ... ❌ FAIL
      └─ Skipping: <failure reason>
```

**On failure:** Mark branch as skipped, continue with remaining branches.

## Phase 4: Integration

Create integration branch and merge passing worktrees:

```bash
# Ensure we're on main and up to date
git checkout main
git pull origin main 2>/dev/null || true

# Create integration branch with timestamp
BRANCH_NAME="integrate/$(date +%Y-%m-%d-%H%M)"
git checkout -b "$BRANCH_NAME"
```

For each passing worktree:
```bash
git merge auto-claude/<worktree-name> --no-ff -m "Integrate: <spec-title>"
```

**On merge conflict:**
1. Show conflicting files: `git diff --name-only --diff-filter=U`
2. Ask user: "Merge conflict detected. Resolve manually and continue, or skip this branch? [resolve/skip]"
3. If skip: `git merge --abort` and mark branch as skipped
4. If resolve: Wait for user, then `git add -A && git commit`

After all merges, run final test suite:
```bash
uv run pytest src/tests/ -v --tb=short
```

## Phase 5: Output

Display summary:

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 Integration Summary
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Branch: <integration-branch-name>

✅ Integrated (N):
  • <worktree-1>
  • <worktree-2>

⏭️ Skipped (N):
  • <worktree-3> (test failure)
  • <worktree-4> (merge conflict)

❌ Rejected (N):
  • <worktree-5> (user rejected)

📈 Total changes: +X lines across Y files
🧪 Final test suite: X passed, Y failed
```

**Based on flags:**

- `--local` (default): Keep integration branch local, report complete
- `--push`: Push integration branch to remote
  ```bash
  git push -u origin "$BRANCH_NAME"
  ```
- `--pr`: Push and create GitHub PR
  ```bash
  git push -u origin "$BRANCH_NAME"
  gh pr create --title "Integration: N auto-claude branches" --body "<PR body with summaries>"
  ```

**PR Body Template:**
```markdown
## Integration: N auto-claude branches

### Included Changes

#### <worktree-name>: <spec-title>
<spec overview, 2-3 sentences>
- <key change 1>
- <key change 2>

### Skipped
- **<worktree-name>**: <reason>

### Test Results
All integrated changes pass E2E tests.
```

## Error Handling

| Scenario | Action |
|----------|--------|
| No complete worktrees | Exit with message |
| All tests fail | Exit with summary, no integration branch |
| Git operation fails | Stop, report error, preserve state |
| Missing spec file | Skip worktree with warning |
| User interrupts | Clean up partial state if possible |

## Flags Reference

| Flag | Effect |
|------|--------|
| `--push` | Push integration branch to remote |
| `--pr` | Push and create GitHub PR |
| `--skip-review` | Skip AI code review phase |
| `--skip-tests` | Skip E2E test phase |
| `--local` | Keep everything local (default) |
