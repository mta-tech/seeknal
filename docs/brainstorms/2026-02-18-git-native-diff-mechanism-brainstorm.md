# Git-Native Diff Mechanism for Seeknal

**Date:** 2026-02-18
**Status:** Brainstorm
**Author:** User + Claude

## What We're Building

A git-like diff experience for Seeknal pipeline files. After a user runs `apply` and then modifies a YAML file (e.g., changes schema in `sources/orders.yml`), Seeknal should detect and display changes with a familiar unified diff format - just like `git diff`.

Two entry points:
1. **`seeknal diff`** - Dedicated command for quick change inspection
2. **Enhanced `seeknal plan`** - Existing plan command shows richer diffs alongside execution plan

## Why This Approach

Seeknal projects are version-controlled with git. Rather than building a parallel versioning system (snapshots, internal tracking), we lean on git as the primary baseline for "what was applied." This is:

- **Natural** - Users already think in git terms (commit, diff, status)
- **Zero overhead** - No duplicate file storage for the common case
- **Lossless** - Git stores exact YAML content, so diffs are precise
- **Familiar UX** - Unified diff format (`-` removed, `+` added) is universally understood

A lightweight fallback (`applied_state.json`) covers non-git and uncommitted scenarios.

## Key Decisions

### 1. Diff Display Format: Unified YAML Diff (git-style)

```
seeknal diff sources/orders

--- applied (git:HEAD)
+++ current (seeknal/sources/orders.yml)

  columns:
    order_id: integer
-   order_date: string
+   order_date: date
    customer_id: integer
+   discount_amount: float
-   old_field: integer

  Category: BREAKING (downstream rebuild required)
  Impact: 3 downstream nodes affected
    -> transform.clean_orders
    -> transform.order_enriched
    -> feature_group.customer_features
```

Raw YAML diff on top, semantic annotations (category + impact) below.

### 2. Baseline Resolution: Layered Priority

When determining the "before" state for diffing:

| Priority | Source | When Used |
|----------|--------|-----------|
| 1 | `git show HEAD:<path>` | File is committed in git repo |
| 2 | `target/applied_state.json` | No git, or file not yet committed |
| 3 | `target/manifest.json` | No snapshot available (structured diff, lossy) |
| 4 | Error | No baseline at all - "Run `seeknal apply` first" |

### 3. Applied State Snapshot (Fallback)

At `apply` time, store YAML content in `target/applied_state.json`:

```json
{
  "schema_version": "1.0",
  "nodes": {
    "source.orders": {
      "file_path": "seeknal/sources/orders.yml",
      "content_hash": "sha256:abc123...",
      "yaml_content": "kind: source\nname: orders\n...",
      "applied_at": "2026-02-18T14:30:00Z"
    }
  }
}
```

This is updated incrementally - only the applied node gets updated, not the full file.

### 4. `seeknal diff` Command Design

```
# Diff all changed files
seeknal diff

# Diff specific node
seeknal diff sources/orders

# Diff specific node type
seeknal diff --type sources
seeknal diff --type transforms

# Show only summary (like git diff --stat)
seeknal diff --stat
```

**Output for `seeknal diff` (no args):**
```
Changes since last apply:

  Modified:
    seeknal/sources/orders.yml     [BREAKING] +2 -1 columns
    seeknal/transforms/clean.yml   [NON_BREAKING] SQL changed

  Untracked (new files):
    seeknal/sources/products.yml

  2 modified, 1 new, 0 deleted
```

**Output for `seeknal diff --stat`:**
```
  sources/orders.yml      | 3 +-
  transforms/clean.yml    | 8 ++++--
  2 files changed, 5 insertions, 2 deletions
```

### 5. Enhanced `seeknal plan` Integration

The existing `plan` command already detects changes. Enhancement:
- Show the unified YAML diff inline for each changed node (collapsible in rich terminals)
- Keep the existing category/impact analysis
- Add a hint: "Run `seeknal diff <node>` for detailed diff"

### 6. Git Integration Details

**Detecting git availability:**
```python
def _get_git_baseline(file_path: Path) -> Optional[str]:
    """Get the committed version of a file, or None."""
    try:
        result = subprocess.run(
            ["git", "show", f"HEAD:{file_path}"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            return result.stdout
        return None  # File not tracked or no commits
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None  # Git not installed or hung
```

**Diff generation:**
Use Python's `difflib.unified_diff` for YAML content comparison. This produces standard unified diff format without requiring git for the actual diff computation.

### 7. Semantic Annotations

After generating the raw diff, layer on Seeknal-specific intelligence using existing `dag/diff.py`:
- **Change category**: BREAKING / NON_BREAKING / METADATA (from existing `ChangeCategory`)
- **Downstream impact**: BFS traversal of DAG from changed node (from existing `ManifestDiff`)
- **Column-level summary**: "+2 columns, -1 column, ~1 type change"

## Scope

### In Scope
- `seeknal diff` command (new)
- `applied_state.json` generation during `apply` (modify existing)
- Enhanced diff display in `seeknal plan` (modify existing)
- Semantic annotations (BREAKING/NON_BREAKING/METADATA)
- Downstream impact display

### Out of Scope (for now)
- Interactive diff viewer / TUI
- Diff between two specific versions/commits
- `seeknal stash` or `seeknal reset` (undo changes)
- Diff for Python pipeline files (`.py`) - YAML only for v1

## Open Questions

None - all resolved during brainstorm.

## Implementation Notes

### Existing Code to Leverage
- `src/seeknal/dag/diff.py` - `ManifestDiff`, `ChangeCategory`, downstream BFS
- `src/seeknal/workflow/state.py` - `calculate_node_hash()` for change detection
- `src/seeknal/workflow/apply.py` - Hook point for writing `applied_state.json`
- `src/seeknal/cli/main.py` - Plan command to enhance, diff command to add

### File Changes Estimate
- `src/seeknal/cli/main.py` - Add `diff` command (~100 lines)
- `src/seeknal/workflow/apply.py` - Add snapshot writing (~30 lines)
- `src/seeknal/dag/diff.py` - Add YAML diff formatting (~80 lines)
- New: `src/seeknal/workflow/diff_engine.py` - Baseline resolution + diff generation (~120 lines)
- Tests: `tests/cli/test_diff_command.py`, `tests/workflow/test_diff_engine.py`
