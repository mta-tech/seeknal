# Change Categorization

Seeknal automatically classifies pipeline changes into three categories to determine rebuild scope and prevent breaking downstream consumers.

## What Is Change Categorization?

Change categorization is an automatic system that:
- Analyzes differences between manifests
- Classifies changes by their impact on downstream nodes
- Determines which nodes need rebuilding
- Prevents breaking changes from silently propagating

When you modify a YAML file, Seeknal computes a diff and assigns each change to one of three categories based on its impact.

## Three Categories

### BREAKING

**Definition**: Changes that require rebuilding this node and all downstream dependents.

**Why**: Downstream nodes may reference fields, columns, or inputs that no longer exist.

**Examples**:
- Column removed or renamed
- Column data type changed
- Input dependency removed
- Node renamed (changes node ID)
- Node type changed (source → transform)
- Entity key changed (for feature groups)

**Downstream impact**: All transitive downstream nodes rebuild (via BFS traversal).

**CLI indicator**: `[BREAKING - rebuilds downstream]`

### NON_BREAKING

**Definition**: Changes that require rebuilding only this node.

**Why**: The change affects this node's logic but doesn't invalidate downstream dependencies.

**Examples**:
- Column added (downstream queries still valid)
- SQL logic changed (same output schema)
- Input dependency added
- Feature definition changed
- Transform SQL modified (preserves columns)
- Aggregation window changed

**Downstream impact**: Downstream nodes also rebuild (data dependency), but not flagged as breaking.

**CLI indicator**: `[CHANGED - rebuild this node]`

### METADATA

**Definition**: Changes that don't require any execution.

**Why**: Only documentation or tags changed - functional logic unchanged.

**Examples**:
- Description updated
- Owner changed
- Tags modified
- Audit rules added (in config)

**Downstream impact**: None. Manifest updates only.

**CLI indicator**: `[METADATA - no rebuild needed]`

## Rules Table

| Change Type | Category | Downstream Impact | Example |
|-------------|----------|-------------------|---------|
| Column removed | BREAKING | All downstream | `total_amount` deleted |
| Column renamed | BREAKING | All downstream | `total_amount` → `amount` |
| Column type changed | BREAKING | All downstream | `INTEGER` → `STRING` |
| Column added | NON_BREAKING | Rebuild only | New `discount` column |
| SQL transform changed | NON_BREAKING | Rebuild only | New WHERE clause |
| Input removed | BREAKING | All downstream | Removed `source.orders` |
| Input added | NON_BREAKING | Rebuild only | Added `source.customers` |
| Feature removed | BREAKING | All downstream | Deleted ML feature |
| Feature added | NON_BREAKING | Rebuild only | New aggregation |
| Node renamed | BREAKING | All downstream | `orders` → `sales` |
| Node type changed | BREAKING | All downstream | Transform → Model |
| Description changed | METADATA | None | Docs update |
| Owner changed | METADATA | None | Team reassignment |
| Tags changed | METADATA | None | Added `pii` tag |

## See Also

- **Concepts**: [Virtual Environments](virtual-environments.md), [Plan](glossary.md#plan), [Fingerprint](glossary.md#fingerprint)
- **Reference**: [CLI Commands](../reference/cli.md), [YAML Schema](../reference/yaml-schema.md)
- **Guides**: [Testing & Audits](../guides/testing-and-audits.md)

## How It Works

### 1. Manifest Diffing

When you run `seeknal diff` or `seeknal plan`, Seeknal:

1. Loads the old manifest (from production or environment)
2. Parses the new manifest (from current YAML files)
3. Compares node-by-node to detect changes

### 2. Change Classification

For each modified node, Seeknal inspects:

```python
# Check columns
if old_columns - new_columns:
    return ChangeCategory.BREAKING  # Column removed

# Check column types
for col in old_columns & new_columns:
    if old_type != new_type:
        return ChangeCategory.BREAKING  # Type changed

# Check inputs
if old_inputs - new_inputs:
    return ChangeCategory.BREAKING  # Input removed

# Check features
if old_features - new_features:
    return ChangeCategory.BREAKING  # Feature removed

# Check metadata fields
if only description/owner/tags changed:
    return ChangeCategory.METADATA

# Default: NON_BREAKING
return ChangeCategory.NON_BREAKING
```

### 3. Downstream Propagation

For BREAKING changes, Seeknal uses BFS to find all downstream dependents:

```
source.orders [BREAKING - column removed]
  ↓
transform.clean_orders [downstream rebuild]
  ↓
feature_group.order_features [downstream rebuild]
  ↓
model.churn_prediction [downstream rebuild]
```

All four nodes rebuild, but only `source.orders` is flagged as BREAKING.

### 4. Execution Plan

The executor:
- Skips METADATA-only nodes (manifest update only)
- Rebuilds NON_BREAKING nodes
- Rebuilds BREAKING nodes + all downstream

## Downstream Impact Calculation

### BFS Traversal

Seeknal uses breadth-first search to find all transitive downstream nodes:

```python
def _bfs_downstream(node_id: str, manifest: Manifest) -> set[str]:
    downstream = set()
    queue = deque([node_id])
    while queue:
        current = queue.popleft()
        for child in manifest.get_downstream_nodes(current):
            if child not in downstream:
                downstream.add(child)
                queue.append(child)
    return downstream
```

This ensures all transitive dependencies are rebuilt, preventing stale data.

### Propagation Rules

- **BREAKING**: This node + all downstream (recursive)
- **NON_BREAKING**: This node + downstream (recursive, but not marked breaking)
- **METADATA**: No execution, manifest only

**Example DAG**:
```
A [BREAKING]
├─ B [downstream]
│  └─ D [downstream]
└─ C [downstream]
   └─ D [downstream]
```

If A is BREAKING, B, C, and D all rebuild (D detected via both paths).

## CLI Command Reference

### `seeknal diff`

Show categorized changes without execution:

```bash
seeknal diff

# Output:
# [BREAKING - rebuilds downstream] transform.customer_metrics
#     columns: removed: total_mrr; added: total_monthly_recurring_revenue
#     Downstream impact (1 nodes):
#       -> REBUILD aggregation.churn_rate_analysis
#
# Summary:
#   1 breaking change(s) — downstream nodes will rebuild
```

### `seeknal plan <env>`

Create environment plan with change categories:

```bash
seeknal plan dev

# Output:
# Environment Plan: dev
# ============================================================
#
# Change Detection
# ============================================================
# Modified Nodes:
#   1. clean_orders
#      Category: NON_BREAKING
#      Changed fields: config
#      Details:
#        - transform: Added 'discount_pct' column
#
# Downstream Impact:
#   2. customer_enriched
#      Reason: Depends on clean_orders
#      Category: NON_BREAKING
#
# Execution Plan:
# ============================================================
#   Total nodes to rebuild: 2
#   Breaking changes: 0
#   Non-breaking changes: 2
#   Metadata-only changes: 0
```

### `seeknal run --env <name>`

Execute plan with categorized rebuilds:

```bash
seeknal run --env dev

# Output:
# Applying Environment: dev
# ============================================================
# ℹ Nodes to execute: 2
#
# Execution
# ============================================================
# 1/2: clean_orders [RUNNING]
#   SUCCESS in 0.03s
#
# 2/2: customer_enriched [RUNNING]
#   SUCCESS in 0.04s
```

## CI/CD Integration Patterns

### Pull Request Checks

```yaml
# .github/workflows/pr-check.yml
name: Pipeline PR Check

on: pull_request

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Install Seeknal
        run: pip install seeknal

      - name: Detect changes
        id: diff
        run: |
          seeknal diff > diff_output.txt
          cat diff_output.txt

      - name: Check for breaking changes
        run: |
          if grep -q "BREAKING" diff_output.txt; then
            echo "⚠️ BREAKING CHANGES DETECTED"
            echo "Review required before merge"
            exit 1
          else
            echo "✓ No breaking changes"
          fi
```

### Environment-Based Deployment

```yaml
# .github/workflows/deploy.yml
name: Deploy Pipeline

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Plan staging deployment
        run: seeknal plan staging

      - name: Apply to staging
        run: seeknal run --env staging --parallel

      - name: Run tests
        run: pytest tests/

      - name: Check for breaking changes
        id: breaking
        run: |
          if seeknal diff | grep -q "BREAKING"; then
            echo "has_breaking=true" >> $GITHUB_OUTPUT
          else
            echo "has_breaking=false" >> $GITHUB_OUTPUT
          fi

      - name: Require manual approval for breaking changes
        if: steps.breaking.outputs.has_breaking == 'true'
        uses: trstringer/manual-approval@v1
        with:
          approvers: team-leads
          issue-title: "Breaking change approval required"

      - name: Promote to production
        run: seeknal promote staging
```

### Automated Notifications

```yaml
- name: Notify breaking changes
  if: steps.breaking.outputs.has_breaking == 'true'
  uses: slackapi/slack-github-action@v1
  with:
    payload: |
      {
        "text": "⚠️ Breaking changes detected in pipeline",
        "blocks": [
          {
            "type": "section",
            "text": {
              "type": "mrkdwn",
              "text": "*Breaking Changes Detected*\nPR: ${{ github.event.pull_request.html_url }}"
            }
          }
        ]
      }
```

## Advanced Features

### Custom Classification Rules

Future extension point for domain-specific rules:

```python
# Future API (not yet implemented)
from seeknal.dag.diff import ChangeClassifier

@ChangeClassifier.register("custom_rule")
def classify_custom(old_node, new_node):
    if old_node.config.get("pii") and not new_node.config.get("pii"):
        return ChangeCategory.BREAKING  # PII flag removed
    return ChangeCategory.METADATA
```

### Change Details

The diff module provides human-readable details for each change:

```python
change.changed_details = {
    "columns": "added: discount_pct, removed: old_price",
    "config": "keys changed: features, entity"
}
```

These details appear in CLI output and can be used for notifications.

### Severity Ordering

Categories have implicit severity for max() comparison:

```python
METADATA < NON_BREAKING < BREAKING
```

If a node has multiple changes (e.g., column added + column removed), the most severe category wins:

```python
col_added = NON_BREAKING
col_removed = BREAKING
result = max(col_added, col_removed, key=_severity)  # BREAKING
```

## Comparison to Other Tools

### vs dbt state:modified

| Feature | Seeknal Change Categorization | dbt state:modified |
|---------|------------------------------|-------------------|
| Granularity | 3 categories (BREAKING/NON_BREAKING/METADATA) | Binary (modified/not) |
| Downstream impact | Automatic BFS propagation | Manual `+` selector |
| Column changes | Detected and categorized | Not detected |
| Type changes | BREAKING | Not detected |
| Metadata-only | Skips execution | Still executes |
| CI/CD integration | Built-in categories | Manual scripting |

### vs SQLMesh change categories

| Feature | Seeknal | SQLMesh |
|---------|---------|---------|
| Categories | BREAKING, NON_BREAKING, METADATA | Breaking, Non-breaking, Forward-only |
| Detection | Hash-based + schema inspection | Plan-based |
| Propagation | BFS downstream | Plan rewrite |
| Execution | Categorical rebuild | Virtual data |
| Virtual layers | No (full rebuild) | Yes (CTE views) |

## Best Practices

### Understanding BREAKING vs NON_BREAKING

**BREAKING** means:
- Downstream queries will fail (column referenced but removed)
- Schema contract violated
- Manual intervention likely needed

**NON_BREAKING** means:
- Downstream queries still valid (new columns ignored by SELECT *)
- Logic changed but interface stable
- Safe to auto-promote (with tests)

### When to Accept Breaking Changes

Breaking changes are sometimes necessary:
- Renaming columns for clarity
- Changing types for correctness
- Removing deprecated features

**Workflow**:
1. `seeknal plan dev` to see downstream impact
2. Update downstream nodes to handle change
3. Test in dev environment
4. Promote together in one deployment

### When to Avoid Breaking Changes

Use non-breaking alternatives when possible:
- Add new column instead of renaming (deprecate old)
- Add type cast logic instead of changing type
- Add new input instead of replacing

**Example**:
```yaml
# BREAKING: Column renamed
SELECT customer_id AS cust_id  # Downstream breaks

# NON_BREAKING: Add new column, keep old
SELECT
  customer_id,
  customer_id AS cust_id  # Downstream unaffected
```

### Testing Change Categories

Write tests to verify expected categories:

```python
# tests/test_change_categorization.py
def test_column_removed_is_breaking():
    old_yaml = {"columns": {"a": "INT", "b": "STRING"}}
    new_yaml = {"columns": {"a": "INT"}}

    category = classify_change(old_yaml, new_yaml)
    assert category == ChangeCategory.BREAKING

def test_column_added_is_non_breaking():
    old_yaml = {"columns": {"a": "INT"}}
    new_yaml = {"columns": {"a": "INT", "b": "STRING"}}

    category = classify_change(old_yaml, new_yaml)
    assert category == ChangeCategory.NON_BREAKING
```

## Troubleshooting

### False positive: BREAKING but safe

**Problem**: Change categorized as BREAKING but downstream actually safe.

**Solution**: This is conservative by design. Review downstream nodes manually. If truly safe, the category is correct (downstream should rebuild to pick up new data).

### False negative: NON_BREAKING but breaks downstream

**Problem**: Change categorized as NON_BREAKING but downstream queries fail.

**Solution**: This indicates an incomplete change detection rule. Report as a bug with example YAML files.

### Metadata changes triggering rebuilds

**Problem**: Only changed description, but node still rebuilds.

**Solution**: Check if functional fields also changed (e.g., transform SQL whitespace differences are ignored, but logic changes are not).

### Downstream rebuild list is incomplete

**Problem**: Expected node to rebuild but not in downstream list.

**Solution**: Check DAG edges. The node may not be a transitive dependent. Use `seeknal plan` to see full dependency graph.

## Implementation Details

### Source Files

- `src/seeknal/dag/diff.py` - ManifestDiff and ChangeCategory
- `src/seeknal/workflow/environment.py` - Plan with categorization
- `src/seeknal/cli/main.py` - CLI diff command

### Key Classes

```python
class ChangeCategory(Enum):
    BREAKING = "breaking"
    NON_BREAKING = "non_breaking"
    METADATA = "metadata"

@dataclass
class NodeChange:
    node_id: str
    change_type: DiffType
    category: ChangeCategory
    changed_fields: list[str]
    changed_details: dict[str, str]
```

### Algorithms

**Column change classification**:
1. Check if columns removed → BREAKING
2. Check if column types changed → BREAKING
3. Check if columns only added → NON_BREAKING

**Config change classification**:
1. Check if entity/source_type changed → BREAKING
2. Check if features removed → BREAKING
3. Check if inputs removed → BREAKING
4. Check if only audits/metadata changed → METADATA
5. Default → NON_BREAKING

**Downstream BFS**:
1. Start from changed node
2. Add all children to queue
3. For each child, recurse to their children
4. Mark all visited nodes for rebuild

## Related Concepts

- [Virtual Environments](virtual-environments.md) - Isolated workspaces for testing changes
- [YAML Pipeline Tutorial](../tutorials/yaml-pipeline-tutorial.md) - Pipeline basics
- [Metrics Change Tracking Tutorial](../tutorials/metrics-change-tracking.md) - Hands-on change categorization
