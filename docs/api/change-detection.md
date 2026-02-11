# Change Detection API Reference

This module provides SQL-aware change detection for efficient incremental rebuilds, automatically categorizing changes as breaking or non-breaking.

## Module: `seeknal.dag.diff`

### Classes

#### `ManifestDiff`

Compare two manifests and detect changes with SQL-aware categorization.

```python
from seeknal.dag.diff import ManifestDiff
from seeknal.dag.manifest import Manifest

old_manifest = Manifest.load("manifest.old.json")
new_manifest = Manifest.load("manifest.new.json")

diff = ManifestDiff.compare(old_manifest, new_manifest)
```

**Methods:**

##### `compare(old: Manifest, new: Manifest) -> ManifestDiff`

Compare two manifests and detect changes.

**Parameters:**
- `old` (Manifest) - Previous manifest
- `new` (Manifest) - Current manifest

**Returns:** `ManifestDiff` object with change information

**Example:**
```python
from seeknal.dag.diff import ManifestDiff
from seeknal.dag.manifest import Manifest

old = Manifest.load("manifest.old.json")
new = Manifest.load("manifest.new.json")

diff = ManifestDiff.compare(old, new)
```

##### `has_changes() -> bool`

Check if any changes were detected.

**Returns:** `bool` - True if any changes detected

**Example:**
```python
if diff.has_changes():
    print("Changes detected!")
```

##### `get_categorized_changes(new_manifest: Manifest) -> dict[str, ChangeCategory]`

Get all changes categorized by impact.

**Parameters:**
- `new_manifest` (Manifest) - Current manifest

**Returns:** `dict[str, ChangeCategory]` - Mapping of node_id to change category

**Example:**
```python
to_rebuild = diff.get_categorized_changes(new_manifest)
for node_id, category in to_rebuild.items():
    print(f"{node_id}: {category.value}")
```

##### `get_nodes_to_rebuild(new_manifest: Manifest) -> dict[str, ChangeCategory]`

Get nodes that need to be rebuilt (BREAKING or NON_BREAKING, excludes METADATA).

**Parameters:**
- `new_manifest` (Manifest) - Current manifest

**Returns:** `dict[str, ChangeCategory]` - Nodes to rebuild with their categories

**Example:**
```python
rebuild_nodes = diff.get_nodes_to_rebuild(new_manifest)
print(f"Need to rebuild {len(rebuild_nodes)} nodes")
```

##### `get_breaking_changes() -> list[str]`

Get list of node IDs with breaking changes.

**Returns:** `list[str]` - Node IDs with breaking changes

**Example:**
```python
breaking = diff.get_breaking_changes()
if breaking:
    print(f"WARNING: {len(breaking)} breaking changes detected!")
```

##### `get_non_breaking_changes() -> list[str]`

Get list of node IDs with non-breaking changes.

**Returns:** `list[str]` - Node IDs with non-breaking changes

**Example:**
```python
non_breaking = diff.get_non_breaking_changes()
print(f"{len(non_breaking)} nodes can be rebuilt independently")
```

##### `get_downstream_impact(node_id: str, dag: DAG) -> list[str]`

Get all downstream nodes affected by a change.

**Parameters:**
- `node_id` (str) - Node that changed
- `dag` (DAG) - Pipeline DAG

**Returns:** `list[str]` - Affected downstream node IDs

**Example:**
```python
affected = diff.get_downstream_impact("source_node", dag)
print(f"Downstream impact: {len(affected)} nodes")
```

**Properties:**

- `added_nodes` (list[str]) - Nodes added in new manifest
- `removed_nodes` (list[str]) - Nodes removed in new manifest
- `modified_nodes` (list[str]) - Nodes modified in new manifest

---

### Enums

#### `ChangeCategory`

Category of change impact on downstream nodes.

```python
from seeknal.dag.diff import ChangeCategory
```

**Values:**

| Value | Description | Downstream Impact |
|-------|-------------|-------------------|
| `BREAKING` | Schema or logic change affecting outputs | Rebuild this + all downstream |
| `NON_BREAKING` | Change only affecting this node | Rebuild this node only |
| `METADATA` | Description, tags, or format-only change | No rebuild needed |

**Example:**
```python
if category == ChangeCategory.BREAKING:
    print("This change requires downstream rebuilds!")
```

---

#### `ChangeType`

Fine-grained types of changes detected.

```python
from seeknal.dag.diff import ChangeType
```

**Values:**

| Value | Description |
|-------|-------------|
| `NODE_ADDED` | New node added to pipeline |
| `NODE_REMOVED` | Node removed from pipeline |
| `COLUMN_ADDED` | Column added to SELECT |
| `COLUMN_REMOVED` | Column removed from SELECT |
| `COLUMN_TYPE_CHANGED` | Column data type changed |
| `SQL_LOGIC_CHANGED` | Query logic modified |
| `TABLE_REMOVED` | Source table dropped |
| `TABLE_ADDED` | New source table referenced |
| `CONFIG_CHANGED` | Configuration updated |
| `METADATA_CHANGED` | Description/tags changed |

**Example:**
```python
if change_type == ChangeType.COLUMN_REMOVED:
    print("Column removal detected - breaking change!")
```

---

## Module: `seeknal.dag.sql_parser`

### Classes

#### `SQLParser`

Parse SQL queries and extract metadata using SQLGlot.

```python
from seeknal.dag.sql_parser import SQLParser

parser = SQLParser()
```

**Methods:**

##### `normalize(sql: str) -> str`

Normalize SQL for comparison (removes formatting differences).

**Parameters:**
- `sql` (str) - SQL query to normalize

**Returns:** `str` - Normalized SQL

**Example:**
```python
parser = SQLParser()

sql1 = "SELECT  id,  name  FROM  users"
sql2 = "SELECT id, name FROM users"

assert parser.normalize(sql1) == parser.normalize(sql2)
```

##### `extract_columns(sql: str) -> list[str]`

Extract output column names from SELECT statement.

**Parameters:**
- `sql` (str) - SQL query

**Returns:** `list[str]` - Column names in SELECT clause

**Example:**
```python
columns = parser.extract_columns("SELECT id, name, email FROM users")
# Returns: ["id", "name", "email"]
```

##### `extract_tables(sql: str) -> list[str]`

Extract table names referenced in SQL.

**Parameters:**
- `sql` (str) - SQL query

**Returns:** `list[str]` - Table names

**Example:**
```python
tables = parser.extract_tables("SELECT * FROM users JOIN orders ON users.id = orders.user_id")
# Returns: ["users", "orders"]
```

##### `extract_dependencies(sql: str) -> list[str]`

Extract upstream node dependencies from SQL.

**Parameters:**
- `sql` (str) - SQL query

**Returns:** `list[str]` - Dependency node names

**Example:**
```python
deps = parser.extract_dependencies("SELECT * FROM {{ source_nodes }}")
# Returns: ["source_nodes"]
```

##### `is_select_query(sql: str) -> bool`

Check if SQL is a SELECT query (vs INSERT, UPDATE, etc.).

**Parameters:**
- `sql` (str) - SQL to check

**Returns:** `bool` - True if SELECT query

---

## Module: `seeknal.dag.sql_diff`

### Classes

#### `SQLDiffer`

Compare SQL queries using AST-based diffing.

```python
from seeknal.dag.sql_diff import SQLDiffer
```

**Methods:**

##### `diff(old_sql: str, new_sql: str) -> list[Edit]`

Compare two SQL queries and return edit operations.

**Parameters:**
- `old_sql` (str) - Original SQL
- `new_sql` (str) - Modified SQL

**Returns:** `list[Edit]` - Edit operations

**Edit Types:**
- `EditType.INSERT` - Content added
- `EditType.DELETE` - Content removed
- `EditType.UPDATE` - Content modified

**Example:**
```python
from seeknal.dag.sql_diff import SQLDiffer

differ = SQLDiffer()
edits = differ.diff(
    old_sql="SELECT id, name FROM users",
    new_sql="SELECT id, name, email FROM users"
)

for edit in edits:
    print(f"{edit.type}: {edit.content}")
# Output: INSERT: email
```

##### `classify_change(old_sql: str, new_sql: str) -> ChangeCategory`

Classify SQL change as breaking, non-breaking, or metadata.

**Parameters:**
- `old_sql` (str) - Original SQL
- `new_sql` (str) - Modified SQL

**Returns:** `ChangeCategory` - Category of change

**Example:**
```python
category = differ.classify_change(
    old_sql="SELECT id, name FROM users",
    new_sql="SELECT id, name, email FROM users"
)
# Returns: ChangeCategory.NON_BREAKING (column added)
```

##### `has_column_changes(old_sql: str, new_sql: str) -> bool`

Check if column set changed between queries.

**Parameters:**
- `old_sql` (str) - Original SQL
- `new_sql` (str) - Modified SQL

**Returns:** `bool` - True if columns changed

---

## Module: `seeknal.dag.lineage`

### Classes

#### `LineageBuilder`

Build column-level lineage tracking data dependencies.

```python
from seeknal.dag.lineage import LineageBuilder
```

**Methods:**

##### `get_column_dependencies(manifest: Manifest, node_id: str, column_name: str) -> list[ColumnDependency]`

Get upstream column dependencies for a specific output column.

**Parameters:**
- `manifest` (Manifest) - Pipeline manifest
- `node_id` (str) - Node containing the column
- `column_name` (str) - Column to trace

**Returns:** `list[ColumnDependency]` - Column dependencies

**Example:**
```python
from seeknal.dag.lineage import LineageBuilder

builder = LineageBuilder()
deps = builder.get_column_dependencies(manifest, "output_table", "user_id")

for dep in deps:
    print(f"output_table.user_id <- {dep.source_table}.{dep.source_column}")
```

##### `build_lineage(manifest: Manifest) -> dict[str, dict[str, list[str]]]`

Build complete column lineage for all nodes.

**Parameters:**
- `manifest` (Manifest) - Pipeline manifest

**Returns:** `dict` - Nested dict: {node_id: {output_col: [input_cols]}}

**Example:**
```python
lineage = builder.build_lineage(manifest)

# Trace where a column comes from
for node_id, columns in lineage.items():
    for output_col, input_cols in columns.items():
        print(f"{node_id}.{output_col} derives from {input_cols}")
```

---

### Data Classes

#### `ColumnDependency`

Represents a column dependency relationship.

**Attributes:**
- `source_table` (str) - Source table name
- `source_column` (str) - Source column name
- `transformation` (str | None) - SQL transformation applied (if any)

**Example:**
```python
dep = ColumnDependency(
    source_table="users",
    source_column="id",
    transformation="UPPER"
)
# Represents: output_col = UPPER(users.id)
```

---

## CLI Commands

### `seeknal plan`

Create a plan showing categorized changes.

**Usage:**
```bash
seeknal plan <environment>
```

**Output:**
```
Changes detected:
  Modified (3):
    ~ source_nodes (columns, sql)
      columns: added: device_type, removed: old_column
      sql: keys changed: sql
      category: BREAKING
      Downstream impact (5 nodes):
        -> REBUILD agg_daily
        -> REBUILD agg_weekly
        -> REBUILD features

    ~ transform_node (sql)
      sql: keys changed: sql
      category: NON_BREAKING

    ~ report_node (metadata)
      description: Updated documentation
      category: METADATA

Summary: 3 modified
         1 BREAKING, 1 NON_BREAKING, 1 METADATA
```

**Options:**
- `--output <file>` - Save plan to file
- `--format json` - Output as JSON

---

## Examples

### Example 1: Detect and Categorize Changes

```python
from seeknal.dag.diff import ManifestDiff, ChangeCategory
from seeknal.dag.manifest import Manifest

old = Manifest.load("manifest.old.json")
new = Manifest.load("manifest.new.json")

diff = ManifestDiff.compare(old, new)

if diff.has_changes():
    to_rebuild = diff.get_nodes_to_rebuild(new)

    for node_id, category in to_rebuild.items():
        if category == ChangeCategory.BREAKING:
            print(f"⚠️  {node_id}: BREAKING - rebuilds downstream")
        elif category == ChangeCategory.NON_BREAKING:
            print(f"✓ {node_id}: NON_BREAKING - rebuild this only")
```

### Example 2: Parse SQL and Extract Columns

```python
from seeknal.dag.sql_parser import SQLParser

parser = SQLParser()

sql = """
SELECT
    user_id,
    COUNT(*) as total_events,
    SUM(amount) as total_amount
FROM events
GROUP BY user_id
"""

columns = parser.extract_columns(sql)
# Returns: ["user_id", "total_events", "total_amount"]

tables = parser.extract_tables(sql)
# Returns: ["events"]
```

### Example 3: Build Column Lineage

```python
from seeknal.dag.lineage import LineageBuilder

builder = LineageBuilder()

# Trace where output_column comes from
deps = builder.get_column_dependencies(manifest, "output_node", "user_id")

for dep in deps:
    print(f"output_node.user_id ← {dep.source_table}.{dep.source_column}")
    if dep.transformation:
        print(f"  (transformation: {dep.transformation})")
```

### Example 4: Classify SQL Changes

```python
from seeknal.dag.sql_diff import SQLDiffer, ChangeCategory

differ = SQLDiffer()

# Column added - NON_BREAKING
category1 = differ.classify_change(
    "SELECT id, name FROM users",
    "SELECT id, name, email FROM users"
)
# Returns: ChangeCategory.NON_BREAKING

# Column removed - BREAKING
category2 = differ.classify_change(
    "SELECT id, name, email FROM users",
    "SELECT id, name FROM users"
)
# Returns: ChangeCategory.BREAKING

# Formatting only - METADATA
category3 = differ.classify_change(
    "SELECT id, name FROM users",
    "SELECT  id,  name  FROM  users"
)
# Returns: ChangeCategory.METADATA
```

---

## See Also

- [Change Detection Guide](../guides/change-detection.md) - User guide for change detection
- [Interval Tracking API](interval-tracking.md) - API for interval tracking
- [Plan/Apply Workflow Guide](../guides/plan-apply-workflow.md) - Safe deployment workflow
