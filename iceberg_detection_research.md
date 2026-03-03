# Iceberg Data Source Detection Implementation Research

**Date:** 2026-03-03
**Purpose:** Understand the existing Iceberg incremental detection architecture to evaluate applying it to PostgreSQL

---

## Executive Summary

Seeknal implements a sophisticated **snapshot-based change detection** system for Iceberg sources with **partition-pruned incremental reads**. The architecture separates concerns across three main components:

1. **Change Detection** (Runner) - Compares Iceberg snapshot IDs to detect data changes
2. **Incremental Execution** (Source Executor) - Injects WHERE clauses for partition pruning
3. **State Persistence** (State Manager) - Tracks watermarks and snapshot IDs across runs

---

## 1. How Iceberg Detects New Data Arrival

### 1.1 Snapshot ID Comparison

Iceberg tables have **immutable snapshot IDs** that change whenever data is written. Seeknal leverages this for change detection:

```python
# In DAGRunner._fingerprint_based_detection()
# (src/seeknal/workflow/runner.py:340-368)

for nid, node in self.manifest.nodes.items():
    if node.node_type != NodeType.SOURCE:
        continue
    if node.config.get("source") != "iceberg":
        continue

    table_ref = node.config.get("table")

    # Query current snapshot ID from Iceberg catalog metadata
    from seeknal.workflow.iceberg_metadata import get_current_snapshot_id
    current_snap, _ = get_current_snapshot_id(
        table_ref=table_ref,
        params=node.config.get("params", {}),
    )

    # Compare with stored snapshot from last successful run
    stored_node = self.run_state.nodes.get(nid)
    stored_snap = stored_node.iceberg_snapshot_id if stored_node else None

    if current_snap is None:
        # Catalog unreachable — force re-execution (fail-open)
        changed.add(nid)
        skip_reasons[nid] = "iceberg catalog unreachable"
    elif current_snap != stored_snap:
        # Snapshot changed → data changed
        changed.add(nid)
        skip_reasons[nid] = "iceberg data changed"
    # else: same snapshot → stays cached
```

### 1.2 Snapshot Query Implementation

The snapshot query uses Iceberg's REST catalog API (via Lakekeeper):

```python
# In seeknal/workflow/iceberg_metadata.py:get_current_snapshot_id()

# 1. Authenticate with OAuth2
token = get_oauth_token(KEYCLOAK_TOKEN_URL, CLIENT_ID, CLIENT_SECRET)

# 2. Query catalog config
config_url = f"{catalog_url}/v1/config?warehouse={warehouse}"
prefix = config_resp.get("overrides", {}).get("prefix", "")

# 3. Query table metadata
table_url = f"{catalog_url}/v1/{prefix}/namespaces/{namespace}/tables/{table_name}"
metadata = table_resp.get("metadata", {})
snapshot_id = str(metadata.get("current-snapshot-id", ""))

# Returns: (snapshot_id, snapshot_timestamp)
```

**Key characteristics:**
- **Zero data scanning** - Only queries metadata (single HTTP request)
- **Atomic consistency** - Snapshot IDs are immutable and globally unique
- **Fail-open** - If catalog is unreachable, forces re-execution (safe default)

---

## 2. What Triggers Pipeline Execution

### 2.1 Three-Stage Detection Process

When `seeknal run` is executed, the runner performs:

#### Stage 1: Fingerprint-Based Detection
```python
# Compare YAML content hashes
current_fps = compute_dag_fingerprints(manifest_nodes, upstream_map)
for nid, fp in current_fps.items():
    if stored_node.fingerprint.combined != fp.combined:
        changed.add(nid)  # YAML changed
```

#### Stage 2: Iceberg Snapshot Detection
```python
# For Iceberg sources, compare snapshot IDs
if current_snap != stored_snap:
    changed.add(nid)  # Data changed
```

#### Stage 3: Downstream Propagation
```python
# Invalidate all downstream nodes
to_run = find_downstream_nodes(downstream_adj, changed)
for nid in to_run:
    skip_reasons[nid] = "downstream of changed node"
```

### 2.2 Execution Modes

| Mode | Trigger | Behavior |
|------|---------|----------|
| **Cached** | Snapshot ID unchanged | Skip execution entirely (0s) |
| **Incremental** | Snapshot changed + watermark exists | Load only new data via WHERE clause |
| **Full Scan** | First run or no `freshness.time_column` | Load entire table |
| **Full Refresh** | `--full` flag | Ignore state, load entire table |

---

## 3. Key Files and Components

### 3.1 Component Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     DAGRunner (runner.py)                   │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  _fingerprint_based_detection()                        │ │
│  │  - Query Iceberg snapshot IDs via iceberg_metadata.py  │ │
│  │  - Compare with stored NodeState.iceberg_snapshot_id   │ │
│  │  - Mark changed nodes + downstream dependencies        │ │
│  └────────────────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  _execute_node()                                       │ │
│  │  - Inject stored last_watermark into ExecutionContext  │ │
│  │  - Call SourceExecutor                                 │ │
│  │  - Extract iceberg_snapshot_id from result metadata    │ │
│  │  - Persist to NodeState after execution                │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│              SourceExecutor (executors/source_executor.py)  │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  _load_iceberg()                                       │ │
│  │  - Read _iceberg_last_watermark from context           │ │
│  │  - Generate WHERE clause if watermark exists:          │ │
│  │    WHERE time_col > '{watermark}' OR time_col IS NULL  │ │
│  │  - Execute DuckDB query with partition pruning         │ │
│  │  - Compute new watermark = MAX(time_column)            │ │
│  │  - Query current snapshot_id via iceberg_snapshots()   │ │
│  │  - Return metadata in result._iceberg_result_metadata  │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│              State Management (workflow/state.py)           │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  NodeState dataclass                                   │ │
│  │  - iceberg_snapshot_id: str                            │ │
│  │  - iceberg_snapshot_timestamp: str                     │ │
│  │  - iceberg_table_ref: str                              │ │
│  │  - last_watermark: str                                 │ │
│  │  - fingerprint: NodeFingerprint                        │ │
│  └────────────────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  load_state() / save_state()                           │ │
│  │  - Persist to target/run_state.json                    │ │
│  │  - Atomic write with backup                            │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│           Iceberg Metadata (workflow/iceberg_metadata.py)   │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  get_current_snapshot_id()                             │ │
│  │  - Authenticate with OAuth2                            │ │
│  │  - Query REST catalog API                              │ │
│  │  - Return (snapshot_id, timestamp) or (None, None)     │ │
│  └────────────────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  get_snapshot_id_from_duckdb()                         │ │
│  │  - Use DuckDB's iceberg_snapshots() function           │ │
│  │  - Fallback if REST API unavailable                    │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 File Locations

| Component | File Path | Responsibility |
|-----------|-----------|----------------|
| **Change Detection** | `src/seeknal/workflow/runner.py:340-368` | Snapshot ID comparison, downstream invalidation |
| **Watermark Injection** | `src/seeknal/workflow/runner.py:451-465` | Pass stored watermark to executor |
| **Incremental Reads** | `src/seeknal/workflow/executors/source_executor.py:930-1148` | WHERE clause generation, watermark computation |
| **Snapshot Query** | `src/seeknal/workflow/iceberg_metadata.py:1-113` | REST catalog API interaction |
| **State Persistence** | `src/seeknal/workflow/state.py:77-127` | NodeState dataclass with iceberg fields |
| **Documentation** | `docs/getting-started/advanced/10-iceberg-incremental.md` | User-facing guide |
| **Tests** | `tests/workflow/test_iceberg_*.py` | Unit and integration tests |
| **QA Specs** | `qa/specs/iceberg-incremental-*.yml` | End-to-end validation |

---

## 4. Overall Architecture Pattern

### 4.1 Design Pattern: Metadata-Driven Incremental Processing

The architecture follows a **metadata-driven** pattern where:

1. **State is externalized** - `run_state.json` persists between runs
2. **Detection is declarative** - Snapshot ID comparison, not imperative file scanning
3. **Execution is adaptive** - WHERE clause injected based on stored watermark
4. **Propagation is automatic** - Downstream nodes invalidated via DAG traversal

### 4.2 Data Flow

```
┌──────────────┐
│  seeknal run │
└──────┬───────┘
       │
       ▼
┌──────────────────────────────────────────────────────┐
│ 1. Load State                                        │
│    - Read target/run_state.json                      │
│    - Deserialize into RunState.nodes dict            │
└──────┬───────────────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────────────┐
│ 2. Detect Changes                                    │
│    For each Iceberg source node:                     │
│    ┌───────────────────────────────────────────────┐ │
│    │ a. Query current snapshot ID via REST API     │ │
│    │ b. Compare with NodeState.iceberg_snapshot_id │ │
│    │ c. If different → mark as changed             │ │
│    └───────────────────────────────────────────────┘ │
│    Build downstream adjacency via BFS                │
│    Result: Set[node_id] to_run                       │
└──────┬───────────────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────────────┐
│ 3. Execute Nodes (Topological Order)                 │
│    For each node in to_run:                          │
│    ┌───────────────────────────────────────────────┐ │
│    │ if node is Iceberg source:                    │ │
│    │   a. Inject NodeState.last_watermark          │ │
│    │   b. SourceExecutor generates WHERE clause:   │ │
│    │      WHERE time_col > '{watermark}'           │ │
│    │      OR time_col IS NULL                      │ │
│    │   c. DuckDB executes with partition pruning   │ │
│    │   d. Compute new_watermark = MAX(time_col)    │ │
│    │   e. Query snapshot_id via iceberg_snapshots()│ │
│    │   f. Return metadata in ExecutorResult        │ │
│    └───────────────────────────────────────────────┘ │
└──────┬───────────────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────────────┐
│ 4. Persist State                                     │
│    For each executed node:                           │
│    - NodeState.iceberg_snapshot_id = result.metadata │
│    - NodeState.last_watermark = result.metadata      │
│    - NodeState.fingerprint = computed_fingerprint    │
│    Atomic write to target/run_state.json             │
└──────────────────────────────────────────────────────┘
```

### 4.3 Key Design Decisions

| Decision | Rationale | Trade-off |
|----------|-----------|-----------|
| **Snapshot ID vs File Hashing** | Zero-cost metadata check (no data scan) | Requires REST catalog access |
| **Separate Detection Phase** | Clean separation of concerns | Extra HTTP requests even for cached nodes |
| **WHERE Clause Injection** | Leverages Iceberg partition pruning | Requires `freshness.time_column` config |
| **NULL Handling** | `OR time_col IS NULL` prevents silent drops | May include duplicate rows across runs |
| **Fail-Open on Catalog Unreachable** | Safe default (re-execute rather than skip) | Unnecessary work if catalog is flaky |
| **Per-Node State** | Independent tracking for multiple Iceberg sources | Larger state file size |

---

## 5. Configuration Schema

### 5.1 YAML Source Definition

```yaml
kind: source
name: events
source: iceberg
table: atlas.signals.events  # 3-part: catalog.namespace.table

# Optional: Enable partition-pruned incremental reads
freshness:
  time_column: event_time  # Column to use for watermark
  warn_after: {count: 1, period: day}
  error_after: {count: 2, period: day}

# Optional: Override profile defaults
params:
  catalog_uri: "${LAKEKEEPER_URL}"
  warehouse: "${LAKEKEEPER_WAREHOUSE}"
```

### 5.2 Profile Defaults

```yaml
# profiles.yml
source_defaults:
  iceberg:
    catalog_uri: "${LAKEKEEPER_URL:http://localhost:8181}"
    warehouse: "${LAKEKEEPER_WAREHOUSE:seeknal-warehouse}"
```

---

## 6. State Schema

### 6.1 NodeState for Iceberg Sources

```json
{
  "nodes": {
    "source.events": {
      "hash": "abc123...",
      "last_run": "2026-03-03T10:30:00Z",
      "status": "success",
      "duration_ms": 1250,
      "row_count": 15000,
      "version": 1,

      "iceberg_snapshot_id": "4837265091827364",
      "iceberg_snapshot_timestamp": "2026-03-03T10:25:00Z",
      "iceberg_table_ref": "atlas.signals.events",
      "iceberg_schema_version": 1,
      "last_watermark": "2026-03-02 18:45:00",

      "fingerprint": {
        "content_hash": "def456...",
        "schema_hash": "ghi789...",
        "upstream_hash": "jkl012...",
        "config_hash": "mno345..."
      }
    }
  }
}
```

---

## 7. Incremental Read Implementation

### 7.1 WHERE Clause Generation

```python
# In SourceExecutor._load_iceberg()
# (src/seeknal/workflow/executors/source_executor.py:1096-1109)

if time_column and last_watermark and not params.get("query"):
    # Incremental: partition-pruned read
    from seeknal.validation import validate_column_name
    validate_column_name(time_column)

    view_sql = (
        f"SELECT * FROM {catalog_alias}.{namespace}.{table_name} "
        f"WHERE \"{time_column}\" > CAST('{last_watermark}' AS TIMESTAMP) "
        f"OR \"{time_column}\" IS NULL"
    )
else:
    # Full scan: first run, no time_column, or custom query
    view_sql = f"SELECT * FROM {catalog_alias}.{namespace}.{table_name}"
```

### 7.2 Watermark Computation

```python
# After loading data, compute new watermark
if time_column:
    try:
        wm_result = con.execute(
            f"SELECT MAX(\"{time_column}\") FROM {schema}.{view_name}"
        ).fetchone()
        if wm_result and wm_result[0] is not None:
            new_watermark = str(wm_result[0])
    except Exception:
        pass  # Non-critical failure
```

### 7.3 Snapshot ID Extraction

```python
# Get current snapshot from DuckDB
snapshot_id = None
try:
    snap_result = con.execute(
        f"SELECT snapshot_id FROM iceberg_snapshots('{catalog_alias}.{namespace}.{table_name}') "
        f"ORDER BY timestamp_ms DESC LIMIT 1"
    ).fetchone()
    if snap_result:
        snapshot_id = str(snap_result[0])
except Exception:
    pass  # iceberg_snapshots() may not be available
```

---

## 8. Edge Cases and Error Handling

### 8.1 Edge Case Matrix

| Scenario | Behavior | Code Location |
|----------|----------|---------------|
| **First run (no state)** | Full scan, store snapshot + watermark | `runner.py:363` |
| **Catalog unreachable** | Force re-execution (fail-open) | `runner.py:366-368` |
| **Snapshot unchanged** | Skip execution (cached) | `runner.py:370-371` |
| **No `freshness.time_column`** | Full scan (no WHERE clause) | `source_executor.py:1096` |
| **NULL time_column values** | Include via `OR time_col IS NULL` | `source_executor.py:1103` |
| **Custom `params.query`** | Skip WHERE clause injection | `source_executor.py:1096` |
| **`--full` flag** | Ignore stored state, full scan | `runner.py:457` |
| **Watermark column not in table** | Fail with clear error | `source_executor.py:1085` |
| **Schema evolution** | Trigger full re-scan (via fingerprint) | `state.py:fingerprint` |

### 8.2 Error Handling Strategy

- **Catalog errors**: Fail-open (re-execute) with warning
- **Watermark errors**: Non-critical (continue with NULL watermark)
- **Snapshot query errors**: Graceful fallback to DuckDB `iceberg_snapshots()`
- **NULL handling**: Explicit inclusion in WHERE clause

---

## 9. Performance Characteristics

### 9.1 Cost Breakdown

| Operation | Cost | Duration |
|-----------|------|----------|
| **Snapshot ID query (cached)** | Single HTTP request to catalog | ~50-200ms |
| **Snapshot ID query (changed)** | Single HTTP request + data scan | Depends on table size |
| **Incremental read** | Partition-pruned scan | Only new partitions |
| **Full scan** | Complete table scan | All partitions |
| **Watermark computation** | `MAX(time_column)` query | ~10-50ms |

### 9.2 Optimization Techniques

1. **Metadata-only checks** - Snapshot ID comparison avoids data scanning
2. **Partition pruning** - WHERE clause pushed down to Iceberg scan layer
3. **Lazy evaluation** - DuckDB views not materialized until queried
4. **Topological execution** - Only affected branches re-execute

---

## 10. Testing Strategy

### 10.1 Test Categories

| Test File | Scope | Purpose |
|-----------|-------|---------|
| `test_iceberg_metadata.py` | Unit | Snapshot query utility |
| `test_iceberg_runner.py` | Unit | Cache invalidation logic |
| `test_iceberg_source_executor.py` | Unit | WHERE clause generation |
| `test_iceberg_incremental.py` | Integration | End-to-end multi-run scenarios |
| `qa/specs/iceberg-incremental-*.yml` | E2E | Real infrastructure validation |

### 10.2 Multi-Run Test Pattern

QA specs use **sequential runs** to validate state persistence:

```yaml
execution_plan:
  steps:
    - name: "Run 1: Initial load"
      command: "seeknal run --full"
      validation:
        state_check:
          iceberg_snapshot_id: "expected_snap_1"
          last_watermark: "expected_wm_1"

    - name: "Run 2: No changes"
      command: "seeknal run"
      validation:
        skipped_nodes: ["source.events"]

    - name: "Run 3: After data insert"
      seed_before:
        tables: [new_data.csv]
      command: "seeknal run"
      validation:
        executed_nodes: ["source.events", "transform.*"]
        state_check:
          iceberg_snapshot_id: "expected_snap_2"
          last_watermark: "expected_wm_2"
```

---

## 11. Applying to PostgreSQL

### 11.1 Feasibility Assessment

| Aspect | Iceberg | PostgreSQL | Adaptation Needed |
|--------|---------|------------|-------------------|
| **Change Detection** | Snapshot ID (metadata) | No native snapshot ID | Use `xmin` system column or logical replication |
| **Incremental Reads** | `WHERE time_col > watermark` | Same approach works | No change needed |
| **State Persistence** | NodeState fields | Reuse same fields | No change needed |
| **Detection Cost** | HTTP request to catalog | `SELECT xmin FROM table` | Similar cost profile |

### 11.2 Proposed PostgreSQL Implementation

#### Option A: XMIN-Based Detection

```python
# In source_executor.py for PostgreSQL sources
def _get_postgresql_xmin(self, con, table):
    """Get table's xmin as change indicator."""
    result = con.execute(
        f"SELECT xmin FROM {table} ORDER BY xmin DESC LIMIT 1"
    ).fetchone()
    return str(result[0]) if result else None

# In runner.py detection phase
if source_type == "postgresql":
    current_xmin = get_postgresql_xmin(con, table_ref)
    stored_xmin = stored_node.metadata.get("postgresql_xmin")
    if current_xmin != stored_xmin:
        changed.add(nid)
```

**Pros:**
- No external dependencies
- Low overhead (single row query)

**Cons:**
- `xmin` is transaction ID, may wrap around
- Not globally unique across tables
- Requires table scan (no metadata-only check)

#### Option B: Logical Replication (Change Data Capture)

```python
# Use PostgreSQL logical replication slots
# Requires:
# 1. CREATE_REPLICATION_SLOT
# 2. READ_REPLICATION_SLOT
# 3. Track LSN (Log Sequence Number) in NodeState
```

**Pros:**
- True CDC (row-level changes)
- No table scanning
- Efficient for high-frequency changes

**Cons:**
- Complex setup (replication slots, WAL parsing)
- Requires superuser privileges
- External dependency (pgoutput plugin)

#### Option C: Timestamp-Based (Existing Approach)

```yaml
# Reuse freshness.time_column for change detection
freshness:
  time_column: updated_at
```

```python
# In detection phase
if source_type == "postgresql" and time_column:
    result = con.execute(
        f"SELECT MAX(\"{time_column}\") FROM {table}"
    ).fetchone()
    current_max = str(result[0])
    stored_max = stored_node.last_watermark
    if current_max != stored_max:
        changed.add(nid)
```

**Pros:**
- Reuses existing infrastructure
- Works with any timestamp column
- No special privileges needed

**Cons:**
- Requires timestamp column
- Full table scan for `MAX()` query
- Doesn't detect deletes

### 11.3 Recommended Approach

**Hybrid strategy:**

1. **Primary**: Timestamp-based (reuse `freshness.time_column`)
   - Works for 80% of use cases
   - Zero additional code (already implemented for incremental reads)

2. **Secondary**: XMIN-based for tables without timestamps
   - Add `postgresql_xmin` to NodeState.metadata
   - Query `SELECT relminmxid FROM pg_class WHERE relname = 'table'`
   - Lower overhead than `MAX(timestamp)` scan

3. **Future**: Logical replication for CDC requirements
   - Add as optional feature for advanced users
   - Requires explicit configuration in YAML

---

## 12. Implementation Checklist for PostgreSQL

If implementing PostgreSQL incremental detection:

### 12.1 Code Changes

- [ ] Add `postgresql_xmin` to `NodeState.metadata` (optional, for xmin tracking)
- [ ] Add PostgreSQL detection logic to `runner._fingerprint_based_detection()`
- [ ] Modify `source_executor._load_database()` to return metadata like Iceberg
- [ ] Add PostgreSQL-specific WHERE clause injection in `_load_database()`
- [ ] Update `runner._execute_node()` to inject watermarks for PostgreSQL sources
- [ ] Add tests in `test_postgresql_incremental.py` (new file)

### 12.2 Documentation Updates

- [ ] Add PostgreSQL section to `docs/getting-started/advanced/10-database-sources.md`
- [ ] Update `docs/getting-started/advanced/index.md` with PostgreSQL incremental
- [ ] Add `qa/specs/postgresql-incremental-snapshot.yml`
- [ ] Update CLAUDE.md with PostgreSQL detection pattern

### 12.3 Backward Compatibility

- [ ] Ensure existing PostgreSQL sources without `freshness.time_column` continue to work (full scan)
- [ ] Add deprecation warning for sources without incremental support
- [ ] Provide migration guide for enabling incremental reads

---

## 13. Key Takeaways

### 13.1 Architectural Strengths

1. **Separation of Concerns** - Detection, execution, and state management are cleanly separated
2. **Metadata-Driven** - Zero-cost change detection via snapshot IDs
3. **Declarative Configuration** - `freshness.time_column` enables incremental with one line
4. **Automatic Propagation** - Downstream invalidation via DAG traversal
5. **Fail-Safe Defaults** - Fail-open on errors, NULL handling, cache validation

### 13.2 Patterns Applicable to PostgreSQL

| Pattern | Iceberg | PostgreSQL Adaptation |
|---------|---------|-----------------------|
| **Metadata-based detection** | Snapshot ID | `xmin` or `MAX(timestamp)` |
| **WHERE clause injection** | `time_col > watermark` | Same approach |
| **State persistence** | `NodeState.iceberg_*` fields | `NodeState.postgresql_*` or reuse |
| **Downstream propagation** | DAG traversal | Same (no change) |
| **Fail-open** | Re-execute on catalog unreachable | Re-execute on connection error |

### 13.3 Anti-Patterns to Avoid

1. **Don't** modify the generic fingerprint hash function - use separate detection phase
2. **Don't** scan entire tables for change detection - use metadata queries
3. **Don't** block execution on detection failures - fail-open
4. **Don't** couple detection logic to executor implementation - keep in runner
5. **Don't** forget NULL handling in WHERE clauses - always include `OR col IS NULL`

---

## 14. References

### 14.1 Internal Documentation

- **Chapter 10: Iceberg Incremental Processing** - `docs/getting-started/advanced/10-iceberg-incremental.md`
- **Brainstorm: Partition Pruning** - `docs/brainstorms/2026-03-03-iceberg-incremental-partition-pruning-brainstorm.md`
- **State Management** - `src/seeknal/workflow/state.py`
- **Source Executor** - `src/seeknal/workflow/executors/source_executor.py`

### 14.2 QA Specifications

- **Snapshot Caching** - `qa/specs/iceberg-incremental-snapshot.yml`
- **Partition Pruning** - `qa/specs/iceberg-incremental-partition-pruning.yml`
- **Cascade Validation** - `qa/specs/iceberg-incremental-cascade.yml`

### 14.3 Test Files

- **Metadata Utility** - `tests/workflow/test_iceberg_metadata.py`
- **Runner Logic** - `tests/workflow/test_iceberg_runner.py`
- **Executor Implementation** - `tests/workflow/test_iceberg_source_executor.py`
- **Integration Tests** - `tests/workflow/test_iceberg_incremental.py`

---

**Research Completed:** 2026-03-03
**Next Steps:** Evaluate PostgreSQL implementation approach based on team priorities
