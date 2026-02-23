# Integration Review: 2026-02-11

## Target Branch
`feat/add-sql-mesth-feat` - Apache Iceberg storage backend for Feature Groups

## Branches Reviewed

| Branch | Commit | Lines Changed | Status |
|---------|--------|---------------|--------|
| `feat/iceberg-datasource-1` | `720bf70` | +2,141 | Issues Found |
| `feat/iceberg-source-1` | `29cf589` | +14,995 | Issues Found |

---

## 1. feat/iceberg-datasource-1

**Title:** YAML and Python script parameterization

### Summary
Adds a YAML/Python parameterization system (`{{ }}` syntax) for workflow scripts. Also removes 2,037 lines of Iceberg-related code including tests, documentation, and entire `IcebergStoreOutput` implementation.

### Key Files
- `src/seeknal/workflow/parameters/` (new module)
  - `resolver.py` - Parameter resolution logic
  - `functions.py` - Built-in parameter functions
  - `helpers.py` - Helper utilities
- `src/seeknal/cli/main.py` - CLI parameterization support
- `src/seeknal/workflow/dag.py` - DAG parameterization
- `tests/workflow/test_parameterization.py`

### Findings

| Severity | Issue | File | Action Required |
|----------|--------|------|----------------|
| **Critical** | Parameter name collision (`run_id`, `run_date`) | `parameters/resolver.py` | Add validation/warnings |
| **Important** | No validation of user parameter names | `parameters/helpers.py` | Add validation for reserved names |
| **Important** | `type` shadows Python built-in | `parameters/helpers.py` | Rename to `param_type` |
| **Important** | Inconsistent type conversion logic | `parameters/resolver.py` | Consolidate into shared utility |
| **Important** | Environment var type loss (all strings) | `python_executor.py` | Document or improve serialization |

### Details

#### 1. Parameter Name Collision (Critical)
```python
# src/seeknal/workflow/parameters/resolver.py:52-70
self._context_values: Dict[str, str] = {
    "run_id": self.run_id,
    "run_date": self.run_date,
}
```
User-defined parameters named `run_id` or `run_date` can silently override system values.

**Fix:** Emit warning when user params collide with reserved names.

#### 2. No Param Name Validation (Important)
```python
# src/seeknal/workflow/parameters/helpers.py:47-48
env_name = f"SEEKNAL_PARAM_{name.upper()}"
value = os.environ.get(env_name, default)
```
User calling `get_param("PATH")` gets system env var, not user parameter.

**Fix:** Validate parameter names are alphanumeric only.

#### 3. Breaking Change: Iceberg Removed
Deletes `IcebergStoreOutput` from exports. Any code using `OfflineStoreEnum.ICEBERG` or `IcebergStoreOutput` will fail.

---

## 2. feat/iceberg-source-1

**Title:** RUN Command Parity with SQLMesh - Full Implementation

### Summary
Implements RUN command parity with SQLMesh, adding SQL parsing, diffing, lineage tracking, and state backends (file and database). Well-structured code with comprehensive test coverage.

### Key Files
- `src/seeknal/dag/` - SQL parsing, diffing, lineage
  - `sql_parser.py` - Parse SQL into DAG structure
  - `sql_diff.py` - Detect SQL changes
  - `lineage.py` - Column-level lineage tracking
  - `diff.py` - Enhanced diff capabilities
- `src/seeknal/state/` - State management
  - `backend.py` - State backend interface
  - `database_backend.py` - PostgreSQL state storage
  - `file_backend.py` - File-based state storage
- `src/seeknal/workflow/`
  - `intervals.py` - Interval tracking for backfills
  - `prefect_integration.py` - Prefect orchestration
- `docs/guides/` - Comprehensive guides
- `docs/tutorials/` - Interactive tutorials

### Findings

| Severity | Issue | File | Action Required |
|----------|--------|------|----------------|
| **Critical** | Path traversal vulnerability | `state/file_backend.py` | Add path sanitization |
| ✅ Safe | SQL injection | `state/database_backend.py` | Already using parameterized queries |
| **Important** | Breaking change: Iceberg removed | `featurestore/featurestore.py` | Confirm if intentional |
| **Important** | Date parsing lacks robust validation | `cli/main.py` | Add range validation |
| **Suggestion** | Missing path_security.py integration | `state/file_backend.py` | Use existing security module |

### Details

#### 1. Path Traversal Vulnerability (Critical)
```python
# src/seeknal/state/file_backend.py
def _get_run_state_path(self, run_id: str) -> Path:
    run_dir = self.base_path / run_id  # NO VALIDATION
    return run_dir / "run_state.json"
```
An attacker could use `run_id = "../../../etc/passwd"` to write files outside intended directory.

**Fix:**
```python
def _get_run_state_path(self, run_id: str) -> Path:
    safe_run_id = run_id.replace("..", "").replace("/", "").replace("\\", "")
    run_dir = self.base_path / safe_run_id
    return run_dir / "run_state.json"
```

#### 2. Breaking Change: Iceberg Removed
The entire `IcebergStoreOutput` class removed. Any code using `OfflineStoreEnum.ICEBERG` will fail.

#### 3. Date Parsing Edge Cases
```python
# cli/main.py - interval commands
try:
    if "T" in start:
        start_dt = datetime.fromisoformat(start)
    else:
        start_dt = datetime.fromisoformat(f"{start}T00:00:00")
except ValueError:
    _echo_error(f"Invalid start date format: {start}")
```
Doesn't catch invalid dates like `2024-13-45` or extreme years.

---

## Recommendations

### Before Integration
1. Fix path traversal vulnerability in `state/file_backend.py`
2. Add parameter name validation in `parameters/helpers.py`
3. Document breaking change (Iceberg removal)

### After Integration
1. Add comprehensive tests for parameter name collision scenarios
2. Consolidate type conversion logic into single utility
3. Integrate `path_security.py` into `FileStateBackend`

## Overall Assessment

Both branches add significant functionality but have security and API compatibility concerns that should be addressed before production deployment.

**Integration Status:** Ready with reservations
**Risk Level:** Medium-High (due to critical security finding)
