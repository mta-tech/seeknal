---
title: feat: Add Second-Order Aggregations to YAML and Python Pipelines
type: feat
date: 2026-01-30
---

# feat: Add Second-Order Aggregations to YAML and Python Pipelines

## Overview

Add support for **second-order aggregations** to Seeknal's YAML and Python pipelines. This enables advanced feature engineering patterns like aggregations of aggregations (e.g., average of user 30-day sums across regions) and cross-time window aggregations (e.g., weekly patterns from daily data).

## Background

Seeknal already has a `SecondOrderAggregator` implementation in `src/seeknal/tasks/*/aggregators/second_order_aggregator.py` that works with DuckDB and Spark. However, this functionality is **not exposed** through the YAML pipeline system or Python decorators.

**Current state:**
- First-order aggregations: Available via `kind: aggregation` YAML node and `@aggregation` decorator
- Second-order aggregations: Only available as low-level API in tasks module

**Problem:** Users cannot define second-order aggregations using the familiar YAML or Python pipeline interfaces.

## Proposed Solution

Create a new `second_order_aggregation` node type that:

1. **Exposes existing functionality** - Reuses `SecondOrderAggregator` from tasks module
2. **Clear separation** - Distinct from first-order `aggregation` node for clarity
3. **YAML support** - Define via `kind: second_order_aggregation` in YAML files
4. **Python support** - Define via `@second_order_aggregation` decorator
5. **Source validation** - Ensures upstream aggregation exists and has expected schema
6. **Flexible output** - Can materialize as feature group or use as intermediate

### Example Usage

**YAML:**
```yaml
kind: second_order_aggregation
name: region_user_metrics
description: "Aggregate user features to region level"
id_col: region_id
feature_date_col: date
source: aggregation.user_daily_features
features:
  total_users:
    basic: [count]
  avg_user_spend_30d:
    basic: [mean, stddev]
    source_feature: total_spend_30d
  weekly_total:
    window: [7, 7]
    basic: [sum]
    source_feature: daily_volume
```

**Python:**
```python
@second_order_aggregation(
    name="region_metrics",
    source="aggregation.user_metrics",
    id_col="region_id",
    feature_date_col="date"
)
def region_metrics(ctx, df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby("region_id").agg({
        "total_spend_30d": ["mean", "std"],
        "transaction_count": "sum"
    })
```

## Technical Approach

### Architecture

Reuse existing `SecondOrderAggregator` implementation through a new workflow executor:

```
YAML/Python Definition
        ↓
SecondOrderAggregationExecutor (new)
        ↓
SecondOrderAggregator (existing, tasks module)
        ↓
DuckDB/Spark Execution
```

**Key decision:** Wrap existing `SecondOrderAggregator` rather than reimplement. This ensures:
- Consistent behavior with existing code
- No duplication of SQL generation logic
- Automatic support for edge cases already handled

### Implementation Phases

#### Phase 1: Core Infrastructure

**Tasks:**
1. [x] Add `SECOND_ORDER_AGGREGATION` to `NodeType` enum in `src/seeknal/dag/manifest.py:28`
2. [x] Add schema to `validators.py` for `second_order_aggregation` node type
3. [x] Create `SecondOrderAggregationExecutor` in `src/seeknal/workflow/executors/second_order_aggregation_executor.py`
4. [x] Register executor in `executors/__init__.py`

**Success criteria:**
- NodeType enum includes new type
- Schema validation works
- Executor registered and discoverable
- Unit tests pass for executor class

**Estimated effort:** 2-3 hours

#### Phase 2: YAML Pipeline Support

**Tasks:**
1. Implement `_execute_duckdb()` method using `SecondOrderAggregator`
2. Implement `_execute_spark()` method for Spark engine
3. Add source resolution logic (load upstream aggregation output)
4. Add source schema validation
5. Create YAML template `second_order_aggregation.yml.j2`
6. Update CLI draft command to support new node type
7. Add integration tests

**Success criteria:**
- Can define `kind: second_order_aggregation` in YAML
- `seeknal parse` generates manifest with new node
- `seeknal run` executes second-order aggregation
- Output DataFrame has correct schema
- Integration tests pass

**Estimated effort:** 4-5 hours

#### Phase 3: Python Decorator Support

**Tasks:**
1. Add `@second_order_aggregation` decorator to `pipeline/decorators.py`
2. Implement pipeline execution logic for decorator
3. Create Python template `second_order_aggregation.py.j2`
4. Add tests for Python decorator usage
5. Update tutorial documentation

**Success criteria:**
- Decorator compiles and registers node
- Pipeline executes via Python
- Tutorial examples work end-to-end
- Tests pass for both DuckDB and Spark

**Estimated effort:** 3-4 hours

#### Phase 4: Polish & Documentation

**Tasks:**
1. Add dry-run support for second-order aggregations
2. Error messages for common failure modes
3. Update YAML pipeline tutorial
4. Add examples to `docs/examples/`
5. Update API documentation
6. Performance testing with large datasets

**Success criteria:**
- `seeknal dry-run` shows preview
- Helpful error messages
- Tutorial is comprehensive
- Examples work in fresh environment
- Performance acceptable (>100K rows in <30s)

**Estimated effort:** 2-3 hours

## Technical Specifications

### Files to Modify

| File | Change | Lines |
|------|--------|-------|
| `src/seeknal/dag/manifest.py` | Add NodeType enum | ~28 |
| `src/seeknal/workflow/validators.py` | Add schema | ~50 |
| `src/seeknal/workflow/draft.py` | Add draft support | ~100 |
| `src/seeknal/pipeline/decorators.py` | Add decorator | ~150 |
| `src/seeknal/workflow/executors/__init__.py` | Register executor | ~5 |

### Files to Create

| File | Purpose | Est. Lines |
|------|---------|------------|
| `src/seeknal/workflow/executors/second_order_aggregation_executor.py` | Main executor | 400 |
| `src/seeknal/workflow/templates/second_order_aggregation.yml.j2` | YAML template | 50 |
| `src/seeknal/workflow/templates/second_order_aggregation.py.j2` | Python template | 60 |
| `tests/workflow/test_second_order_aggregation_executor.py` | Executor tests | 300 |
| `tests/cli/test_second_order_aggregation_draft.py` | CLI tests | 150 |
| `examples/workflow/07_second_order_aggregation.yml` | Example | 80 |

### YAML Schema

```yaml
kind: second_order_aggregation
name: string                    # Required: Node name
description: string             # Optional: Human-readable description
owner: string                   # Optional: Team/person responsible
id_col: string                  # Required: Entity ID column for grouping
feature_date_col: string         # Required: Date column for time operations
application_date_col: string     # Optional: Reference date for window calculations
source: string                  # Required: Upstream aggregation (kind.name)
sources: list                   # Optional: Multiple upstream aggregations
features: map                   # Required: Feature specifications
  feature_name:
    basic: [agg_funcs]           # sum, avg, mean, min, max, count, stddev, std
    source_feature: string       # Name of feature in source to aggregate
    window: [lower, upper]       # Time window for cross-aggregation
    ratio:
      numerator: [l1, u1]
      denominator: [l2, u2]
      aggs: [funcs]
inputs: list                    # Required: Dependencies
tags: list                      # Optional: Organizational tags
materialization:                # Optional: Output configuration
  enabled: boolean
  table: string
```

### Python Decorator Signature

```python
@second_order_aggregation(
    name: str,                    # Node name
    source: str,                  # Upstream aggregation reference
    id_col: str,                  # Grouping column
    feature_date_col: str,         # Date column
    application_date_col: str = None,
    description: str = "",
    owner: str = "",
    tags: List[str] = [],
    materialization: Dict = None
)
def function_name(ctx: PipelineContext, df: pd.DataFrame) -> pd.DataFrame:
    """Docstring"""
    # Aggregation logic
    pass
```

### Executor Interface

```python
@register_executor(NodeType.SECOND_ORDER_AGGREGATION)
class SecondOrderAggregationExecutor(BaseExecutor):
    def _execute_duckdb(self) -> ExecutionResult:
        # 1. Resolve source aggregation
        # 2. Load source DataFrame from intermediate storage
        # 3. Validate source schema
        # 4. Create SecondOrderAggregator instance
        # 5. Call aggregator.transform()
        # 6. Return ExecutionResult
        pass

    def _execute_spark(self) -> ExecutionResult:
        # Similar to DuckDB but using Spark engine
        pass

    def validate(self) -> List[str]:
        # Validate source exists
        # Validate source schema
        # Validate feature specifications
        pass
```

## Alternative Approaches Considered

### Option 1: Extend Existing `aggregation` Node

**Approach:** Add `second_order` section to existing aggregation node.

**Rejected because:**
- Mixed semantics within single node type
- More complex validation logic
- Less clear intent for users
- Harder to document

### Option 2: Transform-Based with Helper Functions

**Approach:** Add `ctx.second_order_aggregate()` helper to transform nodes.

**Rejected because:**
- Less structured (no explicit schema)
- Harder to validate dependencies
- No dedicated YAML syntax
- Inconsistent with existing aggregation pattern

### Option 3: Ad-Hoc SQL in Transforms

**Approach:** Users write their own SQL for second-order aggregations.

**Rejected because:**
- No validation of source schema
- Error-prone (manual SQL writing)
- No consistency guarantee
- Reinventing the wheel

## Acceptance Criteria

### Functional Requirements

- [ ] **YAML Support**: Can define `kind: second_order_aggregation` in YAML files
- [ ] **Python Support**: Can define via `@second_order_aggregation` decorator
- [ ] **Source Resolution**: Correctly loads upstream aggregation output
- [ ] **Schema Validation**: Validates source aggregation has expected features
- [ ] **Feature Types**: Supports basic, window, ratio, percentile, delta aggregations
- [ ] **Materialization**: Can output as feature group or intermediate
- [ ] **CLI Commands**: Works with `parse`, `run`, `dry-run`, `draft`

### Non-Functional Requirements

- [ ] **Performance**: Executes >100K rows in <30 seconds with DuckDB
- [ ] **Security**: All inputs validated, no SQL injection vectors
- [ ] **Compatibility**: Works with both DuckDB and Spark engines
- [ ] **Test Coverage**: >80% code coverage for new code

### Quality Gates

- [ ] **Unit Tests**: All executor methods have unit tests
- [ ] **Integration Tests**: Full pipeline execution tested
- [ ] **Dry-Run Tests**: Preview mode works correctly
- [ ] **Documentation**: Tutorial updated with examples
- [ ] **Code Review**: Changes reviewed by maintainer

## Dependencies & Prerequisites

### Dependencies

- **Existing Code**: `src/seeknal/tasks/*/aggregators/second_order_aggregator.py`
- **NodeType System**: Executor registry pattern
- **BaseExecutor**: Common executor interface

### Prerequisites

- DuckDB or Spark installed and configured
- Existing aggregation node working (as source)
- Intermediate storage available for source output

## Risk Analysis & Mitigation

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Source schema mismatch | High | Medium | Strict validation with clear error messages |
| Performance issues | Medium | Low | Reuse optimized existing aggregator |
| Breaking changes to existing aggregation | High | Low | New node type, no changes to existing |
| Complexity of multi-source support | Medium | Low | Start with single source, extend later |

## Security Considerations

1. **Input Validation**: All user inputs validated via allowlists
2. **SQL Injection**: Use parameterized queries, escape identifiers
3. **Path Traversal**: Resolve symlinks before path validation
4. **Source References**: Validate source exists before execution

## Implementation Sketch

### Executor Flow

```python
def _execute_duckdb(self):
    # 1. Resolve source aggregation
    source_ref = self.node.config.get("source")
    source_node = self.context.get_node(source_ref)

    # 2. Load source output from intermediate storage
    source_path = self.context.get_intermediate_path(source_node)
    source_df = self.context.duckdb.read_parquet(source_path)

    # 3. Validate source schema
    self._validate_source_schema(source_df)

    # 4. Build aggregation specs from YAML
    specs = self._build_aggregation_specs()

    # 5. Create and execute aggregator
    aggregator = SecondOrderAggregator(
        idCol=self.node.config.get("id_col"),
        featureDateCol=self.node.config.get("feature_date_col"),
        specs=specs
    )
    result_df = aggregator.transform()

    # 6. Save to intermediate storage if needed
    if self.should_intermediate():
        self.context.save_intermediate(self.node, result_df)

    return ExecutionResult(
        node_id=self.node.id,
        status=ExecutionStatus.SUCCESS,
        row_count=len(result_df),
        data=result_df
    )
```

### Column Naming

Follow existing `SecondOrderAggregator` naming:
- Basic: `{feature}_{AGG}` → `amount_SUM`, `amount_AVG`
- Days: `{feature}_{AGG}_{lower}_{upper}` → `amount_SUM_1_30`
- Ratio: `{feature}_{AGG}{l1}_{u1}_{AGG}{l2}_{u2}` → `amount_SUM1_7_SUM30_90`
- Since: `SINCE_{AGG}_{feature}_GEO_D`

## Success Metrics

1. **Adoption**: 3+ teams using second-order aggregations within 1 month
2. **Performance**: 100K rows processed in <30s on commodity hardware
3. **Reliability**: <1% failure rate in production
4. **Satisfaction**: Positive feedback from at least 2 users

## Future Considerations

### v2 Potential Features

1. **Multi-source support**: Aggregate from multiple upstream aggregations
2. **Ad-hoc mode**: `ctx.second_order()` in transforms
3. **Custom aggregations**: User-defined aggregation functions
4. **Incremental**: Support for incremental second-order aggregations
5. **Streaming**: Real-time second-order aggregations

### Extensibility Points

- Custom aggregation functions
- Alternative output formats
- Plugin architecture for new aggregation types

## Documentation Plan

### Files to Update

1. **YAML Pipeline Tutorial**: Add section on second-order aggregations
2. **Python Pipeline Tutorial**: Add decorator examples
3. **API Reference**: Document new node type and decorator
4. **Examples**: Add 2-3 example pipelines

### Documentation Structure

```markdown
## Second-Order Aggregations

### What are they?
### When to use
### YAML syntax
### Python decorator
### Examples
  - Region-level metrics
  - Time-series patterns
  - Multi-level hierarchies
### Performance considerations
### Troubleshooting
```

## References & Research

### Internal References

- **Existing Aggregator**: `src/seeknal/tasks/duckdb/aggregators/second_order_aggregator.py:1-256`
- **Aggregation Executor**: `src/seeknal/workflow/executors/aggregation_executor.py:43-150`
- **NodeType System**: `src/seeknal/dag/manifest.py:18-28`
- **Executor Registry**: `src/seeknal/workflow/executors/__init__.py:44-58`
- **Decorator System**: `src/seeknal/pipeline/decorators.py:73-367`
- **Workflow Executors Pattern**: `docs/solutions/workflow-executors.md`
- **Executor Architecture**: `docs/plans/2026-01-26-executor-architecture-design.md`

### Test Patterns

- **Second-Order Tests**: `tests/sparkengine/test_second_order_aggregator.py:1-200`
- **Test Isolation**: `docs/solutions/testing/test-isolation-fixes.md`

### Related Work

- **Brainstorm**: `docs/brainstorms/2026-01-30-second-order-aggregations-brainstorm.md`
- **First-Order Aggregation**: `examples/workflow/06_aggregation_user_history.yml`

## Open Questions

1. **Multi-source in v1?** → Decision: Single source only, extend in v2
2. **Ad-hoc transform mode?** → Decision: Defer to v2
3. **Incremental support?** → Decision: Defer to v2
4. **Strict vs lenient validation?** → Decision: Start with strict

---

**Total Estimated Effort**: 11-15 hours

**Recommended Team Size**: 1 developer

**Suggested Timeline**:
- Phase 1: Day 1 (morning)
- Phase 2: Day 1 (afternoon) + Day 2 (morning)
- Phase 3: Day 2 (afternoon)
- Phase 4: Day 3
