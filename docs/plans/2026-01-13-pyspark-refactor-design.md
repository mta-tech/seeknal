# PySpark Refactor Design Document

**Date**: 2026-01-13
**Status**: Design Phase
**Author**: AI Design Assistant
**Type**: Major Refactoring

## Executive Summary

Replace the Scala-based Spark Engine with a pure PySpark implementation, eliminating JVM and Gradle dependencies while maintaining 100% feature parity. This refactoring simplifies deployment (pip install only) and reduces maintenance burden (single language codebase).

## Goals

1. **Remove Scala/JVM dependency** - No JVM, Scala compiler, or Gradle required
2. **Pure Python codebase** - All Spark logic implemented in PySpark
3. **Full feature parity** - All 26 transformers, aggregators, and connectors
4. **No breaking changes to public API** - Existing user code continues to work
5. **Test-first validation** - Comprehensive tests ensure identical behavior

## Current Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      User Code                               │
│  SparkEngineTask.add_stage().transform().evaluate()         │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│              Python Bindings (JavaWrapper)                   │
│  src/seeknal/tasks/sparkengine/                             │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                 Scala Spark Engine                           │
│  engines/spark-engine/ (Gradle build, JVM)                  │
│  - SparkEngine.scala                                         │
│  - SparkEngineAPI.scala                                      │
│  - 26 transformers, aggregators, connectors                 │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                    Apache Spark                              │
└─────────────────────────────────────────────────────────────┘
```

## Target Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      User Code                               │
│  SparkEngineTask.add_stage().transform().evaluate()         │
│                    (UNCHANGED API)                           │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                PySpark Implementation                        │
│  src/seeknal/tasks/sparkengine/pyspark/                     │
│  - Pure Python, no JavaWrapper                              │
│  - DataFrame API, SQL, UDFs                                 │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                 PySpark (Python Spark)                       │
└─────────────────────────────────────────────────────────────┘
```

## Component Implementation Strategy

### Transformers (26 types)

Most map directly to PySpark DataFrame operations:

| Category | PySpark Equivalent |
|----------|-------------------|
| Column operations | `df.withColumn()`, `df.withColumnRenamed()` |
| Filtering | `df.filter()` / `df.where()` |
| Joins | `df.join()` |
| Aggregations | `df.groupBy().agg()` |
| Window functions | `Window.spec + F.over()` |
| SQL | `spark.sql()` |
| Date operations | `F.date_add()`, `F.datediff()`, `F.dayofweek()` |

**Special cases requiring UDFs**:
- `AddEntropy` - entropy calculation
- `AddLatLongDistance` - haversine distance

### Aggregators

- `FunctionAggregator` → PySpark built-ins (`F.sum()`, `F.count()`, `F.avg()`)
- `ExpressionAggregator` → Spark SQL expressions
- `DayTypeAggregator` → Date filtering + standard aggregation

### Extractors & Loaders

Native PySpark readers/writers:
- `FileSource` → `spark.read.parquet/csv/json()`
- `GenericSource` → `spark.read.format()`
- `HiveSource` → `spark.table()`
- `ParquetWriter` → `df.write.parquet()`
- `HiveLoader` → `df.write.saveAsTable()`

### SerDe

- **Avro**: `spark-avro` package (pure Python)
- **JSON**: Native PySpark support
- **FeatureStore**: PySpark schema APIs

## File Structure

```
src/seeknal/tasks/sparkengine/
├── pyspark/                          # NEW: Pure PySpark implementation
│   ├── __init__.py
│   ├── base.py                       # Base classes
│   ├── spark_engine_task.py          # SparkEngineTask (no JavaWrapper)
│   ├── transformers/
│   │   ├── base.py
│   │   ├── column_operations.py      # ColumnRenamed, AddColumnByExpr
│   │   ├── filtering.py              # FilterByExpr
│   │   ├── joins.py                  # JoinByExpr, JoinById
│   │   ├── windows.py                # AddWindowFunction
│   │   ├── dates.py                  # Date operations
│   │   ├── special.py                # AddEntropy, AddLatLongDistance (UDFs)
│   │   └── sql.py                    # SQL transformer
│   ├── aggregators/
│   │   ├── base.py
│   │   ├── function_aggregator.py
│   │   ├── expression_aggregator.py
│   │   └── day_type_aggregator.py
│   ├── extractors/
│   │   ├── file_source.py
│   │   ├── generic_source.py
│   │   └── hive_source.py
│   ├── loaders/
│   │   ├── parquet_writer.py
│   │   ├── hive_loader.py
│   │   └── generic_loader.py
│   └── serde/
│       ├── avro_serde.py
│       ├── json_serde.py
│       └── feature_store_serde.py

engines/spark-engine/                 # DELETE: Entire directory
```

## Implementation Example

### Before (Scala + JavaWrapper)

```python
from pyspark.sql import SparkSession
from seeknal.tasks.sparkengine import SparkEngineTask

spark = SparkSession.builder.getOrCreate()

# Configures pipeline in Python, delegates to Scala
task = SparkEngineTask(spark)
task.add_input("path/to/data")
    .add_stage("filter", "FilterByExpr", expression="col > 0")
    .transform()
```

### After (Pure PySpark)

```python
from pyspark.sql import SparkSession
from seeknal.tasks.sparkengine import SparkEngineTask

spark = SparkSession.builder.getOrCreate()

# Same API, pure PySpark implementation
task = SparkEngineTask(spark)
task.add_input("path/to/data")
    .add_stage("filter", "FilterByExpr", expression="col > 0")
    .transform()
```

**Internal change**:

```python
# OLD: JavaWrapper bridge
def transform(self):
    java_api = self._new_java_obj("tech.mta.seeknal.SparkEngineAPI")
    return java_api.transform(self._to_yaml())

# NEW: Direct PySpark
def transform(self):
    df = self._get_input_dataframe()
    for stage in self.stages:
        transformer = stage.get_transformer()
        df = transformer.transform(df)
    return df
```

## Testing Strategy (Test-First)

### Phase 1: Capture Baseline

Create test harness to capture Scala engine behavior:

```python
# tests/sparkengine/test_baseline_capture.py
class TestBaselineCapture:
    @pytest.fixture(scope="module")
    def scala_engine_baseline(self):
        # Run each transformer on sample data
        # Capture: schema, row count, sample values
        # Save to tests/fixtures/baseline/
```

### Phase 2: Comparison Tests

Verify PySpark produces identical results:

```python
# tests/sparkengine/test_pyspark_implementation.py
class TestPySparkImplementation:
    def test_transformer_filter_by_expr(self, scala_baseline):
        pyspark_result = FilterByExpr(...).transform(df)
        self.assert_dataframes_equal(scala_baseline, pyspark_result)
```

### Phase 3: Integration Tests

End-to-end pipeline tests with real YAML configs from examples.

## Migration Plan (Big Bang Refactor)

### Phase 1: Pre-Refactoring (Week 1)

1. Create test suite
   - Add `tests/sparkengine/test_baseline_capture.py`
   - Run Scala engine on all 26 transformers
   - Save baseline outputs

2. Create comparison framework
   - `assert_dataframes_equal()` utility
   - Schema comparison, tolerance for floating-point

3. Document current behavior

### Phase 2: Core Implementation (Week 2-3)

1. Create `src/seeknal/tasks/sparkengine/pyspark/`
2. Implement base classes
3. Port transformers (simple → complex)
4. Port aggregators
5. Port extractors, loaders, SerDe
6. Run comparison tests after each component

### Phase 3: Integration (Week 4)

1. Rewrite `SparkEngineTask` (remove JavaWrapper)
2. Implement pipeline execution
3. Run integration tests

### Phase 4: Cutover (Week 5)

1. Replace old implementation files
2. Delete `engines/spark-engine/`
3. Remove Gradle files
4. Final validation
5. Documentation updates

## Dependencies

### Add

```python
dependencies = [
    "pyspark>=3.0.0",
    "spark-avro",  # Avro support
]
```

### Remove

- `findspark` (no JVM to find)
- JVM installation requirement
- Scala/Gradle from documentation

## Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| Semantic differences (nulls, floats, dates) | Baseline tests include edge cases |
| Performance differences | Benchmark tests, document changes |
| Missing PySpark features | Use Spark SQL fallback, write UDFs |
| Avro compatibility | Use `spark-avro` package, test thoroughly |
| Breaking changes for users | Major version bump, migration guide |

### Rollback Plan

- Tag pre-refactor commit as `v2.9.x-scala-final`
- Can revert if critical issues found

## Success Criteria

1. All existing tests pass with PySpark implementation
2. Baseline comparison tests show identical results
3. Integration tests with real YAML configs succeed
4. No JVM/Scala dependencies in final codebase
5. Documentation reflects pure Python setup
6. Performance within acceptable range

## Backward Compatibility

- **Public API**: Unchanged - `SparkEngineTask` interface identical
- **YAML configs**: Compatible - existing pipeline configs work
- **Breaking change**: Major version bump (v3.0.0)
- **Migration**: Users update package, no code changes needed

## Open Questions

1. Should we keep the Scala engine as a fallback option? → **No** (big bang)
2. Do we need to support all 26 transformers? → **Yes** (full parity)
3. How to handle UDFs for complex logic? → **Python UDFs** (native PySpark)

## Next Steps

1. Review and approve this design
2. Set up isolated worktree for implementation
3. Create detailed implementation plan with task breakdown
4. Begin Phase 1: Baseline test capture
