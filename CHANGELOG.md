# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.1.0] - 2026-02-10

### Added - RUN Command Parity with SQLMesh

This release brings comprehensive workflow capabilities for production data pipelines, achieving feature parity with SQLMesh's RUN command functionality.

#### Interval Tracking
- **IntervalCalculator**: Calculate time intervals for incremental processing with cron-based scheduling
  - Support for cron expressions (`0 2 * * *`) and shorthand (`@daily`, `@hourly`)
  - Track completed intervals to prevent duplicate processing
  - Query pending intervals for backfill operations
  - 44 tests passing

#### Change Detection
- **SQL-aware diffing**: Automatically categorize changes as breaking or non-breaking
  - Column-level lineage tracking for impact analysis
  - SQLGlot integration for AST-based SQL comparison
  - Automatic downstream impact calculation
  - 266+ tests passing across all DAG modules
- **Change categories**:
  - `BREAKING` - Schema/logic changes requiring downstream rebuild
  - `NON_BREAKING` - Changes affecting only this node
  - `METADATA` - Description/format changes with no rebuild needed

#### Plan/Apply Workflow
- **Environment Manager**: Safe deployments with isolated testing environments
  - Create plans showing categorized changes before execution
  - Apply changes in isolated dev/staging environments
  - Atomic promotion from dev to production
  - Virtual environments reference production outputs for unchanged nodes
  - TTL-based automatic cleanup (default 7 days)
  - 33 tests passing

#### State Backends
- **Pluggable state backend protocol**: Support for distributed execution
  - `FileBackend` - JSON file storage (default, single-node)
  - `DatabaseBackend` - SQLite/Turso for concurrent access
  - Transactional integrity with atomic updates
  - Optimistic locking for concurrent execution
  - Migration CLI: `seeknal migrate-state --backend database`
  - 25 tests passing

#### Distributed Execution
- **Prefect Integration**: Scheduled pipeline runs with horizontal scaling
  - `seeknal_run_flow()` - Execute full pipeline as Prefect flow
  - `seeknal_backfill_flow()` - Backfill historical data across intervals
  - `create_prefect_deployment()` - Deploy flows for cron-scheduled execution
  - Built-in retry logic and flow monitoring
  - CLI: `prefect worker work-queue` for distributed execution

#### CLI Commands
- Interval tracking:
  - `seeknal intervals show` - Show completed intervals
  - `seeknal intervals pending --schedule @daily` - List pending intervals
  - `seeknal intervals complete --interval "2024-01-01"` - Mark interval complete
- Plan/Apply workflow:
  - `seeknal plan dev` - Create plan for environment
  - `seeknal env apply dev` - Execute plan in environment
  - `seeknal env promote dev prod` - Promote to production
  - `seeknal env list` - List all environments
  - `seeknal env delete dev` - Delete environment
  - `seeknal env cleanup` - Remove expired environments
- State migration:
  - `seeknal migrate-state --backend database` - Migrate state (dry-run)
  - `seeknal migrate-state --backend database --no-dry-run` - Execute migration

#### Documentation
- **[Interval Tracking Guide](docs/guides/interval-tracking.md)** - Time-series incremental processing
- **[Change Detection Guide](docs/guides/change-detection.md)** - SQL-aware change detection
- **[Plan/Apply Workflow Guide](docs/guides/plan-apply-workflow.md)** - Safe deployments
- **[State Backends Guide](docs/guides/state-backends.md)** - Pluggable state storage
- **[Distributed Execution Guide](docs/guides/distributed-execution.md)** - Prefect integration
- **[Workflow API Reference](docs/api/workflow.md)** - Complete API documentation
- Updated Getting Started Guide with workflow feature references

#### Dependencies
- `sqlglot>=20.0.0` - SQL parsing and AST analysis
- `prefect>=3.0.0` (optional) - Distributed execution

### Migration Guide

#### Upgrading from 2.0.0

No breaking changes - all existing APIs remain compatible.

To enable new features:

```bash
# Install new dependencies
pip install sqlglot

# For distributed execution (optional)
pip install prefect

# Migrate state to database backend for production
seeknal migrate-state --backend database --no-dry-run
```

#### Using New Features

```python
# Interval tracking
from seeknal.workflow.intervals import IntervalCalculator

calculator = IntervalCalculator()
intervals = calculator.calculate_intervals(
    start_date="2024-01-01",
    end_date="2024-01-31",
    schedule="@daily"
)

# Change detection
from seeknal.dag.diff import ManifestDiff

diff = ManifestDiff.compare(old_manifest, new_manifest)
to_rebuild = diff.get_nodes_to_rebuild(new_manifest)

# Plan/Apply workflow
from seeknal.workflow.environment import EnvironmentManager

manager = EnvironmentManager(target_path="target")
plan = manager.plan("dev", manifest)
result = manager.apply("dev")
manager.promote("dev", "prod")

# State backend
from seeknal.state.backend import create_state_backend

backend = create_state_backend("database", db_path="target/state.db")

# Prefect integration
from seeknal.workflow.prefect_integration import seeknal_run_flow

results = seeknal_run_flow(project_path="/path/to/project", parallel=True)
```

## [2.0.0] - 2026-01-14

### Breaking Changes
- **SparkEngineTask**: Scala-based implementation replaced with pure PySpark
  - No JVM or Scala installation required
  - Same public API - user code unchanged
  - All transformers ported to PySpark
  - Internal directory structure changed: `pyspark/` → `py_impl/` to avoid namespace collision

### Removed
- Scala spark-engine wrapper code (~3600 lines)
- `findspark` dependency (no longer needed)
- Old transformer implementations using JavaWrapper

### Added
- Pure PySpark transformer implementations:
  - Column operations: ColumnRenamed, FilterByExpr, AddColumnByExpr
  - Joins: JoinById, JoinByExpr
  - SQL: SQL transformer
  - Special: AddEntropy, AddLatLongDistance (with UDFs)
- PySpark aggregator: FunctionAggregator
- PySpark extractors: FileSource, GenericSource
- PySpark loaders: ParquetWriter
- Comprehensive test suite: 22 tests (20 unit tests + 2 integration tests)
- PySpark base classes for transformers, aggregators, extractors, loaders

### Changed
- SparkEngineTask now orchestrates PySpark transformations instead of Scala wrappers
- All transformers use PySpark DataFrame API directly
- Removed dependency on external Scala/Java compilation

### Migration
- Users: Update to v2.0.0 via pip
- No code changes required for existing users - API remains compatible
- See updated CLAUDE.md for PySpark-specific notes

## [1.0.0] - Previous Release
- Initial release with Scala-based Spark engine
