# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
