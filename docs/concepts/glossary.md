# Glossary

Definitions of key terms used throughout Seeknal documentation, organized alphabetically.

---

## Aggregation

A node type that computes aggregate statistics (sum, count, average, etc.) over grouped data. Aggregations are first-level aggregations that take a source or transform as input and produce summarized output at the entity level.

**YAML kind:** `aggregation`
**See also:** [Second-Order Aggregation](#second-order-aggregation), [Transform](#transform), [YAML Schema Reference](../reference/yaml-schema.md)

---

## Apply

The act of executing a plan in an environment. After reviewing changes with `seeknal plan` or `seeknal dry-run`, use `seeknal apply` to move validated YAML files to production and update the manifest. This is the final step in the draft-dry-run-apply workflow.

**CLI command:** `seeknal apply <file.yml>`, `seeknal env apply`
**See also:** [Draft](#draft), [Dry Run](#dry-run), [Plan](#plan), [Virtual Environments](virtual-environments.md)

---

## Audit

The process of reviewing and validating pipeline execution history, data quality, and compliance requirements. Seeknal supports querying historical data for regulatory requirements through time travel capabilities in Iceberg materialization.

**See also:** [Iceberg Materialization](#iceberg-materialization), [Validation](#validation)

---

## Cache

Temporary storage of execution results to improve performance. Seeknal caches execution state and can reuse intermediate results when nodes haven't changed based on fingerprint comparison.

**See also:** [Fingerprint](#fingerprint), [State](#state)

---

## Change Categorization

A system for classifying changes to pipelines into three categories: BREAKING (incompatible schema changes), NON_BREAKING (compatible additions), and METADATA (non-functional changes). This helps teams understand the impact of changes before applying them.

**Categories:** `BREAKING`, `NON_BREAKING`, `METADATA`
**See also:** [Change Categorization Guide](change-categorization.md), [Dry Run](#dry-run)

---

## DAG

Directed Acyclic Graph - a graph structure where nodes (sources, transforms, feature groups) are connected by directed edges with no cycles. Seeknal uses DAGs to represent pipeline dependencies and determine execution order through topological sorting.

**See also:** [Node](#node), [Topological Layer](#topological-layer), [Manifest](#manifest)

---

## Data Leakage

A critical error in ML where future information leaks into training data, causing models to overfit. Seeknal prevents data leakage using point-in-time joins that ensure features are computed only from data available at the time of prediction.

**See also:** [Point-in-Time Join](#point-in-time-join), [Feature Start Time](#feature-start-time)

---

## Dimension

In the context of semantic models, a dimension is a categorical attribute used for grouping and filtering data (e.g., customer_country, product_category). Dimensions are used to slice metrics in business intelligence queries.

**See also:** [Metric](#metric), [Semantic Model](#semantic-model)

---

## Draft

A template-generated YAML file for creating new pipeline nodes. Created using `seeknal draft <type> <name>`, draft files follow the naming convention `draft_<type>_<name>.yml` and must be validated with dry-run before applying.

**CLI command:** `seeknal draft <type> <name>`
**File naming:** `draft_<type>_<name>.yml`
**See also:** [Dry Run](#dry-run), [Apply](#apply)

---

## Dry Run

A validation and preview step that tests YAML files without applying them to production. Executes the pipeline with a limited row count to catch errors early and preview output before applying changes.

**CLI command:** `seeknal dry-run <file.yml>`
**Flags:** `--limit <n>`, `--timeout <s>`, `--schema-only`
**See also:** [Draft](#draft), [Apply](#apply), [Validation](#validation)

---

## DuckDB

A lightweight, in-process SQL database engine used by Seeknal for data processing. DuckDB is the preferred engine for datasets under 100M rows, offering pure Python implementation with no JVM requirement and fast performance for single-node deployments.

**Storage format:** Parquet + JSON metadata
**Module:** `src/seeknal/featurestore/duckdbengine/`
**See also:** [Task](#task), [Spark Engine](#spark-engine)

---

## Entity

A domain object with a unique identifier, used as the join key for feature groups. Entities represent business concepts like customers, products, or transactions. Each feature group is associated with one entity.

**Python class:** `Entity`
**YAML field:** `entity.name`, `entity.join_keys`
**See also:** [Feature Group](#feature-group), [Join Keys](#join-keys)

---

## Environment

An isolated namespace for deploying different versions of pipelines (e.g., dev, staging, prod). Virtual environments enable teams to test changes safely before promoting to production.

**CLI command:** `seeknal env create`, `seeknal env apply`, `seeknal env promote`
**See also:** [Virtual Environments](virtual-environments.md), [Promote](#promote)

---

## Exposure

A pipeline output that exposes data to downstream consumers such as APIs, dashboards, or file exports. Exposures document how pipeline data is used and help track data lineage.

**YAML kind:** `exposure`
**See also:** [Node](#node), [DAG](#dag)

---

## Feature Group

A container for related ML features with shared entity keys, materialization config, and versioning. Feature groups automatically version on schema changes and support both offline (batch) and online (serving) stores.

**YAML kind:** `feature_group`
**Python class:** `FeatureGroup`, `FeatureGroupDuckDB`
**CLI commands:** `seeknal list feature-groups`, `seeknal delete feature-group <name>`
**See also:** [Entity](#entity), [Materialization](#materialization), [Offline Store](#offline-store), [Online Store](#online-store)

---

## Feature Start Time

The timestamp from which features should be materialized. Used in feature group materialization to specify the earliest point in time for feature computation, enabling backfilling and incremental updates.

**Python parameter:** `feature_start_time`
**See also:** [Materialization](#materialization), [Point-in-Time Join](#point-in-time-join)

---

## Fingerprint

A hash computed from a node's definition (SQL, dependencies, parameters) used for change detection. Seeknal compares fingerprints between runs to determine which nodes need re-execution, enabling incremental execution.

**See also:** [State](#state), [Cache](#cache), [Parallel Execution](#parallel-execution)

---

## Flow

A data transformation pipeline that chains multiple tasks together. Flows can mix Spark and DuckDB tasks, executing them sequentially to transform data from input to output.

**Python class:** `Flow`
**Definition:** `Flow(name, input, tasks, output)`
**See also:** [Task](#task), [DuckDB](#duckdb), [Spark Engine](#spark-engine)

---

## Iceberg Materialization

Persisting pipeline outputs to Apache Iceberg table format with ACID transactions, time travel, and schema evolution. Enables atomic commits, rollback capabilities, and querying historical data snapshots.

**Configuration file:** `~/.seeknal/profiles.yml`
**CLI commands:** `seeknal iceberg validate-materialization`, `seeknal iceberg snapshot-list`
**See also:** [Iceberg Materialization Guide](../iceberg-materialization.md), [Materialization](#materialization)

---

## Join Keys

The column(s) used to join features to the target dataset in a feature group. Join keys uniquely identify the entity and are specified in the entity definition.

**YAML field:** `entity.join_keys`
**Example:** `["customer_id"]`, `["msisdn", "movement_type"]`
**See also:** [Entity](#entity), [Feature Group](#feature-group)

---

## Manifest

A JSON file containing the parsed DAG representation of all pipeline nodes with their dependencies, execution order, and metadata. Generated by `seeknal plan` or `seeknal parse` and used by `seeknal run` for execution.

**CLI command:** `seeknal plan`, `seeknal parse`
**See also:** [DAG](#dag), [Plan](#plan), [Topological Layer](#topological-layer)

---

## Materialization

The process of computing and persisting features to storage. Includes offline materialization (batch processing for training data) and online materialization (low-latency serving for inference). Supports multiple modes including append and overwrite.

**Python class:** `Materialization`, `OfflineMaterialization`
**YAML field:** `materialization`
**Modes:** `append`, `overwrite`
**See also:** [Feature Group](#feature-group), [Offline Store](#offline-store), [Online Store](#online-store), [Iceberg Materialization](#iceberg-materialization)

---

## Metric

A quantitative measure computed from data (e.g., total_revenue, average_order_value). In semantic models, metrics are aggregations of measures that can be sliced by dimensions.

**See also:** [Dimension](#dimension), [Semantic Model](#semantic-model)

---

## Model

A machine learning model definition that consumes features and produces predictions. Models are nodes in the pipeline DAG and can depend on feature groups and transforms.

**YAML kind:** `model`
**See also:** [Feature Group](#feature-group), [Node](#node)

---

## Node

A single unit in the pipeline DAG representing a source, transform, feature group, aggregation, model, rule, or exposure. Each node has a unique name, type (kind), dependencies (inputs), and execution logic.

**Supported kinds:** `source`, `transform`, `feature_group`, `aggregation`, `second_order_aggregation`, `model`, `rule`, `exposure`
**See also:** [DAG](#dag), [Topological Layer](#topological-layer)

---

## Offline Store

Storage backend for batch feature computation used in training data. Supports file-based storage (Parquet, CSV) and cloud storage (S3, GCS). Optimized for large-scale batch processing with higher latency than online stores.

**Python class:** `OfflineStore`, `OfflineStoreEnum`
**Storage formats:** `FILE` (Parquet), cloud storage (S3, GCS)
**See also:** [Materialization](#materialization), [Online Store](#online-store), [Feature Group](#feature-group)

---

## Online Store

Storage backend for low-latency feature serving used in inference. Optimized for real-time feature retrieval with TTL (time-to-live) support for feature expiration.

**Python class:** `OnlineStore`, `OnlineStoreEnum`
**Features:** Low-latency retrieval, TTL support
**See also:** [Materialization](#materialization), [Offline Store](#offline-store), [Feature Group](#feature-group)

---

## Parallel Execution

The ability to execute independent nodes concurrently within the same topological layer. Seeknal identifies nodes without dependencies and runs them in parallel to reduce total execution time.

**See also:** [Topological Layer](#topological-layer), [DAG](#dag), [State](#state)

---

## Plan

A preview of changes and execution order before running the pipeline. The plan command generates a manifest showing the DAG structure, topological layers, and identifies which nodes will execute based on state comparison.

**CLI command:** `seeknal plan`, `seeknal parse`
**See also:** [Manifest](#manifest), [Apply](#apply), [Dry Run](#dry-run)

---

## Point-in-Time Join

A temporal join that ensures features are computed only from data available at the prediction time, preventing data leakage. Seeknal uses the event_time_col to perform point-in-time correct joins when materializing historical features.

**Configuration:** `materialization.event_time_col`
**See also:** [Point-in-Time Joins Guide](point-in-time-joins.md), [Data Leakage](#data-leakage), [Materialization](#materialization)

---

## Promote

The act of moving a validated pipeline from one environment to another (e.g., dev to staging to prod). Promotion copies environment-specific configurations and validates compatibility before deployment.

**CLI command:** `seeknal env promote --from dev --to prod`
**See also:** [Environment](#environment), [Virtual Environments](virtual-environments.md), [Apply](#apply)

---

## Rule

A validation or business rule node that checks data quality constraints. Rules can enforce data integrity, completeness, and business logic requirements within the pipeline.

**YAML kind:** `rule`
**See also:** [Validation](#validation), [Node](#node)

---

## Second-Order Aggregation

An aggregation that computes summary statistics over first-order aggregations. For example, computing regional totals from user-level metrics. Second-order aggregations support hierarchical rollups and multi-level analytics.

**YAML kind:** `second_order_aggregation`
**Example use case:** Region-level totals from user-level metrics
**See also:** [Aggregation](#aggregation), [YAML Pipeline Tutorial](../tutorials/yaml-pipeline-tutorial.md)

---

## Semantic Model

A business-oriented abstraction layer that defines metrics, dimensions, and their relationships. Semantic models enable non-technical users to query data using business terminology without writing SQL.

**See also:** [Metric](#metric), [Dimension](#dimension), [Semantic Layer Guide](../guides/semantic-layer.md)

---

## Source

A node representing raw data input from external systems. Sources define the schema, connection parameters, and location of data files or database tables.

**YAML kind:** `source`
**Source types:** `csv`, `parquet`, `json`, `postgresql`, etc.
**See also:** [Node](#node), [Transform](#transform)

---

## Spark Engine

Apache Spark implementation for distributed data processing. Supports Delta Lake format and is optimized for datasets over 100M rows requiring distributed processing. Requires JVM installation.

**Storage format:** Delta Lake
**Module:** `src/seeknal/tasks/sparkengine/`
**Python class:** `SparkEngineTask`
**See also:** [DuckDB](#duckdb), [Task](#task), [Flow](#flow)

---

## State

Execution history tracking that stores fingerprints of previously executed nodes. State enables incremental execution by comparing current definitions with previous runs to identify changed nodes.

**Storage:** SQLite/Turso database
**See also:** [Fingerprint](#fingerprint), [Cache](#cache), [Parallel Execution](#parallel-execution)

---

## Task

A unit of data transformation logic within a Flow. Tasks can be DuckDB tasks (DuckDBTask) or Spark tasks (SparkEngineTask) and contain SQL statements or transformation operations.

**Python classes:** `DuckDBTask`, `SparkEngineTask`
**Usage:** `task.add_sql("SELECT * FROM __THIS__")`
**See also:** [Flow](#flow), [DuckDB](#duckdb), [Spark Engine](#spark-engine)

---

## Topological Layer

A group of nodes at the same dependency depth in the DAG. Nodes in the same layer have no dependencies on each other and can be executed in parallel. Layers are ordered from 0 (sources) to N (final outputs).

**Example:** Layer 0: sources, Layer 1: transforms, Layer 2: feature groups
**See also:** [DAG](#dag), [Parallel Execution](#parallel-execution), [Manifest](#manifest)

---

## Transform

A SQL transformation node that processes data from sources or other transforms. Transforms define reusable SQL logic and support DuckDB SQL syntax including CTEs, window functions, and joins.

**YAML kind:** `transform`
**YAML field:** `transform` (contains SQL)
**Reference syntax:** `ref: kind.name` (e.g., `source.customers`)
**See also:** [Source](#source), [Feature Group](#feature-group), [Node](#node)

---

## Validation

The process of checking data quality, schema correctness, and business rule compliance. Validation occurs during dry-run for YAML syntax and schema, and via rule nodes for data quality constraints.

**CLI command:** `seeknal dry-run` (YAML validation), `seeknal validate-features` (feature validation)
**See also:** [Dry Run](#dry-run), [Rule](#rule), [Audit](#audit)

---

## Virtual Environments

Isolated namespaces for deploying different versions of pipelines (dev, staging, prod). Each environment maintains separate state, allowing teams to test changes in isolation before promoting to production.

**CLI commands:** `seeknal env create`, `seeknal env list`, `seeknal env promote`
**See also:** [Virtual Environments Guide](virtual-environments.md), [Environment](#environment), [Promote](#promote)

---

## Workspace

A project context that contains configuration, database connection, and environment settings. Every Seeknal operation requires a workspace context, established via decorators like `@require_workspace`.

**Python decorator:** `@require_workspace`
**See also:** [Project](#project), [Environment](#environment)

---

## Project

A logical grouping of pipelines, feature groups, and configurations stored in the database. Projects provide isolation between different teams or use cases and are required for most Seeknal operations.

**Python class:** `Project`
**Python decorator:** `@require_project`
**CLI command:** `seeknal init --name <project_name>`
**See also:** [Workspace](#workspace), [Entity](#entity)

---

*Last updated: 2026-02-09*
