# Full-suite failure baseline — 2026-08-25

> This is a measurement, not a target or an allowance to leave failures
> unresolved. Its only purpose is to distinguish a new failure introduced by a
> change from the known state of this checkout.

## Measurement

- Commit measured: `58c9f7a`
- Invocation: `PYTHONPATH=src ../signal/.venv/bin/python -m pytest -q`
- Result: 4,944 passed, 310 skipped, 68 failed in 825.33 seconds.

For a later change, the affected subset must pass and the full-suite failure
set must not gain an ID or change an existing failure's category. This baseline
does not waive any listed failure.

## Known failures

### Ambient Atlas or configuration

These tests inherit local Atlas or configuration state even though their
assertions are not all about that state.

- `tests/ask/test_repl_ingest.py::test_auto_register_ask_ingest`
- `tests/ask/test_source_config.py::test_write_source_context_syncs_attached_database_context`
- `tests/ask/test_upload_to_s3_tool.py::test_upload_sets_pending_upload_with_server_expiry`
- `tests/ask/test_upload_to_s3_tool.py::test_filename_derived_from_question`
- `tests/ask/test_upload_to_s3_tool.py::test_upload_storage_unreachable_returns_error`
- `tests/ask/test_upload_to_s3_tool.py::test_upload_empty_result_returns_nothing`
- `tests/cli/test_auth_config.py::test_config_show_marks_env_override`

### External Iceberg services

- `tests/integration/test_iceberg_real_infra.py::TestInfrastructureValidation::test_atlas_dev_server_reachable`
- `tests/integration/test_iceberg_real_infra.py::TestInfrastructureValidation::test_minio_container_running`
- `tests/integration/test_iceberg_real_infra.py::TestInfrastructureValidation::test_lakekeeper_container_running`
- `tests/integration/test_iceberg_real_infra.py::TestInfrastructureValidation::test_lakekeeper_port_accessible`
- `tests/integration/test_iceberg_real_infra.py::TestInfrastructureValidation::test_minio_port_accessible`

### Spark runtime version mismatch

The measured run used a Python 3.11 driver with a Python 3.9 Spark worker.

- `tests/sparkengine/test_advanced_transformers.py::test_point_in_time_past`
- `tests/sparkengine/test_advanced_transformers.py::test_point_in_time_with_length`
- `tests/sparkengine/test_advanced_transformers.py::test_point_in_time_future`
- `tests/sparkengine/test_advanced_transformers.py::test_point_in_time_with_spine_dataframe`
- `tests/sparkengine/test_advanced_transformers.py::test_join_tables_by_expr_inner`
- `tests/sparkengine/test_advanced_transformers.py::test_join_tables_by_expr_left`
- `tests/sparkengine/test_advanced_transformers.py::test_join_tables_by_expr_multiple_tables`
- `tests/sparkengine/test_advanced_transformers.py::test_join_tables_by_expr_with_dataframe`
- `tests/sparkengine/test_aggregators.py::test_function_aggregator_sum`
- `tests/sparkengine/test_aggregators.py::test_function_aggregator_count`
- `tests/sparkengine/test_aggregators.py::test_function_aggregator_group_by`
- `tests/sparkengine/test_column_operations_transformer.py::test_column_renamed`
- `tests/sparkengine/test_column_operations_transformer.py::test_column_renamed_multiple`
- `tests/sparkengine/test_column_operations_transformer.py::test_filter_by_expr`
- `tests/sparkengine/test_column_operations_transformer.py::test_add_column_by_expr`
- `tests/sparkengine/test_e2e_pipelines.py::test_full_pipeline`
- `tests/sparkengine/test_e2e_pipelines.py::test_pipeline_with_join`
- `tests/sparkengine/test_extractors.py::test_file_source_parquet`
- `tests/sparkengine/test_extractors.py::test_generic_source`
- `tests/sparkengine/test_join_transformers.py::test_join_by_id`
- `tests/sparkengine/test_join_transformers.py::test_join_by_expr`
- `tests/sparkengine/test_join_transformers.py::test_join_by_id_left`
- `tests/sparkengine/test_loaders.py::test_parquet_writer`
- `tests/sparkengine/test_second_order_aggregator.py::test_second_order_aggregator_basic`
- `tests/sparkengine/test_second_order_aggregator.py::test_second_order_aggregator_basic_days`
- `tests/sparkengine/test_second_order_aggregator.py::test_second_order_aggregator_ratio`
- `tests/sparkengine/test_second_order_aggregator.py::test_second_order_aggregator_since`
- `tests/sparkengine/test_second_order_aggregator.py::test_feature_builder_basic`
- `tests/sparkengine/test_second_order_aggregator.py::test_feature_builder_rolling`
- `tests/sparkengine/test_second_order_aggregator.py::test_feature_builder_ratio`
- `tests/sparkengine/test_second_order_aggregator.py::test_feature_builder_since`
- `tests/sparkengine/test_second_order_aggregator.py::test_feature_builder_multiple_features`
- `tests/sparkengine/test_second_order_aggregator.py::test_feature_builder_chained`
- `tests/sparkengine/test_spark_engine_task.py::test_spark_engine_task_simple_pipeline`
- `tests/sparkengine/test_spark_engine_task.py::test_spark_engine_task_with_output`
- `tests/sparkengine/test_special_transformers.py::test_add_entropy`
- `tests/sparkengine/test_special_transformers.py::test_add_latlong_distance`
- `tests/sparkengine/test_sql_transformer.py::test_sql_transformer`
- `tests/sparkengine/test_sql_transformer.py::test_sql_transformer_custom_view`

### Iceberg dependency drift or unavailable runtime API

The measured runtime lacks `DuckDBIcebergExtension.create_rest_catalog`; related
Iceberg E2E cases also need a configured Spark context.

- `tests/e2e/test_iceberg_e2e.py::TestIcebergFeatureGroupCreation::test_create_feature_group_with_iceberg_storage`
- `tests/e2e/test_iceberg_e2e.py::TestIcebergWriteOperations::test_write_features_in_append_mode`
- `tests/e2e/test_iceberg_e2e.py::TestIcebergWriteOperations::test_write_features_in_overwrite_mode`
- `tests/e2e/test_iceberg_e2e.py::TestIcebergBackwardCompatibility::test_hive_table_still_works`
- `tests/e2e/test_iceberg_e2e.py::TestIcebergBackwardCompatibility::test_file_storage_still_works`
- `tests/e2e/test_iceberg_e2e.py::TestIcebergDeleteOperation::test_delete_iceberg_table`
- `tests/e2e/test_iceberg_feature_group_e2e.py::TestIcebergFeatureGroupCreation::test_create_feature_group_with_iceberg_storage`
- `tests/e2e/test_iceberg_feature_group_e2e.py::TestIcebergWriteOperation::test_write_to_iceberg_append_mode`
- `tests/e2e/test_version_materialization.py::TestVersionMaterializationWorkflow::test_materialize_with_version_displays_parameters`
- `tests/e2e/test_version_materialization.py::TestVersionMaterializationWorkflow::test_materialize_with_different_versions`
- `tests/e2e/test_version_materialization.py::TestVersionMaterializationWorkflow::test_materialize_without_version_uses_latest`
- `tests/e2e/test_version_materialization.py::TestVersionRollbackWorkflow::test_version_rollback_materialize_older_version`
- `tests/e2e/test_version_materialization.py::TestVersionCLIIntegration::test_materialize_help_shows_version_option`
- `tests/e2e/test_version_materialization.py::TestVersionBackwardCompatibility::test_materialize_without_version_works`
- `tests/e2e/test_version_materialization.py::TestVersionBackwardCompatibility::test_existing_cli_commands_still_work`

### Genuine code or assertion failures

- `tests/ask/test_skills.py::TestAgentUsesSkillDirectories::test_base_prompt_is_lean`
  - Measured at 169 lines. This predates action delivery: the same count was
    present at `5a391e0`.
- `tests/cli/test_run.py::TestSeeknalRunErrorHandling::test_missing_dependency`
  - The asserted missing-dependency text is absent from the observed CLI output.
