"""PyIceberg-backed mutations for advanced Iceberg materialization modes."""

from __future__ import annotations

import os
import re
import math
from collections import Counter
from dataclasses import dataclass
from typing import Any, Optional, Sequence

from seeknal.workflow.materialization.config import CatalogConfig


class IcebergMutationError(Exception):
    """Raised when an advanced Iceberg mutation cannot be completed safely."""

    failure_category = "preflight_failed"


class IcebergCommitError(IcebergMutationError):
    """Raised when publication fails and the remote commit status may be unknown."""

    def __init__(self, message: str, failure_category: str) -> None:
        super().__init__(message)
        self.failure_category = failure_category


@dataclass(frozen=True)
class IcebergMutationResult:
    """Internal result returned to the public materialization writer."""

    snapshot_id: Optional[str]
    row_count: int
    inserted_row_count: int
    updated_row_count: Optional[int]
    affected_partition_count: Optional[int]
    outcome: str
    snapshot_verified: bool


def _quoted_relation(name: str) -> str:
    parts = name.split(".")
    if not parts or any(not part for part in parts):
        raise IcebergMutationError(f"Invalid DuckDB source relation: {name!r}")
    return ".".join(f'"{part.replace(chr(34), chr(34) * 2)}"' for part in parts)


def _table_identifier(catalog_name: str, table_name: str) -> tuple[str, ...]:
    parts = tuple(table_name.split("."))
    if len(parts) == 3:
        return parts[1:]
    return parts


def _version_tuple(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in re.findall(r"\d+", version)[:3])


def _validate_pyiceberg_capabilities() -> None:
    try:
        import pyiceberg
        from pyiceberg.io.pyarrow import (
            _check_pyarrow_schema_compatible,
            _pyarrow_to_schema_without_ids,
        )
        from pyiceberg.schema import assign_fresh_schema_ids
        from pyiceberg.table import Table, Transaction
        from pyiceberg.table.update import AssertRefSnapshotId
    except ImportError as exc:
        raise IcebergMutationError(
            "Advanced Iceberg writes require PyIceberg 0.10.0 or newer with PyArrow write support"
        ) from exc

    if _version_tuple(pyiceberg.__version__) < (0, 10, 0):
        raise IcebergMutationError(
            f"Advanced Iceberg writes require PyIceberg 0.10.0 or newer; found {pyiceberg.__version__}"
        )

    required_callables = (
        _check_pyarrow_schema_compatible,
        _pyarrow_to_schema_without_ids,
        assign_fresh_schema_ids,
        AssertRefSnapshotId,
        getattr(Table, "upsert", None),
        getattr(Table, "dynamic_partition_overwrite", None),
        getattr(Transaction, "upsert", None),
        getattr(Transaction, "dynamic_partition_overwrite", None),
        getattr(Transaction, "_apply", None),
    )
    if not all(callable(candidate) for candidate in required_callables):
        raise IcebergMutationError(
            "Installed PyIceberg lacks APIs required for atomic upsert and insert_overwrite"
        )


def _load_catalog(catalog_name: str, config: CatalogConfig) -> Any:
    if not config.verify_tls:
        raise IcebergMutationError(
            "Advanced Iceberg writes do not support verify_tls=false; "
            "configure a trusted CA for the PyIceberg client"
        )

    from pyiceberg.catalog import load_catalog

    properties: dict[str, str] = {
        "type": getattr(config.type, "value", config.type),
        "uri": config.uri,
        "warehouse": config.warehouse,
    }
    if config.bearer_token:
        properties["token"] = config.bearer_token

    environment_properties = {
        "AWS_ENDPOINT_URL": "s3.endpoint",
        "AWS_REGION": "s3.region",
        "AWS_ACCESS_KEY_ID": "s3.access-key-id",
        "AWS_SECRET_ACCESS_KEY": "s3.secret-access-key",
        "AWS_SESSION_TOKEN": "s3.session-token",
    }
    for variable, property_name in environment_properties.items():
        if value := os.environ.get(variable):
            properties[property_name] = value

    try:
        return load_catalog(catalog_name, **properties)
    except Exception as exc:
        raise IcebergMutationError(
            f"Failed to connect to Iceberg catalog at {config.uri}: {exc}"
        ) from exc


def _freeze_arrow_batch(con: Any, view_name: str) -> Any:
    try:
        result = con.execute(f"SELECT * FROM {_quoted_relation(view_name)}")
        if hasattr(result, "fetch_arrow_table"):
            return result.fetch_arrow_table()
        return result.arrow().read_all()
    except Exception as exc:
        raise IcebergMutationError(
            f"Failed to freeze source relation {view_name!r} as an Arrow batch: {exc}"
        ) from exc


def _validate_columns(batch: Any, columns: Sequence[str], label: str) -> None:
    missing = [column for column in columns if column not in batch.column_names]
    if missing:
        raise IcebergMutationError(
            f"{label} columns are missing from the source batch: {', '.join(missing)}"
        )


def _validate_unique_column_names(batch: Any) -> None:
    duplicates = [name for name, count in Counter(batch.column_names).items() if count > 1]
    if duplicates:
        raise IcebergMutationError(
            f"Source batch contains duplicate column names: {', '.join(sorted(duplicates))}"
        )


def _validate_non_null(batch: Any, columns: Sequence[str], label: str) -> None:
    null_columns = [column for column in columns if batch[column].null_count]
    if null_columns:
        raise IcebergMutationError(
            f"{label} columns must not contain null values: {', '.join(null_columns)}"
        )


def _validate_finite(batch: Any, columns: Sequence[str], label: str) -> None:
    for column in columns:
        if any(
            isinstance(value, float) and not math.isfinite(value)
            for value in batch[column].to_pylist()
        ):
            raise IcebergMutationError(
                f"{label} columns must contain only finite values: {column}"
            )


def _row_tuples(batch: Any, columns: Sequence[str]) -> list[tuple[Any, ...]]:
    values = [batch[column].to_pylist() for column in columns]
    return list(zip(*values))


def _validate_source_keys(batch: Any, unique_keys: Sequence[str]) -> None:
    _validate_columns(batch, unique_keys, "unique_keys")
    _validate_non_null(batch, unique_keys, "unique_keys")
    _validate_finite(batch, unique_keys, "unique_keys")
    duplicates = [key for key, count in Counter(_row_tuples(batch, unique_keys)).items() if count > 1]
    if duplicates:
        raise IcebergMutationError(
            "Source batch contains duplicate rows for unique_keys; no upsert was executed"
        )


def _validate_schema(table: Any, batch: Any) -> Any:
    try:
        from pyiceberg.io.pyarrow import _check_pyarrow_schema_compatible
    except ImportError as exc:
        raise IcebergMutationError("PyArrow support is required for advanced Iceberg writes") from exc

    target_names = list(table.schema().column_names)
    source_names = list(batch.column_names)
    if set(target_names) != set(source_names):
        missing = sorted(set(target_names) - set(source_names))
        extra = sorted(set(source_names) - set(target_names))
        details = []
        if missing:
            details.append(f"missing columns: {', '.join(missing)}")
        if extra:
            details.append(f"extra columns: {', '.join(extra)}")
        raise IcebergMutationError("Source schema does not match target schema (" + "; ".join(details) + ")")

    reordered = batch.select(target_names)
    try:
        _check_pyarrow_schema_compatible(
            table.schema(),
            reordered.schema,
            format_version=table.metadata.format_version,
        )
    except (TypeError, ValueError) as exc:
        raise IcebergMutationError(f"Source schema is incompatible with target schema: {exc}") from exc
    return reordered


def _identity_partition_columns(table: Any, mode: str) -> list[str]:
    try:
        from pyiceberg.transforms import IdentityTransform
    except ImportError as exc:
        raise IcebergMutationError("PyIceberg partition support is unavailable") from exc

    specs = table.metadata.specs()
    if len(specs) != 1:
        raise IcebergMutationError(f"Partition spec evolution is not supported for {mode}")
    spec = table.spec()
    columns: list[str] = []
    for field in spec.fields:
        if not isinstance(field.transform, IdentityTransform):
            raise IcebergMutationError(f"{mode} supports identity partition transforms only")
        columns.append(table.schema().find_column_name(field.source_id))
    return columns


def _validate_partition_spec(
    table: Any,
    partition_by: Sequence[str],
    mode: str,
    *,
    require_partitioned: bool,
) -> None:
    actual = _identity_partition_columns(table, mode)
    if not actual:
        if require_partitioned:
            raise IcebergMutationError(f"{mode} requires a partitioned Iceberg table")
        if partition_by:
            raise IcebergMutationError(
                f"partition_by does not match the unpartitioned target for {mode}"
            )
        return
    if list(partition_by) != actual:
        raise IcebergMutationError(
            f"partition_by does not match the target identity partition spec: expected {actual}, got {list(partition_by)}"
        )


def _validate_target_key_uniqueness(table: Any, batch: Any, unique_keys: Sequence[str]) -> None:
    if batch.num_rows == 0:
        return
    try:
        from pyiceberg.table import upsert_util

        match_filter = upsert_util.create_match_filter(batch, list(unique_keys))
        target_keys = table.scan(
            row_filter=match_filter,
            selected_fields=tuple(unique_keys),
        ).to_arrow()
    except Exception as exc:
        raise IcebergMutationError(f"Failed to validate target unique keys: {exc}") from exc
    counts = Counter(_row_tuples(target_keys, unique_keys))
    if any(count > 1 for count in counts.values()):
        raise IcebergMutationError(
            "Target table contains duplicate rows matching source unique_keys; upsert is ambiguous"
        )


def _current_snapshot_id(table: Any) -> Optional[int]:
    return table.metadata.current_snapshot_id


def _readback_snapshot(
    catalog: Any,
    identifier: tuple[str, ...],
    committed_snapshot_id: Optional[int],
) -> tuple[Optional[str], bool]:
    try:
        table = catalog.load_table(identifier)
    except Exception:
        return None, False
    if committed_snapshot_id is None:
        return None, True
    if any(snapshot.snapshot_id == committed_snapshot_id for snapshot in table.metadata.snapshots):
        return str(committed_snapshot_id), True
    return None, False


def _partition_spec_for_create(batch: Any, partition_by: Sequence[str]) -> tuple[Any, Any]:
    from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import assign_fresh_schema_ids
    from pyiceberg.transforms import IdentityTransform

    schema = assign_fresh_schema_ids(_pyarrow_to_schema_without_ids(batch.schema))
    fields = []
    for index, column in enumerate(partition_by):
        source_field = schema.find_field(column)
        fields.append(PartitionField(source_field.field_id, 1000 + index, IdentityTransform(), column))
    return schema, PartitionSpec(*fields)


def _stage_create_with_data(
    catalog: Any,
    identifier: tuple[str, ...],
    batch: Any,
    schema: Any,
    partition_spec: Any,
) -> Any:
    from pyiceberg.exceptions import NamespaceAlreadyExistsError

    namespace = identifier[:-1]
    if namespace:
        try:
            catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            pass
    transaction = catalog.create_table_transaction(
        identifier,
        schema=schema,
        partition_spec=partition_spec,
        properties={"format-version": "2"},
    )
    transaction.append(batch)
    return transaction


def write_advanced_iceberg_mutation(
    con: Any,
    catalog_name: str,
    table_name: str,
    view_name: str,
    mode: str,
    *,
    unique_keys: Optional[Sequence[str]],
    partition_by: Optional[Sequence[str]],
    create_table: bool,
    max_batch_bytes: int,
    catalog_config: CatalogConfig,
) -> IcebergMutationResult:
    """Validate and atomically publish one advanced Iceberg mutation."""
    _validate_pyiceberg_capabilities()
    if mode not in {"upsert", "insert_overwrite"}:
        raise IcebergMutationError(f"Unsupported advanced Iceberg write mode: {mode}")

    batch = _freeze_arrow_batch(con, view_name)
    _validate_unique_column_names(batch)
    if batch.nbytes > max_batch_bytes:
        raise IcebergMutationError(
            f"Arrow batch size {batch.nbytes} bytes exceeds max_batch_bytes={max_batch_bytes}"
        )

    keys = list(unique_keys or [])
    partitions = list(partition_by or [])
    if mode == "upsert":
        _validate_source_keys(batch, keys)
        if partitions:
            _validate_columns(batch, partitions, "partition_by")
            _validate_non_null(batch, partitions, "partition_by")
            _validate_finite(batch, partitions, "partition_by")
    else:
        _validate_columns(batch, partitions, "partition_by")
        _validate_non_null(batch, partitions, "partition_by")
        _validate_finite(batch, partitions, "partition_by")

    catalog = _load_catalog(catalog_name, catalog_config)
    identifier = _table_identifier(catalog_name, table_name)
    from pyiceberg.exceptions import NoSuchTableError

    try:
        table = catalog.load_table(identifier)
    except NoSuchTableError:
        table = None
    except Exception as exc:
        raise IcebergMutationError(
            f"Failed to load Iceberg table {'.'.join(identifier)}: {exc}"
        ) from exc

    if table is None:
        if not create_table:
            raise IcebergMutationError(f"Iceberg table {'.'.join(identifier)} does not exist and create_table=false")
        if batch.num_rows == 0:
            return IcebergMutationResult(None, 0, 0, 0, 0, "noop", False)
        try:
            schema, partition_spec = _partition_spec_for_create(batch, partitions)
        except Exception as exc:
            raise IcebergMutationError(f"Source schema cannot create an Iceberg table: {exc}") from exc
        transaction = _stage_create_with_data(
            catalog,
            identifier,
            batch,
            schema,
            partition_spec,
        )
        table = _commit_transaction(
            transaction,
            f"new Iceberg table {'.'.join(identifier)}",
        )
        committed_snapshot_id = _current_snapshot_id(table)
        snapshot_id, verified = _readback_snapshot(catalog, identifier, committed_snapshot_id)
        return IcebergMutationResult(
            snapshot_id=snapshot_id,
            row_count=batch.num_rows,
            inserted_row_count=batch.num_rows,
            updated_row_count=0 if mode == "upsert" else None,
            affected_partition_count=(len(set(_row_tuples(batch, partitions))) if mode == "insert_overwrite" else None),
            outcome="committed",
            snapshot_verified=verified,
        )

    batch = _validate_schema(table, batch)
    if mode == "upsert":
        _validate_columns(batch, keys, "unique_keys")
        _validate_target_key_uniqueness(table, batch, keys)
        if partitions:
            _validate_partition_spec(
                table,
                partitions,
                mode,
                require_partitioned=False,
            )
    else:
        _validate_partition_spec(
            table,
            partitions,
            mode,
            require_partitioned=True,
        )

    if batch.num_rows == 0:
        snapshot_id = _current_snapshot_id(table)
        return IcebergMutationResult(
            str(snapshot_id) if snapshot_id is not None else None,
            0,
            0,
            0,
            0,
            "noop",
            True,
        )

    from pyiceberg.table.update import AssertRefSnapshotId

    before_snapshot = _current_snapshot_id(table)
    transaction = table.transaction()
    transaction._apply(  # PyIceberg 0.10 has no public requirement-only method.
        (),
        (AssertRefSnapshotId(ref="main", snapshot_id=before_snapshot),),
    )
    try:
        if mode == "upsert":
            upsert_result = transaction.upsert(batch, join_cols=keys)
            inserted = upsert_result.rows_inserted
            updated = upsert_result.rows_updated
            affected_partitions = None
        else:
            transaction.dynamic_partition_overwrite(batch)
            inserted = batch.num_rows
            updated = None
            affected_partitions = len(set(_row_tuples(batch, partitions)))
    except Exception as exc:
        raise IcebergMutationError(f"Failed to stage Iceberg {mode}: {exc}") from exc

    committed_table = _commit_transaction(transaction, f"Iceberg {mode}")
    committed_snapshot_id = _current_snapshot_id(committed_table)
    snapshot_id, verified = _readback_snapshot(catalog, identifier, committed_snapshot_id)
    outcome = "noop" if mode == "upsert" and inserted == 0 and updated == 0 else "committed"
    return IcebergMutationResult(
        snapshot_id=snapshot_id,
        row_count=batch.num_rows,
        inserted_row_count=inserted,
        updated_row_count=updated,
        affected_partition_count=affected_partitions,
        outcome=outcome,
        snapshot_verified=verified,
    )


def _commit_transaction(transaction: Any, operation: str) -> Any:
    from pyiceberg.exceptions import CommitFailedException

    try:
        return transaction.commit_transaction()
    except CommitFailedException as exc:
        raise IcebergCommitError(
            f"Failed to commit {operation}: {exc}",
            failure_category="conflict",
        ) from exc
    except Exception as exc:
        raise IcebergCommitError(
            f"Failed to commit {operation}; commit status may be unknown: {exc}",
            failure_category="commit_unknown",
        ) from exc
