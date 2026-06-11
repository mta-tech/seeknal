"""Context-free Iceberg writer used by the heartbeat daemon.

The function accepts a source file (CSV / JSON / Parquet) and writes it to an
Iceberg table via PyIceberg. Catalog connectivity is supplied through
``catalog_uri`` + ``warehouse`` — both resolved upstream from ``profiles.yml``.

If PyIceberg or the catalog are unreachable, ``write_iceberg_ingested_table``
raises an ``IcebergWriteError`` so the caller (the heartbeat IngestWriter) can
record a quarantine event without crashing the daemon.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional

logger = logging.getLogger("seeknal.iceberg.ingest_writer")

_TABLE_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")


class IcebergWriteError(Exception):
    """Raised when the Iceberg write cannot complete (no pyiceberg, catalog
    unreachable, schema rejected, etc.)."""


@dataclass
class IcebergWriteResult:
    rows: int
    bytes: int
    snapshot_id: Optional[str] = None


def _read_source_to_arrow(source_path: Path):
    """Load a source file into a pyarrow Table. Lazy imports so the base
    install doesn't need duckdb/pyarrow until Iceberg is actually used."""
    suffix = source_path.suffix.lower()
    import duckdb  # local — heavy import

    safe = str(source_path.resolve()).replace("'", "''")
    if suffix == ".csv":
        reader = f"read_csv_auto('{safe}')"
    elif suffix == ".tsv":
        reader = f"read_csv_auto('{safe}', delim='\\t')"
    elif suffix in (".json", ".jsonl"):
        reader = f"read_json_auto('{safe}')"
    elif suffix == ".parquet":
        reader = f"read_parquet('{safe}')"
    else:
        raise IcebergWriteError(f"Unsupported suffix for Iceberg ingest: {suffix}")

    con = duckdb.connect(":memory:")
    try:
        return con.execute(f"SELECT * FROM {reader}").arrow()
    finally:
        con.close()


def write_iceberg_ingested_table(
    *,
    source_path: Path,
    table_name: str,
    catalog_uri: str,
    warehouse: str,
    namespace: str = "ingested",
    mode: Literal["create", "append"] = "create",
    business_key: Optional[str] = None,
) -> IcebergWriteResult:
    """Write ``source_path`` to ``namespace.table_name`` in the Iceberg catalog.

    Args:
        source_path: Local file (CSV/JSON/Parquet).
        table_name: Lowercase alphanumeric + underscores.
        catalog_uri: REST catalog URI (e.g. http://lakekeeper:8181).
        warehouse: Warehouse name configured on the catalog.
        namespace: Iceberg namespace (default ``ingested``).
        mode: ``create`` (first write) or ``append`` (subsequent).
        business_key: Reserved for future upsert support. Currently unused.

    Raises:
        IcebergWriteError: if PyIceberg is unavailable, the catalog can't be
            reached, or the write fails.
    """
    del business_key  # accepted for forward-compat; ignored in create/append modes.

    if not _TABLE_NAME_RE.match(table_name):
        raise IcebergWriteError(
            f"Invalid Iceberg table name '{table_name}'. "
            "Must match ^[a-z][a-z0-9_]*$."
        )

    try:
        from pyiceberg.catalog import load_catalog  # type: ignore
    except ImportError as exc:
        raise IcebergWriteError(
            "PyIceberg is not installed. Configure ingested_target=parquet "
            "in HEARTBEAT.md or install pyiceberg."
        ) from exc

    arrow_table = _read_source_to_arrow(source_path)
    rows = arrow_table.num_rows
    nbytes = arrow_table.nbytes

    try:
        catalog = load_catalog(
            "heartbeat",
            **{
                "type": "rest",
                "uri": catalog_uri,
                "warehouse": warehouse,
            },
        )
    except Exception as exc:  # noqa: BLE001
        raise IcebergWriteError(
            f"Failed to connect to Iceberg catalog at {catalog_uri}: {exc}"
        ) from exc

    full_name = f"{namespace}.{table_name}"

    try:
        try:
            catalog.create_namespace(namespace)
        except Exception:  # namespace already exists
            pass

        try:
            table = catalog.load_table(full_name)
            if mode == "create":
                raise IcebergWriteError(
                    f"Iceberg table {full_name} already exists; use mode='append'"
                )
            table.append(arrow_table)
        except Exception as load_exc:
            if mode == "append" and "does not exist" not in str(load_exc).lower():
                raise IcebergWriteError(
                    f"Failed loading Iceberg table {full_name}: {load_exc}"
                ) from load_exc
            table = catalog.create_table(full_name, schema=arrow_table.schema)
            table.append(arrow_table)

        snapshot_id = None
        try:
            snapshot = table.current_snapshot()
            snapshot_id = str(snapshot.snapshot_id) if snapshot else None
        except Exception:  # noqa: BLE001
            snapshot_id = None

        return IcebergWriteResult(rows=rows, bytes=nbytes, snapshot_id=snapshot_id)
    except IcebergWriteError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise IcebergWriteError(
            f"Iceberg write failed for {full_name}: {exc}"
        ) from exc
