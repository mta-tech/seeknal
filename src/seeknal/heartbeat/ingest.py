"""Heartbeat IngestWriter — Parquet or Iceberg backend selected per config.

ParquetWriter is the default: uses DuckDB to read CSV/JSON/Parquet and writes
``target/ingested/<table>/data.parquet`` with a companion ``_meta.json``.

IcebergWriter delegates to :func:`seeknal.iceberg.ingest_writer.write_iceberg_ingested_table`,
resolving ``catalog_uri`` + ``warehouse`` from ``profiles.yml`` source_defaults.

Any backend exception → ``IngestResult(status='quarantined', error=...)``.
"""

from __future__ import annotations

import json
import logging
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from seeknal.heartbeat.recorder import IngestResult

logger = logging.getLogger("seeknal.heartbeat.ingest")

TABLE_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")
SUPPORTED_SUFFIXES = {".csv", ".tsv", ".json", ".jsonl", ".parquet"}
RESERVED_KEYWORDS = {
    "select",
    "from",
    "where",
    "join",
    "group",
    "order",
    "by",
    "at",
    "in",
    "on",
    "as",
    "to",
    "with",
    "having",
    "union",
    "table",
    "and",
    "or",
    "not",
    "is",
    "null",
    "limit",
    "offset",
    "case",
    "when",
    "then",
    "else",
    "end",
}


def _utcnow_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat(timespec="seconds")


def derive_table_name(path: Path) -> str:
    """Lowercased stem with non-word chars replaced by ``_``. Validates."""
    stem = path.stem
    candidate = re.sub(r"[^a-zA-Z0-9_]+", "_", stem).strip("_").lower()
    if not candidate or not TABLE_NAME_RE.match(candidate):
        raise ValueError(
            f"Cannot derive valid table name from filename '{path.name}'. "
            "Must yield ^[a-z][a-z0-9_]*$."
        )
    if candidate in RESERVED_KEYWORDS:
        raise ValueError(
            f"Filename '{path.name}' would map to reserved SQL keyword "
            f"'{candidate}'. Rename the file."
        )
    return candidate


class ParquetWriter:
    """Writes ``target/ingested/<table>/data.parquet`` using DuckDB."""

    def __init__(self, project_root: Path):
        self._project_root = Path(project_root)
        self._base = self._project_root / "target" / "ingested"

    def write(
        self,
        *,
        source_path: Path,
        table_name: str,
        checksum: Optional[str] = None,
    ) -> IngestResult:
        try:
            import duckdb  # type: ignore
        except ImportError as exc:
            return IngestResult(
                file=str(source_path),
                table=table_name,
                target="parquet",
                status="failed",
                error=f"DuckDB not installed: {exc}",
            )

        suffix = source_path.suffix.lower()
        if suffix not in SUPPORTED_SUFFIXES:
            return IngestResult(
                file=str(source_path),
                table=table_name,
                target="parquet",
                status="quarantined",
                error=f"Unsupported source suffix: {suffix}",
            )

        out_dir = self._base / table_name
        out_dir.mkdir(parents=True, exist_ok=True)
        out_parquet = out_dir / "data.parquet"
        meta_path = out_dir / "_meta.json"

        safe_in = str(source_path.resolve()).replace("'", "''")
        safe_out = str(out_parquet.resolve()).replace("'", "''")

        if suffix == ".csv":
            reader = f"read_csv_auto('{safe_in}')"
        elif suffix == ".tsv":
            reader = f"read_csv_auto('{safe_in}', delim='\\t')"
        elif suffix in (".json", ".jsonl"):
            reader = f"read_json_auto('{safe_in}')"
        elif suffix == ".parquet":
            reader = f"read_parquet('{safe_in}')"
        else:  # pragma: no cover — guarded above
            reader = ""

        con = duckdb.connect(":memory:")
        try:
            row = con.execute(f"SELECT COUNT(*) FROM {reader}").fetchone()
            row_count = int(row[0]) if row else 0
            con.execute(
                f"COPY (SELECT * FROM {reader}) TO '{safe_out}' (FORMAT PARQUET)"
            )
        except Exception as exc:  # noqa: BLE001
            return IngestResult(
                file=str(source_path),
                table=table_name,
                target="parquet",
                status="quarantined",
                error=str(exc),
            )
        finally:
            con.close()

        try:
            size_bytes = out_parquet.stat().st_size
        except OSError:
            size_bytes = 0

        meta: dict[str, Any] = {
            "source_file": str(source_path),
            "rows": row_count,
            "bytes": size_bytes,
            "ingested_at": _utcnow_iso(),
            "checksum_sha256": checksum,
            "table": table_name,
        }
        meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True))

        return IngestResult(
            file=str(source_path),
            table=table_name,
            rows=row_count,
            bytes=size_bytes,
            target="parquet",
            status="ok",
        )


class IcebergWriter:
    """Writes via :mod:`seeknal.iceberg.ingest_writer`."""

    def __init__(
        self,
        *,
        catalog_uri: str,
        warehouse: str,
        namespace: str = "ingested",
        existing_tables: Optional[set[str]] = None,
    ):
        self._catalog_uri = catalog_uri
        self._warehouse = warehouse
        self._namespace = namespace
        self._existing: set[str] = existing_tables or set()

    def write(
        self,
        *,
        source_path: Path,
        table_name: str,
        checksum: Optional[str] = None,
    ) -> IngestResult:
        del checksum  # forward-compat; not yet used in Iceberg sidecar
        from seeknal.iceberg.ingest_writer import (
            IcebergWriteError,
            write_iceberg_ingested_table,
        )

        mode = "append" if table_name in self._existing else "create"
        try:
            res = write_iceberg_ingested_table(
                source_path=source_path,
                table_name=table_name,
                catalog_uri=self._catalog_uri,
                warehouse=self._warehouse,
                namespace=self._namespace,
                mode=mode,
            )
        except IcebergWriteError as exc:
            return IngestResult(
                file=str(source_path),
                table=table_name,
                target="iceberg",
                status="quarantined",
                error=str(exc),
            )

        self._existing.add(table_name)
        return IngestResult(
            file=str(source_path),
            table=table_name,
            rows=res.rows,
            bytes=res.bytes,
            target="iceberg",
            status="ok",
        )


def quarantine_file(path: Path, quarantine_dir: Path) -> Path:
    """Move ``path`` into ``quarantine_dir`` with a timestamp suffix on collision."""
    quarantine_dir = Path(quarantine_dir)
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    dest = quarantine_dir / path.name
    if dest.exists():
        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        dest = quarantine_dir / f"{path.stem}.{ts}{path.suffix}"
    shutil.move(str(path), str(dest))
    return dest


class IngestWriter:
    """Backend dispatcher selected by ``HeartbeatConfig.ingested_target``."""

    def __init__(
        self,
        project_root: Path,
        *,
        target: str = "parquet",
        iceberg_catalog_uri: Optional[str] = None,
        iceberg_warehouse: Optional[str] = None,
        iceberg_namespace: str = "ingested",
    ):
        self._project_root = Path(project_root)
        self._target = target
        if target == "parquet":
            self._backend = ParquetWriter(self._project_root)
        elif target == "iceberg":
            if not iceberg_catalog_uri or not iceberg_warehouse:
                raise ValueError(
                    "Iceberg target requires catalog_uri and warehouse "
                    "(set source_defaults.iceberg in profiles.yml)."
                )
            self._backend = IcebergWriter(
                catalog_uri=iceberg_catalog_uri,
                warehouse=iceberg_warehouse,
                namespace=iceberg_namespace,
            )
        else:
            raise ValueError(
                f"Unknown target '{target}'. Accepted: parquet, iceberg."
            )

    @property
    def target(self) -> str:
        return self._target

    def write(
        self,
        source_path: Path,
        *,
        table_name: Optional[str] = None,
        checksum: Optional[str] = None,
    ) -> IngestResult:
        try:
            resolved_table = table_name or derive_table_name(source_path)
        except ValueError as exc:
            return IngestResult(
                file=str(source_path),
                table="",
                target=self._target,  # type: ignore[arg-type]
                status="quarantined",
                error=str(exc),
            )
        return self._backend.write(
            source_path=source_path,
            table_name=resolved_table,
            checksum=checksum,
        )
