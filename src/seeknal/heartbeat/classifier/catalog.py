"""Source catalog — persistent memory of known source tables.

Stored at ``.seeknal/source_catalog.yml``. Comments preserved on round-trip
via ruamel.yaml's round-trip parser. Atomic writes via tempfile + os.replace.
"""

from __future__ import annotations

import logging
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from seeknal.heartbeat.classifier.fingerprint import similarity
from seeknal.heartbeat.classifier.models import Fingerprint
from seeknal.utils.path_security import is_insecure_path

logger = logging.getLogger("seeknal.heartbeat.classifier.catalog")


@dataclass
class CatalogEntry:
    source_table: str
    fingerprint: Fingerprint
    canonical_columns: dict[str, dict[str, Any]] = field(default_factory=dict)
    write_mode: str = "append"
    business_key: list[str] = field(default_factory=list)
    last_seen: str = ""
    confirmed_by: str = ""
    sample_path: str = ""
    stale_schema: bool = False
    confidence_threshold_override: Optional[float] = None


@dataclass
class CatalogMatch:
    source_table: str
    confidence: float
    entry: CatalogEntry


def _load_yaml(path: Path) -> Any:
    """Use ruamel.yaml round-trip if available, else safe_load."""
    try:
        from ruamel.yaml import YAML

        yaml_rt = YAML(typ="rt")
        with open(path, "r") as fh:
            return yaml_rt.load(fh)
    except ImportError:
        import yaml

        with open(path, "r") as fh:
            return yaml.safe_load(fh)


def _dump_yaml(data: Any, fh) -> None:
    try:
        from ruamel.yaml import YAML

        yaml_rt = YAML(typ="rt")
        yaml_rt.dump(data, fh)
    except ImportError:
        import yaml

        yaml.safe_dump(data, fh, sort_keys=False)


@dataclass
class Catalog:
    schema_version: int = 1
    entries: dict[str, CatalogEntry] = field(default_factory=dict)
    source_path: Optional[Path] = None

    @classmethod
    def load(cls, project_root: Path) -> "Catalog":
        """Load catalog from ``<project_root>/.seeknal/source_catalog.yml``."""
        path = Path(project_root) / ".seeknal" / "source_catalog.yml"
        if is_insecure_path(str(path)):
            raise ValueError(
                f"Insecure catalog path: {path!s}. "
                "Use a project-local .seeknal/ directory."
            )
        if not path.exists():
            return cls(source_path=path)

        try:
            raw = _load_yaml(path)
        except Exception as exc:  # noqa: BLE001
            logger.critical(
                "source_catalog.yml at %s is corrupt (%s); "
                "renaming to .corrupt and starting empty.",
                path,
                exc,
            )
            from datetime import datetime, timezone

            ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            try:
                path.rename(path.with_suffix(f".yml.corrupt.{ts}"))
            except OSError:
                pass
            return cls(source_path=path)

        if raw is None:
            return cls(source_path=path)
        if not isinstance(raw, dict):
            raise ValueError(
                f"source_catalog.yml at {path} did not parse to a mapping."
            )

        catalog = cls(source_path=path)
        catalog.schema_version = int(raw.get("schema_version", 1))
        for name, body in (raw.get("sources") or {}).items():
            if not isinstance(body, dict):
                continue
            fp_raw = body.get("fingerprint") or {}
            fp = Fingerprint(
                column_names=list(fp_raw.get("column_names", []) or []),
                column_types=list(fp_raw.get("column_types", []) or []),
                value_regexes=list(fp_raw.get("value_regexes", []) or []),
                row_count_sample=int(fp_raw.get("row_count_sample", 0) or 0),
                column_count=int(
                    fp_raw.get("column_count", len(fp_raw.get("column_names", []) or []))
                    or 0
                ),
            )
            catalog.entries[name] = CatalogEntry(
                source_table=name,
                fingerprint=fp,
                canonical_columns=dict(body.get("canonical_columns") or {}),
                write_mode=str(body.get("write_mode", "append")),
                business_key=list(body.get("business_key") or []),
                last_seen=str(body.get("last_seen", "") or ""),
                confirmed_by=str(body.get("confirmed_by", "") or ""),
                sample_path=str(body.get("sample_path", "") or ""),
                stale_schema=bool(body.get("stale_schema", False)),
                confidence_threshold_override=body.get(
                    "confidence_threshold_override"
                ),
            )
        return catalog

    def save(self, path: Optional[Path] = None) -> Path:
        target = Path(path or self.source_path)
        if is_insecure_path(str(target)):
            raise ValueError(
                f"Insecure catalog save path: {target!s}. "
                "Use a project-local .seeknal/ directory."
            )
        target.parent.mkdir(parents=True, exist_ok=True)
        payload: dict[str, Any] = {
            "schema_version": self.schema_version,
            "sources": {},
        }
        for name, entry in self.entries.items():
            body: dict[str, Any] = {
                "fingerprint": {
                    "column_names": list(entry.fingerprint.column_names),
                    "column_types": list(entry.fingerprint.column_types),
                    "value_regexes": list(entry.fingerprint.value_regexes),
                    "row_count_sample": entry.fingerprint.row_count_sample,
                    "column_count": entry.fingerprint.column_count,
                },
                "canonical_columns": dict(entry.canonical_columns),
                "write_mode": entry.write_mode,
                "business_key": list(entry.business_key),
            }
            if entry.last_seen:
                body["last_seen"] = entry.last_seen
            if entry.confirmed_by:
                body["confirmed_by"] = entry.confirmed_by
            if entry.sample_path:
                body["sample_path"] = entry.sample_path
            if entry.stale_schema:
                body["stale_schema"] = True
            if entry.confidence_threshold_override is not None:
                body["confidence_threshold_override"] = (
                    entry.confidence_threshold_override
                )
            payload["sources"][name] = body

        fd, tmp_path = tempfile.mkstemp(
            prefix=".source_catalog.",
            suffix=".yml.tmp",
            dir=str(target.parent),
        )
        try:
            with os.fdopen(fd, "w") as fh:
                _dump_yaml(payload, fh)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, target)
        except Exception:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass
            raise
        self.source_path = target
        return target

    def find_best_match(
        self, fp: Fingerprint, threshold: float = 0.85
    ) -> Optional[CatalogMatch]:
        best: Optional[CatalogMatch] = None
        for name, entry in self.entries.items():
            score = similarity(fp, entry.fingerprint)
            override = entry.confidence_threshold_override or threshold
            if score >= override:
                if best is None or score > best.confidence:
                    best = CatalogMatch(
                        source_table=name, confidence=score, entry=entry
                    )
        return best

    def upsert(self, entry: CatalogEntry) -> None:
        self.entries[entry.source_table] = entry


__all__ = ["Catalog", "CatalogEntry", "CatalogMatch"]
