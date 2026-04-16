from __future__ import annotations

import os
import warnings
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

import yaml  # ty: ignore[unresolved-import]

from seeknal.utils.path_security import is_insecure_path


def _ledger_path() -> Path:
    xdg = os.environ.get("XDG_DATA_HOME")
    if xdg:
        base = Path(xdg) / "seeknal"
    else:
        base = Path.home() / ".seeknal"
    return base / "published_reports.yml"


@dataclass
class LedgerEntry:
    slug: str
    server: str
    share_url: str
    report_name: str
    published_at: str


def _load_raw() -> list[dict]:
    path = _ledger_path()
    if not path.exists():
        return []
    try:
        with open(path, "r") as fh:
            data = yaml.safe_load(fh)
        if not isinstance(data, list):
            return []
        return data
    except Exception as exc:
        warnings.warn(f"Publish ledger: could not read {path}: {exc}")
        return []


def _save_raw(entries: list[dict]) -> None:
    path = _ledger_path()
    parent = str(path.parent)
    if is_insecure_path(parent):
        warnings.warn(f"Publish ledger: insecure parent path '{parent}', skipping write")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as fh:
        yaml.safe_dump(entries, fh, default_flow_style=False)


def append_entry(entry: LedgerEntry) -> None:
    entries = _load_raw()
    entries.append(asdict(entry))
    _save_raw(entries)


def list_entries() -> list[LedgerEntry]:
    raw = _load_raw()
    result: list[LedgerEntry] = []
    for item in raw:
        try:
            result.append(LedgerEntry(**item))
        except Exception:
            pass
    return result


def find_by_report_name(name: str) -> list[LedgerEntry]:
    return [e for e in list_entries() if e.report_name == name]
