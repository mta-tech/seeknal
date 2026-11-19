"""Fail-closed startup validation and PII placement/routing guards for Seeknal Serving."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import yaml

from seeknal.serving.audit import verify_audit_config_offline
from seeknal.serving.enforce import VALID_MASKS, VALID_OPS
from seeknal.serving.placement import (
    VALID_ENGINE_KINDS,
    VALID_TWIN_METHODS,
    Placement,
    PlacementError,
    TableEntry,
)
from seeknal.serving.router import (
    PII_CROSS_ENGINE_CODE,
    PiiCrossEngineError,
    RoutingError,
    check_pii_query_mixing,
)
from seeknal.serving.server.config import (
    AuditStartupError,
    DuckDBConnectionConfig,
    LakebaseConnectionConfig,
    ServerConfig,
    ServerConfigError,
    UniqueKeySafeLoader,
    _DuplicateJsonKeyError,
    _format_json_error,
    _format_yaml_error,
    _reject_duplicate_json_keys,
    check_and_parse_secrets_env_file,
    check_extension_files_on_disk,
)

_SAFE_IDENT_RE = re.compile(r"^[\w][\w.-]{0,127}$", re.UNICODE)
_SIMPLE_LOWER_ENUM_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
_DISC_LOGICAL_SEGMENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CANARY_TOKEN_RE = re.compile(r"(canary|sentinel|opaque)", re.IGNORECASE)
_SUSPICIOUS_ENUM_RE = re.compile(
    r"(://|@|secret|password|passwd|token|bearer|private|canary|sentinel|opaque|dsn|credential)",
    re.IGNORECASE,
)
_URI_USERINFO_RE = re.compile(r"://[^@\s/]{0,256}(?:/[^@\s/]{0,256}){0,8}@")
_DSN_PASSWORD_EXTRACT_RE = re.compile(r"://[^:/\s@]{1,128}:([^@\s]{1,256})@")
_BEARER_SCRUB_RE = re.compile(r"\bBearer\s+\S+", re.IGNORECASE)
_KV_SCRUB_RE = re.compile(
    r"((?:password|passwd|secret|token|auth\s+key)\s*[:=]\s*)([^\s,;'\"]+)",
    re.IGNORECASE,
)
_CANARY_SCRUB_RE = re.compile(
    r"(?i)[A-Za-z0-9_.-]{0,64}(?:canary|sentinel|opaque)[A-Za-z0-9_.-]{0,64}"
)
_SENSITIVE_ENV_KEY_RE = re.compile(
    r"(SECRET|PASSWORD|PASSWD|TOKEN|DSN|CREDENTIAL|CANARY|SENTINEL|PRIVATE|AUTH|KEY)",
    re.IGNORECASE,
)
_ROLE_TOP_KEYS = frozenset({"roles", "role_policies", "default_role", "role_bindings", "role_grants"})


class StartupValidationError(ServerConfigError):
    """Raised when one or more fail-closed startup validation checks fail."""

    def __init__(self, problems: Sequence[str] | str) -> None:
        if isinstance(problems, str):
            raw_list = [problems]
        else:
            raw_list = [str(p) for p in problems if str(p).strip()]
        if not raw_list:
            raw_list = ["Startup validation failed"]
        deduped = tuple(dict.fromkeys(raw_list))
        self.problems: tuple[str, ...] = deduped
        super().__init__("\n".join(deduped))


@dataclass(frozen=True)
class ValidationReport:
    """Structured result of offline/startup validation."""

    mode: str
    ok: bool = True
    placement: Placement | None = None
    schemas: dict[str, list[str]] | None = None
    skipped: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()
    role_model: str = "per_request_es256_grant_tokens"
    static_role_references_checked: int = 0


def _collect_secret_values(
    env: Mapping[str, str] | None,
    *,
    cfg: ServerConfig | None = None,
    raw_cfg: Mapping[str, Any] | None = None,
) -> tuple[str, ...]:
    """Gather secret values from env, secrets_env, and secret files for redaction."""
    candidates: set[str] = set()
    proc_env = env if env is not None else os.environ

    for k, v in proc_env.items():
        if isinstance(v, str) and len(v) >= 4 and _SENSITIVE_ENV_KEY_RE.search(str(k)):
            candidates.add(v)
            # If DSN has user:password@host, also capture password segment
            if "://" in v and "@" in v:
                m = _DSN_PASSWORD_EXTRACT_RE.search(v)
                if m and len(m.group(1)) >= 3:
                    candidates.add(m.group(1))

    if cfg is not None:
        for v in cfg.secrets_env.values():
            if isinstance(v, str) and len(v) >= 3:
                candidates.add(v)
        for ref_var in cfg.get_referenced_env_vars():
            val = cfg.secrets_env.get(ref_var) or proc_env.get(ref_var)
            if isinstance(val, str) and len(val) >= 3:
                candidates.add(val)
                if "://" in val and "@" in val:
                    m = _DSN_PASSWORD_EXTRACT_RE.search(val)
                    if m and len(m.group(1)) >= 3:
                        candidates.add(m.group(1))
        # Also include contents of any configured secret files
        sec_file_paths: list[str] = []
        for conn in cfg.connections.values():
            accounts = getattr(conn, "accounts", None)
            if isinstance(accounts, Mapping):
                for acc in accounts.values():
                    pf = getattr(acc, "password_file", None)
                    if isinstance(pf, str) and pf.strip():
                        sec_file_paths.append(pf.strip())
        if cfg.placement.discovery_config is not None:
            disc = cfg.placement.discovery_config
            if (
                disc.lakekeeper
                and disc.lakekeeper.oauth
                and disc.lakekeeper.oauth.client_secret_file
            ):
                sec_file_paths.append(disc.lakekeeper.oauth.client_secret_file)
            for eng in disc.engines.values():
                if eng.api_secret_file:
                    sec_file_paths.append(eng.api_secret_file)
        for sf_path in sec_file_paths:
            if os.path.isfile(sf_path):
                try:
                    with open(sf_path, "r", encoding="utf-8") as fh:
                        sf_val = fh.read().strip()
                    if len(sf_val) >= 3:
                        candidates.add(sf_val)
                except Exception:
                    pass

    if raw_cfg is not None and isinstance(raw_cfg, Mapping):
        sef = raw_cfg.get("secrets_env_file")
        if isinstance(sef, str) and sef.strip() and os.path.exists(sef.strip()):
            try:
                parsed = check_and_parse_secrets_env_file(sef.strip())
                for v in parsed.values():
                    if isinstance(v, str) and len(v) >= 3:
                        candidates.add(v)
            except Exception:
                pass

    return tuple(sorted(candidates, key=len, reverse=True))


def _scrub(text: str, secret_values: Sequence[str] = ()) -> str:
    """Scrub secret values, URI credentials, bearer/password tokens, and canary markers from diagnostic text."""
    out = str(text)
    for sec in secret_values:
        if sec and sec in out:
            out = out.replace(sec, "<redacted>")
    if "://" in out and "@" in out:
        out = _URI_USERINFO_RE.sub("://<redacted>@", out)
    if "bearer" in out.lower():
        out = _BEARER_SCRUB_RE.sub("Bearer <redacted>", out)
    if ":" in out or "=" in out:
        out = _KV_SCRUB_RE.sub(r"\1<redacted>", out)
    out_lower = out.lower()
    if "canary" in out_lower or "sentinel" in out_lower or "opaque" in out_lower:
        out = _CANARY_SCRUB_RE.sub("<redacted>", out)
    return out


def _safe_ident(
    value: Any,
    secret_values: Sequence[str] = (),
    *,
    fallback: str = "<redacted>",
) -> str:
    """Format an identifier safely without leaking URIs, DSNs, secrets, or canaries."""
    if not isinstance(value, str):
        return f"<{type(value).__name__}>"
    s = value.strip()
    if not s:
        return "<empty>"
    for sec in secret_values:
        if sec and sec in s:
            return fallback
    if "://" in s or "@" in s or _CANARY_TOKEN_RE.search(s):
        return fallback
    if not _SAFE_IDENT_RE.fullmatch(s):
        return fallback
    return s


def _safe_enum_token(value: Any, secret_values: Sequence[str] = ()) -> str:
    """Return a strictly lowercase simple enum token or '<invalid>' (never echoes secrets/DSNs/mixed-case sentinels)."""
    if not isinstance(value, str):
        return "<invalid>"
    s = value.strip()
    if not s or _SUSPICIOUS_ENUM_RE.search(s):
        return "<invalid>"
    for sec in secret_values:
        if sec and sec in s:
            return "<invalid>"
    if not _SIMPLE_LOWER_ENUM_RE.fullmatch(s):
        return "<invalid>"
    return s


def _check_secret_files_all(
    cfg: ServerConfig,
    problems: list[str],
    secret_values: Sequence[str],
) -> None:
    """Collect all secret-file existence and permission errors without failing after the first."""
    secret_files: list[tuple[str, str]] = []

    for conn in cfg.connections.values():
        accounts = getattr(conn, "accounts", None)
        if isinstance(accounts, Mapping):
            for acc_name, acc in accounts.items():
                pfile = getattr(acc, "password_file", None)
                if pfile:
                    secret_files.append(
                        (
                            pfile,
                            f"StarRocks account '{_safe_ident(acc_name, secret_values)}' password_file",
                        )
                    )

    if cfg.placement.discovery_config is not None:
        disc = cfg.placement.discovery_config
        if (
            disc.lakekeeper
            and disc.lakekeeper.oauth
            and disc.lakekeeper.oauth.client_secret_file
        ):
            secret_files.append(
                (
                    disc.lakekeeper.oauth.client_secret_file,
                    "Lakekeeper oauth client_secret_file",
                )
            )
        for eng in disc.engines.values():
            if eng.api_secret_file:
                secret_files.append(
                    (
                        eng.api_secret_file,
                        f"Cube engine '{_safe_ident(eng.name, secret_values)}' api_secret_file",
                    )
                )

    for path, desc in secret_files:
        if not os.path.exists(path):
            problems.append(_scrub(f"{desc} not found: '{path}'", secret_values))
        else:
            st = os.stat(path)
            if (st.st_mode & 0o044) != 0:
                problems.append(
                    _scrub(f"{desc} '{path}' is group- or world-readable", secret_values)
                )


def _collect_role_definitions_and_references(
    *,
    raw_cfg: Mapping[str, Any] | None,
    raw_placement: Mapping[str, Any] | None,
    raw_discovery: Mapping[str, Any] | None,
    problems: list[str],
    secret_values: Sequence[str],
) -> tuple[dict[str, Mapping[str, Any]], int]:
    """Validate static role definitions and references if any are present in config/placement.

    Returns ``(defined_role_policies, role_references_checked)``.
    """
    defined_roles: dict[str, Mapping[str, Any]] = {}
    referenced_roles: list[tuple[str, str]] = []  # (role_name, location)

    containers: list[tuple[str, Mapping[str, Any]]] = []
    if isinstance(raw_cfg, Mapping):
        containers.append(("config", raw_cfg))
    if isinstance(raw_placement, Mapping):
        containers.append(("placement", raw_placement))
        pol = raw_placement.get("policy")
        if isinstance(pol, Mapping):
            containers.append(("placement.policy", pol))
    if isinstance(raw_discovery, Mapping):
        containers.append(("placement.discovery", raw_discovery))
        pol = raw_discovery.get("policy")
        if isinstance(pol, Mapping):
            containers.append(("placement.discovery.policy", pol))

    # 1. Collect role definitions from 'roles' / 'role_policies' mappings
    for ctx_label, container in containers:
        for def_key in ("roles", "role_policies"):
            if def_key not in container:
                continue
            raw_defs = container[def_key]
            if isinstance(raw_defs, Mapping):
                for r_name, r_spec in raw_defs.items():
                    if not isinstance(r_name, str) or not r_name.strip():
                        problems.append(
                            f"Empty or invalid role name in '{ctx_label}.{def_key}'"
                        )
                        continue
                    r_clean = r_name.strip()
                    if not isinstance(r_spec, Mapping):
                        problems.append(
                            f"Unknown role '{_safe_ident(r_clean, secret_values)}': "
                            f"entry in '{ctx_label}.{def_key}' has no policy mapping definition"
                        )
                    else:
                        defined_roles[r_clean] = r_spec
            elif isinstance(raw_defs, (list, tuple)):
                for item in raw_defs:
                    r_str = str(item).strip() if isinstance(item, str) else ""
                    referenced_roles.append((r_str or "<invalid>", f"{ctx_label}.{def_key}"))
            else:
                problems.append(
                    f"Invalid '{ctx_label}.{def_key}': expected mapping of role policy definitions"
                )

    # 2. Collect role references across containers, tables, cubes, and filters
    for ctx_label, container in containers:
        if "default_role" in container and container["default_role"] is not None:
            dr = container["default_role"]
            if isinstance(dr, str) and dr.strip():
                referenced_roles.append((dr.strip(), f"{ctx_label}.default_role"))
            else:
                problems.append(f"Invalid role reference in '{ctx_label}.default_role'")

        for bind_key in ("role_bindings", "role_grants"):
            if bind_key not in container or container[bind_key] is None:
                continue
            b_val = container[bind_key]
            if isinstance(b_val, Mapping):
                for principal, target_roles in b_val.items():
                    loc = f"{ctx_label}.{bind_key}.{_safe_ident(str(principal), secret_values)}"
                    if isinstance(target_roles, str):
                        referenced_roles.append((target_roles.strip(), loc))
                    elif isinstance(target_roles, (list, tuple)):
                        for r_item in target_roles:
                            if isinstance(r_item, str) and r_item.strip():
                                referenced_roles.append((r_item.strip(), loc))
                            else:
                                problems.append(f"Invalid role reference in '{loc}'")
                    elif isinstance(target_roles, Mapping) and "role" in target_roles:
                        r_item = target_roles["role"]
                        if isinstance(r_item, str) and r_item.strip():
                            referenced_roles.append((r_item.strip(), loc))
            elif isinstance(b_val, (list, tuple)):
                for idx, entry in enumerate(b_val):
                    loc = f"{ctx_label}.{bind_key}[{idx}]"
                    if isinstance(entry, Mapping) and "role" in entry:
                        r_item = entry["role"]
                        if isinstance(r_item, str) and r_item.strip():
                            referenced_roles.append((r_item.strip(), loc))
                        else:
                            problems.append(f"Invalid role reference in '{loc}'")

    if isinstance(raw_placement, Mapping):
        for section_name in ("tables", "cubes"):
            sec = raw_placement.get(section_name)
            if not isinstance(sec, Mapping):
                continue
            for obj_name, obj_spec in sec.items():
                if not isinstance(obj_spec, Mapping):
                    continue
                safe_obj = _safe_ident(str(obj_name), secret_values)
                for r_field in ("roles", "allowed_roles"):
                    if r_field not in obj_spec or obj_spec[r_field] is None:
                        continue
                    rv = obj_spec[r_field]
                    loc = f"placement.{section_name}.{safe_obj}.{r_field}"
                    if isinstance(rv, str) and rv.strip():
                        referenced_roles.append((rv.strip(), loc))
                    elif isinstance(rv, (list, tuple)):
                        for item in rv:
                            if isinstance(item, str) and item.strip():
                                referenced_roles.append((item.strip(), loc))
                            else:
                                problems.append(f"Invalid role reference in '{loc}'")
                    elif isinstance(rv, Mapping):
                        for r_key in rv.keys():
                            referenced_roles.append((str(r_key).strip(), loc))

                for role_pol_field in ("role_masks", "role_row_filters", "role_policies"):
                    if role_pol_field in obj_spec and isinstance(obj_spec[role_pol_field], Mapping):
                        loc = f"placement.{section_name}.{safe_obj}.{role_pol_field}"
                        for r_key in obj_spec[role_pol_field].keys():
                            referenced_roles.append((str(r_key).strip(), loc))

                for rf_field in ("row_filters", "filters"):
                    rf_val = obj_spec.get(rf_field)
                    if isinstance(rf_val, (list, tuple)):
                        for idx, rf_item in enumerate(rf_val):
                            if isinstance(rf_item, Mapping) and "role" in rf_item:
                                r_val = rf_item["role"]
                                loc = f"placement.{section_name}.{safe_obj}.{rf_field}[{idx}].role"
                                if isinstance(r_val, str) and r_val.strip():
                                    referenced_roles.append((r_val.strip(), loc))
                                else:
                                    problems.append(f"Invalid role reference in '{loc}'")

    for role_name, loc in referenced_roles:
        if role_name not in defined_roles:
            problems.append(
                f"Unknown role '{_safe_ident(role_name, secret_values)}' referenced in "
                f"'{loc}' without a matching role policy definition"
            )

    return defined_roles, len(referenced_roles)


def _validate_table_column_refs_in_spec(
    *,
    tbl_name: str,
    spec: Mapping[str, Any],
    cols_lower: set[str],
    context_prefix: str,
    problems: list[str],
    secret_values: Sequence[str],
) -> None:
    """Validate columns/masks/row_filters column references against schemas[tbl_name]."""
    safe_tbl = _safe_ident(tbl_name, secret_values)

    # 1. Explicit columns list (if present)
    if "columns" in spec and spec["columns"] is not None:
        raw_cols = spec["columns"]
        if isinstance(raw_cols, (list, tuple)):
            for col in raw_cols:
                col_str = str(col).strip()
                if not col_str or col_str.lower() not in cols_lower:
                    problems.append(
                        f"Unknown column '{_safe_ident(col_str, secret_values)}' in "
                        f"'{context_prefix}.columns' for table '{safe_tbl}' (not in schemas)"
                    )

    # 2. Masks
    if "masks" in spec and spec["masks"] is not None:
        raw_masks = spec["masks"]
        if isinstance(raw_masks, Mapping):
            for k, v in raw_masks.items():
                if isinstance(v, Mapping):
                    # Nested role -> {col: method}
                    for col_k, method_v in v.items():
                        col_str = str(col_k).strip()
                        if not col_str or col_str.lower() not in cols_lower:
                            problems.append(
                                f"Unknown column '{_safe_ident(col_str, secret_values)}' in "
                                f"'{context_prefix}.masks' for table '{safe_tbl}' (not in schemas)"
                            )
                        if isinstance(method_v, str) and method_v.lower() not in VALID_MASKS:
                            problems.append(
                                f"Unknown mask method '{_safe_enum_token(method_v, secret_values)}' "
                                f"for column '{_safe_ident(col_str, secret_values)}' on table '{safe_tbl}'"
                            )
                else:
                    col_str = str(k).strip()
                    if not col_str or col_str.lower() not in cols_lower:
                        problems.append(
                            f"Unknown column '{_safe_ident(col_str, secret_values)}' in "
                            f"'{context_prefix}.masks' for table '{safe_tbl}' (not in schemas)"
                        )
                    if isinstance(v, str) and v.lower() not in VALID_MASKS:
                        problems.append(
                            f"Unknown mask method '{_safe_enum_token(v, secret_values)}' "
                            f"for column '{_safe_ident(col_str, secret_values)}' on table '{safe_tbl}'"
                        )
        elif isinstance(raw_masks, (list, tuple)):
            for idx, item in enumerate(raw_masks):
                if isinstance(item, str):
                    col_str = item.strip()
                elif isinstance(item, Mapping) and "column" in item:
                    col_str = str(item["column"]).strip()
                else:
                    problems.append(
                        f"Invalid mask entry at '{context_prefix}.masks[{idx}]' for table '{safe_tbl}'"
                    )
                    continue
                if not col_str or col_str.lower() not in cols_lower:
                    problems.append(
                        f"Unknown column '{_safe_ident(col_str, secret_values)}' in "
                        f"'{context_prefix}.masks' for table '{safe_tbl}' (not in schemas)"
                    )

    # 3. Row filters
    for rf_key in ("row_filters", "filters"):
        if rf_key not in spec or spec[rf_key] is None:
            continue
        raw_rfs = spec[rf_key]
        if isinstance(raw_rfs, (list, tuple)):
            for idx, item in enumerate(raw_rfs):
                if not isinstance(item, Mapping) or "column" not in item:
                    problems.append(
                        f"Invalid row filter at '{context_prefix}.{rf_key}[{idx}]' for table '{safe_tbl}'"
                    )
                    continue
                col_str = str(item["column"]).strip()
                if not col_str or col_str.lower() not in cols_lower:
                    problems.append(
                        f"Unknown column '{_safe_ident(col_str, secret_values)}' in "
                        f"'{context_prefix}.{rf_key}' for table '{safe_tbl}' (not in schemas)"
                    )
                op_val = item.get("op")
                if isinstance(op_val, str) and op_val.lower() not in VALID_OPS:
                    problems.append(
                        f"Unknown row filter op '{_safe_enum_token(op_val, secret_values)}' "
                        f"for column '{_safe_ident(col_str, secret_values)}' on table '{safe_tbl}'"
                    )
        elif isinstance(raw_rfs, Mapping):
            for k, v in raw_rfs.items():
                if isinstance(v, (list, tuple)) and v and isinstance(v[0], Mapping) and "column" in v[0]:
                    # Nested role -> list of filter dicts
                    for item in v:
                        if isinstance(item, Mapping) and "column" in item:
                            col_str = str(item["column"]).strip()
                            if not col_str or col_str.lower() not in cols_lower:
                                problems.append(
                                    f"Unknown column '{_safe_ident(col_str, secret_values)}' in "
                                    f"'{context_prefix}.{rf_key}' for table '{safe_tbl}' (not in schemas)"
                                )
                else:
                    col_str = str(k).strip()
                    if not col_str or col_str.lower() not in cols_lower:
                        problems.append(
                            f"Unknown column '{_safe_ident(col_str, secret_values)}' in "
                            f"'{context_prefix}.{rf_key}' for table '{safe_tbl}' (not in schemas)"
                        )


def _load_raw_placement_file(
    path: str | os.PathLike,
    problems: list[str],
    secret_values: Sequence[str],
) -> Mapping[str, Any] | None:
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.load(f, Loader=UniqueKeySafeLoader)
    except FileNotFoundError:
        problems.append(
            f"Static placement file not found: '{path}' — verify 'placement.static.placement_file'"
        )
        return None
    except yaml.YAMLError as exc:
        problems.append(_scrub(_format_yaml_error(exc, "static placement file", path), secret_values))
        return None
    except OSError as exc:
        problems.append(
            _scrub(
                f"Failed to read static placement file '{path}': {exc.strerror or type(exc).__name__}",
                secret_values,
            )
        )
        return None

    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        problems.append(
            f"Static placement file '{path}' must contain a YAML mapping at root, got {type(raw).__name__}"
        )
        return None
    return raw


def _load_raw_schemas_file(
    path: str | os.PathLike,
    problems: list[str],
    secret_values: Sequence[str],
) -> dict[str, list[str]] | None:
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
    except FileNotFoundError:
        problems.append(
            f"Static schemas file not found: '{path}' — verify 'placement.static.schemas_file'"
        )
        return None
    except OSError as exc:
        problems.append(
            _scrub(
                f"Failed to read static schemas file '{path}': {exc.strerror or type(exc).__name__}",
                secret_values,
            )
        )
        return None

    try:
        raw = json.loads(content, object_pairs_hook=_reject_duplicate_json_keys)
    except _DuplicateJsonKeyError as exc:
        problems.append(
            f"Static schemas file '{path}' contains duplicate JSON key "
            f"'{_safe_ident(exc.key, secret_values)}' — remove duplicate key entry"
        )
        return None
    except json.JSONDecodeError as exc:
        problems.append(_scrub(_format_json_error(exc, "static schemas file", path), secret_values))
        return None

    if not isinstance(raw, dict):
        problems.append(
            f"Static schemas file '{path}' root must be a JSON object mapping logical "
            f"table/cube names to column lists, got {type(raw).__name__}"
        )
        return None

    schemas_by_casefold: dict[str, str] = {}
    validated: dict[str, list[str]] = {}
    for key, cols in raw.items():
        if not isinstance(key, str) or not key.strip():
            problems.append(
                f"Static schemas file '{path}' contains an empty or non-string table/cube key"
            )
            continue
        folded = key.casefold()
        if folded in schemas_by_casefold:
            prev = schemas_by_casefold[folded]
            problems.append(
                f"Static schemas file '{path}' has case-insensitive key collision between "
                f"'{_safe_ident(prev, secret_values)}' and '{_safe_ident(key, secret_values)}'"
            )
            continue
        schemas_by_casefold[folded] = key

        safe_key = _safe_ident(key, secret_values)
        if not isinstance(cols, list):
            problems.append(
                f"Static schemas file '{path}' entry for '{safe_key}' must be a JSON list "
                f"of column names, got {type(cols).__name__}"
            )
            continue

        seen_cols: set[str] = set()
        clean_cols: list[str] = []
        for idx, col in enumerate(cols):
            if not isinstance(col, str) or not col.strip():
                problems.append(
                    f"Static schemas file '{path}' entry '{safe_key}' has invalid column at index {idx}"
                )
                continue
            cf = col.casefold()
            cl = col.lower()
            if cf in seen_cols or cl in seen_cols:
                problems.append(
                    f"Static schemas file '{path}' entry '{safe_key}' has duplicate column "
                    f"'{_safe_ident(col, secret_values)}' at index {idx}"
                )
                continue
            seen_cols.add(cf)
            seen_cols.add(cl)
            clean_cols.append(col)
        validated[key] = clean_cols

    return validated


def _validate_static_catalog(
    *,
    cfg: ServerConfig,
    pl_raw: Mapping[str, Any],
    placement_label: str,
    validated_schemas: dict[str, list[str]] | None,
    schemas_label: str,
    defined_roles: Mapping[str, Mapping[str, Any]],
    problems: list[str],
    secret_values: Sequence[str],
) -> Placement | None:
    """Validate static placement, engine wiring, schemas, columns, table policies, and PII rules."""
    for section in ("engines", "tables", "cubes", "policy"):
        sec_val = pl_raw.get(section)
        if sec_val is not None and not isinstance(sec_val, Mapping):
            problems.append(
                f"Static placement file '{placement_label}' section '{section}' must be a mapping, "
                f"got {type(sec_val).__name__}"
            )

    engines_raw = pl_raw.get("engines") if isinstance(pl_raw.get("engines"), Mapping) else {}
    tables_raw = pl_raw.get("tables") if isinstance(pl_raw.get("tables"), Mapping) else {}
    cubes_raw = pl_raw.get("cubes") if isinstance(pl_raw.get("cubes"), Mapping) else {}
    policy_raw = pl_raw.get("policy") if isinstance(pl_raw.get("policy"), Mapping) else {}

    # 1. Validate engines and connection wiring
    declared_engines: dict[str, str] = {}  # eng_name -> kind
    for eng_name, eng_spec in engines_raw.items():
        if not isinstance(eng_name, str) or not eng_name.strip():
            problems.append(f"Static placement file '{placement_label}' section 'engines' has empty engine name")
            continue
        eng_str = eng_name.strip()
        safe_eng = _safe_ident(eng_str, secret_values)
        if not isinstance(eng_spec, Mapping):
            problems.append(
                f"Static placement file '{placement_label}' entry 'engines.{safe_eng}' must be a mapping, "
                f"got {type(eng_spec).__name__}"
            )
            continue
        kind = str(eng_spec.get("kind", "")).strip()
        if kind not in VALID_ENGINE_KINDS:
            problems.append(
                f"Static placement file '{placement_label}' section 'engines' declares unknown engine kind "
                f"'{_safe_enum_token(kind, secret_values)}' for engine '{safe_eng}'"
            )
        declared_engines[eng_str] = kind

        if eng_str not in cfg.connections:
            problems.append(
                f"Static placement file '{placement_label}' declares engine '{safe_eng}' "
                f"(kind '{_safe_enum_token(kind, secret_values)}') in 'placement.engines', "
                f"but no matching entry 'connections.{safe_eng}' is configured"
            )
        else:
            conn_kind = getattr(cfg.connections[eng_str], "kind", None)
            if kind in VALID_ENGINE_KINDS and conn_kind != kind:
                problems.append(
                    f"Engine '{safe_eng}' kind mismatch between static placement "
                    f"('{placement_label}', kind '{_safe_enum_token(kind, secret_values)}') and 'connections.{safe_eng}' "
                    f"(kind '{_safe_enum_token(str(conn_kind), secret_values)}')"
                )

    # Validate policy engine-kind orders and policy.pii_engines
    for order_field in ("small_scan_order", "analytics_order"):
        if order_field in policy_raw:
            raw_order = policy_raw[order_field]
            if not isinstance(raw_order, (list, tuple)):
                problems.append(
                    f"Static placement file '{placement_label}' policy.{order_field} must be a list of engine kinds"
                )
            else:
                for k_item in raw_order:
                    k_str = str(k_item).strip()
                    if k_str not in VALID_ENGINE_KINDS:
                        problems.append(
                            f"Static placement file '{placement_label}' unknown engine kind "
                            f"'{_safe_enum_token(k_str, secret_values)}' in policy.{order_field}"
                        )

    pii_engines_set: set[str] = set()
    if "pii_engines" in policy_raw:
        raw_pe = policy_raw["pii_engines"]
        if not isinstance(raw_pe, (list, tuple)):
            problems.append(
                "Policy 'pii_engines' must be a list or tuple of non-empty engine name strings"
            )
        else:
            for item in raw_pe:
                if not isinstance(item, str) or not item.strip():
                    problems.append(
                        "Policy 'pii_engines' must contain only non-empty engine name strings"
                    )
                    continue
                pe_name = item.strip()
                pii_engines_set.add(pe_name)
                safe_pe = _safe_ident(pe_name, secret_values)
                if pe_name not in declared_engines:
                    problems.append(
                        f"Unknown engine '{safe_pe}' in policy.pii_engines (not declared in placement.engines)"
                    )
                elif pe_name not in cfg.connections:
                    problems.append(
                        f"Unknown engine '{safe_pe}' in policy.pii_engines (not configured in connections)"
                    )

    # Pre-collect pii_tables from top-level placement and placement.policy so PII rules fold in
    effective_pii_lower: set[str] = set()
    for pii_src_val in (pl_raw.get("pii_tables"), policy_raw.get("pii_tables")):
        if isinstance(pii_src_val, (list, tuple)):
            for p_item in pii_src_val:
                if isinstance(p_item, str) and p_item.strip():
                    effective_pii_lower.add(p_item.strip().lower())

    # 2. Validate tables (serves, PII rules, collisions)
    known_tables_by_lower: dict[str, str] = {}
    known_tables_by_casefold: dict[str, str] = {}
    for tbl_name, tbl_spec in tables_raw.items():
        if not isinstance(tbl_name, str) or not tbl_name.strip():
            problems.append("Placement table name cannot be empty")
            continue
        tbl_str = tbl_name.strip()
        safe_tbl = _safe_ident(tbl_str, secret_values)
        if not isinstance(tbl_spec, Mapping):
            problems.append(f"Placement table '{safe_tbl}' spec must be a mapping")
            continue

        t_lower = tbl_str.lower()
        t_fold = tbl_str.casefold()
        if t_lower in known_tables_by_lower or t_fold in known_tables_by_casefold:
            prev = known_tables_by_lower.get(t_lower) or known_tables_by_casefold.get(t_fold, "")
            problems.append(
                f"Static placement file '{placement_label}' table '{safe_tbl}' collides case-insensitively "
                f"(collides under casefold) with '{_safe_ident(prev, secret_values)}'"
            )
        else:
            known_tables_by_lower[t_lower] = tbl_str
            known_tables_by_casefold[t_fold] = tbl_str

        pii_val = tbl_spec.get("pii", False)
        if not isinstance(pii_val, bool):
            problems.append(
                f"Static placement file '{placement_label}' table '{safe_tbl}' field 'pii' "
                f"must be a boolean, got {type(pii_val).__name__}"
            )
            is_pii = t_lower in effective_pii_lower
        else:
            is_pii = pii_val or (t_lower in effective_pii_lower)

        serve_raw = tbl_spec.get("serve")
        if serve_raw is not None and not isinstance(serve_raw, Mapping):
            problems.append(
                f"Static placement file '{placement_label}' field 'tables.{safe_tbl}.serve' "
                f"must be a mapping, got {type(serve_raw).__name__}"
            )
            continue
        if not serve_raw:
            problems.append(
                f"Static placement file '{placement_label}' table '{safe_tbl}' must define a non-empty 'serve' mapping"
            )
            continue

        for s_eng, s_val in serve_raw.items():
            s_eng_str = str(s_eng).strip()
            safe_s_eng = _safe_ident(s_eng_str, secret_values)
            if s_eng_str not in declared_engines:
                problems.append(
                    f"Table '{safe_tbl}' serve references unknown engine '{safe_s_eng}'"
                )
            elif s_eng_str not in cfg.connections:
                problems.append(
                    f"Table '{safe_tbl}' serve engine '{safe_s_eng}' has no entry in connections"
                )

            if isinstance(s_val, str):
                if not s_val.strip():
                    problems.append(
                        f"Table '{safe_tbl}' has empty physical name for engine '{safe_s_eng}'"
                    )
            elif isinstance(s_val, Mapping):
                phys = s_val.get("name", "")
                if not isinstance(phys, str) or not phys.strip():
                    problems.append(
                        f"Table '{safe_tbl}' has empty physical name for engine '{safe_s_eng}'"
                    )
            else:
                problems.append(
                    f"Table '{safe_tbl}' serve has invalid format for engine '{safe_s_eng}'"
                )

        if is_pii:
            if len(serve_raw) != 1:
                problems.append(
                    f"PII table '{safe_tbl}' must have exactly one serve, got {len(serve_raw)}"
                )
            for s_eng in serve_raw.keys():
                s_eng_str = str(s_eng).strip()
                safe_s_eng = _safe_ident(s_eng_str, secret_values)
                eng_kind = declared_engines.get(s_eng_str)
                conn_obj = cfg.connections.get(s_eng_str)
                if eng_kind is None and conn_obj is not None:
                    eng_kind = getattr(conn_obj, "kind", None)
                if eng_kind == "duckdb" and s_eng_str not in pii_engines_set:
                    profile_note = ""
                    if isinstance(conn_obj, DuckDBConnectionConfig):
                        profile_note = f" (profile '{conn_obj.profile}')"
                    problems.append(
                        f"PII table '{safe_tbl}' cannot be served on duckdb engine "
                        f"'{safe_s_eng}'{profile_note} unless listed in policy.pii_engines"
                    )

    # 3. Validate cubes
    known_cubes_by_lower: dict[str, str] = {}
    known_cubes_by_casefold: dict[str, str] = {}
    for cube_name, cube_spec in cubes_raw.items():
        if not isinstance(cube_name, str) or not cube_name.strip():
            problems.append("Placement cube name cannot be empty")
            continue
        cube_str = cube_name.strip()
        safe_cube = _safe_ident(cube_str, secret_values)
        if not isinstance(cube_spec, Mapping):
            problems.append(f"Placement cube '{safe_cube}' spec must be a mapping")
            continue

        c_lower = cube_str.lower()
        c_fold = cube_str.casefold()
        if c_lower in known_tables_by_lower or c_fold in known_tables_by_casefold:
            prev = known_tables_by_lower.get(c_lower) or known_tables_by_casefold.get(c_fold, "")
            problems.append(
                f"Static placement file '{placement_label}' cube '{safe_cube}' collides case-insensitively "
                f"(collides under casefold) with table '{_safe_ident(prev, secret_values)}'"
            )
        elif c_lower in known_cubes_by_lower or c_fold in known_cubes_by_casefold:
            prev = known_cubes_by_lower.get(c_lower) or known_cubes_by_casefold.get(c_fold, "")
            problems.append(
                f"Static placement file '{placement_label}' cube '{safe_cube}' collides case-insensitively "
                f"(collides under casefold) with cube '{_safe_ident(prev, secret_values)}'"
            )
        else:
            known_cubes_by_lower[c_lower] = cube_str
            known_cubes_by_casefold[c_fold] = cube_str

        cube_eng = str(cube_spec.get("engine", "")).strip()
        safe_c_eng = _safe_ident(cube_eng, secret_values)
        if cube_eng not in declared_engines:
            problems.append(
                f"Static placement file '{placement_label}' section 'cubes' cube '{safe_cube}' "
                f"references unknown engine '{safe_c_eng}'"
            )
        else:
            if declared_engines[cube_eng] != "cube":
                problems.append(
                    f"Static placement file '{placement_label}' section 'cubes' cube '{safe_cube}' "
                    f"engine '{safe_c_eng}' is not of kind 'cube'"
                )
            if cube_eng not in cfg.connections:
                problems.append(
                    f"Static placement file '{placement_label}' section 'cubes' cube '{safe_cube}' "
                    f"engine '{safe_c_eng}' has no entry in connections"
                )

        cube_pii_val = cube_spec.get("pii", False)
        if not isinstance(cube_pii_val, bool):
            problems.append(
                f"Static placement file '{placement_label}' cube '{safe_cube}' field 'pii' "
                f"must be a boolean, got {type(cube_pii_val).__name__}"
            )
            is_cube_pii = c_lower in effective_pii_lower
        else:
            is_cube_pii = cube_pii_val or (c_lower in effective_pii_lower)

        if is_cube_pii and cube_eng not in pii_engines_set:
            problems.append(
                f"PII cube '{safe_cube}' engine '{safe_c_eng}' must be listed in policy.pii_engines"
            )

        raw_dims_pre = cube_spec.get("dimensions")
        dims_list = (
            [str(d) for d in raw_dims_pre]
            if isinstance(raw_dims_pre, (list, tuple))
            else []
        )
        dims_set = set(dims_list)
        mv_raw = cube_spec.get("mask_variants")
        if mv_raw is not None:
            if not isinstance(mv_raw, Mapping):
                problems.append(
                    f"Static placement file '{placement_label}' section 'cubes' cube '{safe_cube}' "
                    f"mask_variants must be a mapping"
                )
            else:
                for base, methods in mv_raw.items():
                    base_str = str(base)
                    if base_str not in dims_set:
                        problems.append(
                            f"Static placement file '{placement_label}' section 'cubes' unknown dimension column "
                            f"'{_safe_ident(base_str, secret_values)}' in mask_variants for cube '{safe_cube}'"
                        )
                    if isinstance(methods, (list, tuple)):
                        for m in methods:
                            if str(m) not in VALID_TWIN_METHODS:
                                problems.append(
                                    f"Static placement file '{placement_label}' section 'cubes' cube '{safe_cube}' "
                                    f"mask_variants has invalid method '{_safe_enum_token(str(m), secret_values)}' "
                                    f"for '{_safe_ident(base_str, secret_values)}'"
                                )
                    else:
                        problems.append(
                            f"Static placement file '{placement_label}' section 'cubes' cube '{safe_cube}' "
                            f"mask_variants methods for '{_safe_ident(base_str, secret_values)}' must be a list"
                        )

    # 4. Validate table references in placement/policy/roles
    for container_label, container in (("placement", pl_raw), ("placement.policy", policy_raw)):
        if "pii_tables" in container and container["pii_tables"] is not None:
            pt_val = container["pii_tables"]
            if isinstance(pt_val, (list, tuple)):
                for pt in pt_val:
                    if not isinstance(pt, str) or not pt.strip():
                        problems.append(
                            f"'{container_label}.pii_tables' must contain only non-empty table name strings"
                        )
                        continue
                    pt_str = pt.strip()
                    pt_low = pt_str.lower()
                    if pt_low not in known_tables_by_lower and pt_low not in known_cubes_by_lower:
                        problems.append(
                            f"Unknown table '{_safe_ident(pt_str, secret_values)}' referenced in "
                            f"'{container_label}.pii_tables'"
                        )
            else:
                problems.append(f"'{container_label}.pii_tables' must be a list of table names")

        for tbl_map_key in ("table_policies", "masks", "row_filters"):
            if container_label == "placement" and tbl_map_key == "tables":
                continue
            tm_val = container.get(tbl_map_key)
            if isinstance(tm_val, Mapping):
                for ref_tbl in tm_val.keys():
                    ref_str = str(ref_tbl).strip()
                    if ref_str.lower() not in known_tables_by_lower:
                        problems.append(
                            f"Unknown table '{_safe_ident(ref_str, secret_values)}' referenced in "
                            f"'{container_label}.{tbl_map_key}'"
                        )

    if isinstance(policy_raw.get("tables"), Mapping):
        for ref_tbl in policy_raw["tables"].keys():
            ref_str = str(ref_tbl).strip()
            if ref_str.lower() not in known_tables_by_lower:
                problems.append(
                    f"Unknown table '{_safe_ident(ref_str, secret_values)}' referenced in "
                    f"'placement.policy.tables'"
                )

    for r_name, r_spec in defined_roles.items():
        r_tables = r_spec.get("tables")
        if isinstance(r_tables, Mapping):
            for ref_tbl in r_tables.keys():
                ref_str = str(ref_tbl).strip()
                if ref_str.lower() not in known_tables_by_lower:
                    problems.append(
                        f"Unknown table '{_safe_ident(ref_str, secret_values)}' referenced in "
                        f"role '{_safe_ident(r_name, secret_values)}' policy"
                    )

    # 5. Validate schemas <-> placement table/cube coverage and column existence
    if validated_schemas is not None:
        schemas_by_lower: dict[str, list[str]] = {}
        schemas_key_by_casefold: dict[str, str] = {}
        for sch_key, cols in validated_schemas.items():
            schemas_by_lower[sch_key.lower()] = cols
            schemas_key_by_casefold[sch_key.casefold()] = sch_key

        expected_by_casefold: dict[str, str] = {}
        expected_by_lower: dict[str, str] = {}
        for logical_name in list(known_tables_by_lower.values()) + list(known_cubes_by_lower.values()):
            expected_by_casefold[logical_name.casefold()] = logical_name
            expected_by_lower[logical_name.lower()] = logical_name

        for folded, pl_name in sorted(expected_by_casefold.items()):
            if folded not in schemas_key_by_casefold:
                problems.append(
                    f"Static schemas file '{schemas_label}' is missing schema entry for placement "
                    f"table/cube '{_safe_ident(pl_name, secret_values)}'"
                )
            else:
                sch_name = schemas_key_by_casefold[folded]
                if sch_name.lower() != pl_name.lower():
                    problems.append(
                        f"Static schemas file '{schemas_label}' key '{_safe_ident(sch_name, secret_values)}' "
                        f"matches placement name '{_safe_ident(pl_name, secret_values)}' by casefold "
                        f"but differs under lowercase"
                    )

        for folded, sch_name in sorted(schemas_key_by_casefold.items()):
            if folded not in expected_by_casefold:
                problems.append(
                    f"Static schemas file '{schemas_label}' contains unknown stale/extra schema key "
                    f"'{_safe_ident(sch_name, secret_values)}' not present in placement tables or cubes"
                )

        # Check table columns: keys, masks, row_filters, columns
        for t_lower, tbl_str in known_tables_by_lower.items():
            cols = schemas_by_lower.get(t_lower)
            if cols is None:
                continue
            cols_lower = {c.lower() for c in cols}
            tbl_spec = tables_raw.get(tbl_str, {})
            if not isinstance(tbl_spec, Mapping):
                continue

            safe_tbl = _safe_ident(tbl_str, secret_values)
            raw_keys = tbl_spec.get("keys")
            if raw_keys is not None and not isinstance(raw_keys, (list, tuple)):
                problems.append(
                    f"Table '{safe_tbl}' field 'keys' must be a list of column names"
                )
            else:
                for key_col in raw_keys or []:
                    k_str = str(key_col).strip()
                    if not k_str or k_str.lower() not in cols_lower:
                        problems.append(
                            f"Unknown key column '{_safe_ident(k_str, secret_values)}' on table "
                            f"'{safe_tbl}' (not found in schemas for '{safe_tbl}')"
                        )

            _validate_table_column_refs_in_spec(
                tbl_name=tbl_str,
                spec=tbl_spec,
                cols_lower=cols_lower,
                context_prefix=f"placement.tables.{safe_tbl}",
                problems=problems,
                secret_values=secret_values,
            )

            # Policy-level table specs (if present)
            for pol_tbl_container_name, pol_tbl_container in (
                ("placement.policy.tables", policy_raw.get("tables")),
                ("placement.table_policies", pl_raw.get("table_policies")),
            ):
                if isinstance(pol_tbl_container, Mapping):
                    for pk, pv in pol_tbl_container.items():
                        if str(pk).strip().lower() == t_lower and isinstance(pv, Mapping):
                            _validate_table_column_refs_in_spec(
                                tbl_name=tbl_str,
                                spec=pv,
                                cols_lower=cols_lower,
                                context_prefix=f"{pol_tbl_container_name}.{safe_tbl}",
                                problems=problems,
                                secret_values=secret_values,
                            )

            for field_kind in ("masks", "row_filters"):
                for root_label, root_map in (
                    (f"placement.policy.{field_kind}", policy_raw.get(field_kind)),
                    (f"placement.{field_kind}", pl_raw.get(field_kind)),
                ):
                    if isinstance(root_map, Mapping):
                        for pk, pv in root_map.items():
                            if str(pk).strip().lower() == t_lower:
                                _validate_table_column_refs_in_spec(
                                    tbl_name=tbl_str,
                                    spec={field_kind: pv},
                                    cols_lower=cols_lower,
                                    context_prefix=f"{root_label}.{safe_tbl}",
                                    problems=problems,
                                    secret_values=secret_values,
                                )

            # Role-level table column refs (if roles defined)
            for r_name, r_spec in defined_roles.items():
                r_tables = r_spec.get("tables")
                if isinstance(r_tables, Mapping):
                    for pk, pv in r_tables.items():
                        if str(pk).strip().lower() == t_lower and isinstance(pv, Mapping):
                            _validate_table_column_refs_in_spec(
                                tbl_name=tbl_str,
                                spec=pv,
                                cols_lower=cols_lower,
                                context_prefix=f"roles.{_safe_ident(r_name, secret_values)}.tables.{safe_tbl}",
                                problems=problems,
                                secret_values=secret_values,
                            )

        # Check cube columns: measures, dimensions, mask_variants
        for c_lower, cube_str in known_cubes_by_lower.items():
            cols = schemas_by_lower.get(c_lower)
            if cols is None:
                continue
            cols_lower = {c.lower() for c in cols}
            cube_spec = cubes_raw.get(cube_str, {})
            if not isinstance(cube_spec, Mapping):
                continue

            safe_cube = _safe_ident(cube_str, secret_values)
            raw_measures = cube_spec.get("measures")
            if raw_measures is not None and not isinstance(raw_measures, (list, tuple)):
                problems.append(
                    f"Cube '{safe_cube}' field 'measures' must be a list of column names"
                )
            else:
                for m_col in raw_measures or []:
                    m_str = str(m_col).strip()
                    if not m_str or m_str.lower() not in cols_lower:
                        problems.append(
                            f"Unknown measure column '{_safe_ident(m_str, secret_values)}' on cube "
                            f"'{safe_cube}' (not found in schemas for '{safe_cube}')"
                        )

            raw_dimensions = cube_spec.get("dimensions")
            if raw_dimensions is not None and not isinstance(raw_dimensions, (list, tuple)):
                problems.append(
                    f"Cube '{safe_cube}' field 'dimensions' must be a list of column names"
                )
            else:
                for d_col in raw_dimensions or []:
                    d_str = str(d_col).strip()
                    if not d_str or d_str.lower() not in cols_lower:
                        problems.append(
                            f"Unknown dimension column '{_safe_ident(d_str, secret_values)}' on cube "
                            f"'{safe_cube}' (not found in schemas for '{safe_cube}')"
                        )
            mv_raw = cube_spec.get("mask_variants")
            if isinstance(mv_raw, Mapping):
                for base in mv_raw.keys():
                    b_str = str(base).strip()
                    if not b_str or b_str.lower() not in cols_lower:
                        problems.append(
                            f"Unknown mask_variants column '{_safe_ident(b_str, secret_values)}' on cube "
                            f"'{safe_cube}' (not found in schemas for '{safe_cube}')"
                        )

    if problems:
        return None

    try:
        return Placement.from_dict(pl_raw)
    except (PlacementError, TypeError, ValueError, AttributeError, OverflowError):
        problems.append(
            f"Invalid static placement in '{placement_label}' — verify 'engines', 'tables', "
            f"'cubes', and 'policy' definitions"
        )
        return None


def _validate_discovery_table_ref_offline(
    raw_name: Any,
    *,
    default_catalog: str,
    field_label: str,
    problems: list[str],
    secret_values: Sequence[str],
) -> None:
    if not isinstance(raw_name, str) or not raw_name:
        problems.append(
            f"Discovery '{field_label}' must contain only non-empty 3-part table name strings"
        )
        return
    if '"' in raw_name or "'" in raw_name or any(ch.isspace() for ch in raw_name):
        problems.append(
            f"Invalid table name in discovery '{field_label}': quotes and whitespace are not permitted "
            f"(expected unquoted '{_safe_ident(default_catalog, secret_values)}.<namespace>.<table>')"
        )
        return
    parts = raw_name.split(".")
    if len(parts) != 3 or not all(_DISC_LOGICAL_SEGMENT_RE.fullmatch(p) for p in parts):
        problems.append(
            f"Invalid table name '{_safe_ident(raw_name, secret_values)}' in discovery '{field_label}': "
            f"expected 3-part '<default_catalog>.<namespace>.<table>'"
        )
        return
    if parts[0].lower() != default_catalog.lower():
        problems.append(
            f"Discovery table '{_safe_ident(raw_name, secret_values)}' in '{field_label}' has catalog "
            f"prefix '{_safe_ident(parts[0], secret_values)}', expected default_catalog "
            f"'{_safe_ident(default_catalog, secret_values)}'"
        )


def _validate_discovery_offline(
    *,
    cfg: ServerConfig,
    problems: list[str],
    secret_values: Sequence[str],
) -> None:
    """Validate offline-checkable discovery engine wiring and PII configuration."""
    disc_cfg = cfg.placement.discovery_config
    assert disc_cfg is not None

    for eng_name, eng_cfg in disc_cfg.engines.items():
        safe_eng = _safe_ident(eng_name, secret_values)
        safe_eng_kind = _safe_enum_token(eng_cfg.kind, secret_values)
        if eng_cfg.kind not in VALID_ENGINE_KINDS:
            problems.append(
                f"Unknown engine kind '{safe_eng_kind}' "
                f"for discovery engine '{safe_eng}'"
            )
        if eng_name not in cfg.connections:
            problems.append(
                f"Discovery engine 'placement.discovery.engines.{safe_eng}' "
                f"(kind '{safe_eng_kind}') has no matching entry 'connections.{safe_eng}'"
            )
        else:
            conn_kind = getattr(cfg.connections[eng_name], "kind", None)
            if conn_kind != eng_cfg.kind:
                problems.append(
                    f"Discovery engine 'placement.discovery.engines.{safe_eng}' has kind "
                    f"'{safe_eng_kind}', but 'connections.{safe_eng}' has kind "
                    f"'{_safe_enum_token(conn_kind, secret_values)}'"
                )

    if disc_cfg.lakebase is not None:
        lb_eng = disc_cfg.lakebase.engine
        safe_lb = _safe_ident(lb_eng, secret_values)
        if lb_eng not in disc_cfg.engines:
            problems.append(
                f"Discovery 'placement.discovery.lakebase.engine' references unknown engine '{safe_lb}'"
            )
        if lb_eng not in cfg.connections:
            problems.append(
                f"Discovery 'placement.discovery.lakebase.engine' references '{safe_lb}', "
                f"but 'connections.{safe_lb}' is not configured"
            )
        else:
            lb_conn = cfg.connections[lb_eng]
            if not isinstance(lb_conn, LakebaseConnectionConfig):
                conn_kind = getattr(lb_conn, "kind", None)
                problems.append(
                    f"Discovery 'placement.discovery.lakebase.engine' ('{safe_lb}') requires "
                    f"'connections.{safe_lb}' to have kind 'lakebase', got "
                    f"'{_safe_enum_token(conn_kind, secret_values)}'"
                )

        for st_name in disc_cfg.lakebase.synced_tables.keys():
            _validate_discovery_table_ref_offline(
                st_name,
                default_catalog=disc_cfg.default_catalog,
                field_label="lakebase.synced_tables",
                problems=problems,
                secret_values=secret_values,
            )

    for pt in disc_cfg.pii_tables:
        _validate_discovery_table_ref_offline(
            pt,
            default_catalog=disc_cfg.default_catalog,
            field_label="pii_tables",
            problems=problems,
            secret_values=secret_values,
        )

    if disc_cfg.lakekeeper is None:
        if disc_cfg.pii_tables:
            problems.append(
                "Discovery 'pii_tables' cannot be configured when 'placement.discovery.lakekeeper' is not configured"
            )
        if disc_cfg.lakebase is not None and any(
            st.pii for st in disc_cfg.lakebase.synced_tables.values()
        ):
            problems.append(
                "Discovery 'synced_tables' with 'pii: true' cannot be configured when 'placement.discovery.lakekeeper' is not configured"
            )

    pii_engines_set = set(disc_cfg.pii_engines)
    for pe in disc_cfg.pii_engines:
        safe_pe = _safe_ident(pe, secret_values)
        if pe not in disc_cfg.engines:
            problems.append(
                f"Unknown engine '{safe_pe}' in discovery pii_engines "
                f"(not declared in placement.discovery.engines)"
            )
        elif pe not in cfg.connections:
            problems.append(
                f"Unknown engine '{safe_pe}' in discovery pii_engines "
                f"(not configured in connections)"
            )

    # Offline PII serving feasibility check for discovery pii_tables and synced pii tables
    synced_lower = (
        {k.lower() for k in disc_cfg.lakebase.synced_tables.keys()}
        if disc_cfg.lakebase is not None
        else set()
    )
    has_starrocks = any(e.kind == "starrocks" for e in disc_cfg.engines.values())
    has_approved_duckdb = any(
        e.kind == "duckdb"
        and e.profile == "iceberg_direct"
        and e.name in pii_engines_set
        for e in disc_cfg.engines.values()
    )
    unapproved_duck_names = [
        _safe_ident(e.name, secret_values)
        for e in disc_cfg.engines.values()
        if e.kind == "duckdb" and e.name not in pii_engines_set
    ]

    for pt in disc_cfg.pii_tables:
        safe_pt = _safe_ident(pt, secret_values)
        in_synced = isinstance(pt, str) and pt.lower() in synced_lower
        if not (in_synced or has_starrocks or has_approved_duckdb):
            if unapproved_duck_names:
                unapproved_str = ", ".join(unapproved_duck_names)
                problems.append(
                    f"Discovery PII table '{safe_pt}' has no approved serving engine "
                    f"(not in lakebase.synced_tables, no starrocks engine, and duckdb engine(s) "
                    f"[{unapproved_str}] are not listed in pii_engines)"
                )
            else:
                problems.append(
                    f"Discovery PII table '{safe_pt}' has no valid serving engine configured "
                    f"(not in lakebase.synced_tables and no starrocks or approved iceberg_direct pii_engines)"
                )


def run_initial_discovery_refresh_or_raise(
    refresher: Any,
    *,
    discover_fn: Any = None,
    cfg: ServerConfig | None = None,
    env: Mapping[str, str] | None = None,
    secret_values: Sequence[str] = (),
) -> None:
    """Run the initial catalog discovery refresh before serving and fail closed on error."""
    refresher_secrets_raw = (
        getattr(refresher, "secret_values", None) if refresher is not None else None
    )
    refresher_secrets: tuple[str, ...] = ()
    if isinstance(refresher_secrets_raw, (list, tuple, set, frozenset)):
        refresher_secrets = tuple(
            s for s in refresher_secrets_raw if isinstance(s, str) and s
        )
    elif isinstance(refresher_secrets_raw, Sequence) and not isinstance(
        refresher_secrets_raw, (str, bytes)
    ):
        refresher_secrets = tuple(
            s for s in refresher_secrets_raw if isinstance(s, str) and s
        )

    merged_secrets = tuple(
        sorted(
            dict.fromkeys(
                (
                    *_collect_secret_values(env, cfg=cfg),
                    *refresher_secrets,
                    *secret_values,
                )
            ),
            key=len,
            reverse=True,
        )
    )
    if refresher is None:
        if discover_fn is not None:
            try:
                res = discover_fn()
                Placement.from_dict(res.placement)
            except Exception as exc:
                raise ServerConfigError(
                    _scrub(
                        f"Initial discovery refresh failed: {exc}",
                        merged_secrets,
                    )
                ) from exc
        return

    captured_exc: BaseException | None = None
    has_discover_attr = hasattr(refresher, "discover_fn")
    orig_discover_fn = getattr(refresher, "discover_fn", None) if has_discover_attr else None
    target_fn = discover_fn if discover_fn is not None else orig_discover_fn

    if has_discover_attr and callable(target_fn):
        def _capturing_discover() -> Any:
            nonlocal captured_exc
            try:
                res = target_fn()
                Placement.from_dict(res.placement)
                return res
            except BaseException as exc:
                captured_exc = exc
                raise

        try:
            refresher.discover_fn = _capturing_discover
            ok = refresher.refresh_once()
        except Exception as exc:
            captured_exc = exc
            ok = False
        finally:
            refresher.discover_fn = orig_discover_fn
    else:
        try:
            ok = refresher.refresh_once()
        except Exception as exc:
            captured_exc = exc
            ok = False

    if not ok:
        if captured_exc is None:
            tracked_err = getattr(refresher, "last_refresh_error", None)
            if isinstance(tracked_err, list) and tracked_err and tracked_err[0] is not None:
                captured_exc = tracked_err[0]
        scrubbed_arr = getattr(refresher, "last_refresh_error_scrubbed", None)
        if isinstance(scrubbed_arr, list) and scrubbed_arr and scrubbed_arr[0]:
            raise ServerConfigError(
                _scrub(
                    f"Initial discovery refresh failed: {scrubbed_arr[0]}",
                    merged_secrets,
                )
            ) from captured_exc
        if captured_exc is not None:
            raise ServerConfigError(
                _scrub(
                    f"Initial discovery refresh failed: {captured_exc}",
                    merged_secrets,
                )
            ) from captured_exc
        last_err = getattr(refresher, "last_error", None) or "unknown_error"
        raise ServerConfigError(
            _scrub(
                f"Initial discovery refresh failed ({last_err})",
                merged_secrets,
            )
        )


def validate_startup(
    config: ServerConfig | Mapping[str, Any] | str | os.PathLike,
    *,
    env: Mapping[str, str] | None = None,
    raw_placement: Mapping[str, Any] | None = None,
    raw_schemas: Mapping[str, Sequence[str]] | None = None,
    check_grant_keys: bool = True,
) -> ValidationReport:
    """Perform comprehensive fail-closed startup validation.

    Validates engine declarations and connection wiring, table/cube schema alignment,
    key/measure/dimension/mask/row_filter column existence, static role references (if any),
    PII placement invariants, grant public keys, environment variables, and secret files.

    Raises:
        StartupValidationError: subclass of ``ServerConfigError`` containing all collected
            secret-safe problem messages when any validation check fails.
    """
    problems: list[str] = []
    raw_cfg: Mapping[str, Any] | None = None
    cfg: ServerConfig | None = None

    initial_secrets = _collect_secret_values(env)

    # 1. Load raw config if path/mapping
    if isinstance(config, ServerConfig):
        cfg = config
    elif isinstance(config, (str, os.PathLike)):
        try:
            with open(config, "r", encoding="utf-8") as f:
                loaded = yaml.load(f, Loader=UniqueKeySafeLoader)
        except FileNotFoundError as exc:
            raise StartupValidationError(
                (_scrub(f"Config file not found: '{config}'", initial_secrets),)
            ) from exc
        except yaml.YAMLError as exc:
            raise StartupValidationError(
                (_scrub(_format_yaml_error(exc, "config file", config), initial_secrets),)
            ) from exc
        except OSError as exc:
            raise StartupValidationError(
                (
                    _scrub(
                        f"Failed to read config file '{config}': {exc.strerror or type(exc).__name__}",
                        initial_secrets,
                    ),
                )
            ) from exc

        if not isinstance(loaded, Mapping):
            raise StartupValidationError(
                (f"Config file '{config}' must contain a mapping, got {type(loaded).__name__}",)
            )
        raw_cfg = loaded
    elif isinstance(config, Mapping):
        raw_cfg = config
    else:
        raise TypeError(f"Unsupported config type: {type(config).__name__}")

    secret_values = _collect_secret_values(env, cfg=cfg, raw_cfg=raw_cfg)

    if cfg is None:
        assert raw_cfg is not None
        try:
            cfg = ServerConfig.from_dict(raw_cfg, load_keys=False, env=env)
        except ServerConfigError as exc:
            problems.append(_scrub(str(exc), secret_values))

    secret_values = _collect_secret_values(env, cfg=cfg, raw_cfg=raw_cfg)

    # If ServerConfig itself could not be parsed, still check any static role references in raw_cfg/placement
    if cfg is None:
        raw_disc = None
        raw_pl_for_roles = raw_placement
        if isinstance(raw_cfg, Mapping) and isinstance(raw_cfg.get("placement"), Mapping):
            d_sub = raw_cfg["placement"].get("discovery")
            if isinstance(d_sub, Mapping):
                raw_disc = d_sub
            st_sub = raw_cfg["placement"].get("static")
            if (
                raw_pl_for_roles is None
                and isinstance(st_sub, Mapping)
                and isinstance(st_sub.get("placement_file"), str)
                and st_sub["placement_file"].strip()
            ):
                raw_pl_for_roles = _load_raw_placement_file(
                    st_sub["placement_file"].strip(), problems, secret_values
                )
        _collect_role_definitions_and_references(
            raw_cfg=raw_cfg,
            raw_placement=raw_pl_for_roles,
            raw_discovery=raw_disc,
            problems=problems,
            secret_values=secret_values,
        )
        raise StartupValidationError([_scrub(p, secret_values) for p in problems])

    # 2. Validate grant keys, environment variables, secret files, and iceberg_direct extension files
    for eng_name, conn in cfg.connections.items():
        if isinstance(conn, DuckDBConnectionConfig) and conn.profile == "iceberg_direct":
            safe_eng = _safe_ident(eng_name, secret_values)
            if "iceberg" in conn.extensions and "avro" not in conn.extensions:
                problems.append(
                    f"DuckDB iceberg_direct connection '{safe_eng}' requires 'avro' in 'extensions' when 'iceberg' is configured"
                )
            try:
                check_extension_files_on_disk(
                    conn.extension_directory,
                    conn.extensions,
                    eng_name=safe_eng,
                    only_when_populated=True,
                )
            except ServerConfigError as exc:
                problems.append(_scrub(str(exc), secret_values))
    if check_grant_keys:
        try:
            cfg.load_grant_keys()
        except ServerConfigError as exc:
            problems.append(_scrub(str(exc), secret_values))

    seen_vars: set[str] = set()
    for var_name in cfg.get_referenced_env_vars():
        if var_name in seen_vars:
            continue
        seen_vars.add(var_name)
        try:
            cfg.get_env(var_name, env)
        except ServerConfigError as exc:
            problems.append(_scrub(str(exc), secret_values))

    _check_secret_files_all(cfg, problems, secret_values)

    if cfg.audit is not None:
        try:
            verify_audit_config_offline(cfg.audit, error_cls=AuditStartupError)
        except ServerConfigError as exc:
            problems.append(_scrub(str(exc), secret_values))

    # 3. Mode-specific validation (static vs discovery)
    if cfg.placement.is_static:
        static_cfg = cfg.placement.static
        assert static_cfg is not None

        if raw_placement is not None:
            pl_raw: Mapping[str, Any] | None = raw_placement
            placement_label = "<inline placement>"
        else:
            placement_label = static_cfg.placement_file
            pl_raw = _load_raw_placement_file(
                static_cfg.placement_file, problems, secret_values
            )

        if raw_schemas is not None:
            validated_schemas: dict[str, list[str]] | None = {
                str(k): [str(c) for c in cols] for k, cols in raw_schemas.items()
            }
            schemas_label = "<inline schemas>"
        else:
            validated_schemas = _load_raw_schemas_file(
                static_cfg.schemas_file, problems, secret_values
            )
            schemas_label = static_cfg.schemas_file

        defined_roles, role_refs_checked = _collect_role_definitions_and_references(
            raw_cfg=raw_cfg,
            raw_placement=pl_raw,
            raw_discovery=None,
            problems=problems,
            secret_values=secret_values,
        )

        placement_obj: Placement | None = None
        if pl_raw is not None:
            placement_obj = _validate_static_catalog(
                cfg=cfg,
                pl_raw=pl_raw,
                placement_label=placement_label,
                validated_schemas=validated_schemas,
                schemas_label=schemas_label,
                defined_roles=defined_roles,
                problems=problems,
                secret_values=secret_values,
            )

        if problems:
            raise StartupValidationError([_scrub(p, secret_values) for p in problems])

        notes: list[str] = []
        if role_refs_checked == 0 and not defined_roles:
            notes.append("no_static_role_references")

        return ValidationReport(
            mode="static",
            ok=True,
            placement=placement_obj,
            schemas=validated_schemas,
            skipped=(),
            notes=tuple(notes),
            static_role_references_checked=role_refs_checked,
        )

    # Discovery mode offline validation
    raw_disc = cfg.placement.discovery if isinstance(cfg.placement.discovery, Mapping) else None
    defined_roles, role_refs_checked = _collect_role_definitions_and_references(
        raw_cfg=raw_cfg,
        raw_placement=None,
        raw_discovery=raw_disc,
        problems=problems,
        secret_values=secret_values,
    )
    _validate_discovery_offline(
        cfg=cfg,
        problems=problems,
        secret_values=secret_values,
    )

    if problems:
        raise StartupValidationError([_scrub(p, secret_values) for p in problems])

    notes_disc: list[str] = []
    if role_refs_checked == 0 and not defined_roles:
        notes_disc.append("no_static_role_references")

    return ValidationReport(
        mode="discovery",
        ok=True,
        placement=None,
        schemas=None,
        skipped=("live_catalog_discovery",),
        notes=tuple(notes_disc),
        static_role_references_checked=role_refs_checked,
    )


def run_validate_cli(
    config_path: str | os.PathLike,
    *,
    env: Mapping[str, str] | None = None,
) -> int:
    """Run offline startup validation for the CLI ``validate`` subcommand.

    Prints ``ok`` (plus any offline skip note in discovery mode) and returns ``0`` on
    success, or prints one ``Error: ...`` line per problem to ``stderr`` and returns ``2``.
    """
    secret_values = _collect_secret_values(env)
    try:
        report = validate_startup(config_path, env=env)
    except StartupValidationError as exc:
        for problem in exc.problems:
            print(f"Error: {_scrub(problem, secret_values)}", file=sys.stderr)
        return 2
    except ServerConfigError as exc:
        print(f"Error: {_scrub(str(exc), secret_values)}", file=sys.stderr)
        return 2
    except Exception:
        print("Error: Unexpected failure during startup validation", file=sys.stderr)
        return 2

    print("ok")
    if report.skipped:
        print(f"note: skipped offline checks requiring {', '.join(report.skipped)}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Standalone CLI entry point for ``python -m seeknal.serving.startup_validation``."""
    parser = argparse.ArgumentParser(
        prog="python -m seeknal.serving validate",
        description="Validate Seeknal Serving configuration, placement, schemas, and PII rules offline",
    )
    parser.add_argument(
        "--config",
        "-c",
        required=True,
        help="Path to server YAML configuration file",
    )
    if argv is None:
        argv = sys.argv[1:]
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 2
    return run_validate_cli(args.config)


if __name__ == "__main__":
    sys.exit(main())
