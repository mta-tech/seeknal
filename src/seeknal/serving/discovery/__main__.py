"""CLI entry point for Seeknal Serving catalog discovery."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import httpx
import yaml
from yaml.constructor import ConstructorError

from seeknal.serving.discovery.build import discover
from seeknal.serving.discovery.config import (
    DiscoveryConfig,
    DiscoveryConfigError,
    DiscoveryError,
    _safe_disc_label,
)
from seeknal.serving.placement import UniqueKeySafeLoader

_URI_PASSWORD_RE = re.compile(r"(://[^:@/\s]+:)([^@/\s]+)(@)")


def _sanitize_tokens(text: str, env: Mapping[str, str]) -> str:
    """Sanitize any environment secret/token/DSN values from text output."""
    sanitized = _URI_PASSWORD_RE.sub(r"\1***\3", text)
    for key, val in env.items():
        k_low = key.lower()
        if (
            any(
                marker in k_low
                for marker in ("token", "secret", "dsn", "password", "passwd", "key")
            )
            and val
            and val in sanitized
        ):
            sanitized = sanitized.replace(val, "***")
    return sanitized


def _load_yaml_config_and_connections(
    path: str | os.PathLike,
) -> tuple[DiscoveryConfig, Mapping[str, Any] | None, str | None]:
    """Load either a standalone discovery YAML or a full serving.yaml."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw_data = yaml.load(f, Loader=UniqueKeySafeLoader)
    except FileNotFoundError as exc:
        raise DiscoveryConfigError(f"Config file not found: {path}") from exc
    except ConstructorError as exc:
        mark = getattr(exc, "problem_mark", None)
        loc = (
            f" at line {mark.line + 1}, column {mark.column + 1}"
            if mark is not None
            else ""
        )
        if hasattr(exc, "duplicate_key") or "duplicate YAML key" in str(
            getattr(exc, "problem", "")
        ):
            dup_key = getattr(exc, "duplicate_key", None)
            safe_dup = (
                _safe_disc_label(str(dup_key)) if dup_key is not None else "<redacted>"
            )
            raise DiscoveryConfigError(
                f"Invalid YAML in config file '{path}'{loc}: duplicate YAML key '{safe_dup}'"
            ) from exc
        raise DiscoveryConfigError(
            f"Invalid YAML in config file '{path}'{loc}"
        ) from exc
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        loc = (
            f" at line {mark.line + 1}, column {mark.column + 1}"
            if mark is not None
            else ""
        )
        raise DiscoveryConfigError(
            f"Invalid YAML in config file '{path}'{loc}"
        ) from exc
    except Exception as exc:
        raise DiscoveryConfigError(f"Failed to read config file: {path}") from exc

    if not isinstance(raw_data, Mapping):
        raise DiscoveryConfigError(
            f"Config file must contain a mapping, got {type(raw_data).__name__}"
        )

    if "placement" in raw_data and isinstance(raw_data["placement"], Mapping):
        pl_map = raw_data["placement"]
        if "discovery" not in pl_map or not isinstance(pl_map["discovery"], Mapping):
            raise DiscoveryConfigError(
                f"Config file '{path}' contains 'placement' without a 'discovery' mapping"
            )
        disc_map = dict(pl_map["discovery"])
        if "default_catalog" not in disc_map and "default_catalog" in raw_data:
            disc_map["default_catalog"] = raw_data["default_catalog"]
        connections_map = (
            raw_data["connections"]
            if isinstance(raw_data.get("connections"), Mapping)
            else None
        )
        secrets_env_file = (
            str(raw_data["secrets_env_file"]).strip()
            if isinstance(raw_data.get("secrets_env_file"), str)
            and raw_data["secrets_env_file"].strip()
            else None
        )
        return DiscoveryConfig.from_dict(disc_map), connections_map, secrets_env_file

    return DiscoveryConfig.from_dict(raw_data), None, None


def _read_dsn_from_spec(
    spec: Mapping[str, Any],
    env: Mapping[str, str],
    *,
    label: str,
) -> str | None:
    dsn_env = spec.get("dsn_env")
    dsn_file = spec.get("dsn_file")
    if isinstance(dsn_env, str) and dsn_env.strip():
        val = env.get(dsn_env.strip())
        if not val:
            raise DiscoveryConfigError(
                f"Missing required environment variable '{dsn_env.strip()}' for {label}"
            )
        return val
    if isinstance(dsn_file, str) and dsn_file.strip():
        fpath = dsn_file.strip()
        if not os.path.exists(fpath):
            raise DiscoveryConfigError(f"Secret file not found for {label}: '{fpath}'")
        st = os.stat(fpath)
        if (st.st_mode & 0o044) != 0:
            raise DiscoveryConfigError(
                f"Secret file '{fpath}' for {label} is group- or world-readable"
            )
        with open(fpath, "r", encoding="utf-8") as fh:
            return fh.read().strip()
    return None


def _build_default_postgres_connects(
    cfg: DiscoveryConfig,
    connections_map: Mapping[str, Any] | None,
    env: Mapping[str, str],
) -> dict[str, Callable[[], Any]]:
    """Build psycopg2 connect factories from serving.yaml connections when not injected."""
    if not connections_map:
        return {}
    needed_engines: dict[str, str] = {}
    for eng_name, src_cfg in cfg.postgres_sources.items():
        needed_engines[eng_name] = src_cfg.account
    if cfg.lakebase is not None and cfg.lakebase.engine not in needed_engines:
        needed_engines[cfg.lakebase.engine] = "interactive"

    built: dict[str, Callable[[], Any]] = {}
    for eng_name, account in needed_engines.items():
        conn_spec = connections_map.get(eng_name)
        if not isinstance(conn_spec, Mapping):
            continue
        dsn: str | None = None
        accounts_map = conn_spec.get("accounts")
        if isinstance(accounts_map, Mapping) and isinstance(
            accounts_map.get(account), Mapping
        ):
            dsn = _read_dsn_from_spec(
                accounts_map[account],
                env,
                label=f"connections.{_safe_disc_label(eng_name)}.accounts.{account}",
            )
        if dsn is None:
            dsn = _read_dsn_from_spec(
                conn_spec,
                env,
                label=f"connections.{_safe_disc_label(eng_name)}",
            )
        if dsn is None:
            continue

        timeout_s = int(float(conn_spec.get("connect_timeout_seconds", 5)))
        tls_spec = (
            conn_spec.get("tls") if isinstance(conn_spec.get("tls"), Mapping) else None
        )
        session_spec = (
            conn_spec.get("session")
            if isinstance(conn_spec.get("session"), Mapping)
            else None
        )

        def _make_factory(
            dsn_val: str,
            conn_timeout: int,
            tls_cfg: Mapping[str, Any] | None,
            sess_cfg: Mapping[str, Any] | None,
            ro_flag: bool,
        ) -> Callable[[], Any]:
            def _connect() -> Any:
                import psycopg2  # type: ignore[import-untyped]

                kwargs: dict[str, Any] = {"connect_timeout": conn_timeout}
                if tls_cfg is not None:
                    if isinstance(tls_cfg.get("sslmode"), str):
                        kwargs["sslmode"] = tls_cfg["sslmode"]
                    if isinstance(tls_cfg.get("sslrootcert_file"), str):
                        kwargs["sslrootcert"] = tls_cfg["sslrootcert_file"]
                opts: list[str] = []
                if ro_flag:
                    opts.append("-c default_transaction_read_only=on")
                if sess_cfg is not None and sess_cfg.get("startup_options", True):
                    if isinstance(sess_cfg.get("search_path"), str):
                        opts.append(f"-c search_path={sess_cfg['search_path']}")
                    if isinstance(sess_cfg.get("application_name"), str):
                        kwargs["application_name"] = sess_cfg["application_name"]
                if opts:
                    kwargs["options"] = " ".join(opts)
                return psycopg2.connect(dsn_val, **kwargs)

            return _connect

        kind = str(conn_spec.get("kind", "postgres"))
        mode = str(conn_spec.get("mode", "read_only"))
        lb_sub = (
            conn_spec.get("lakebase")
            if isinstance(conn_spec.get("lakebase"), Mapping)
            else None
        )
        ep_type = (
            str(lb_sub.get("endpoint_type", "read_write"))
            if lb_sub is not None
            else "read_write"
        )
        ro = (mode != "read_write") if kind == "postgres" else (ep_type == "read_only")
        built[eng_name] = _make_factory(dsn, timeout_s, tls_spec, session_spec, ro)

    return built


def _load_secrets_env_file(path: str) -> dict[str, str]:
    loaded: dict[str, str] = {}
    if not os.path.exists(path):
        return loaded
    st = os.stat(path)
    if (st.st_mode & 0o044) != 0:
        raise DiscoveryConfigError(
            f"secrets_env_file '{path}' is group- or world-readable"
        )
    with open(path, "r", encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k:
                loaded[k] = v
    return loaded


def main(
    argv: Sequence[str] | None = None,
    *,
    http_client: httpx.Client | None = None,
    lakebase_connect: Callable[[], Any] | None = None,
    postgres_connects: Mapping[str, Any] | None = None,
    storage_classes: Mapping[str, str] | None = None,
    env: Mapping[str, str] | None = None,
) -> int:
    """Run one discovery iteration and write placement YAML (and optional schemas JSON)."""
    resolved_env: dict[str, str] = dict(os.environ if env is None else env)
    raw_args = sys.argv[1:] if argv is None else list(argv)

    parser = argparse.ArgumentParser(
        prog="seeknal.serving.discovery",
        description="Seeknal Serving catalog discovery",
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to standalone discovery YAML or full serving.yaml",
    )
    parser.add_argument(
        "--emit",
        default=None,
        help="Output path for static placement YAML (makes --placement-out and --schemas-out optional)",
    )
    parser.add_argument(
        "--placement-out",
        default=None,
        help="Output path for placement YAML",
    )
    parser.add_argument(
        "--schemas-out",
        default=None,
        help="Output path for schemas JSON",
    )

    try:
        args = parser.parse_args(raw_args)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 2
    except Exception as exc:
        msg = _sanitize_tokens(str(exc), resolved_env)
        sys.stderr.write(f"Error: {msg}\n")
        return 2

    if args.emit is not None and args.placement_out is not None:
        sys.stderr.write("Error: Cannot specify both --emit and --placement-out\n")
        return 2
    if args.emit is None and (args.placement_out is None or args.schemas_out is None):
        sys.stderr.write(
            "Error: Either --emit <placement.yaml> or both --placement-out and --schemas-out are required\n"
        )
        return 2

    placement_target = args.emit if args.emit is not None else args.placement_out

    try:
        cfg, connections_map, secrets_env_file = _load_yaml_config_and_connections(
            args.config
        )
        if secrets_env_file:
            file_secrets = _load_secrets_env_file(secrets_env_file)
            for sk, sv in file_secrets.items():
                resolved_env.setdefault(sk, sv)

        effective_pg_connects = postgres_connects
        if (
            effective_pg_connects is None
            and lakebase_connect is None
            and connections_map is not None
        ):
            auto_connects = _build_default_postgres_connects(
                cfg, connections_map, resolved_env
            )
            if auto_connects:
                effective_pg_connects = auto_connects

        result = discover(
            cfg,
            http_client=http_client,
            lakebase_connect=lakebase_connect,
            postgres_connects=effective_pg_connects,
            storage_classes=storage_classes,
            connections=connections_map,
            env=resolved_env,
        )

        for w in result.warnings:
            sys.stderr.write(f"Warning: {_sanitize_tokens(w, resolved_env)}\n")

        assert placement_target is not None
        with open(placement_target, "w", encoding="utf-8") as f:
            yaml.safe_dump(result.placement, f, sort_keys=False)

        if args.schemas_out is not None:
            with open(args.schemas_out, "w", encoding="utf-8") as f:
                json.dump(result.schemas, f, indent=2)

        return 0

    except (DiscoveryError, DiscoveryConfigError) as exc:
        msg = _sanitize_tokens(str(exc), resolved_env)
        sys.stderr.write(f"Error: {msg}\n")
        return 2
    except Exception as exc:
        msg = _sanitize_tokens(str(exc), resolved_env)
        sys.stderr.write(f"Error: {msg}\n")
        return 2


if __name__ == "__main__":
    sys.exit(main())
