"""CLI entry point for Seeknal Serving catalog discovery."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

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

if TYPE_CHECKING:
    from seeknal.serving.server.config import ServerConfig

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


def _load_yaml_config_and_server_config(
    path: str | os.PathLike,
    *,
    env: Mapping[str, str],
) -> tuple[DiscoveryConfig, ServerConfig | None]:
    """Load either a standalone discovery YAML or a full validated ServerConfig YAML."""
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

    if "placement" in raw_data or "connections" in raw_data:
        pl_raw = raw_data.get("placement")
        if not isinstance(pl_raw, Mapping):
            raise DiscoveryConfigError(
                f"Config file '{path}' contains 'connections' without a valid 'placement' mapping"
            )
        if "discovery" not in pl_raw or not isinstance(pl_raw["discovery"], Mapping):
            raise DiscoveryConfigError(
                f"Config file '{path}' contains 'placement' without a 'discovery' mapping"
            )
        disc_map = dict(pl_raw["discovery"])
        if "default_catalog" not in disc_map and "default_catalog" in raw_data:
            disc_map["default_catalog"] = raw_data["default_catalog"]

        srv_dict: dict[str, Any] = dict(raw_data)
        srv_dict["placement"] = {**dict(pl_raw), "discovery": disc_map}
        if "grant_keys" not in srv_dict:
            srv_dict["grant_keys"] = {
                "jwks_file": "__discovery_cli_unused__.jwks.json"
            }

        from seeknal.serving.server.config import ServerConfig, ServerConfigError

        try:
            server_cfg = ServerConfig.from_dict(
                srv_dict,
                load_keys=False,
                env=env,
            )
        except ServerConfigError as exc:
            raise DiscoveryConfigError(str(exc)) from exc

        assert server_cfg.placement.discovery_config is not None
        return server_cfg.placement.discovery_config, server_cfg

    return DiscoveryConfig.from_dict(raw_data), None


def _build_validated_discovery_connects(
    cfg: DiscoveryConfig,
    server_cfg: ServerConfig,
    env: Mapping[str, str],
) -> tuple[Callable[[], Any] | None, dict[str, Callable[[], Any]], list[httpx.Client]]:
    """Build hardened Postgres/Lakebase discovery connect factories via ServerConfig & app builders."""
    from seeknal.serving.server.app import (
        _build_discovery_postgres_connects,
        _make_discovery_source_connect,
    )
    from seeknal.serving.server.config import (
        LakebaseConnectionConfig,
        PostgresConnectionConfig,
    )

    http_clients: list[httpx.Client] = []
    connectors_by_endpoint: dict[str, Any] = {}

    def _get_http_client() -> httpx.Client:
        if not http_clients:
            http_clients.append(httpx.Client())
        return http_clients[0]

    lb_connect: Callable[[], Any] | None = None
    if cfg.lakebase is not None:
        lb_eng = cfg.lakebase.engine
        conn_obj = server_cfg.connections.get(lb_eng)
        if isinstance(conn_obj, (PostgresConnectionConfig, LakebaseConnectionConfig)):
            lb_connect = _make_discovery_source_connect(
                eng_name=lb_eng,
                conn_cfg=conn_obj,
                account_name="interactive",
                cfg=server_cfg,
                env=env,
                connect_factories=None,
                metrics=None,
                get_http_client=_get_http_client,
                connectors_by_endpoint=connectors_by_endpoint,
            )

    pg_connects = _build_discovery_postgres_connects(
        cfg=server_cfg,
        disc_cfg=cfg,
        env=env,
        connect_factories=None,
        metrics=None,
        get_http_client=_get_http_client,
        connectors_by_endpoint=connectors_by_endpoint,
    )
    return lb_connect, pg_connects, http_clients


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

    owned_cs_clients: list[httpx.Client] = []
    try:
        cfg, server_cfg = _load_yaml_config_and_server_config(
            args.config,
            env=resolved_env,
        )
        if server_cfg is not None and server_cfg.secrets_env:
            for sk, sv in server_cfg.secrets_env.items():
                resolved_env.setdefault(sk, sv)

        effective_lb_connect = lakebase_connect
        effective_pg_connects = postgres_connects
        if (
            effective_pg_connects is None
            and effective_lb_connect is None
            and server_cfg is not None
        ):
            auto_lb, auto_pg, owned_cs_clients = _build_validated_discovery_connects(
                cfg, server_cfg, resolved_env
            )
            if auto_lb is not None:
                effective_lb_connect = auto_lb
            if auto_pg:
                effective_pg_connects = auto_pg

        result = discover(
            cfg,
            http_client=http_client,
            lakebase_connect=effective_lb_connect,
            postgres_connects=effective_pg_connects,
            storage_classes=storage_classes,
            connections=server_cfg.connections if server_cfg is not None else None,
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
    finally:
        for hc in owned_cs_clients:
            try:
                hc.close()
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(main())
