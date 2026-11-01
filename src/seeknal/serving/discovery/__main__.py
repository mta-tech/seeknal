"""CLI entry point for Seeknal Serving catalog discovery."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import httpx
import yaml

from seeknal.serving.discovery.build import discover
from seeknal.serving.discovery.config import (
    DiscoveryConfig,
    DiscoveryConfigError,
    DiscoveryError,
)


def _sanitize_tokens(text: str, env: Mapping[str, str]) -> str:
    """Sanitize any environment token values from text output."""
    sanitized = text
    for key, val in env.items():
        if "token" in key.lower() and val and val in sanitized:
            sanitized = sanitized.replace(val, "***")
    return sanitized


def main(
    argv: Sequence[str] | None = None,
    *,
    http_client: httpx.Client | None = None,
    lakebase_connect: Callable[[], Any] | None = None,
    env: Mapping[str, str] | None = None,
) -> int:
    """Run one discovery iteration and write placement YAML and schemas JSON."""
    resolved_env = os.environ if env is None else env
    raw_args = sys.argv[1:] if argv is None else list(argv)

    parser = argparse.ArgumentParser(
        prog="seeknal.serving.discovery",
        description="Seeknal Serving catalog discovery",
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to discovery YAML configuration file",
    )
    parser.add_argument(
        "--placement-out",
        required=True,
        help="Output path for placement YAML",
    )
    parser.add_argument(
        "--schemas-out",
        required=True,
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

    try:
        cfg = DiscoveryConfig.from_yaml(args.config)

        if http_client is None:
            with httpx.Client(timeout=cfg.timeout_seconds) as client:
                result = discover(
                    cfg,
                    http_client=client,
                    lakebase_connect=lakebase_connect,
                    env=resolved_env,
                )
        else:
            result = discover(
                cfg,
                http_client=http_client,
                lakebase_connect=lakebase_connect,
                env=resolved_env,
            )

        for w in result.warnings:
            sys.stderr.write(f"Warning: {w}\n")

        with open(args.placement_out, "w", encoding="utf-8") as f:
            yaml.safe_dump(result.placement, f, sort_keys=False)

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
