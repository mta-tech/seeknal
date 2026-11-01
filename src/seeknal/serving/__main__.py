"""CLI entry point for Seeknal Serving server."""

from __future__ import annotations

import argparse
import ssl
import sys
from collections.abc import Callable, Sequence
from typing import Any

import uvicorn

from seeknal.serving.server.app import build_server
from seeknal.serving.server.config import ServerConfig


def main(
    argv: Sequence[str] | None = None, *, run: Callable[..., Any] = uvicorn.run
) -> int:
    """CLI entry point for seeknal.serving."""
    parser = argparse.ArgumentParser(
        prog="python -m seeknal.serving",
        description="Seeknal Serving deployable server",
    )
    parser.add_argument(
        "--config",
        "-c",
        required=True,
        help="Path to server YAML configuration file",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate configuration, grant keys, and environment variables without starting the server",
    )

    if argv is None:
        argv = sys.argv[1:]

    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 2

    if args.check:
        try:
            cfg = ServerConfig.from_yaml(args.config, load_keys=True)
            cfg.check_env()
            print("config ok")
            return 0
        except Exception as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 2

    try:
        cfg = ServerConfig.from_yaml(args.config, load_keys=True)
        handle = build_server(cfg)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    run_kwargs: dict[str, Any] = {
        "host": cfg.listen.host,
        "port": cfg.listen.port,
    }

    if cfg.tls is not None:
        run_kwargs["ssl_certfile"] = cfg.tls.cert_file
        run_kwargs["ssl_keyfile"] = cfg.tls.key_file
        if cfg.tls.client_ca_file is not None:
            run_kwargs["ssl_ca_certs"] = cfg.tls.client_ca_file
            if cfg.tls.require_client_cert:
                run_kwargs["ssl_cert_reqs"] = ssl.CERT_REQUIRED

    run(handle.app, **run_kwargs)
    return 0


if __name__ == "__main__":
    sys.exit(main())
