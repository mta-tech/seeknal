"""CLI entry point for Seeknal Serving server."""

from __future__ import annotations

import argparse
import ssl
import sys
import threading
from collections.abc import Callable, Sequence
from typing import Any

import uvicorn

from seeknal.serving.buildinfo import format_build_info, get_build_info
from seeknal.serving.server.app import build_server
from seeknal.serving.server.config import ServerConfig, ServerConfigError
from seeknal.serving.startup_validation import (
    StartupValidationError,
    run_validate_cli,
)


def main(
    argv: Sequence[str] | None = None,
    *,
    run: Callable[..., Any] = uvicorn.run,
    ops_run: Callable[..., Any] | None = None,
) -> int:
    """CLI entry point for seeknal.serving."""
    parser = argparse.ArgumentParser(
        prog="python -m seeknal.serving",
        description="Seeknal Serving deployable server",
    )
    parser.add_argument(
        "--config",
        "-c",
        default=None,
        help="Path to server YAML configuration file",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate configuration, grant keys, environment variables, and runtime wiring without starting the server",
    )
    parser.add_argument(
        "--version",
        action="store_true",
        help="Print build and runtime version metadata and exit",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Format --version output as single-line JSON",
    )

    if argv is None:
        argv_list = list(sys.argv[1:])
    else:
        argv_list = list(argv)

    if argv_list and argv_list[0] == "validate":
        val_parser = argparse.ArgumentParser(
            prog="python -m seeknal.serving validate",
            description="Validate Seeknal Serving configuration, placement, schemas, columns, and PII rules offline",
        )
        val_parser.add_argument(
            "--config",
            "-c",
            required=True,
            help="Path to server YAML configuration file",
        )
        try:
            val_args = val_parser.parse_args(argv_list[1:])
        except SystemExit as exc:
            return exc.code if isinstance(exc.code, int) else 2
        return run_validate_cli(val_args.config)

    parser.add_argument(
        "command",
        nargs="?",
        choices=["validate"],
        default=None,
        help="Optional subcommand ('validate' runs offline startup validation)",
    )

    try:
        args = parser.parse_args(argv_list)
        if not args.version and not args.config:
            parser.error("the following arguments are required: --config/-c")
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 2

    if args.version:
        print(format_build_info(get_build_info(), as_json=args.json))
        return 0

    if args.command == "validate":
        return run_validate_cli(args.config)

    if args.check:
        try:
            cfg = ServerConfig.from_yaml(args.config, load_keys=True)
            cfg.validate_runtime_wiring()
            print("config ok")
            return 0
        except StartupValidationError as exc:
            for prob in exc.problems:
                print(f"Error: {prob}", file=sys.stderr)
            return 2
        except ServerConfigError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 2
        except Exception:
            print(
                "Error: Unexpected failure during configuration check",
                file=sys.stderr,
            )
            return 2

    try:
        cfg = ServerConfig.from_yaml(args.config, load_keys=True)
        handle = build_server(cfg)
    except StartupValidationError as exc:
        for prob in exc.problems:
            print(f"Error: {prob}", file=sys.stderr)
        return 2
    except ServerConfigError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    except Exception:
        print("Error: Unexpected failure during server startup", file=sys.stderr)
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

    ops_server: uvicorn.Server | None = None
    ops_thread: threading.Thread | None = None
    if cfg.ops is not None and cfg.ops.enabled and handle.ops_app is not None:
        if ops_run is not None:
            ops_thread = threading.Thread(
                target=ops_run,
                args=(handle.ops_app,),
                kwargs={"host": cfg.ops.host, "port": cfg.ops.port},
                name="seeknal-ops-listener",
                daemon=True,
            )
            ops_thread.start()
        else:
            ops_uv_cfg = uvicorn.Config(
                handle.ops_app,
                host=cfg.ops.host,
                port=cfg.ops.port,
                log_level="warning",
            )
            ops_server = uvicorn.Server(ops_uv_cfg)
            ops_server.install_signal_handlers = lambda: None  # type: ignore[method-assign]
            ops_thread = threading.Thread(
                target=ops_server.run,
                name="seeknal-ops-listener",
                daemon=True,
            )
            ops_thread.start()

    try:
        run(handle.app, **run_kwargs)
    finally:
        if ops_server is not None:
            ops_server.should_exit = True
        if ops_thread is not None and ops_thread.is_alive():
            ops_thread.join(timeout=5.0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
