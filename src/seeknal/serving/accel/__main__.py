"""CLI entrypoint for Seeknal Serving DuckDB acceleration (python -m seeknal.serving.accel)."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Optional, Sequence

from seeknal.serving.accel.iceberg_source import (
    IcebergSource,
    build_iceberg_table_specs,
    create_iceberg_target_factory,
    load_iceberg_source_config,
    plan_iceberg_load_sql,
    plan_iceberg_source_setup,
)
from seeknal.serving.accel.loader import (
    BadInputError,
    LoadError,
    log_event,
    plan_sql,
    run_load,
    scrub_text,
)
from seeknal.serving.accel.placement import (
    generate_placement,
    generate_schemas,
    write_placement_files,
)
from seeknal.serving.accel.settings import (
    DEFAULT_KEEP,
    DEFAULT_PII_MODE,
    VALID_PII_MODES,
    VALID_SOURCE_TYPES,
    get_expected_duckdb_version,
    get_extension_dir,
    validate_extension_dir,
    validate_pii_mode,
)
from seeknal.serving.accel.tables import build_table_specs

__all__ = [
    "build_parser",
    "generate_placement",
    "generate_schemas",
    "handle_load",
    "handle_placement",
    "main",
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="seeknal.serving.accel",
        description="Seeknal Serving DuckDB acceleration cache loader and placement tools.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcommand: load
    load_p = subparsers.add_parser(
        "load",
        help="Materialize Postgres Lakebase or Lakekeeper Iceberg tables into a DuckDB cache file.",
    )
    load_p.add_argument(
        "--source-type",
        choices=VALID_SOURCE_TYPES,
        default="postgres",
        help="Source backend to read from ('postgres' default or 'iceberg')",
    )
    load_p.add_argument(
        "--source-config",
        type=Path,
        default=None,
        help="JSON or YAML config file for Iceberg catalog, S3, tables, and snapshot pins",
    )
    load_p.add_argument(
        "--pii-mode",
        choices=VALID_PII_MODES,
        default=None,
        help="PII column handling mode: 'null' (typed NULL, default) or 'exclude' (omit column)",
    )
    load_p.add_argument(
        "--columns",
        type=Path,
        default=None,
        help="Path to columns.json",
    )
    load_p.add_argument(
        "--pii",
        type=Path,
        default=None,
        help="Path to pii_columns.json",
    )
    load_p.add_argument(
        "--registry",
        type=Path,
        default=None,
        help="Path to registry.json",
    )
    load_p.add_argument(
        "--cache-dir",
        required=True,
        type=Path,
        help="Directory to store versioned DuckDB cache files and mart.duckdb symlink",
    )
    load_p.add_argument(
        "--keep",
        type=int,
        default=DEFAULT_KEEP,
        help="Number of cached DuckDB files to retain",
    )
    load_p.add_argument(
        "--as-of-date",
        type=str,
        default=None,
        help="As-of date in YYYY-MM-DD",
    )
    load_p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print redacted setup/load SQL plan and touch nothing",
    )
    load_p.add_argument(
        "--expect-duckdb-version",
        type=str,
        default=get_expected_duckdb_version(),
        help="Expected DuckDB version matching Seeknal Serving",
    )
    load_p.add_argument(
        "--allow-version-mismatch",
        action="store_true",
        help="Allow running when duckdb version differs from expected version",
    )
    load_p.add_argument(
        "--allow-non-hashed-pii",
        action="store_true",
        help="Allow caching columns with non-sha256 PII in 'null' mode (requires --i-understand-pii-leaves-unmasked)",
    )
    load_p.add_argument(
        "--i-understand-pii-leaves-unmasked",
        action="store_true",
        help="Explicitly acknowledge that --allow-non-hashed-pii caches non-hashed PII unmasked",
    )
    load_p.add_argument(
        "--extension-dir",
        type=str,
        default=get_extension_dir(),
        help="Directory with pre-staged DuckDB extensions for offline/air-gapped execution",
    )

    # Subcommand: placement
    place_p = subparsers.add_parser(
        "placement",
        help="Generate Serving placement and schemas files.",
    )
    place_p.add_argument(
        "--columns",
        required=True,
        type=Path,
        help="Path to columns.json",
    )
    place_p.add_argument(
        "--registry",
        type=Path,
        default=None,
        help="Path to registry.json",
    )
    place_p.add_argument(
        "--freshness",
        type=Path,
        default=None,
        help="Path to freshness.json",
    )
    place_p.add_argument(
        "--pii",
        type=Path,
        default=None,
        help="Optional path to pii_columns.json (used when --pii-mode=exclude)",
    )
    place_p.add_argument(
        "--pii-mode",
        choices=VALID_PII_MODES,
        default=DEFAULT_PII_MODE,
        help="PII column mode for schema generation ('null' default or 'exclude')",
    )
    place_p.add_argument(
        "--no-duck",
        action="store_true",
        help="Generate Lakebase-only placement",
    )
    place_p.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Directory to write placement.json and schemas.json",
    )
    place_p.add_argument(
        "--placement-out",
        type=Path,
        default=None,
        help="File to write placement.json",
    )
    place_p.add_argument(
        "--schemas-out",
        type=Path,
        default=None,
        help="File to write schemas.json",
    )

    return parser


def _check_duckdb_version_cli(args: argparse.Namespace) -> bool:
    """Run DuckDB version pin check before modifying disk; return True if OK."""
    try:
        from seeknal.serving.accel.engines import check_duckdb_version

        check_duckdb_version(
            allow_mismatch=args.allow_version_mismatch,
            expected_version=args.expect_duckdb_version,
        )
        return True
    except RuntimeError as exc:
        log_event(
            "version_mismatch_refusal",
            level="ERROR",
            error_type="BadInputError",
            message=scrub_text(str(exc)),
        )
        return False
    except Exception as exc:
        log_event(
            "version_check_failed",
            level="ERROR",
            error_type=type(exc).__name__,
            message=scrub_text(str(exc)),
        )
        return False


def _handle_postgres_load(args: argparse.Namespace) -> int:
    if args.source_config is not None:
        log_event(
            "bad_input",
            level="ERROR",
            error_type="BadInputError",
            message="--source-config is only supported when --source-type is 'iceberg'",
        )
        return 2

    for p_name in ("columns", "pii", "registry"):
        p: Optional[Path] = getattr(args, p_name)
        if p is None:
            log_event("input_missing", level="ERROR", field=p_name, path="<missing>")
            return 2
        if not p.exists():
            log_event("input_missing", level="ERROR", field=p_name, path=str(p))
            return 2

    try:
        effective_pii_mode = validate_pii_mode(args.pii_mode)
        specs = build_table_specs(
            columns=args.columns,
            pii_policy=args.pii,
            registry=args.registry,
            allow_non_hashed_pii=args.allow_non_hashed_pii,
            pii_mode=effective_pii_mode,
            i_understand_pii_leaves_unmasked=args.i_understand_pii_leaves_unmasked,
        )
    except (ValueError, json.JSONDecodeError, BadInputError) as exc:
        log_event(
            "bad_input",
            level="ERROR",
            error_type=type(exc).__name__,
            message=scrub_text(str(exc)),
        )
        return 2
    except Exception as exc:
        log_event(
            "bad_input",
            level="ERROR",
            error_type=type(exc).__name__,
            message=scrub_text(str(exc)),
        )
        return 2

    if args.dry_run:
        plan = plan_sql(specs, pii_mode=effective_pii_mode, source_type="postgres")
        for stmt in plan:
            print(stmt)
        return 0

    if args.extension_dir:
        try:
            args.extension_dir = validate_extension_dir(args.extension_dir)
        except ValueError as exc:
            log_event(
                "bad_input",
                level="ERROR",
                error_type="BadInputError",
                message=scrub_text(str(exc)),
            )
            return 2

    if not _check_duckdb_version_cli(args):
        return 2

    source = None
    try:
        from seeknal.serving.accel.engines import (
            LakebaseSource,
            create_target_factory,
            get_installed_duckdb_version,
        )

        actual_version = get_installed_duckdb_version()
        source = LakebaseSource(
            allow_version_mismatch=args.allow_version_mismatch,
            expected_version=args.expect_duckdb_version,
            extension_dir=args.extension_dir,
        )
        target_factory = create_target_factory(
            allow_version_mismatch=args.allow_version_mismatch,
            expected_version=args.expect_duckdb_version,
            extension_dir=args.extension_dir,
        )

        run_load(
            specs=specs,
            source=source,
            target_factory=target_factory,
            cache_dir=args.cache_dir,
            clock=lambda: datetime.now(timezone.utc),
            keep=args.keep,
            as_of_date=args.as_of_date,
            duckdb_version=actual_version,
            pii_mode=effective_pii_mode,
            source_type="postgres",
        )
        return 0
    except (LoadError, RuntimeError, Exception) as exc:
        log_event(
            "load_execution_failed",
            level="ERROR",
            error_type=type(exc).__name__,
            message=scrub_text(str(exc)),
        )
        return 3
    finally:
        if source is not None:
            source.close()


def _handle_iceberg_load(args: argparse.Namespace) -> int:
    if args.source_config is None:
        log_event(
            "input_missing",
            level="ERROR",
            field="source_config",
            path="<missing>",
        )
        return 2
    if not args.source_config.exists():
        log_event(
            "input_missing",
            level="ERROR",
            field="source_config",
            path=str(args.source_config),
        )
        return 2

    for opt_name in ("columns", "pii", "registry"):
        opt_path: Optional[Path] = getattr(args, opt_name)
        if opt_path is not None and not opt_path.exists():
            log_event(
                "input_missing",
                level="ERROR",
                field=opt_name,
                path=str(opt_path),
            )
            return 2

    try:
        iceberg_cfg = load_iceberg_source_config(
            args.source_config,
            extension_dir=args.extension_dir,
        )
        effective_pii_mode = validate_pii_mode(
            args.pii_mode if args.pii_mode is not None else iceberg_cfg.pii_mode
        )
        specs = build_iceberg_table_specs(
            iceberg_cfg,
            columns=args.columns,
            pii_policy=args.pii,
            registry=args.registry,
            allow_non_hashed_pii=args.allow_non_hashed_pii,
            pii_mode=effective_pii_mode,
            i_understand_pii_leaves_unmasked=args.i_understand_pii_leaves_unmasked,
        )
        setup_steps = plan_iceberg_source_setup(
            iceberg_cfg,
            extension_dir=args.extension_dir,
        )
        load_stmts = plan_iceberg_load_sql(
            specs,
            pii_mode=effective_pii_mode,
            allowed_aliases=set(iceberg_cfg.iceberg_catalogs.keys()),
        )
    except (ValueError, json.JSONDecodeError, BadInputError) as exc:
        log_event(
            "bad_input",
            level="ERROR",
            error_type=type(exc).__name__,
            message=scrub_text(str(exc)),
        )
        return 2
    except Exception as exc:
        log_event(
            "bad_input",
            level="ERROR",
            error_type=type(exc).__name__,
            message=scrub_text(str(exc)),
        )
        return 2

    if args.dry_run:
        for step in setup_steps:
            print(step.safe_sql)
        for stmt in load_stmts:
            print(stmt)
        return 0

    eff_ext_dir = args.extension_dir or iceberg_cfg.extension_directory
    try:
        validated_ext_dir = validate_extension_dir(eff_ext_dir)
    except ValueError as exc:
        log_event(
            "bad_input",
            level="ERROR",
            error_type="BadInputError",
            message=scrub_text(str(exc)),
        )
        return 2

    if not _check_duckdb_version_cli(args):
        return 2

    source = None
    try:
        from seeknal.serving.accel.engines import get_installed_duckdb_version

        actual_version = get_installed_duckdb_version()
        source = IcebergSource(
            iceberg_cfg,
            allow_version_mismatch=args.allow_version_mismatch,
            expected_version=args.expect_duckdb_version,
            extension_dir=validated_ext_dir,
        )
        target_factory = create_iceberg_target_factory(
            iceberg_cfg,
            allow_version_mismatch=args.allow_version_mismatch,
            expected_version=args.expect_duckdb_version,
            extension_dir=validated_ext_dir,
        )

        run_load(
            specs=specs,
            source=source,
            target_factory=target_factory,
            cache_dir=args.cache_dir,
            clock=lambda: datetime.now(timezone.utc),
            keep=args.keep,
            as_of_date=args.as_of_date,
            duckdb_version=actual_version,
            pii_mode=effective_pii_mode,
            source_type="iceberg",
        )
        return 0
    except (LoadError, RuntimeError, Exception) as exc:
        log_event(
            "load_execution_failed",
            level="ERROR",
            error_type=type(exc).__name__,
            message=scrub_text(str(exc)),
        )
        return 3
    finally:
        if source is not None:
            source.close()


def handle_load(args: argparse.Namespace) -> int:
    if args.allow_non_hashed_pii and not getattr(
        args, "i_understand_pii_leaves_unmasked", False
    ):
        log_event(
            "bad_input",
            level="ERROR",
            error_type="BadInputError",
            message="--allow-non-hashed-pii requires --i-understand-pii-leaves-unmasked",
        )
        return 2

    if args.as_of_date is not None:
        try:
            datetime.strptime(args.as_of_date, "%Y-%m-%d")
        except ValueError:
            log_event("invalid_as_of_date", level="ERROR", date=args.as_of_date)
            return 2

    if args.source_type == "iceberg":
        return _handle_iceberg_load(args)
    return _handle_postgres_load(args)


def handle_placement(args: argparse.Namespace) -> int:
    if not args.columns.exists():
        log_event(
            "input_missing",
            level="ERROR",
            field="columns",
            path=str(args.columns),
        )
        return 2
    if args.pii is not None and not args.pii.exists():
        log_event(
            "input_missing",
            level="ERROR",
            field="pii",
            path=str(args.pii),
        )
        return 2

    try:
        write_placement_files(
            columns_path=args.columns,
            registry_path=args.registry,
            freshness_path=args.freshness,
            duck=not args.no_duck,
            out_dir=args.out_dir,
            placement_out=args.placement_out,
            schemas_out=args.schemas_out,
            pii_policy_path=args.pii,
            pii_mode=args.pii_mode,
        )
        return 0
    except Exception as exc:
        log_event(
            "placement_failed",
            level="ERROR",
            error_type=type(exc).__name__,
            message=scrub_text(str(exc)),
        )
        return 2


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return 2 if exc.code != 0 else 0

    if args.command == "load":
        return handle_load(args)
    if args.command == "placement":
        return handle_placement(args)
    return 2


if __name__ == "__main__":
    sys.exit(main())
