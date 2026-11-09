"""Command-line interface for seeknal.runtime."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from seeknal.serving.server.config import parse_jwks

from .project_pin import PinError, read_runtime_pin, resolve_pin
from .release import ManifestError, image_reference, load_manifest, manifest_sha256
from .run_grant import RunGrantError, require_plan_covered, verify_run_grant
from .run_plan import (
    RunPlan,
    RunPlanError,
    build_run_plan,
    canonical_plan_json,
    plan_sha256,
)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="seeknal.runtime",
        description="Atlas Runtime release manifest validator and pin resolver.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # validate PATH...
    val_parser = subparsers.add_parser(
        "validate", help="Validate one or more runtime manifests."
    )
    val_parser.add_argument("paths", nargs="+", help="Paths to manifest YAML files.")

    # resolve --project DIR [--env ENV] --releases DIR [--registry REG]
    res_parser = subparsers.add_parser(
        "resolve", help="Resolve a project runtime pin against release manifests."
    )
    res_parser.add_argument(
        "--project", required=True, help="Seeknal project directory."
    )
    res_parser.add_argument(
        "--env", default=None, help="Environment name (e.g. dev, prod)."
    )
    res_parser.add_argument(
        "--releases", required=True, help="Directory containing release manifests."
    )
    res_parser.add_argument(
        "--registry",
        default="ghcr.io/mta-tech",
        help="Container registry prefix (default: ghcr.io/mta-tech).",
    )
    res_parser.add_argument(
        "--channel",
        default="stable",
        help="Release channel to resolve against (default: stable).",
    )

    # plan --project DIR [--env ENV] [--profile PATH] [--out FILE]
    plan_parser = subparsers.add_parser(
        "plan", help="Build and print canonical run plan manifest."
    )
    plan_parser.add_argument(
        "--project", required=True, help="Seeknal project directory."
    )
    plan_parser.add_argument(
        "--env", default=None, help="Environment name (e.g. dev, prod)."
    )
    plan_parser.add_argument(
        "--profile", default=None, help="Path to profiles.yml."
    )
    plan_parser.add_argument(
        "--out", default=None, help="Output file path."
    )

    # verify-grant --grant FILE --jwks FILE [--plan FILE]
    vg_parser = subparsers.add_parser(
        "verify-grant", help="Verify run grant against JWKS and optional plan coverage."
    )
    vg_parser.add_argument(
        "--grant", required=True, help="Path to run grant token file."
    )
    vg_parser.add_argument(
        "--jwks", required=True, help="Path to JWKS file."
    )
    vg_parser.add_argument(
        "--plan", default=None, help="Path to canonical plan JSON file."
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint returning exit code."""
    if argv is None:
        argv = sys.argv[1:]

    parser = create_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 2

    if args.command == "validate":
        all_valid = True
        for path_str in args.paths:
            p = Path(path_str)
            if not p.is_file():
                print(f"ERR {path_str}: file not found")
                all_valid = False
                continue

            try:
                rel = load_manifest(p)
                sha = manifest_sha256(p)
                print(f"OK {rel.name} {sha}")
            except ManifestError as exc:
                all_valid = False
                print(f"ERR {path_str}:")
                for err in exc.errors:
                    print(f"  {err}")
            except Exception as exc:
                all_valid = False
                print(f"ERR {path_str}: {exc}")

        return 0 if all_valid else 1

    if args.command == "resolve":
        project_dir = Path(args.project)
        releases_dir = Path(args.releases)

        try:
            pin = read_runtime_pin(project_dir, environment=args.env)
        except PinError as exc:
            print(f"ERR: {exc}", file=sys.stderr)
            return 1

        if pin is None:
            print(
                f"Project at '{project_dir}' has no runtime pin configured",
                file=sys.stderr,
            )
            return 2

        if not releases_dir.is_dir():
            print(
                f"ERR: Releases directory '{releases_dir}' not found", file=sys.stderr
            )
            return 1

        releases = []
        for yaml_path in sorted(releases_dir.glob("*.yaml")):
            try:
                rel = load_manifest(yaml_path)
                releases.append(rel)
            except Exception as exc:
                print(
                    f"Skipping invalid manifest '{yaml_path}': {exc}", file=sys.stderr
                )

        try:
            resolved = resolve_pin(pin, releases, channel=args.channel)
        except PinError as exc:
            print(f"ERR: {exc}", file=sys.stderr)
            return 1

        img_ref = image_reference(resolved, "atlas-rt", registry=args.registry)
        print(f"{resolved.name} {img_ref}")
        return 0

    if args.command == "plan":
        try:
            plan = build_run_plan(
                args.project,
                env=args.env,
                profile_path=args.profile,
            )
            raw_bytes = canonical_plan_json(plan)
            digest = plan_sha256(plan)
            if args.out:
                Path(args.out).write_bytes(raw_bytes)
            print(raw_bytes.decode("utf-8"))
            print(f"plan_sha256={digest}", file=sys.stderr)
            return 0
        except RunPlanError as exc:
            print(f"ERR: {exc}", file=sys.stderr)
            return 2
        except Exception as exc:
            print(f"ERR: {exc}", file=sys.stderr)
            return 2

    if args.command == "verify-grant":
        try:
            grant_path = Path(args.grant)
            if not grant_path.is_file():
                print(f"ERR: Grant file not found: {args.grant}", file=sys.stderr)
                return 1

            jwks_path = Path(args.jwks)
            if not jwks_path.is_file():
                print(f"ERR: JWKS file not found: {args.jwks}", file=sys.stderr)
                return 1

            grant_token = grant_path.read_text("utf-8").strip()
            jwks_content = jwks_path.read_text("utf-8")
            public_keys = parse_jwks(jwks_content)

            grant = verify_run_grant(grant_token, public_keys)

            if args.plan:
                plan_path = Path(args.plan)
                if not plan_path.is_file():
                    print(f"ERR: Plan file not found: {args.plan}", file=sys.stderr)
                    return 1
                plan_dict = json.loads(plan_path.read_text("utf-8"))
                plan = RunPlan.from_dict(plan_dict)
                require_plan_covered(grant, plan)

            print("OK")
            return 0
        except Exception as exc:
            print(f"ERR: {exc}", file=sys.stderr)
            return 1

    return 2


if __name__ == "__main__":
    sys.exit(main())
