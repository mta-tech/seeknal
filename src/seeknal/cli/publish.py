from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import typer

publish_app = typer.Typer(
    name="publish",
    help="Publish Evidence.dev reports to a Seeknal Report Server.",
    no_args_is_help=True,
)


def _resolve_profile(publish_profile: str, server: Optional[str], api_key: Optional[str]) -> tuple[str, Optional[str]]:
    resolved_server = server
    resolved_key = api_key

    if not resolved_server:
        try:
            from seeknal.workflow.materialization.profile_loader import ProfileLoader

            # Prefer the project's local profiles.yml over ~/.seeknal/profiles.yml
            # so publish config lives next to the project it publishes.
            local_profiles = Path("profiles.yml")
            loader = (
                ProfileLoader(profile_path=local_profiles)
                if local_profiles.exists()
                else ProfileLoader()
            )
            profile_data = loader.load_publish_profile(publish_profile)
            resolved_server = profile_data.get("server", "")
            if not resolved_key:
                resolved_key = profile_data.get("api_key")
        except Exception as exc:
            if not server:
                typer.echo(typer.style(f"Could not load publish profile '{publish_profile}': {exc}", fg=typer.colors.RED))
                raise typer.Exit(1)

    if not resolved_server:
        typer.echo(typer.style("No server configured. Use --server or set up a publish profile.", fg=typer.colors.RED))
        raise typer.Exit(1)

    return resolved_server, resolved_key


@publish_app.command("report")
def publish_report(
    report_name: str = typer.Argument(..., help="Name of the report to publish"),
    publish_profile: str = typer.Option("default", "--publish-profile", help="Publish profile name from profiles.yml"),
    server: Optional[str] = typer.Option(None, "--server", help="Report server URL (overrides profile)"),
    api_key: Optional[str] = typer.Option(None, "--api-key", help="API key (overrides profile)"),
) -> None:
    """Publish a built Evidence.dev report to the report server."""
    build_dir = Path("target") / "reports" / report_name / "build"
    if not build_dir.exists():
        typer.echo(typer.style(
            f"Build directory not found: {build_dir}\n"
            "Run your Evidence.dev build first, then re-run this command.",
            fg=typer.colors.RED,
        ))
        raise typer.Exit(1)

    resolved_server, resolved_key = _resolve_profile(publish_profile, server, api_key)

    from seeknal.publish.packager import package_build
    from seeknal.publish.client import PublishClient
    from seeknal.publish.ledger import append_entry, find_by_report_name, LedgerEntry

    typer.echo(f"Packaging {build_dir} ...")
    try:
        tarball = package_build(build_dir)
    except Exception as exc:
        typer.echo(typer.style(f"Packaging failed: {exc}", fg=typer.colors.RED))
        raise typer.Exit(1)

    try:
        typer.echo(f"Publishing to {resolved_server} ...")
        client = PublishClient(resolved_server, resolved_key)
        response = client.publish(tarball, report_name)
    except Exception as exc:
        tarball.unlink(missing_ok=True)
        typer.echo(typer.style(f"Publish failed: {exc}", fg=typer.colors.RED))
        raise typer.Exit(1)
    finally:
        tarball.unlink(missing_ok=True)

    existing = find_by_report_name(report_name)
    if existing:
        typer.echo(typer.style(
            f"Note: report '{report_name}' was previously published ({len(existing)} time(s)). "
            "New slug created.",
            fg=typer.colors.YELLOW,
        ))

    from datetime import datetime, timezone
    entry = LedgerEntry(
        slug=response.slug,
        server=resolved_server,
        share_url=response.share_url,
        report_name=report_name,
        published_at=response.created_at,
    )
    try:
        append_entry(entry)
    except Exception as exc:
        typer.echo(typer.style(f"Warning: could not save to ledger: {exc}", fg=typer.colors.YELLOW))

    typer.echo("")
    typer.echo(typer.style("Report published successfully!", fg=typer.colors.GREEN))
    typer.echo(f"  Share URL:  {typer.style(response.share_url, fg=typer.colors.CYAN)}")
    typer.echo("")
    typer.echo(typer.style(
        f"  SAVE THIS NOW — owner secret (cannot be retrieved later):",
        fg=typer.colors.YELLOW,
    ))
    typer.echo(typer.style(f"  {response.owner_secret}", fg=typer.colors.YELLOW))
    typer.echo("")


@publish_app.command("revoke")
def publish_revoke(
    slug: str = typer.Argument(..., help="Slug of the published report to revoke"),
    owner_secret: Optional[str] = typer.Option(None, "--owner-secret", help="Owner secret for the report"),
    owner_secret_stdin: bool = typer.Option(False, "--owner-secret-stdin", help="Read owner secret from stdin"),
    publish_profile: str = typer.Option("default", "--publish-profile", help="Publish profile name"),
    server: Optional[str] = typer.Option(None, "--server", help="Report server URL (overrides profile)"),
) -> None:
    """Revoke a published report, making it inaccessible."""
    if owner_secret_stdin:
        owner_secret = sys.stdin.readline().strip()

    if not owner_secret:
        typer.echo(typer.style("Owner secret is required. Use --owner-secret or --owner-secret-stdin.", fg=typer.colors.RED))
        raise typer.Exit(1)

    resolved_server, _ = _resolve_profile(publish_profile, server, None)

    from seeknal.publish.client import PublishClient

    try:
        client = PublishClient(resolved_server)
        response = client.revoke(slug, owner_secret)
    except Exception as exc:
        typer.echo(typer.style(f"Revoke failed: {exc}", fg=typer.colors.RED))
        raise typer.Exit(1)

    typer.echo(typer.style(f"Report '{slug}' revoked at {response.revoked_at}.", fg=typer.colors.GREEN))


@publish_app.command("list")
def publish_list() -> None:
    """List all previously published reports from the local ledger."""
    from seeknal.publish.ledger import list_entries

    entries = list_entries()
    if not entries:
        typer.echo("No published reports found in ledger.")
        return

    col_widths = {
        "report_name": max(len("REPORT"), max(len(e.report_name) for e in entries)),
        "slug": max(len("SLUG"), max(len(e.slug) for e in entries)),
        "share_url": max(len("SHARE URL"), max(len(e.share_url) for e in entries)),
        "published_at": max(len("PUBLISHED AT"), max(len(e.published_at) for e in entries)),
    }

    header = (
        f"{'REPORT':<{col_widths['report_name']}}  "
        f"{'SLUG':<{col_widths['slug']}}  "
        f"{'PUBLISHED AT':<{col_widths['published_at']}}  "
        f"SHARE URL"
    )
    typer.echo(header)
    typer.echo("-" * (len(header) + col_widths["share_url"]))

    for e in entries:
        typer.echo(
            f"{e.report_name:<{col_widths['report_name']}}  "
            f"{e.slug:<{col_widths['slug']}}  "
            f"{e.published_at:<{col_widths['published_at']}}  "
            f"{e.share_url}"
        )
