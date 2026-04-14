"""Publish a built Evidence.dev report to a Seeknal Report Server.

The agent uses this tool AFTER a successful `generate_report` call, when the
user wants a shareable URL hosted on a Seeknal Report Server instance. The
flow mirrors `publish_to_proof` (markdown memos): the agent must first use
`ask_user` to get explicit approval with the discriminator label
`Publish to Seeknal Report Server`, then call this tool.

The tool packages the already-built `target/reports/{slug}/build/` tree as
a tar.gz and POSTs it to the configured Seeknal Report Server endpoint,
returning the share URL the user can hand to others.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from seeknal.ask.agents.tools._context import (
    get_tool_context,
    require_seeknal_report_publish_approval,
)


def _get_publish_profile(project_path: Path) -> dict:
    """Load the publish profile from the project's profiles.yml (preferred)
    or ~/.seeknal/profiles.yml (fallback). Returns an empty dict on failure.
    """
    try:
        from seeknal.workflow.materialization.profile_loader import ProfileLoader

        local_profiles = project_path / "profiles.yml"
        loader = (
            ProfileLoader(profile_path=local_profiles)
            if local_profiles.exists()
            else ProfileLoader()
        )
        return loader.load_publish_profile("default")
    except Exception:
        return {}


def _resolve_server(server: Optional[str], project_path: Path) -> Optional[str]:
    """Resolve the report server URL from an override, env, or profiles.yml."""
    if server:
        return server
    env_server = os.environ.get("SEEKNAL_PUBLISH_SERVER")
    if env_server:
        return env_server
    return _get_publish_profile(project_path).get("server")


def _resolve_api_key(api_key: Optional[str], project_path: Path) -> Optional[str]:
    """Resolve the report server API key from an override, env, or profiles.yml."""
    if api_key:
        return api_key
    env_key = os.environ.get("SEEKNAL_PUBLISH_TOKEN")
    if env_key:
        return env_key
    return _get_publish_profile(project_path).get("api_key")


def _find_build_dir(project_path: Path, report_name: str) -> Optional[Path]:
    """Locate the built Evidence report directory for a given report name.

    Uses the same slugification as the scaffolder so passing either the
    original title or the slug works.
    """
    from seeknal.ask.report.scaffolder import _slugify

    slug = _slugify(report_name) if report_name else ""
    if not slug:
        return None
    build_dir = project_path / "target" / "reports" / slug / "build"
    return build_dir if build_dir.is_dir() else None


def _do_publish(
    *,
    report_name: str,
    server: str,
    api_key: Optional[str],
    build_dir: Path,
    report_title: Optional[str],
) -> str:
    """Core publish logic — package the build tree and POST to the server."""
    from seeknal.publish.client import PublishClient
    from seeknal.publish.exceptions import (
        PackageTooLargeError,
        PublishAuthError,
        PublishContentTypeError,
        PublishServerError,
        PublishTooLargeError,
    )
    from seeknal.publish.ledger import LedgerEntry, append_entry
    from seeknal.publish.packager import package_build

    try:
        tarball = package_build(build_dir)
    except PackageTooLargeError as exc:
        return f"Error: build too large to publish — {exc}"
    except Exception as exc:  # noqa: BLE001
        return f"Error packaging build directory: {exc}"

    try:
        client = PublishClient(server, api_key)
        response = client.publish(tarball, report_name, report_title)
    except PublishAuthError:
        return (
            "Error: the Seeknal Report Server rejected the API key (401). "
            "Check SEEKNAL_PUBLISH_TOKEN or the publish profile in profiles.yml."
        )
    except PublishContentTypeError:
        return "Error: the Seeknal Report Server rejected the upload Content-Type."
    except PublishTooLargeError:
        return (
            "Error: the build tarball exceeds the server's max upload size. "
            "Ask the server operator to raise SEEKNAL_REPORT_SERVER_MAX_UPLOAD_BYTES "
            "or slim down the report."
        )
    except PublishServerError as exc:
        return f"Error: Seeknal Report Server returned {exc}"
    except Exception as exc:  # noqa: BLE001
        return f"Error publishing to Seeknal Report Server: {exc}"
    finally:
        try:
            tarball.unlink(missing_ok=True)
        except Exception:  # noqa: BLE001
            pass

    try:
        append_entry(
            LedgerEntry(
                slug=response.slug,
                server=server,
                share_url=response.share_url,
                report_name=report_name,
                published_at=response.created_at,
            )
        )
    except Exception:  # noqa: BLE001
        pass

    full_url = (
        response.share_url
        if response.share_url.startswith("http")
        else f"{server.rstrip('/')}{response.share_url}"
    )

    return (
        "Report published successfully to the Seeknal Report Server.\n"
        f"Share URL: {full_url}\n"
        f"Slug: {response.slug}\n"
        "Owner secret (save this — the server will not return it again): "
        f"{response.owner_secret}\n"
        "To revoke later, run: "
        f"seeknal publish revoke {response.slug} --owner-secret <the-value-above>"
    )


async def publish_to_seeknal_report(
    report_name: str,
    report_title: Optional[str] = None,
    server: Optional[str] = None,
    api_key: Optional[str] = None,
) -> str:
    """Publish a built Evidence.dev report to a Seeknal Report Server.

    See the `publish-to-seeknal-report` skill for when to use, the approval-
    gate menu (discriminator: "Publish to Seeknal Report Server"), the
    SEEKNAL_PUBLISH_SERVER / SEEKNAL_PUBLISH_TOKEN environment variables, the
    profiles.yml fallback, and error paths.

    Args:
        report_name: The report name (or slug) used in the prior generate_report call.
        report_title: Optional human-readable title sent as X-Seeknal-Report-Title.
        server: Optional per-call override for the Seeknal Report Server URL.
        api_key: Optional per-call override for the API key.
    """
    guard = require_seeknal_report_publish_approval("publish_to_seeknal_report")
    if guard:
        return guard

    if not report_name or not report_name.strip():
        return "Error: report_name is required."

    ctx = get_tool_context()
    resolved_server = _resolve_server(server, ctx.project_path)
    if not resolved_server:
        return (
            "Error: no Seeknal Report Server URL configured. Pass `server=...`, "
            "set SEEKNAL_PUBLISH_SERVER, or add a `publish.default.server` entry "
            "to profiles.yml."
        )

    resolved_key = _resolve_api_key(api_key, ctx.project_path)

    build_dir = _find_build_dir(ctx.project_path, report_name)
    if build_dir is None:
        return (
            f"Error: no built report found for '{report_name}'. "
            "Run `generate_report` first, then retry publishing."
        )

    return _do_publish(
        report_name=report_name,
        server=resolved_server,
        api_key=resolved_key,
        build_dir=build_dir,
        report_title=report_title or report_name,
    )
