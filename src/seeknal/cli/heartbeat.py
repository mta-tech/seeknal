"""`seeknal heartbeat` CLI sub-app.

Commands:
  start  — run the polling daemon (loops until SIGTERM/SIGINT).
  tick   — run a single tick and exit (for cron/k8s).
  status — print a tail of recent runs.
  review — interactive quarantine review (placeholder for Step 14 CLI parity).
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional

import typer

from seeknal.heartbeat.config import HeartbeatConfig
from seeknal.heartbeat.recorder import RunRecorder
from seeknal.heartbeat.runner import HeartbeatRunner

app = typer.Typer(
    help=(
        "Heartbeat polling daemon. Reads HEARTBEAT.md, scans inbox folders, "
        "ingests new files, runs the DAG + exposures, and records each tick "
        "to target/heartbeat/runs.jsonl."
    )
)


def _echo_info(msg: str) -> None:
    typer.secho(msg, fg=typer.colors.CYAN)


def _echo_success(msg: str) -> None:
    typer.secho(msg, fg=typer.colors.GREEN)


def _echo_error(msg: str) -> None:
    typer.secho(msg, fg=typer.colors.RED, err=True)


def _load_profile_loader(profile_path: Optional[Path]):
    try:
        from seeknal.workflow.materialization.profile_loader import ProfileLoader

        return ProfileLoader(profile_path=profile_path)
    except Exception as exc:  # noqa: BLE001
        _echo_error(f"Failed to load profile loader: {exc}")
        return None


def _resolve_project_root() -> Path:
    return Path.cwd().resolve()


@app.command("start")
def start_cmd(
    profile: Optional[Path] = typer.Option(
        None, "--profile", "-p", help="Path to profiles.yml override."
    ),
):
    """Start the heartbeat daemon (loops until SIGTERM/SIGINT)."""
    project_root = _resolve_project_root()
    profile_loader = _load_profile_loader(profile)
    try:
        config = HeartbeatConfig.load(project_root, profile_loader=profile_loader)
    except Exception as exc:  # noqa: BLE001
        _echo_error(f"Failed to load HEARTBEAT.md: {exc}")
        raise typer.Exit(2)
    if profile is not None:
        config.profile_path = profile
    if not config.enabled or config.interval_seconds <= 0:
        _echo_info(
            f"Heartbeat disabled (enabled={config.enabled}, "
            f"interval={config.interval_seconds}s). Run `seeknal heartbeat tick` "
            f"for one-shot ingestion."
        )
        raise typer.Exit(0)
    runner = HeartbeatRunner(config, project_root, profile_loader=profile_loader)
    _echo_info(
        f"Starting heartbeat (interval={config.interval_seconds}s, "
        f"target={config.ingested_target}, project={config.project_name})"
    )
    asyncio.run(runner.run_forever())


@app.command("tick")
def tick_cmd(
    profile: Optional[Path] = typer.Option(
        None, "--profile", "-p", help="Path to profiles.yml override."
    ),
):
    """Run a single heartbeat tick and exit."""
    project_root = _resolve_project_root()
    profile_loader = _load_profile_loader(profile)
    try:
        config = HeartbeatConfig.load(project_root, profile_loader=profile_loader)
    except Exception as exc:  # noqa: BLE001
        _echo_error(f"Failed to load HEARTBEAT.md: {exc}")
        raise typer.Exit(2)
    if profile is not None:
        config.profile_path = profile
    runner = HeartbeatRunner(config, project_root, profile_loader=profile_loader)
    run = asyncio.run(runner.tick_once(reason="manual"))
    _echo_info(
        f"Run {run.run_id}: status={run.status} "
        f"ingested={len(run.ingested)} "
        f"dag_failed={run.dag.nodes_failed} exposure={len(run.exposure)} "
        f"duration={run.duration_ms}ms"
    )
    if run.errors:
        for err in run.errors:
            _echo_error(f"  ! {err}")


@app.command("status")
def status_cmd(
    limit: int = typer.Option(10, "--limit", "-n", help="Number of recent runs to show."),
):
    """Print a tail of recent runs from runs.jsonl."""
    project_root = _resolve_project_root()
    recorder = RunRecorder(project_root / "target" / "heartbeat")
    rows = recorder.tail(limit=limit)
    if not rows:
        _echo_info("No heartbeat runs recorded yet.")
        return
    for row in rows:
        run_id = row.get("run_id", "?")
        status = row.get("status", "?")
        ingested = len(row.get("ingested", []))
        dag = row.get("dag", {}) or {}
        dag_failed = dag.get("nodes_failed", 0)
        expo_count = len(row.get("exposure", []))
        duration = row.get("duration_ms", 0)
        reason = row.get("reason")
        suffix = f" ({reason})" if reason else ""
        _echo_info(
            f"{run_id[:8]}  status={status}{suffix}  ingested={ingested}  "
            f"dag_failed={dag_failed}  exposure={expo_count}  "
            f"duration={duration}ms"
        )


@app.command("review")
def review_cmd(
    run_id: str = typer.Argument(..., help="Run ID containing quarantined items."),
    confirm: bool = typer.Option(False, "--confirm", help="Confirm the first pending item."),
    reject: bool = typer.Option(False, "--reject", help="Reject the first pending item."),
    edit: Optional[str] = typer.Option(
        None,
        "--edit",
        help="Apply edit-text directives (key=value lines) to the first pending item.",
    ),
):
    """Interactive quarantine review (CLI parity with Telegram flow).

    Flags map to the Telegram inline-keyboard actions. Without flags, the
    command lists pending items and prints next-step hints.
    """
    from seeknal.heartbeat.quarantine.review import (
        QuarantineSidecar,
        apply_review_decision,
        list_pending_reviews,
    )

    project_root = _resolve_project_root()
    quarantine_dir = (
        project_root / "target" / "heartbeat" / "quarantine" / "needs_review" / run_id
    )
    if not quarantine_dir.exists():
        _echo_error(f"No quarantine entries for run {run_id} (dir not found).")
        raise typer.Exit(1)
    sidecars = sorted(quarantine_dir.glob("*.review.yml"))
    if not sidecars:
        _echo_info(f"No pending review items in {quarantine_dir}.")
        return

    _echo_info(f"Pending review for run {run_id} ({len(sidecars)} items):")
    for sidecar in sidecars:
        try:
            sc = QuarantineSidecar.load(sidecar)
            _echo_info(
                f"  - {sidecar.name}  status={sc.status}  target={sc.target_table or '?'}"
            )
        except Exception as exc:  # noqa: BLE001
            _echo_error(f"  - {sidecar.name}: {exc}")

    if not (confirm or reject or edit):
        _echo_info(
            "Use --confirm, --reject, or --edit 'key=value\\n...' to act on the first item."
        )
        return

    pending = list_pending_reviews(project_root)
    target = next((p for p in pending if p.parent == quarantine_dir), None)
    if target is None:
        _echo_error("No pending items left for this run_id.")
        raise typer.Exit(1)

    if confirm:
        result = apply_review_decision(
            target, decision="confirm", resolved_by="cli"
        )
    elif reject:
        result = apply_review_decision(
            target, decision="reject", resolved_by="cli"
        )
    else:
        result = apply_review_decision(
            target, decision="edit", resolved_by="cli", edit_text=edit
        )

    if result.get("ok"):
        _echo_success(f"Decision applied: {result.get('decision')}")
    else:
        for err in result.get("errors", []):
            _echo_error(err)
        if "error" in result:
            _echo_error(result["error"])
        raise typer.Exit(1)


__all__ = ["app"]
