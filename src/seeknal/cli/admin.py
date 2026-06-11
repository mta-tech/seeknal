"""`seeknal admin` CLI — governance commands for the v0.5 access surface.

Sub-commands:
  access list / grant / revoke / pending / set-default / export / import
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer

from seeknal.ask.access.pending import PendingQueue
from seeknal.ask.access.policy import AccessPolicy
from seeknal.ask.access.roles import Role

app = typer.Typer(help="Admin commands for managing Ask access policy.")
access_app = typer.Typer(help="Manage .seeknal/access.yml and the pending queue.")
app.add_typer(access_app, name="access")


def _echo_info(msg: str) -> None:
    typer.secho(msg, fg=typer.colors.CYAN)


def _echo_success(msg: str) -> None:
    typer.secho(msg, fg=typer.colors.GREEN)


def _echo_error(msg: str) -> None:
    typer.secho(msg, fg=typer.colors.RED, err=True)


def _policy_path() -> Path:
    return Path.cwd() / ".seeknal" / "access.yml"


def _pending_path() -> Path:
    return Path.cwd() / ".seeknal" / "access_pending.json"


def _load_policy() -> AccessPolicy:
    return AccessPolicy.load(_policy_path())


def _load_pending() -> PendingQueue:
    return PendingQueue.load(_pending_path())


def _resolve_user_id(value: str) -> tuple[Optional[int], Optional[str]]:
    """Parse "12345" → (12345, None) or "alice" → (None, "alice")."""
    if value.isdigit():
        return int(value), None
    return None, value


@access_app.command("list")
def list_cmd():
    """Print current allowlist, pending requests, and deny_list."""
    policy = _load_policy()
    pending = _load_pending()

    _echo_info(
        f"Default role: {policy.default_role.label() if policy.default_role else 'none'}"
    )
    _echo_info(f"Allowlist ({len(policy.allowlist)} entries):")
    for entry in policy.allowlist:
        ident = (
            f"user={entry.telegram_user_id}"
            if entry.telegram_user_id is not None
            else f"chat={entry.telegram_chat_id}"
        )
        role = entry.role.label() if entry.role else "—"
        username = entry.telegram_username or "—"
        _echo_info(f"  {ident}  role={role}  @{username}  notes={entry.notes!r}")

    _echo_info(f"Pending ({len(pending.requests)} entries):")
    for req in pending.requests:
        _echo_info(
            f"  user={req.telegram_user_id}  @{req.telegram_username or '—'}  "
            f"requested={req.requested_at}"
        )

    _echo_info(f"Deny list ({len(policy.deny_list)} entries):")
    for entry in policy.deny_list:
        _echo_info(
            f"  user={entry.telegram_user_id}  reason={entry.reason!r}  "
            f"added={entry.added_at}"
        )


@access_app.command("grant")
def grant_cmd(
    user_id: int = typer.Argument(..., help="Telegram numeric user_id."),
    role: str = typer.Argument(..., help="viewer | analyst | operator | engineer | admin"),
    username: Optional[str] = typer.Option(None, "--username", "-u"),
    notes: str = typer.Option("", "--notes"),
):
    """Grant a role to a Telegram user. Overwrites prior entry on the same user_id."""
    policy = _load_policy()
    try:
        role_enum = Role.from_string(role)
    except ValueError as exc:
        _echo_error(str(exc))
        raise typer.Exit(1)
    policy.grant(
        telegram_user_id=user_id, role=role_enum, username=username, notes=notes
    )
    policy.save(_policy_path())
    _echo_success(
        f"Granted {role_enum.label()} to user_id={user_id}."
    )


@access_app.command("revoke")
def revoke_cmd(
    user_id: int = typer.Argument(..., help="Telegram numeric user_id."),
    deny: bool = typer.Option(False, "--deny", help="Also add to deny_list."),
    reason: str = typer.Option("", "--reason"),
):
    """Remove a user from the allowlist; optionally add to deny_list."""
    policy = _load_policy()
    removed = policy.revoke(user_id, deny_reason=reason if deny else None)
    policy.save(_policy_path())
    if removed:
        _echo_success(f"Revoked user_id={user_id}.")
    else:
        _echo_info(f"user_id={user_id} was not in the allowlist.")
    if deny:
        _echo_success(f"Added user_id={user_id} to deny_list.")


@access_app.command("pending")
def pending_cmd(
    approve: Optional[int] = typer.Option(None, "--approve", help="Approve a pending user_id."),
    role: Optional[str] = typer.Option(None, "--role", help="Role for --approve."),
    deny: Optional[int] = typer.Option(None, "--deny", help="Deny a pending user_id."),
    reason: str = typer.Option("", "--reason"),
):
    """Inspect or resolve pending access requests."""
    pending = _load_pending()
    if approve is None and deny is None:
        if not pending.requests:
            _echo_info("No pending requests.")
            return
        for req in pending.requests:
            _echo_info(
                f"  user={req.telegram_user_id}  @{req.telegram_username or '—'}  "
                f"intro={req.intro_message[:60]!r}  ttl_until={req.ttl_until}"
            )
        return

    if approve is not None:
        if not role:
            _echo_error("--approve requires --role.")
            raise typer.Exit(1)
        try:
            role_enum = Role.from_string(role)
        except ValueError as exc:
            _echo_error(str(exc))
            raise typer.Exit(1)
        req = pending.get(approve)
        if req is None:
            _echo_error(f"No pending request for user_id={approve}.")
            raise typer.Exit(1)
        policy = _load_policy()
        policy.grant(
            telegram_user_id=approve,
            role=role_enum,
            username=req.telegram_username,
        )
        policy.save(_policy_path())
        pending.remove(approve)
        pending.save(_pending_path())
        _echo_success(f"Approved user_id={approve} as {role_enum.label()}.")

    if deny is not None:
        policy = _load_policy()
        if reason:
            policy.revoke(deny, deny_reason=reason)
            policy.save(_policy_path())
        pending.remove(deny)
        pending.save(_pending_path())
        _echo_success(f"Denied user_id={deny}.")


@access_app.command("set-default")
def set_default_cmd(
    role: str = typer.Argument(..., help="none | viewer | analyst | operator | engineer | admin"),
):
    """Change the default role applied to listed-but-roleless callers."""
    policy = _load_policy()
    if role.lower() == "none":
        policy.default_role = None
    else:
        try:
            policy.default_role = Role.from_string(role)
        except ValueError as exc:
            _echo_error(str(exc))
            raise typer.Exit(1)
    policy.save(_policy_path())
    _echo_success(f"default_role set to {role}.")


@access_app.command("export")
def export_cmd(
    target: Path = typer.Argument(..., help="Path to write the export to."),
):
    """Dump the policy to a JSON file (for backup)."""
    policy = _load_policy()
    payload = {
        "schema_version": policy.schema_version,
        "default_role": policy.default_role.label() if policy.default_role else "none",
        "allowlist": [entry.to_yaml() for entry in policy.allowlist],
        "deny_list": [entry.to_yaml() for entry in policy.deny_list],
    }
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, sort_keys=True))
    _echo_success(f"Wrote {target}.")


@access_app.command("import")
def import_cmd(
    source: Path = typer.Argument(..., help="Path to a JSON export to import."),
    overwrite: bool = typer.Option(False, "--overwrite", help="Replace current policy."),
):
    """Restore policy from a JSON export."""
    if not source.exists():
        _echo_error(f"Source file does not exist: {source}")
        raise typer.Exit(1)
    payload = json.loads(source.read_text())
    policy_path = _policy_path()
    if policy_path.exists() and not overwrite:
        _echo_error(
            f"Policy already exists at {policy_path}. Pass --overwrite to replace."
        )
        raise typer.Exit(1)
    policy = AccessPolicy.empty(source_path=policy_path)
    policy.schema_version = int(payload.get("schema_version", 1))
    default_role = payload.get("default_role")
    if default_role and default_role != "none":
        policy.default_role = Role.from_string(default_role)
    for entry in payload.get("allowlist", []) or []:
        role_str = entry.get("role")
        role_enum = Role.from_string(role_str) if role_str else None
        if role_enum is None:
            continue
        policy.grant(
            telegram_user_id=entry.get("telegram_user_id"),
            telegram_chat_id=entry.get("telegram_chat_id"),
            role=role_enum,
            username=entry.get("telegram_username"),
            notes=entry.get("notes", "") or "",
            added_at=entry.get("added_at", "") or "",
            added_by=entry.get("added_by", "") or "",
        )
    # Deny list
    from seeknal.ask.access.policy import DenyEntry

    for entry in payload.get("deny_list", []) or []:
        user_id = entry.get("telegram_user_id")
        if not isinstance(user_id, int):
            continue
        policy.deny_list.append(
            DenyEntry(
                telegram_user_id=user_id,
                reason=entry.get("reason", "") or "",
                added_at=entry.get("added_at", "") or "",
            )
        )
    policy.save(policy_path)
    _echo_success(f"Imported policy from {source}.")


__all__ = ["app"]
