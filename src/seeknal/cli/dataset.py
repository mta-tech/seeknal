"""Seeknal CLI - Atlas data catalog.

A first-class command group for browsing and querying the Atlas data catalog from
the engine side. Active only when Atlas is configured (``ATLAS_API_URL`` set); every
command exits with code 2 and a hint when it is not.

Usage:
    seeknal dataset list [-q TEXT] [--type T] [--namespace NS]
    seeknal dataset show <dataset> [--sample]
    seeknal dataset annotate <dataset> --tag pii --description "..."
    seeknal dataset request-access <dataset> --reason "need it for X"
    seeknal dataset query <dataset> [--limit N]

Reads (``list``/``show``/``query``) flow the logged-in user's token to Atlas; the
access decision and any column masking come from the governance gate, so the CLI
shows exactly what the user is permitted to see.
"""

from __future__ import annotations

import json as _json
import os
import re
from typing import Any, List, Optional

import httpx
import typer
from tabulate import tabulate

from seeknal.integrations.atlas_catalog import (
    AtlasCatalogClient,
    Dataset,
    create_catalog_client_from_env,
)
from seeknal.integrations.atlas_client import (
    AtlasAuthError,
    AtlasContractError,
    AtlasPolicyDenied,
    SESSION_EXPIRED_HINT,
)
from seeknal.integrations.atlas_governance import (
    AccessDecision,
    apply_column_masks,
    create_governance_gate_from_env,
    refresh_access_token,
    user_email_from_credentials,
    user_token_from_credentials,
)
from seeknal.ui.output import echo_error, echo_info, echo_success

dataset_app = typer.Typer(
    name="dataset",
    help="Browse and query the Atlas data catalog (requires ATLAS_API_URL).",
)

#: Seconds to wait for the Atlas governance API before giving up.
_REQUEST_TIMEOUT_SECONDS = 30.0
#: Max columns ``show`` prints inline before truncating with a "+N more" line.
_SHOW_COLUMN_LIMIT = 50
_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}$")
#: A dotted ``namespace.table`` identifier (a governed Lakekeeper table that may not
#: be a registered catalog asset). No slashes/spaces — distinguishes it from an
#: asset's path-style namespace.
_TABLE_ID_RE = re.compile(r"^[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)+$")


def _require_catalog() -> AtlasCatalogClient:
    """Return a catalog client or exit(2) when Atlas is not configured."""

    client = create_catalog_client_from_env()
    if client is None:
        echo_error("Atlas is not configured (set ATLAS_API_URL).")
        raise typer.Exit(2)
    return client


def _resolve_dataset(client: AtlasCatalogClient, ref: str) -> Dataset:
    """Resolve a dataset reference (id or name) to a :class:`Dataset`.

    A UUID-looking ``ref`` is fetched directly; otherwise the catalog is searched and
    the best name/fqn match (else the first hit) is returned. Exits 1 when nothing
    matches.
    """

    if _UUID_RE.match(ref):
        try:
            return client.get_dataset(ref)
        except AtlasAuthError as exc:
            echo_error(str(exc))
            raise typer.Exit(1) from exc
        except AtlasContractError:
            pass  # fall through to a name search

    try:
        matches = client.list_datasets(query=ref, limit=10)
    except AtlasAuthError as exc:
        echo_error(str(exc))
        raise typer.Exit(1) from exc
    except AtlasContractError as exc:
        echo_error(f"Catalog lookup failed: {exc}")
        raise typer.Exit(1) from exc
    for dataset in matches:
        if ref in (dataset.name, dataset.fqn, dataset.id):
            return dataset
    if matches:
        return matches[0]

    # Not a registered catalog asset. If it looks like a governed table identifier
    # (``namespace.table``), treat it as a direct dataset so show/query/access-check
    # work against governed Lakekeeper tables that aren't in the unified catalog.
    if _TABLE_ID_RE.match(ref):
        namespace, _, name = ref.rpartition(".")
        return Dataset(id=ref, name=name or ref, namespace=namespace, asset_type="table")

    echo_error(f"No dataset found matching '{ref}'.")
    raise typer.Exit(1)


def _dataset_dict(dataset: Dataset) -> dict[str, Any]:
    return {
        "id": dataset.id,
        "name": dataset.name,
        "namespace": dataset.namespace,
        "asset_type": dataset.asset_type,
        "source_system": dataset.source_system,
        "description": dataset.description,
        "tags": list(dataset.tags),
        "fqn": dataset.fqn,
        "columns": [{"name": name, "type": col_type} for name, col_type in dataset.columns],
    }


def _extract_sample(data: Any) -> tuple[list[str], list[list[Any]]]:
    """Normalise a ``/api/sample`` response into ``(columns, rows)``.

    Tolerant of the common shapes: ``{columns, rows}``, ``{schema, data}``, a list of
    row dicts, or a list of row sequences. The Atlas backend wraps each row as
    ``{"values": {col: val}}``, so unwrap that envelope before projecting columns.
    """

    def _row_dict(row: Any) -> dict[str, Any]:
        if isinstance(row, dict):
            inner = row.get("values")
            if isinstance(inner, dict):
                return inner
            return row
        return {}

    if isinstance(data, dict):
        raw_cols = data.get("columns") or data.get("schema") or []
        columns = [c.get("name") if isinstance(c, dict) else c for c in raw_cols]
        raw_rows = data.get("rows") or data.get("data") or []
        if raw_rows and isinstance(raw_rows[0], dict):
            if not columns:
                columns = list(_row_dict(raw_rows[0]).keys())
            rows = [[_row_dict(r).get(c) for c in columns] for r in raw_rows]
        else:
            rows = [list(r) for r in raw_rows]
        return [str(c) for c in columns], rows
    if isinstance(data, list):
        if data and isinstance(data[0], dict):
            columns = list(_row_dict(data[0]).keys())
            return columns, [[_row_dict(r).get(c) for c in columns] for r in data]
        return [], [list(r) for r in data]
    return [], []


def _print_sample(
    client: AtlasCatalogClient,
    dataset: Dataset,
    limit: int,
    decision: Optional[AccessDecision],
) -> None:
    """Fetch + render a row sample, applying any masking the gate requires."""

    try:
        data = client.sample(dataset.fqn, limit=limit)
    except AtlasAuthError as exc:
        echo_error(str(exc))
        return
    except AtlasContractError as exc:
        echo_error(f"Sample failed: {exc}")
        return
    columns, rows = _extract_sample(data)
    if decision is not None and decision.masked_columns and rows and columns:
        masked = apply_column_masks(
            [dict(zip(columns, row)) for row in rows], decision.masked_columns
        )
        rows = [[m.get(c) for c in columns] for m in masked]
    if not rows:
        echo_info("(no rows)")
        return
    typer.echo(tabulate(rows, headers=columns, tablefmt="simple"))
    echo_info(f"{len(rows)} row(s).")


@dataset_app.command("list")
def list_datasets(
    query: Optional[str] = typer.Option(None, "-q", "--query", help="Search text."),
    asset_type: Optional[str] = typer.Option(None, "--type", help="Filter by asset type."),
    namespace: Optional[str] = typer.Option(None, "--namespace", help="Filter by namespace."),
    limit: int = typer.Option(50, "--limit", help="Maximum number of datasets."),
    as_json: bool = typer.Option(False, "--json", help="Output JSON instead of a table."),
) -> None:
    """List datasets in the Atlas catalog."""

    client = _require_catalog()
    try:
        datasets = client.list_datasets(
            query=query, asset_type=asset_type, namespace=namespace, limit=limit
        )
    except AtlasContractError as exc:
        echo_error(f"Failed to list datasets: {exc}")
        raise typer.Exit(1)

    if as_json:
        typer.echo(_json.dumps([_dataset_dict(d) for d in datasets], indent=2))
        return
    if not datasets:
        echo_info("No datasets found.")
        return
    rows = [[d.name, d.namespace, d.asset_type, ", ".join(d.tags)] for d in datasets]
    typer.echo(tabulate(rows, headers=["Name", "Namespace", "Type", "Tags"], tablefmt="simple"))
    echo_info(f"{len(datasets)} dataset(s).")


@dataset_app.command("show")
def show_dataset(
    dataset: str = typer.Argument(..., help="Dataset id or name."),
    sample: bool = typer.Option(False, "-s", "--sample", help="Include a row sample."),
    limit: int = typer.Option(20, "--limit", help="Sample row limit."),
    as_json: bool = typer.Option(False, "--json", help="Output JSON instead of text."),
) -> None:
    """Show a dataset's metadata, your live access decision, and (optionally) a sample."""

    client = _require_catalog()
    resolved = _resolve_dataset(client, dataset)

    gate = create_governance_gate_from_env()
    decision: Optional[AccessDecision] = None
    if gate is not None:
        try:
            decision = gate.check_access(resource=resolved.fqn, action="read")
        except AtlasContractError:
            decision = None

    if as_json:
        out = _dataset_dict(resolved)
        if decision is not None:
            out["access"] = {
                "allowed": decision.allowed,
                "masked_columns": list(decision.masked_columns),
                "reason": decision.reason,
            }
        typer.echo(_json.dumps(out, indent=2))
        return

    echo_success(resolved.name)
    typer.echo(f"  namespace : {resolved.namespace}")
    typer.echo(f"  type      : {resolved.asset_type}")
    typer.echo(f"  source    : {resolved.source_system}")
    if resolved.description:
        typer.echo(f"  description: {resolved.description}")
    if resolved.tags:
        typer.echo(f"  tags      : {', '.join(resolved.tags)}")
    typer.echo(f"  id        : {resolved.id}")
    if resolved.columns:
        typer.echo(f"  columns   : {len(resolved.columns)}")
        for col_name, col_type in resolved.columns[:_SHOW_COLUMN_LIMIT]:
            suffix = f" ({col_type})" if col_type else ""
            typer.echo(f"    - {col_name}{suffix}")
        if len(resolved.columns) > _SHOW_COLUMN_LIMIT:
            typer.echo(f"    … (+{len(resolved.columns) - _SHOW_COLUMN_LIMIT} more)")
    if decision is not None:
        mark = "ALLOW" if decision.allowed else "DENY"
        masked = (
            f" (masked: {', '.join(decision.masked_columns)})"
            if decision.masked_columns
            else ""
        )
        typer.echo(f"  access    : {mark}{masked}")
        if not decision.allowed and decision.reason:
            typer.echo(f"  reason    : {decision.reason}")
    if sample:
        _print_sample(client, resolved, limit, decision)


@dataset_app.command("annotate")
def annotate_dataset(
    dataset: str = typer.Argument(..., help="Dataset id or name."),
    tags: Optional[List[str]] = typer.Option(None, "--tag", help="Tag to add (repeatable)."),
    description: Optional[str] = typer.Option(None, "--description", help="Set the description."),
    owners: Optional[List[str]] = typer.Option(None, "--owner", help="Owner to add (repeatable)."),
) -> None:
    """Annotate a dataset (tags / description / owners) in the Atlas catalog."""

    if not tags and description is None and not owners:
        echo_error("Provide at least one of --tag, --description, --owner.")
        raise typer.Exit(1)
    client = _require_catalog()
    resolved = _resolve_dataset(client, dataset)
    try:
        client.annotate(
            resolved.id,
            tags=tags or None,
            description=description,
            owners=owners or None,
        )
    except AtlasContractError as exc:
        echo_error(f"Annotation failed: {exc}")
        raise typer.Exit(1)
    echo_success(f"Annotated {resolved.name}.")


@dataset_app.command("request-access")
def request_access(
    dataset: str = typer.Argument(..., help="Dataset id or name."),
    reason: str = typer.Option(..., "--reason", help="Justification for the request."),
    access: str = typer.Option("read", "--access", help="Access type (read, write)."),
    duration: str = typer.Option("90d", "--duration", help="Requested grant duration."),
    priority: str = typer.Option("medium", "--priority", help="Request priority."),
) -> None:
    """Request access to a dataset via the Atlas governance backend."""

    client = _require_catalog()
    resolved = _resolve_dataset(client, dataset)

    base = (
        os.getenv("SEEKNAL_API_URL") or os.getenv("ATLAS_API_URL") or "http://localhost:8000"
    ).rstrip("/")
    body: dict[str, Any] = {
        "requester": user_email_from_credentials() or os.getenv("USER", ""),
        "entityName": resolved.fqn,
        "entityUrn": "",
        "requestedAccess": access,
        "reason": reason,
        "duration": duration,
        "priority": priority,
        "type": "dataset",
    }
    def _post_access_request(bearer: str | None) -> httpx.Response:
        headers = {"Content-Type": "application/json"}
        if bearer:
            headers["Authorization"] = f"Bearer {bearer}"
        return httpx.post(
            f"{base}/governance/access-requests",
            json=body,
            headers=headers,
            timeout=_REQUEST_TIMEOUT_SECONDS,
        )

    try:
        response = _post_access_request(user_token_from_credentials())
        # On an expired session, refresh the access token once and retry.
        if response.status_code == 401:
            new_token = refresh_access_token()
            if new_token:
                response = _post_access_request(new_token)
    except httpx.HTTPError as exc:
        echo_error(f"Access request failed: {exc}")
        raise typer.Exit(1)
    if response.status_code == 401:
        echo_error(SESSION_EXPIRED_HINT)
        raise typer.Exit(1)
    if not (200 <= response.status_code < 300):
        detail = response.text.strip() or f"HTTP {response.status_code}"
        echo_error(f"Access request rejected ({response.status_code}): {detail[:200]}")
        raise typer.Exit(1)
    try:
        record = response.json()
    except ValueError:
        record = {}
    echo_success(
        f"Access request {record.get('id', '(unknown)')} created for {resolved.fqn}"
    )
    echo_info(f"Status: {record.get('status', 'pending')} | access: {access} | duration: {duration}")


@dataset_app.command("query")
def query_dataset(
    dataset: str = typer.Argument(..., help="Dataset id or name."),
    limit: int = typer.Option(20, "--limit", help="Row limit."),
) -> None:
    """Preview a governed dataset (access-checked + masked).

    This is a quick, governed ``SELECT *`` preview. For ad-hoc SQL across Atlas
    datasets, use the REPL (``seeknal repl``) — when Atlas is connected it lists and
    queries the governed catalog directly.
    """

    client = _require_catalog()
    resolved = _resolve_dataset(client, dataset)

    gate = create_governance_gate_from_env()
    decision: Optional[AccessDecision] = None
    if gate is not None:
        try:
            decision = gate.enforce_access(resource=resolved.fqn, action="read")
        except AtlasPolicyDenied as exc:
            echo_error(str(exc))
            echo_info(f'Run: seeknal dataset request-access {resolved.name} --reason "<why>"')
            raise typer.Exit(1)
        except AtlasContractError as exc:
            echo_error(f"Access check failed: {exc}")
            raise typer.Exit(1)

    _print_sample(client, resolved, limit, decision)


def _dataset_urn(dataset: Dataset) -> str:
    """Best-effort DataHub dataset URN for a lineage lookup.

    Prefers the urn the portal already carried in ``metadata``/``id``; otherwise
    composes the iceberg URN from the fqn so a governed table resolved as a direct
    ``namespace.table`` identifier still looks up lineage.
    """

    urn = str(dataset.metadata.get("urn") or "")
    if urn.startswith("urn:"):
        return urn
    if dataset.id.startswith("urn:"):
        return dataset.id
    platform = str(dataset.metadata.get("platform") or dataset.source_system or "iceberg")
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{dataset.fqn},PROD)"


def _name_from_urn(urn: str) -> str:
    match = re.match(r"urn:li:dataset:\(urn:li:dataPlatform:\w+,([^,]+),", urn)
    return match.group(1) if match else ""


def _lineage_nodes(raw: Any) -> list[dict[str, str]]:
    """Normalise a lineage relationship list into ``{name, type}`` dicts."""

    nodes: list[dict[str, str]] = []
    for node in raw or []:
        if not isinstance(node, dict):
            continue
        urn = str(node.get("urn", ""))
        name = node.get("name") or _name_from_urn(urn) or urn
        nodes.append(
            {"name": str(name), "type": str(node.get("type") or node.get("platform") or "")}
        )
    return nodes


def _print_lineage(label: str, nodes: list[dict[str, str]]) -> None:
    typer.echo(f"  {label}:")
    if not nodes:
        typer.echo("    (none)")
        return
    for node in nodes:
        suffix = f" [{node['type']}]" if node["type"] else ""
        typer.echo(f"    - {node['name']}{suffix}")


@dataset_app.command("lineage")
def lineage_dataset(
    dataset: str = typer.Argument(..., help="Dataset id or name."),
    as_json: bool = typer.Option(False, "--json", help="Output JSON instead of text."),
) -> None:
    """Show a dataset's upstream/downstream lineage (DataHub-backed)."""

    client = _require_catalog()
    resolved = _resolve_dataset(client, dataset)
    try:
        data = client.lineage(_dataset_urn(resolved))
    except AtlasAuthError as exc:
        echo_error(str(exc))
        raise typer.Exit(1)
    except AtlasContractError as exc:
        echo_error(f"Lineage lookup failed: {exc}")
        raise typer.Exit(1)

    upstream = _lineage_nodes(data.get("upstreamLineage") or data.get("upstream"))
    downstream = _lineage_nodes(data.get("downstreamLineage") or data.get("downstream"))

    if as_json:
        typer.echo(
            _json.dumps(
                {"dataset": resolved.fqn, "upstream": upstream, "downstream": downstream},
                indent=2,
            )
        )
        return

    echo_success(f"Lineage for {resolved.fqn}")
    _print_lineage("upstream", upstream)
    _print_lineage("downstream", downstream)
    if not upstream and not downstream:
        echo_info("No lineage recorded in DataHub for this dataset.")


__all__ = ["dataset_app"]
