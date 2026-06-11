"""
Seeknal Atlas CLI - Atlas Data Platform integration commands.

This module provides CLI commands for Atlas Data Platform integration,
enabling governance, lineage tracking, and API operations from the
Seeknal command line.

Usage:
    seeknal atlas api start           Start the Atlas Seeknal API server
    seeknal atlas api status          Check API server status

    seeknal atlas governance stats    Get governance statistics
    seeknal atlas governance policies List governance policies

    seeknal atlas lineage show <fg>   Show lineage for a feature group
    seeknal atlas lineage publish     Publish lineage to DataHub

Examples:
    # Start the Atlas API server
    $ seeknal atlas api start --port 8000

    # Check governance compliance
    $ seeknal atlas governance stats

    # View feature group lineage
    $ seeknal atlas lineage show user_features

Requires: pip install seeknal[atlas]
"""

import typer
from typing import Optional
from pathlib import Path
import sys
import os


# Create the Atlas command group
atlas_app = typer.Typer(
    name="atlas",
    help="""Atlas Data Platform integration commands.

Provides access to Atlas governance, lineage, and API capabilities
from the Seeknal CLI.

Commands:
  api         Manage the Atlas Seeknal API server
  governance  Data governance operations (policies, access, audit)
  lineage     Lineage tracking and publishing

Enable with: pip install seeknal[atlas]
""",
    add_completion=False,
)

# Sub-groups
api_app = typer.Typer(
    name="api",
    help="Manage the Atlas Seeknal API server",
)
atlas_app.add_typer(api_app, name="api")

governance_app = typer.Typer(
    name="governance",
    help="Data governance operations",
)
atlas_app.add_typer(governance_app, name="governance")

lineage_app = typer.Typer(
    name="lineage",
    help="Lineage tracking and publishing",
)
atlas_app.add_typer(lineage_app, name="lineage")


def _check_atlas_installed() -> bool:
    """Check if atlas-data-platform is installed."""
    try:
        import atlas.seeknal
        return True
    except ImportError:
        return False


from seeknal.ui.output import (
    echo_success as _echo_success,
    echo_error as _echo_error,
    echo_warning as _echo_warning,
    echo_info as _echo_info,
)


def _require_atlas():
    """Check if Atlas is available, exit with helpful message if not."""
    if not _check_atlas_installed():
        _echo_error("Atlas Data Platform is not installed.")
        typer.echo("")
        typer.echo("Install Atlas integration with:")
        typer.echo(typer.style("  pip install seeknal[atlas]", fg=typer.colors.CYAN))
        typer.echo("")
        typer.echo("Or install Atlas directly:")
        typer.echo(typer.style("  pip install atlas-data-platform", fg=typer.colors.CYAN))
        raise typer.Exit(1)


# =============================================================================
# API Commands
# =============================================================================


@api_app.command("start")
def api_start(
    host: str = typer.Option(
        "0.0.0.0", "--host", "-h",
        help="Host to bind the API server"
    ),
    port: int = typer.Option(
        8000, "--port", "-p",
        help="Port to bind the API server"
    ),
    reload: bool = typer.Option(
        False, "--reload", "-r",
        help="Enable auto-reload for development"
    ),
    workers: int = typer.Option(
        1, "--workers", "-w",
        help="Number of worker processes"
    ),
    log_level: str = typer.Option(
        "info", "--log-level", "-l",
        help="Log level (debug, info, warning, error)"
    ),
):
    """Start the Atlas Seeknal API server.

    Launches a FastAPI server providing REST endpoints for:
    - Entity management (/entities)
    - Feature group management (/feature-groups)
    - Flow execution (/flows)
    - Governance operations (/governance)

    Example:
        seeknal atlas api start --port 8000 --reload
    """
    _require_atlas()

    typer.echo(f"Starting Atlas Seeknal API server on {host}:{port}")
    typer.echo(f"  Workers: {workers}")
    typer.echo(f"  Log level: {log_level}")
    typer.echo(f"  Reload: {reload}")
    typer.echo("")

    try:
        import uvicorn
        from atlas.seeknal.api import get_app

        _echo_info(f"API docs available at http://{host}:{port}/docs")

        uvicorn.run(
            "atlas.seeknal.api:get_app",
            host=host,
            port=port,
            reload=reload,
            workers=workers,
            log_level=log_level,
            factory=True,
        )
    except ImportError as e:
        _echo_error(f"Missing dependency: {e}")
        typer.echo("Install with: pip install uvicorn[standard]")
        raise typer.Exit(1)
    except Exception as e:
        _echo_error(f"Failed to start API server: {e}")
        raise typer.Exit(1)


@api_app.command("status")
def api_status(
    host: str = typer.Option(
        "localhost", "--host", "-h",
        help="API server host to check"
    ),
    port: int = typer.Option(
        8000, "--port", "-p",
        help="API server port to check"
    ),
):
    """Check the Atlas Seeknal API server status.

    Verifies if the API server is running and healthy by
    calling the /health endpoint.
    """
    import httpx

    url = f"http://{host}:{port}/health"
    typer.echo(f"Checking API status at {url}...")

    try:
        response = httpx.get(url, timeout=5.0)

        if response.status_code == 200:
            data = response.json()
            _echo_success("API server is running")
            typer.echo(f"  Version: {data.get('version', 'unknown')}")
            typer.echo(f"  Healthy: {data.get('healthy', False)}")
            typer.echo(f"  Uptime: {data.get('uptime_seconds', 0):.1f}s")

            deps = data.get('dependencies', {})
            if deps:
                typer.echo("  Dependencies:")
                for dep, status in deps.items():
                    status_str = typer.style("✓", fg=typer.colors.GREEN) if status else typer.style("✗", fg=typer.colors.RED)
                    typer.echo(f"    {status_str} {dep}")
        else:
            _echo_warning(f"API returned status {response.status_code}")
    except httpx.ConnectError:
        _echo_error(f"Cannot connect to API server at {url}")
        typer.echo("")
        typer.echo("Start the server with:")
        typer.echo(typer.style("  seeknal atlas api start", fg=typer.colors.CYAN))
        raise typer.Exit(1)
    except Exception as e:
        _echo_error(f"Error checking API status: {e}")
        raise typer.Exit(1)


# =============================================================================
# Governance Commands
# =============================================================================


@governance_app.command("stats")
def governance_stats(
    host: str = typer.Option(
        "localhost", "--host", "-h",
        help="API server host"
    ),
    port: int = typer.Option(
        8000, "--port", "-p",
        help="API server port"
    ),
    format: str = typer.Option(
        "table", "--format", "-f",
        help="Output format (table, json)"
    ),
):
    """Get governance statistics from Atlas.

    Shows counts for policies, violations, pending requests,
    and audit events.

    Example:
        seeknal atlas governance stats
    """
    import httpx
    import json

    url = f"http://{host}:{port}/governance/stats"

    try:
        response = httpx.get(url, timeout=10.0)
        response.raise_for_status()
        data = response.json()

        if format == "json":
            typer.echo(json.dumps(data, indent=2))
        else:
            typer.echo("")
            typer.echo(typer.style("Governance Statistics", bold=True))
            typer.echo("=" * 40)
            typer.echo(f"  Total Policies:       {data.get('total_policies', 0)}")
            typer.echo(f"  Active Violations:    {data.get('active_violations', 0)}")
            typer.echo(f"  Pending Requests:     {data.get('pending_requests', 0)}")
            typer.echo(f"  Audit Events:         {data.get('audit_events', 0)}")
            typer.echo("")

    except httpx.ConnectError:
        _echo_error(f"Cannot connect to API server at http://{host}:{port}")
        typer.echo("Start the server with: seeknal atlas api start")
        raise typer.Exit(1)
    except httpx.HTTPStatusError as e:
        _echo_error(f"API error: {e.response.status_code}")
        raise typer.Exit(1)
    except Exception as e:
        _echo_error(f"Error: {e}")
        raise typer.Exit(1)


@governance_app.command("policies")
def governance_policies(
    status: Optional[str] = typer.Option(
        None, "--status", "-s",
        help="Filter by status (active, draft, archived)"
    ),
    policy_type: Optional[str] = typer.Option(
        None, "--type", "-t",
        help="Filter by type (access_control, retention, privacy)"
    ),
    host: str = typer.Option(
        "localhost", "--host", "-h",
        help="API server host"
    ),
    port: int = typer.Option(
        8000, "--port", "-p",
        help="API server port"
    ),
    format: str = typer.Option(
        "table", "--format", "-f",
        help="Output format (table, json)"
    ),
):
    """List governance policies.

    Example:
        seeknal atlas governance policies --status active
    """
    import httpx
    import json
    from tabulate import tabulate

    url = f"http://{host}:{port}/governance/policies"
    params = {}
    if status:
        params["status"] = status
    if policy_type:
        params["type"] = policy_type

    try:
        response = httpx.get(url, params=params, timeout=10.0)
        response.raise_for_status()
        data = response.json()

        policies = data.get("policies", [])

        if format == "json":
            typer.echo(json.dumps(data, indent=2))
        else:
            if not policies:
                _echo_info("No policies found")
                return

            typer.echo("")
            typer.echo(typer.style(f"Governance Policies ({data.get('total', 0)})", bold=True))
            typer.echo("-" * 60)

            headers = ["ID", "Name", "Type", "Status", "Resources"]
            rows = [
                [
                    p.get("id", ""),
                    p.get("name", "")[:30],
                    p.get("type", ""),
                    p.get("status", ""),
                    p.get("resources", 0),
                ]
                for p in policies
            ]

            typer.echo(tabulate(rows, headers=headers, tablefmt="simple"))
            typer.echo("")

    except httpx.ConnectError:
        _echo_error(f"Cannot connect to API server")
        raise typer.Exit(1)
    except Exception as e:
        _echo_error(f"Error: {e}")
        raise typer.Exit(1)


@governance_app.command("violations")
def governance_violations(
    severity: Optional[str] = typer.Option(
        None, "--severity", "-s",
        help="Filter by severity (critical, high, medium, low)"
    ),
    status: Optional[str] = typer.Option(
        None, "--status",
        help="Filter by status (open, in_progress, resolved)"
    ),
    host: str = typer.Option(
        "localhost", "--host", "-h",
        help="API server host"
    ),
    port: int = typer.Option(
        8000, "--port", "-p",
        help="API server port"
    ),
    format: str = typer.Option(
        "table", "--format", "-f",
        help="Output format (table, json)"
    ),
):
    """List policy violations.

    Example:
        seeknal atlas governance violations --severity critical
    """
    import httpx
    import json
    from tabulate import tabulate

    url = f"http://{host}:{port}/governance/violations"
    params = {}
    if severity:
        params["severity"] = severity
    if status:
        params["status"] = status

    try:
        response = httpx.get(url, params=params, timeout=10.0)
        response.raise_for_status()
        data = response.json()

        violations = data.get("violations", [])

        if format == "json":
            typer.echo(json.dumps(data, indent=2))
        else:
            if not violations:
                _echo_info("No violations found")
                return

            typer.echo("")
            typer.echo(typer.style(f"Policy Violations ({data.get('total', 0)})", bold=True))
            typer.echo("-" * 70)

            # Color-code severity
            def severity_style(s):
                colors = {
                    "critical": typer.colors.RED,
                    "high": typer.colors.BRIGHT_RED,
                    "medium": typer.colors.YELLOW,
                    "low": typer.colors.GREEN,
                }
                return typer.style(s, fg=colors.get(s, typer.colors.WHITE))

            headers = ["ID", "Severity", "Type", "Resource", "Status"]
            rows = [
                [
                    v.get("id", ""),
                    severity_style(v.get("severity", "")),
                    v.get("type", ""),
                    v.get("resource", "")[:25],
                    v.get("status", ""),
                ]
                for v in violations
            ]

            typer.echo(tabulate(rows, headers=headers, tablefmt="simple"))
            typer.echo("")

    except httpx.ConnectError:
        _echo_error(f"Cannot connect to API server")
        raise typer.Exit(1)
    except Exception as e:
        _echo_error(f"Error: {e}")
        raise typer.Exit(1)


@governance_app.command("access-requests")
def governance_access_requests(
    status: Optional[str] = typer.Option(
        None, "--status", "-s",
        help="Filter by status (pending, approved, denied)"
    ),
    host: str = typer.Option(
        "localhost", "--host", "-h",
        help="API server host"
    ),
    port: int = typer.Option(
        8000, "--port", "-p",
        help="API server port"
    ),
    format: str = typer.Option(
        "table", "--format", "-f",
        help="Output format (table, json)"
    ),
):
    """List access requests.

    Example:
        seeknal atlas governance access-requests --status pending
    """
    import httpx
    import json
    from tabulate import tabulate

    url = f"http://{host}:{port}/governance/access-requests"
    params = {}
    if status:
        params["status"] = status

    try:
        response = httpx.get(url, params=params, timeout=10.0)
        response.raise_for_status()
        data = response.json()

        requests = data.get("requests", [])

        if format == "json":
            typer.echo(json.dumps(data, indent=2))
        else:
            if not requests:
                _echo_info("No access requests found")
                return

            typer.echo("")
            typer.echo(typer.style(f"Access Requests ({data.get('total', 0)})", bold=True))
            typer.echo("-" * 70)

            headers = ["ID", "Requester", "Resource", "Access", "Status"]
            rows = [
                [
                    r.get("id", ""),
                    r.get("requester", "")[:20],
                    r.get("resource", "")[:25],
                    r.get("access_type", ""),
                    r.get("status", ""),
                ]
                for r in requests
            ]

            typer.echo(tabulate(rows, headers=headers, tablefmt="simple"))
            typer.echo("")

    except httpx.ConnectError:
        _echo_error(f"Cannot connect to API server")
        raise typer.Exit(1)
    except Exception as e:
        _echo_error(f"Error: {e}")
        raise typer.Exit(1)


@governance_app.command("audit-logs")
def governance_audit_logs(
    category: Optional[str] = typer.Option(
        None, "--category", "-c",
        help="Filter by category (access, policy, compliance, system)"
    ),
    limit: int = typer.Option(
        20, "--limit", "-l",
        help="Maximum number of events to show"
    ),
    host: str = typer.Option(
        "localhost", "--host", "-h",
        help="API server host"
    ),
    port: int = typer.Option(
        8000, "--port", "-p",
        help="API server port"
    ),
    format: str = typer.Option(
        "table", "--format", "-f",
        help="Output format (table, json)"
    ),
):
    """View audit logs.

    Example:
        seeknal atlas governance audit-logs --category access --limit 10
    """
    import httpx
    import json
    from tabulate import tabulate

    url = f"http://{host}:{port}/governance/audit-logs"
    params = {"limit": limit}
    if category:
        params["category"] = category

    try:
        response = httpx.get(url, params=params, timeout=10.0)
        response.raise_for_status()
        data = response.json()

        events = data.get("events", [])

        if format == "json":
            typer.echo(json.dumps(data, indent=2))
        else:
            if not events:
                _echo_info("No audit events found")
                return

            typer.echo("")
            typer.echo(typer.style(f"Audit Logs ({data.get('total', 0)} total)", bold=True))
            typer.echo("-" * 80)

            headers = ["Timestamp", "User", "Action", "Category"]
            rows = [
                [
                    e.get("timestamp", "")[:16],
                    e.get("user", "")[:20],
                    e.get("action", "")[:30],
                    e.get("category", ""),
                ]
                for e in events
            ]

            typer.echo(tabulate(rows, headers=headers, tablefmt="simple"))
            typer.echo("")

    except httpx.ConnectError:
        _echo_error(f"Cannot connect to API server")
        raise typer.Exit(1)
    except Exception as e:
        _echo_error(f"Error: {e}")
        raise typer.Exit(1)


@governance_app.command("status")
def governance_status():
    """Show whether runtime Atlas governance enforcement is active.

    Enforcement activates when ``ATLAS_API_URL`` is configured. When active, Seeknal
    delegates data-access decisions and column masking to the Atlas backend on every
    governed read — the rules live in Atlas, not in editable local files.

    Example:
        seeknal atlas governance status
    """
    from seeknal.integrations.atlas_governance import create_governance_gate_from_env

    gate = create_governance_gate_from_env()
    typer.echo("")
    typer.echo(typer.style("Atlas Runtime Governance", bold=True))
    typer.echo("=" * 40)
    if gate is None:
        _echo_warning("Inactive — ATLAS_API_URL is not set.")
        typer.echo("Reads pass through ungoverned. Set ATLAS_API_URL to enable enforcement.")
        typer.echo("")
        return
    _echo_success("Active — access decisions delegated to Atlas.")
    typer.echo(f"  Endpoint:  {gate.config.base_url}")
    typer.echo(f"  Token:     {'set' if gate.config.token else '(none)'}")
    typer.echo(f"  Fail mode: {'fail-open' if gate.fail_open else 'fail-closed'}")
    typer.echo("")


@governance_app.command("check")
def governance_check(
    resource: str = typer.Argument(
        ..., help="Resource identifier, e.g. prod.gold.customer"
    ),
    action: str = typer.Option(
        "read", "--action", "-a", help="Action to check (read, write, ...)"
    ),
    actor: Optional[str] = typer.Option(
        None, "--actor", help="Actor (defaults to ATLAS_ACTOR or the current user)"
    ),
):
    """Check the Atlas access decision for a resource (exit 1 if denied).

    Uses the configured Atlas backend (``ATLAS_API_URL``). The exit code is scriptable:
    a non-zero exit means access is denied, so this can gate shell pipelines.

    Example:
        seeknal atlas governance check prod.gold.customer --action read
    """
    from seeknal.integrations.atlas_governance import create_governance_gate_from_env

    gate = create_governance_gate_from_env()
    if gate is None:
        _echo_error("Atlas governance is not configured (set ATLAS_API_URL).")
        raise typer.Exit(2)

    decision = gate.check_access(resource=resource, action=action, actor=actor)
    typer.echo("")
    if decision.allowed:
        _echo_success(f"ALLOW  {action} {resource}")
        if decision.masked_columns:
            typer.echo(f"  Masked columns: {', '.join(decision.masked_columns)}")
        typer.echo(f"  Classification: {decision.classification}")
        typer.echo("")
    else:
        _echo_error(f"DENY   {action} {resource}")
        if decision.reason:
            typer.echo(f"  Reason: {decision.reason}")
        typer.echo("")
        raise typer.Exit(1)


# =============================================================================
# Lineage Commands
# =============================================================================


@lineage_app.command("show")
def lineage_show(
    name: str = typer.Argument(..., help="Feature group or entity name"),
    direction: str = typer.Option(
        "both", "--direction", "-d",
        help="Lineage direction (upstream, downstream, both)"
    ),
    depth: int = typer.Option(
        2, "--depth",
        help="Maximum depth of lineage traversal"
    ),
    host: str = typer.Option(
        "localhost", "--host", "-h",
        help="API server host"
    ),
    port: int = typer.Option(
        8000, "--port", "-p",
        help="API server port"
    ),
    format: str = typer.Option(
        "tree", "--format", "-f",
        help="Output format (tree, json)"
    ),
):
    """Show lineage for a dataset, feature group, or entity.

    Reads DataHub-backed upstream/downstream lineage through the Atlas catalog
    (the same source as ``seeknal dataset lineage``). Requires only
    ``ATLAS_API_URL`` — the optional ``atlas-data-platform`` package is not
    needed.

    Example:
        seeknal atlas lineage show user_features --direction upstream
    """
    import json as _json

    # Reuse the proven resolver/reader from the `dataset` command group so this
    # command is a real DataHub-backed reader, not a placeholder.
    from seeknal.cli.dataset import (
        _require_catalog,
        _resolve_dataset,
        _dataset_urn,
        _lineage_nodes,
        _print_lineage,
    )
    from seeknal.integrations.atlas_client import (
        AtlasAuthError,
        AtlasContractError,
    )

    client = _require_catalog()
    resolved = _resolve_dataset(client, name)
    try:
        data = client.lineage(_dataset_urn(resolved))
    except AtlasAuthError as exc:
        _echo_error(str(exc))
        raise typer.Exit(1)
    except AtlasContractError as exc:
        _echo_error(f"Lineage lookup failed: {exc}")
        raise typer.Exit(1)

    upstream = _lineage_nodes(data.get("upstreamLineage") or data.get("upstream"))
    downstream = _lineage_nodes(
        data.get("downstreamLineage") or data.get("downstream")
    )

    if format == "json":
        typer.echo(
            _json.dumps(
                {
                    "dataset": resolved.fqn,
                    "upstream": upstream,
                    "downstream": downstream,
                },
                indent=2,
            )
        )
        return

    _echo_success(f"Lineage for {resolved.fqn}")
    if direction in ("upstream", "both"):
        _print_lineage("upstream", upstream)
    if direction in ("downstream", "both"):
        _print_lineage("downstream", downstream)
    if not upstream and not downstream:
        _echo_info("No lineage recorded in DataHub for this dataset.")


@lineage_app.command("publish")
def lineage_publish(
    pipeline: str = typer.Argument(..., help="Pipeline or flow name"),
    run_id: Optional[str] = typer.Option(
        None, "--run-id", "-r",
        help="Run ID (auto-generated if not provided)"
    ),
    inputs: Optional[str] = typer.Option(
        None, "--inputs", "-i",
        help="Comma-separated list of input datasets"
    ),
    outputs: Optional[str] = typer.Option(
        None, "--outputs", "-o",
        help="Comma-separated list of output datasets"
    ),
    host: str = typer.Option(
        "localhost", "--host", "-h",
        help="API server host"
    ),
    port: int = typer.Option(
        8000, "--port", "-p",
        help="API server port"
    ),
):
    """Publish lineage to DataHub.

    Records the data flow between input and output datasets
    for a given pipeline execution.

    Example:
        seeknal atlas lineage publish transform_orders \\
            --inputs raw.orders,raw.customers \\
            --outputs clean.orders
    """
    if not inputs or not outputs:
        _echo_error("Both --inputs and --outputs are required")
        raise typer.Exit(1)

    import uuid

    from seeknal.integrations.atlas_client import (
        AtlasContractError,
        create_atlas_contract_client_from_env,
    )

    # Route through the always-available contract client (same
    # /api/contracts/lineage/publish path the apply flow uses) so manual
    # lineage publishing works in a standard install without the optional
    # atlas-data-platform package.
    client = create_atlas_contract_client_from_env()
    if client is None:
        _echo_error("Atlas is not configured (set ATLAS_API_URL).")
        raise typer.Exit(2)

    final_run_id = run_id or str(uuid.uuid4())
    input_list = [i.strip() for i in inputs.split(",") if i.strip()]
    output_list = [o.strip() for o in outputs.split(",") if o.strip()]

    typer.echo(f"Publishing lineage for: {pipeline}")
    typer.echo(f"  Run ID: {final_run_id}")
    typer.echo(f"  Inputs: {input_list}")
    typer.echo(f"  Outputs: {output_list}")
    typer.echo("")

    project = os.getenv("SEEKNAL_PROJECT_NAME") or "seeknal"
    environment = os.getenv("ATLAS_ENVIRONMENT", os.getenv("SEEKNAL_ENV", "dev"))

    def _ref(dataset_name: str) -> dict:
        return {
            "asset_type": "dataset",
            "name": dataset_name,
            "namespace": f"{project}/{environment}/dataset",
            "source_system": "seeknal",
            "source_id": f"dataset:{dataset_name}",
            "metadata": {"pipeline": pipeline, "run_id": final_run_id},
        }

    upstreams = [_ref(name) for name in input_list]
    try:
        for output_name in output_list:
            client.publish_lineage(
                project_name=project,
                environment=environment,
                asset=_ref(output_name),
                upstreams=upstreams,
            )
    except AtlasContractError as exc:
        _echo_error(f"Failed to publish lineage: {exc}")
        raise typer.Exit(1)

    _echo_success("Lineage published successfully")
    typer.echo(f"  Upstream datasets: {len(input_list)}")
    typer.echo(f"  Downstream datasets: {len(output_list)}")


# =============================================================================
# Info Command
# =============================================================================


@atlas_app.command("info")
def atlas_info():
    """Show Atlas integration information.

    Displays version information and configuration status.
    """
    typer.echo("")
    typer.echo(typer.style("Atlas Data Platform Integration", bold=True))
    typer.echo("=" * 40)

    # Check Atlas installation
    if _check_atlas_installed():
        try:
            import atlas.seeknal
            _echo_success("Atlas is installed")

            # Show available modules
            typer.echo("")
            typer.echo("Available modules:")
            modules = [
                ("API Server", "atlas.seeknal.api"),
                ("Governance Tools", "atlas.seeknal.governance_tools"),
                ("Feature Tools", "atlas.seeknal.feature_tools"),
                ("Quality Tools", "atlas.seeknal.quality_tools"),
                ("Platform Tools", "atlas.seeknal.platform_tools"),
            ]
            for name, module in modules:
                try:
                    __import__(module, fromlist=[""])
                    typer.echo(f"  ✓ {name}")
                except ImportError:
                    typer.echo(f"  ✗ {name} (not available)")

            # Show environment configuration
            typer.echo("")
            typer.echo("Configuration (from environment):")
            env_vars = [
                "DATAHUB_GMS_URL",
                "OPENFGA_URL",
                "KEYCLOAK_URL",
                "LAKEKEEPER_URL",
                "CUBE_URL",
            ]
            for var in env_vars:
                value = os.environ.get(var)
                if value:
                    typer.echo(f"  {var}: {value[:30]}...")
                else:
                    typer.echo(typer.style(f"  {var}: (not set)", dim=True))

        except Exception as e:
            _echo_warning(f"Error checking Atlas: {e}")
    else:
        _echo_warning("Atlas is not installed")
        typer.echo("")
        typer.echo("Install with:")
        typer.echo(typer.style("  pip install seeknal[atlas]", fg=typer.colors.CYAN))
        typer.echo("")
        typer.echo("Or install Atlas directly:")
        typer.echo(typer.style("  pip install atlas-data-platform", fg=typer.colors.CYAN))

    typer.echo("")


# Export for registration with main CLI
__all__ = ["atlas_app"]
