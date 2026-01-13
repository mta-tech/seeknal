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


def _echo_error(message: str):
    """Print error message in red."""
    typer.echo(typer.style(f"✗ {message}", fg=typer.colors.RED))


def _echo_success(message: str):
    """Print success message in green."""
    typer.echo(typer.style(f"✓ {message}", fg=typer.colors.GREEN))


def _echo_info(message: str):
    """Print info message in blue."""
    typer.echo(typer.style(f"ℹ {message}", fg=typer.colors.BLUE))


def _echo_warning(message: str):
    """Print warning message in yellow."""
    typer.echo(typer.style(f"⚠ {message}", fg=typer.colors.YELLOW))


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
    """Show lineage for a feature group or entity.

    Displays upstream and downstream dependencies.

    Example:
        seeknal atlas lineage show user_features --direction upstream
    """
    _require_atlas()

    typer.echo(f"Fetching lineage for: {name}")
    typer.echo(f"  Direction: {direction}")
    typer.echo(f"  Depth: {depth}")
    typer.echo("")

    # Note: In a full implementation, this would call the DataHub lineage API
    # For now, we provide a placeholder
    _echo_info("Lineage visualization requires a running Atlas API and DataHub")
    typer.echo("")
    typer.echo("To enable lineage:")
    typer.echo("  1. Start the Atlas API: seeknal atlas api start")
    typer.echo("  2. Configure DATAHUB_GMS_URL environment variable")
    typer.echo("  3. Publish lineage: seeknal atlas lineage publish")


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
    _require_atlas()

    import uuid

    if not inputs or not outputs:
        _echo_error("Both --inputs and --outputs are required")
        raise typer.Exit(1)

    final_run_id = run_id or str(uuid.uuid4())
    input_list = [i.strip() for i in inputs.split(",")]
    output_list = [o.strip() for o in outputs.split(",")]

    typer.echo(f"Publishing lineage for: {pipeline}")
    typer.echo(f"  Run ID: {final_run_id}")
    typer.echo(f"  Inputs: {input_list}")
    typer.echo(f"  Outputs: {output_list}")
    typer.echo("")

    try:
        from atlas.seeknal.governance_tools import (
            create_lineage_publishing_tool,
            create_governance_tools_config,
        )

        config = create_governance_tools_config()
        tool = create_lineage_publishing_tool(config)

        # Convert to URNs (simplified)
        def to_urn(name: str) -> str:
            return f"urn:li:dataset:(urn:li:dataPlatform:iceberg,{name},PROD)"

        input_urns = [to_urn(i) for i in input_list]
        output_urns = [to_urn(o) for o in output_list]

        result = tool.publish_lineage(
            pipeline_name=pipeline,
            run_id=final_run_id,
            inputs=input_urns,
            outputs=output_urns,
        )

        if result.success:
            _echo_success("Lineage published successfully")
            typer.echo(f"  Upstream datasets: {result.upstream_count}")
            typer.echo(f"  Downstream datasets: {result.downstream_count}")
            typer.echo(f"  Latency: {result.latency_ms:.1f}ms")
        else:
            _echo_error(f"Failed to publish lineage: {result.error_message}")
            raise typer.Exit(1)

    except ImportError:
        _echo_warning("DataHub client not fully configured")
        typer.echo("")
        typer.echo("Lineage publishing requires:")
        typer.echo("  - DATAHUB_GMS_URL environment variable")
        typer.echo("  - DATAHUB_TOKEN environment variable (optional)")
    except Exception as e:
        _echo_error(f"Error publishing lineage: {e}")
        raise typer.Exit(1)


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
