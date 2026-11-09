"""Atlas Runtime governed CatalogIO and GovernedContext."""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from seeknal.serving.server.config import (
    ServerConfigError,
    check_file_not_group_world_readable,
    parse_jwks,
)
from seeknal.workflow.materialization.operations import _qi, _sql_str

from .controller_client import (
    NodeNotAuthorized,
    RunControllerClient,
    RunControllerError,
    RunControllerUnavailable,
    RunRevoked,
)
from .run_grant import (
    RunGrant,
    RunGrantError,
    require_plan_covered,
    verify_run_grant,
)
from .run_plan import PlanNode, RunPlan, build_run_plan

logger = logging.getLogger(__name__)

STATIC_CREDENTIAL_VARS = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "KEYCLOAK_CLIENT_SECRET",
    "LAKEKEEPER_TOKEN",
)

REQUIRED_GOVERNED_VARS = (
    "ATLAS_RUN_GRANT_FILE",
    "ATLAS_RUN_GRANT_JWKS_FILE",
    "ATLAS_LAKEKEEPER_TOKEN_FILE",
    "ATLAS_LAKEKEEPER_URI",
    "ATLAS_LAKEKEEPER_WAREHOUSE",
    "ATLAS_RUN_CONTROLLER_URL",
    "ATLAS_CONNECTIONS_DIR",
)


class GovernedConfigError(RunGrantError, ServerConfigError):
    """Raised when governed mode configuration, permissions, or grant validation fails."""


class NotInRunGrant(RunGrantError):
    """Raised when a table or connection action is not authorized by the run grant."""


def _catalog_endpoint(uri: str) -> str:
    """Normalize catalog URI to endpoint ending in /catalog."""
    base = (uri or "").rstrip("/")
    if not base.endswith("/catalog"):
        return f"{base}/catalog"
    return base


@dataclass
class GovernedContext:
    """Loaded and validated context for an Atlas Runtime governed job."""

    grant: RunGrant
    grant_file: str
    grant_token: str
    jwks_file: str
    public_keys: dict[str, str]
    lakekeeper_token_file: str
    lakekeeper_uri: str
    lakekeeper_warehouse: str
    run_controller_url: str
    connections_dir: str
    controller_client: RunControllerClient
    plan: RunPlan | None = None
    project: str | None = None

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        project_path: str | Path | None = None,
    ) -> GovernedContext:
        """Load and validate governed runtime settings from environment."""
        if env is None:
            env = os.environ

        mode = env.get("ATLAS_RUNTIME_MODE")
        if mode != "governed":
            raise GovernedConfigError(
                f"Governed mode requires ATLAS_RUNTIME_MODE='governed', got '{mode}'"
            )

        # Refuse static credentials
        for var in STATIC_CREDENTIAL_VARS:
            if var in env and env[var] is not None:
                raise GovernedConfigError(
                    f"Static credential '{var}' is set; governed mode prohibits static credentials"
                )

        # Require all settings
        for var in REQUIRED_GOVERNED_VARS:
            val = env.get(var)
            if not val or not str(val).strip():
                raise GovernedConfigError(
                    f"Missing required environment variable '{var}' for governed mode"
                )

        grant_file = str(env["ATLAS_RUN_GRANT_FILE"]).strip()
        if not os.path.exists(grant_file):
            raise GovernedConfigError(f"Run grant file not found: '{grant_file}'")
        try:
            check_file_not_group_world_readable(grant_file, "Run grant file")
        except ServerConfigError as exc:
            raise GovernedConfigError(str(exc)) from exc
        grant_token = Path(grant_file).read_text(encoding="utf-8").strip()

        jwks_file = str(env["ATLAS_RUN_GRANT_JWKS_FILE"]).strip()
        if not os.path.exists(jwks_file):
            raise GovernedConfigError(f"JWKS file not found: '{jwks_file}'")
        try:
            check_file_not_group_world_readable(jwks_file, "JWKS file")
        except ServerConfigError as exc:
            raise GovernedConfigError(str(exc)) from exc

        token_file = str(env["ATLAS_LAKEKEEPER_TOKEN_FILE"]).strip()
        if not os.path.exists(token_file):
            raise GovernedConfigError(f"Lakekeeper token file not found: '{token_file}'")
        try:
            check_file_not_group_world_readable(token_file, "Lakekeeper token file")
        except ServerConfigError as exc:
            raise GovernedConfigError(str(exc)) from exc

        connections_dir = str(env["ATLAS_CONNECTIONS_DIR"]).strip()
        if not os.path.isdir(connections_dir):
            raise GovernedConfigError(f"Connections directory not found: '{connections_dir}'")
        try:
            for item in os.listdir(connections_dir):
                if item.endswith(".json"):
                    item_path = os.path.join(connections_dir, item)
                    check_file_not_group_world_readable(item_path, f"Connection file '{item}'")
        except ServerConfigError as exc:
            raise GovernedConfigError(str(exc)) from exc

        try:
            jwks_raw = json.loads(Path(jwks_file).read_text(encoding="utf-8"))
            public_keys = parse_jwks(jwks_raw)
        except Exception as exc:
            raise GovernedConfigError(f"Failed to parse JWKS from '{jwks_file}': {exc}") from exc

        try:
            grant = verify_run_grant(grant_token, public_keys)
        except Exception as exc:
            raise GovernedConfigError(f"Run grant verification failed: {exc}") from exc

        controller_url = str(env["ATLAS_RUN_CONTROLLER_URL"]).strip()
        controller_client = RunControllerClient(base_url=controller_url, grant_token=grant_token)

        plan = None
        if project_path is not None:
            try:
                env_name = env.get("SEEKNAL_ENVIRONMENT") or env.get("ATLAS_ENVIRONMENT")
                plan = build_run_plan(project_path, env=env_name)
            except Exception as exc:
                logger.debug("Failed to build run plan from %s: %s", project_path, exc)

        project = None
        if env.get("SEEKNAL_PROJECT_NAME"):
            project = str(env["SEEKNAL_PROJECT_NAME"]).strip()
        elif env.get("ATLAS_PROJECT"):
            project = str(env["ATLAS_PROJECT"]).strip()
        elif project_path is not None:
            project = Path(project_path).name

        return cls(
            grant=grant,
            grant_file=grant_file,
            grant_token=grant_token,
            jwks_file=jwks_file,
            public_keys=public_keys,
            lakekeeper_token_file=token_file,
            lakekeeper_uri=str(env["ATLAS_LAKEKEEPER_URI"]).strip(),
            lakekeeper_warehouse=str(env["ATLAS_LAKEKEEPER_WAREHOUSE"]).strip(),
            run_controller_url=controller_url,
            connections_dir=connections_dir,
            controller_client=controller_client,
            plan=plan,
            project=project,
        )


class CatalogIO:
    """Governed I/O choke point for DuckDB Iceberg, PostgreSQL, and node execution."""

    ctx: GovernedContext

    def __init__(self, ctx: GovernedContext, project: str | None = None) -> None:
        self.ctx = ctx
        self.project = project or getattr(ctx, "project", None)
        self._attached: dict[tuple[int, str], str] = {}
        self._lineage_emitter: Any | None = None

    @property
    def lineage_emitter(self) -> Any:
        """One LineageEmitter per process, created lazily."""
        global _PROCESS_LINEAGE_EMITTER
        if self._lineage_emitter is not None:
            return self._lineage_emitter
        if _PROCESS_LINEAGE_EMITTER is None:
            from seeknal.runtime.lineage import LineageEmitter

            _PROCESS_LINEAGE_EMITTER = LineageEmitter(
                base_url=self.ctx.run_controller_url,
                grant_token=self.ctx.grant_token,
            )
        return _PROCESS_LINEAGE_EMITTER

    @lineage_emitter.setter
    def lineage_emitter(self, emitter: Any) -> None:
        global _PROCESS_LINEAGE_EMITTER
        self._lineage_emitter = emitter
        _PROCESS_LINEAGE_EMITTER = emitter

    def _read_token(self) -> str:
        token_path = self.ctx.lakekeeper_token_file
        if not os.path.exists(token_path):
            raise GovernedConfigError(f"Lakekeeper token file not found: '{token_path}'")
        try:
            check_file_not_group_world_readable(token_path, "Lakekeeper token file")
        except ServerConfigError as exc:
            raise GovernedConfigError(str(exc)) from exc
        return Path(token_path).read_text(encoding="utf-8").strip()

    def attach_iceberg(self, con: Any, alias: str) -> None:
        """Load extensions and ATTACH Iceberg catalog using vended credentials.

        Re-attaches automatically if the Lakekeeper token in the token file changed.
        Never sets static S3 credentials.
        """
        token = self._read_token()
        key = (id(con), alias)
        last_token = self._attached.get(key)

        if last_token == token:
            return

        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        con.execute("INSTALL iceberg")
        con.execute("LOAD iceberg")

        endpoint = _catalog_endpoint(self.ctx.lakekeeper_uri)
        warehouse = self.ctx.lakekeeper_warehouse
        quoted_alias = _qi(alias)

        if key in self._attached:
            try:
                con.execute(f"DETACH {quoted_alias}")
            except Exception:
                pass

        sql = (
            f"ATTACH '{_sql_str(warehouse)}' AS {quoted_alias} ("
            f"TYPE ICEBERG, "
            f"ENDPOINT '{_sql_str(endpoint)}', "
            f"AUTHORIZATION_TYPE 'oauth2', "
            f"TOKEN '{_sql_str(token)}', "
            f"ACCESS_DELEGATION_MODE 'vended_credentials'"
            f")"
        )
        try:
            con.execute(sql)
        except Exception as exc:
            if "already exists" in str(exc).lower():
                try:
                    con.execute(f"DETACH {quoted_alias}")
                    con.execute(sql)
                except Exception:
                    raise exc
            else:
                raise
        self._attached[key] = token

    def check_read(self, table: str) -> None:
        """Ensure 3-part table is authorized for reading in the run grant."""
        norm_table = str(table).strip().lower()
        allowed = {r.table.lower() for r in self.ctx.grant.reads}
        if norm_table not in allowed:
            raise NotInRunGrant(
                f"Table '{table}' is not in run grant reads: allowed={sorted(allowed)}"
            )

    def check_write(self, table: str) -> None:
        """Ensure 3-part table is authorized for writing in the run grant."""
        norm_table = str(table).strip().lower()
        allowed = {w.table.lower() for w in self.ctx.grant.writes}
        if norm_table not in allowed:
            raise NotInRunGrant(
                f"Table '{table}' is not in run grant writes: allowed={sorted(allowed)}"
            )

    def connection(self, name: str, action: str) -> dict[str, Any]:
        """Return parsed connection dictionary from ATLAS_CONNECTIONS_DIR."""
        action_lower = action.strip().lower()
        authorized = False
        for c in self.ctx.grant.connections:
            if c.name == name and action_lower in [act.lower() for act in c.actions]:
                authorized = True
                break

        if not authorized:
            raise NotInRunGrant(
                f"Connection '{name}' with action '{action}' is not authorized in run grant"
            )

        conn_file = Path(self.ctx.connections_dir) / f"{name}.json"
        if not conn_file.exists():
            raise GovernedConfigError(f"Connection file not found: '{conn_file}'")

        try:
            check_file_not_group_world_readable(str(conn_file), f"Connection file for '{name}'")
        except ServerConfigError as exc:
            raise GovernedConfigError(str(exc)) from exc

        try:
            return json.loads(conn_file.read_text(encoding="utf-8"))
        except Exception as exc:
            raise GovernedConfigError(f"Failed to parse connection JSON '{conn_file}': {exc}") from exc

    def node_start(self, node_id: str | PlanNode) -> None:
        """Notify controller of node start; raises on revocation or denial."""
        if isinstance(node_id, PlanNode):
            plan_node = node_id
        else:
            nid = str(node_id)
            plan_node = None
            if self.ctx.plan is not None:
                for n in self.ctx.plan.nodes:
                    if n.id == nid:
                        plan_node = n
                        break
            if plan_node is None:
                plan_node = PlanNode(
                    id=nid,
                    kind="node",
                    reads=(),
                    writes=(),
                    connections=(),
                )

        self.ctx.controller_client.node_start(plan_node)

    def emit_node_event(
        self,
        event_type: str,
        node_id: str | PlanNode,
        *,
        error: Any = None,
        schema: Any = None,
        snapshot_ids: Any = None,
        sql: str | None = None,
        input_refs: Any = None,
    ) -> None:
        """Emit OpenLineage RunEvent for a node lifecycle transition.

        Args:
            event_type: "START", "COMPLETE", or "FAIL"
            node_id: Node identifier string or PlanNode instance
            error: Optional error for FAIL events
            schema: Optional output schema
            snapshot_ids: Optional mapping of table -> snapshot_id
            sql: Optional SQL query for column lineage derivation
            input_refs: Optional mapping of input ref name -> target node/table
        """
        try:
            # 1. Resolve PlanNode
            if isinstance(node_id, PlanNode):
                plan_node = node_id
                nid = plan_node.id
            else:
                nid = str(node_id)
                plan_node = None
                if self.ctx.plan is not None:
                    for n in self.ctx.plan.nodes:
                        if n.id == nid:
                            plan_node = n
                            break

                if plan_node is None:
                    plan_node = PlanNode(
                        id=nid,
                        kind="transform" if "transform" in nid else "node",
                        reads=(),
                        writes=(),
                        connections=(),
                    )

            # 2. Derive column lineage if SQL is given
            col_lineage = None
            if sql and isinstance(sql, str) and sql.strip():
                # Map node's input refs to plan's read tables where node reads Iceberg directly; otherwise pass {}
                input_tables: dict[str, str] = {}
                if plan_node.reads:
                    for tbl in plan_node.reads:
                        parts = tbl.split(".")
                        if len(parts) == 3:
                            input_tables[parts[2]] = tbl
                            input_tables[f"{parts[1]}.{parts[2]}"] = tbl
                        input_tables[tbl] = tbl

                    if isinstance(input_refs, dict):
                        for ref_name, ref_val in input_refs.items():
                            ref_str = str(ref_val)
                            for tbl in plan_node.reads:
                                if ref_name.lower() in tbl.lower() or ref_str.lower() in tbl.lower():
                                    input_tables[ref_name] = tbl

                if input_tables:
                    from seeknal.runtime.lineage import column_lineage_for_sql

                    col_lineage = column_lineage_for_sql(sql, input_tables=input_tables)
                    if not col_lineage:
                        col_lineage = None

            # 3. Project name
            project = (
                self.project
                or getattr(self.ctx, "project", None)
                or getattr(self.ctx.plan, "project", None)
                or os.getenv("SEEKNAL_PROJECT_NAME")
                or os.getenv("ATLAS_PROJECT")
                or "default"
            )

            # 4. Build and emit event
            from seeknal.runtime.lineage import build_node_event

            event = build_node_event(
                event_type,
                grant=self.ctx.grant,
                node=plan_node,
                project=project,
                lakekeeper_uri=self.ctx.lakekeeper_uri,
                schema=schema,
                column_lineage=col_lineage,
                snapshot_ids=snapshot_ids,
                error=error,
            )

            self.lineage_emitter.emit(event)

        except Exception as exc:
            logger.warning("Failed to emit lineage event for node %s (%s): %s", node_id, event_type, exc)


_CATALOG_IO: CatalogIO | None = None
_PROCESS_LINEAGE_EMITTER: Any | None = None


def get_catalog_io(project_path: str | Path | None = None) -> CatalogIO | None:
    """Process-wide accessor returning CatalogIO in governed mode or None."""
    global _CATALOG_IO
    if _CATALOG_IO is not None:
        ctx = getattr(_CATALOG_IO, "ctx", None)
        if project_path is not None and ctx is not None and getattr(ctx, "plan", None) is None:
            try:
                env_name = os.getenv("SEEKNAL_ENVIRONMENT") or os.getenv("ATLAS_ENVIRONMENT")
                ctx.plan = build_run_plan(project_path, env=env_name)
            except Exception as exc:
                logger.debug("Failed to build run plan from %s: %s", project_path, exc)
        return _CATALOG_IO
    if os.getenv("ATLAS_RUNTIME_MODE") == "governed":
        ctx = GovernedContext.from_env(project_path=project_path)
        _CATALOG_IO = CatalogIO(ctx)
        return _CATALOG_IO
    return None


def set_catalog_io(cio: CatalogIO | None) -> None:
    """Set process-wide CatalogIO (used by admit_run or tests)."""
    global _CATALOG_IO, _PROCESS_LINEAGE_EMITTER
    _CATALOG_IO = cio
    if cio is None:
        _PROCESS_LINEAGE_EMITTER = None


def admit_run(
    project_path: str | Path,
    env: Mapping[str, str] | str | None = None,
) -> CatalogIO | None:
    """Admission helper: build plan, verify grant, require plan coverage."""
    if isinstance(env, str):
        env_dict = os.environ
        env_name = env
    elif isinstance(env, Mapping):
        env_dict = env
        env_name = env.get("SEEKNAL_ENVIRONMENT") or env.get("ATLAS_ENVIRONMENT")
    else:
        env_dict = os.environ
        env_name = os.getenv("SEEKNAL_ENVIRONMENT") or os.getenv("ATLAS_ENVIRONMENT")

    ctx = GovernedContext.from_env(env_dict)
    plan = build_run_plan(project_path, env=env_name)
    require_plan_covered(ctx.grant, plan)
    ctx.plan = plan
    ctx.project = Path(project_path).name
    cio = CatalogIO(ctx, project=ctx.project)
    set_catalog_io(cio)
    return cio
