"""Atlas Runtime OpenLineage RunEvent builder and emitter for governed runs."""

from __future__ import annotations

import logging
import re
import uuid
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit

import httpx
from sqlglot import exp, parse

from seeknal.dag.sql_parser import SQLDialect, SQLParser

logger = logging.getLogger(__name__)

OPENLINEAGE_SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent"
PRODUCER = "https://github.com/mta-tech/seeknal/runtime"

RUN_NAMESPACE = uuid.UUID("6f1d0c6e-6a63-5c9b-9a3e-2c0c5d3a7a10")

PARENT_RUN_FACET_SCHEMA_URL = "https://openlineage.io/spec/facets/1-0-1/ParentRunFacet.json#/$defs/ParentRunFacet"
JOB_TYPE_JOB_FACET_SCHEMA_URL = "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json#/$defs/JobTypeJobFacet"
SCHEMA_DATASET_FACET_SCHEMA_URL = "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet"
DATASET_VERSION_DATASET_FACET_SCHEMA_URL = "https://openlineage.io/spec/facets/1-0-1/DatasetVersionDatasetFacet.json#/$defs/DatasetVersionDatasetFacet"
COLUMN_LINEAGE_DATASET_FACET_SCHEMA_URL = "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet"
ERROR_MESSAGE_RUN_FACET_SCHEMA_URL = "https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json#/$defs/ErrorMessageRunFacet"

CREDENTIAL_REDACT_PATTERN = re.compile(r"(?i)(password|secret|token)=\S+")


def _extract_host(lakekeeper_uri: str) -> str:
    """Extract host/authority from lakekeeper URI with userinfo stripped and host lowercased."""
    uri = lakekeeper_uri.strip()
    if "://" not in uri:
        uri = f"http://{uri}"
    split = urlsplit(uri)
    netloc = split.netloc or split.path.strip("/")
    if "@" in netloc:
        netloc = netloc.split("@", 1)[1]
    if ":" in netloc:
        host_part, port_part = netloc.rsplit(":", 1)
        return f"{host_part.lower()}:{port_part}"
    return netloc.lower()


def dataset_for_table(table: str, *, lakekeeper_uri: str) -> dict[str, str]:
    """Return OpenLineage dataset identifier for a 3-part Iceberg table.

    Format:
        {"namespace": "iceberg://<host>/<warehouse-or-catalog>", "name": "<namespace>.<table>"}

    Raises:
        ValueError: If table is not a 3-part 'catalog.namespace.table' name.
    """
    if not isinstance(table, str):
        raise ValueError(f"Table must be a string, got {type(table).__name__}")

    parts = table.strip().split(".")
    if len(parts) != 3 or not all(parts):
        raise ValueError(
            f"Table must be 3-part format 'catalog.namespace.table', got '{table}'"
        )

    catalog, namespace, table_name = parts
    host = _extract_host(lakekeeper_uri)
    return {
        "namespace": f"iceberg://{host}/{catalog}",
        "name": f"{namespace}.{table_name}",
    }


def _redact_and_truncate_error(error: Any) -> str:
    """Sanitize error message by redacting credentials and truncating to 2000 chars."""
    if error is None:
        return ""
    msg = str(error)
    redacted = CREDENTIAL_REDACT_PATTERN.sub(r"\1=[REDACTED]", msg)
    if len(redacted) > 2000:
        redacted = redacted[:2000]
    return redacted


def build_node_event(
    event_type: str,
    *,
    grant: Any,
    node: Any,
    project: str,
    lakekeeper_uri: str,
    event_time: Any | None = None,
    schema: list[dict[str, Any]] | dict[str, str] | None = None,
    column_lineage: Mapping[str, list[tuple[str, str]]] | None = None,
    snapshot_ids: Mapping[str, Any] | None = None,
    error: Any | None = None,
) -> dict[str, Any]:
    """Build an OpenLineage RunEvent for a DAG node.

    Args:
        event_type: "START", "COMPLETE", or "FAIL"
        grant: RunGrant instance or mapping with ws and jti
        node: PlanNode instance or mapping with id, kind, reads, writes
        project: Project name string
        lakekeeper_uri: REST catalog URI string
        event_time: Optional datetime or ISO-8601 string
        schema: Optional schema fields for output dataset
        column_lineage: Optional mapping of output_column -> [(input_table, input_column), ...]
        snapshot_ids: Optional mapping of table -> snapshot_id
        error: Optional error object or message for FAIL events

    Returns:
        Canonical OpenLineage 1.x RunEvent dictionary.
    """
    event_type_str = str(event_type).upper()
    if event_type_str not in ("START", "COMPLETE", "FAIL"):
        raise ValueError(f"Invalid eventType '{event_type}', expected START, COMPLETE, or FAIL")

    if event_time is None:
        event_time_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    elif isinstance(event_time, datetime):
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)
        event_time_str = event_time.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    else:
        event_time_str = str(event_time)
        if not event_time_str.endswith("Z") and not ("+" in event_time_str or "-" in event_time_str[10:]):
            event_time_str += "Z"

    grant_jti = grant.jti if hasattr(grant, "jti") else grant["jti"]
    grant_ws = grant.ws if hasattr(grant, "ws") else grant["ws"]

    node_id = node.id if hasattr(node, "id") else node["id"]
    raw_kind = getattr(node, "kind", getattr(node, "get", lambda k, d=None: d)("kind", "transform"))
    node_kind = raw_kind.value if hasattr(raw_kind, "value") else str(raw_kind)

    # Deterministic UUID v5 from (grant.jti, node.id)
    run_id = str(uuid.uuid5(RUN_NAMESPACE, f"{grant_jti}:{node_id}"))

    run_facets: dict[str, Any] = {
        "parent": {
            "_producer": PRODUCER,
            "_schemaURL": PARENT_RUN_FACET_SCHEMA_URL,
            "run": {
                "runId": grant_jti,
            },
            "job": {
                "namespace": f"atlas://{grant_ws}/{project}",
                "name": project,
            },
        }
    }

    if event_type_str == "FAIL":
        run_facets["errorMessage"] = {
            "_producer": PRODUCER,
            "_schemaURL": ERROR_MESSAGE_RUN_FACET_SCHEMA_URL,
            "message": _redact_and_truncate_error(error),
            "programmingLanguage": "python",
        }

    job_facets: dict[str, Any] = {
        "jobType": {
            "_producer": PRODUCER,
            "_schemaURL": JOB_TYPE_JOB_FACET_SCHEMA_URL,
            "processingType": "BATCH",
            "integration": "SEEKNAL",
            "jobType": node_kind,
        }
    }

    # Inputs from node.reads
    reads = node.reads if hasattr(node, "reads") else node.get("reads", ())
    inputs: list[dict[str, Any]] = []
    for tbl in reads:
        ds = dict(dataset_for_table(tbl, lakekeeper_uri=lakekeeper_uri))
        ds_facets: dict[str, Any] = {}
        if snapshot_ids and tbl in snapshot_ids:
            ds_facets["version"] = {
                "_producer": PRODUCER,
                "_schemaURL": DATASET_VERSION_DATASET_FACET_SCHEMA_URL,
                "datasetVersion": str(snapshot_ids[tbl]),
            }
        if ds_facets:
            ds["facets"] = ds_facets
        inputs.append(ds)

    # Outputs from node.writes
    writes = node.writes if hasattr(node, "writes") else node.get("writes", ())
    outputs: list[dict[str, Any]] = []
    for tbl in writes:
        ds = dict(dataset_for_table(tbl, lakekeeper_uri=lakekeeper_uri))
        ds_facets = {}

        if schema is not None:
            if isinstance(schema, list):
                fields = schema
            elif isinstance(schema, dict):
                fields = [{"name": k, "type": v} for k, v in schema.items()]
            else:
                fields = []
            ds_facets["schema"] = {
                "_producer": PRODUCER,
                "_schemaURL": SCHEMA_DATASET_FACET_SCHEMA_URL,
                "fields": fields,
            }

        if snapshot_ids and tbl in snapshot_ids:
            ds_facets["version"] = {
                "_producer": PRODUCER,
                "_schemaURL": DATASET_VERSION_DATASET_FACET_SCHEMA_URL,
                "datasetVersion": str(snapshot_ids[tbl]),
            }

        if column_lineage:
            fields_col_lineage: dict[str, Any] = {}
            for out_col, in_col_list in column_lineage.items():
                in_fields = []
                for in_tbl, in_col in in_col_list:
                    in_ds = dataset_for_table(in_tbl, lakekeeper_uri=lakekeeper_uri)
                    in_fields.append({
                        "namespace": in_ds["namespace"],
                        "name": in_ds["name"],
                        "field": in_col,
                    })
                fields_col_lineage[out_col] = {
                    "inputFields": in_fields,
                }
            ds_facets["columnLineage"] = {
                "_producer": PRODUCER,
                "_schemaURL": COLUMN_LINEAGE_DATASET_FACET_SCHEMA_URL,
                "fields": fields_col_lineage,
            }

        if ds_facets:
            ds["facets"] = ds_facets
        outputs.append(ds)

    return {
        "eventType": event_type_str,
        "eventTime": event_time_str,
        "run": {
            "runId": run_id,
            "facets": run_facets,
        },
        "job": {
            "namespace": f"atlas://{grant_ws}/{project}",
            "name": node_id,
            "facets": job_facets,
        },
        "inputs": inputs,
        "outputs": outputs,
        "producer": PRODUCER,
        "schemaURL": OPENLINEAGE_SCHEMA_URL,
    }


def column_lineage_for_sql(
    sql: str,
    *,
    input_tables: Mapping[str, str],
    dialect: str = "duckdb",
) -> dict[str, list[tuple[str, str]]]:
    """Derive column-level lineage from SQL using SQLGlot AST analysis.

    Maps table and alias references to plan tables using `input_tables`.

    Args:
        sql: SQL query string.
        input_tables: Mapping of table name or alias -> 3-part plan table.
        dialect: SQL dialect for parsing (default: "duckdb").

    Returns:
        Mapping of output_column -> [(plan_table, input_column), ...].
        Returns {} when lineage cannot be derived or SQL is invalid.
    """
    if not sql or not isinstance(sql, str) or not sql.strip():
        return {}

    try:
        # Resolve dialect enum
        try:
            dialect_enum = SQLDialect(dialect.lower())
        except Exception:
            dialect_enum = SQLDialect.GENERIC

        parser = SQLParser(dialect=dialect_enum)
        parse_result = parser.parse(sql)
        if not parse_result.is_valid:
            return {}

        parsed = parse(sql, read=parser.read)
        if not parsed:
            return {}

        statement = parsed[0] if isinstance(parsed, list) else parsed
        if not isinstance(statement, exp.Select):
            return {}

        # Build alias -> table name mapping from the AST
        alias_to_table: dict[str, str] = {}
        tables_in_query: list[str] = []
        for tbl in statement.find_all(exp.Table):
            tname = tbl.name
            talias = tbl.alias
            if tname:
                tables_in_query.append(tname)
                alias_to_table[tname] = tname
                alias_to_table[tname.lower()] = tname
            if talias:
                alias_to_table[talias] = tname
                alias_to_table[talias.lower()] = tname

        # Case-insensitive view of input_tables
        input_tables_lower = {k.lower(): v for k, v in input_tables.items()}

        def resolve_table(tbl_ref: str | None) -> str | None:
            if not tbl_ref:
                if len(input_tables) == 1:
                    return next(iter(input_tables.values()))
                if len(tables_in_query) == 1:
                    t = tables_in_query[0]
                    return input_tables.get(t) or input_tables_lower.get(t.lower())
                return None

            # 1. Direct match in input_tables
            if tbl_ref in input_tables:
                return input_tables[tbl_ref]
            if tbl_ref.lower() in input_tables_lower:
                return input_tables_lower[tbl_ref.lower()]

            # 2. Look up base table via alias
            base_tbl = alias_to_table.get(tbl_ref) or alias_to_table.get(tbl_ref.lower())
            if base_tbl:
                if base_tbl in input_tables:
                    return input_tables[base_tbl]
                if base_tbl.lower() in input_tables_lower:
                    return input_tables_lower[base_tbl.lower()]

            return None

        result: dict[str, list[tuple[str, str]]] = {}
        for projection in statement.expressions:
            if isinstance(projection, exp.Column):
                out_name = projection.alias or projection.name
            elif isinstance(projection, exp.Alias):
                out_name = projection.alias
            elif hasattr(projection, "alias") and projection.alias:
                out_name = projection.alias
            else:
                continue

            col_deps: list[tuple[str, str]] = []
            for col in projection.find_all(exp.Column):
                tbl_ref = col.table
                if hasattr(tbl_ref, "name"):
                    tbl_ref_str: str | None = tbl_ref.name
                elif tbl_ref:
                    tbl_ref_str = str(tbl_ref)
                else:
                    tbl_ref_str = None

                col_name = col.name
                resolved = resolve_table(tbl_ref_str)
                if resolved and col_name:
                    pair = (resolved, col_name)
                    if pair not in col_deps:
                        col_deps.append(pair)

            if col_deps:
                result[out_name] = col_deps

        return result

    except Exception as exc:
        logger.debug(f"Failed to derive column lineage from SQL: {exc}")
        return {}


class LineageEmitter:
    """Best-effort OpenLineage HTTP emitter with bounded in-memory buffering."""

    def __init__(
        self,
        base_url: str,
        grant_token: str,
        *,
        timeout_seconds: float = 5,
        transport: httpx.BaseTransport | None = None,
        max_buffer: int = 1000,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.grant_token = grant_token
        self.timeout_seconds = timeout_seconds
        self.transport = transport
        self.max_buffer = max_buffer

        self._buffer: list[dict[str, Any]] = []
        self._sent: int = 0
        self._dropped: int = 0
        self._failed_attempts: int = 0

    def _sanitize(self, message: str) -> str:
        """Strip sensitive token from error/log strings."""
        if self.grant_token and self.grant_token in message:
            return message.replace(self.grant_token, "[REDACTED]")
        return message

    def _send(self, event: dict[str, Any]) -> bool:
        """Send a single RunEvent payload to the Catalog lineage endpoint."""
        url = f"{self.base_url}/api/runtime/v1/lineage"
        headers = {
            "Authorization": f"Bearer {self.grant_token}",
            "Content-Type": "application/json",
        }
        try:
            with httpx.Client(transport=self.transport, timeout=self.timeout_seconds) as client:
                response = client.post(url, headers=headers, json=event)
                if 200 <= response.status_code < 300:
                    return True
                logger.warning(
                    "Catalog lineage endpoint returned status %s: %s",
                    response.status_code,
                    self._sanitize(response.text[:200]),
                )
                return False
        except Exception as exc:
            logger.warning(
                "Network or client error sending lineage event: %s (%s)",
                type(exc).__name__,
                self._sanitize(str(exc)),
            )
            return False

    def _buffer_event(self, event: dict[str, Any]) -> None:
        """Buffer event, dropping oldest if buffer capacity is exceeded."""
        if self.max_buffer <= 0:
            self._dropped += 1
            return

        if len(self._buffer) >= self.max_buffer:
            self._buffer.pop(0)
            self._dropped += 1
        self._buffer.append(event)

    def emit(self, event: dict[str, Any]) -> None:
        """Emit a RunEvent.

        Attempts delivery immediately; on non-2xx or network error, buffers the event.
        Never raises exceptions out of emit.
        """
        try:
            success = self._send(event)
            if success:
                self._sent += 1
            else:
                self._failed_attempts += 1
                self._buffer_event(event)
        except Exception as exc:
            self._failed_attempts += 1
            logger.warning(
                "Unexpected error during lineage emit: %s (%s)",
                type(exc).__name__,
                self._sanitize(str(exc)),
            )
            self._buffer_event(event)

    def flush(self) -> None:
        """Retry buffered events in order, stopping at the first failure."""
        while self._buffer:
            event = self._buffer[0]
            try:
                success = self._send(event)
                if success:
                    self._buffer.pop(0)
                    self._sent += 1
                else:
                    self._failed_attempts += 1
                    break
            except Exception as exc:
                self._failed_attempts += 1
                logger.warning(
                    "Unexpected error during lineage buffer flush: %s (%s)",
                    type(exc).__name__,
                    self._sanitize(str(exc)),
                )
                break

    def stats(self) -> dict[str, int]:
        """Return delivery statistics."""
        return {
            "sent": self._sent,
            "buffered": len(self._buffer),
            "dropped": self._dropped,
            "failed_attempts": self._failed_attempts,
        }
