from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


def get_current_snapshot_id(
    table_ref: str,
    params: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """Query the current snapshot ID for an Iceberg table via REST catalog API.

    Args:
        table_ref: 3-part Iceberg table name (catalog.namespace.table)
        params: Optional connection params (catalog_uri, warehouse)

    Returns:
        Tuple of (snapshot_id, snapshot_timestamp) as strings,
        or (None, None) if unreachable or error.
    """
    try:
        parts = table_ref.split(".")
        if len(parts) != 3:
            logger.warning(f"Invalid table_ref '{table_ref}': expected catalog.namespace.table")
            return (None, None)
        _, namespace, table_name = parts

        params = params or {}
        catalog_uri = params.get("catalog_uri", os.getenv("LAKEKEEPER_URL", ""))
        warehouse = params.get("warehouse", os.getenv("LAKEKEEPER_WAREHOUSE", "seeknal-warehouse"))

        if not catalog_uri:
            return (None, None)

        token_url = os.getenv("KEYCLOAK_TOKEN_URL", "")
        client_id = os.getenv("KEYCLOAK_CLIENT_ID", "")
        client_secret = os.getenv("KEYCLOAK_CLIENT_SECRET", "")

        if not (token_url and client_id and client_secret):
            return (None, None)

        token_data = (
            f"grant_type=client_credentials"
            f"&client_id={client_id}"
            f"&client_secret={client_secret}"
        ).encode()
        req = urllib.request.Request(token_url, data=token_data)
        token_response = json.loads(urllib.request.urlopen(req).read())
        token = token_response["access_token"]

        base_url = catalog_uri.rstrip("/")
        if "/catalog" not in base_url:
            catalog_url = f"{base_url}/catalog"
        else:
            catalog_url = base_url

        config_req = urllib.request.Request(
            f"{catalog_url}/v1/config?warehouse={warehouse}",
            headers={"Authorization": f"Bearer {token}"},
        )
        config_resp = json.loads(urllib.request.urlopen(config_req).read())
        prefix = config_resp.get("overrides", {}).get("prefix", "")

        # Lakekeeper returns empty prefix — fall back to warehouse UUID
        if not prefix:
            warehouse_id = os.getenv("LAKEKEEPER_WAREHOUSE_ID", "")
            if warehouse_id:
                prefix = warehouse_id

        table_url = f"{catalog_url}/v1/{prefix}/namespaces/{namespace}/tables/{table_name}"
        table_req = urllib.request.Request(
            table_url,
            headers={"Authorization": f"Bearer {token}"},
        )
        table_resp = json.loads(urllib.request.urlopen(table_req).read())
        metadata = table_resp.get("metadata", {})
        snapshot_id = str(metadata.get("current-snapshot-id", ""))

        snapshots = metadata.get("snapshots", [])
        snapshot_ts = None
        for snap in snapshots:
            if str(snap.get("snapshot-id", "")) == snapshot_id:
                ts_ms = snap.get("timestamp-ms", 0)
                snapshot_ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
                break

        return (snapshot_id, snapshot_ts) if snapshot_id else (None, None)

    except Exception as e:
        logger.warning(f"Failed to query Iceberg snapshot for '{table_ref}': {e}")
        return (None, None)


def get_snapshot_id_from_duckdb(
    con: Any,
    catalog_alias: str,
    namespace: str,
    table_name: str,
) -> Optional[str]:
    """Query snapshot ID using DuckDB's iceberg_snapshots() function.

    Args:
        con: DuckDB connection object.
        catalog_alias: Catalog alias as attached in DuckDB.
        namespace: Iceberg namespace.
        table_name: Iceberg table name.

    Returns:
        snapshot_id as string, or None if not available.
    """
    try:
        result = con.execute(
            f"SELECT snapshot_id FROM iceberg_snapshots('{catalog_alias}.{namespace}.{table_name}') "
            f"ORDER BY timestamp_ms DESC LIMIT 1"
        ).fetchone()
        return str(result[0]) if result else None
    except Exception as e:
        logger.debug(f"iceberg_snapshots() not available: {e}")
        return None
