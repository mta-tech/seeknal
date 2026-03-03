from __future__ import annotations

import json
from io import BytesIO
from unittest.mock import MagicMock, patch

from seeknal.workflow.iceberg_metadata import get_current_snapshot_id, get_snapshot_id_from_duckdb

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SNAPSHOT_ID = "7384742295606287416"
SNAPSHOT_TS_MS = 1700000000000
EXPECTED_TOKEN = "test-access-token"


def _make_urlopen_side_effect(token_resp, config_resp, table_resp):
    """Return a side_effect callable that sequences three urllib responses."""
    responses = [
        BytesIO(json.dumps(token_resp).encode()),
        BytesIO(json.dumps(config_resp).encode()),
        BytesIO(json.dumps(table_resp).encode()),
    ]
    call_count = {"n": 0}

    def side_effect(_req, *_args, **_kwargs):
        idx = call_count["n"]
        call_count["n"] += 1
        return responses[idx]

    return side_effect


TOKEN_RESP = {"access_token": EXPECTED_TOKEN}
CONFIG_RESP = {"overrides": {"prefix": "c008ea5c-fb89-11f0-aa64-c32ca2f52144"}}
TABLE_RESP = {
    "metadata": {
        "current-snapshot-id": int(SNAPSHOT_ID),
        "snapshots": [
            {
                "snapshot-id": int(SNAPSHOT_ID),
                "timestamp-ms": SNAPSHOT_TS_MS,
            }
        ],
    }
}

OAUTH_ENV = {
    "KEYCLOAK_TOKEN_URL": "http://keycloak/token",
    "KEYCLOAK_CLIENT_ID": "seeknal",
    "KEYCLOAK_CLIENT_SECRET": "secret",
}


# ---------------------------------------------------------------------------
# Tests for get_current_snapshot_id
# ---------------------------------------------------------------------------


class TestGetCurrentSnapshotId:
    def test_success(self):
        params = {"catalog_uri": "http://lakekeeper/catalog", "warehouse": "seeknal-warehouse"}

        with patch("urllib.request.urlopen", side_effect=_make_urlopen_side_effect(TOKEN_RESP, CONFIG_RESP, TABLE_RESP)), \
             patch.dict("os.environ", OAUTH_ENV):
            snapshot_id, snapshot_ts = get_current_snapshot_id("atlas.myns.mytable", params)

        assert snapshot_id == SNAPSHOT_ID
        assert snapshot_ts is not None
        assert "2023" in snapshot_ts  # SNAPSHOT_TS_MS corresponds to Nov 2023

    def test_no_catalog_uri(self):
        with patch.dict("os.environ", {**OAUTH_ENV}, clear=False):
            # Ensure LAKEKEEPER_URL is absent
            with patch.dict("os.environ", {"LAKEKEEPER_URL": ""}, clear=False):
                result = get_current_snapshot_id("atlas.myns.mytable", {})
        assert result == (None, None)

    def test_no_oauth_creds(self):
        params = {"catalog_uri": "http://lakekeeper/catalog"}
        # Ensure OAuth env vars are absent
        with patch.dict(
            "os.environ",
            {"KEYCLOAK_TOKEN_URL": "", "KEYCLOAK_CLIENT_ID": "", "KEYCLOAK_CLIENT_SECRET": ""},
            clear=False,
        ):
            result = get_current_snapshot_id("atlas.myns.mytable", params)
        assert result == (None, None)

    def test_catalog_unreachable(self):
        import urllib.error

        params = {"catalog_uri": "http://lakekeeper/catalog", "warehouse": "seeknal-warehouse"}
        with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("connection refused")), \
             patch.dict("os.environ", OAUTH_ENV):
            result = get_current_snapshot_id("atlas.myns.mytable", params)

        assert result == (None, None)

    def test_invalid_table_ref_two_parts(self):
        result = get_current_snapshot_id("only.two")
        assert result == (None, None)

    def test_invalid_table_ref_one_part(self):
        result = get_current_snapshot_id("onepart")
        assert result == (None, None)


# ---------------------------------------------------------------------------
# Tests for get_snapshot_id_from_duckdb
# ---------------------------------------------------------------------------


class TestGetSnapshotIdFromDuckdb:
    def test_success(self):
        mock_con = MagicMock()
        mock_con.execute.return_value.fetchone.return_value = (int(SNAPSHOT_ID),)

        result = get_snapshot_id_from_duckdb(mock_con, "atlas", "myns", "mytable")

        assert result == SNAPSHOT_ID
        mock_con.execute.assert_called_once()

    def test_not_available(self):
        mock_con = MagicMock()
        mock_con.execute.side_effect = Exception("Function iceberg_snapshots not found")

        result = get_snapshot_id_from_duckdb(mock_con, "atlas", "myns", "mytable")

        assert result is None
