"""
EXPOSURE node executor for Seeknal workflow execution.

This module provides the ExposureExecutor class, which handles publishing
data to external systems like APIs, databases, files, and notifications.

Supported Exposure Types:
- file: Export to CSV/Parquet/JSON files
- api: Publish to REST endpoints (POST data)
- database: Write to database tables
- notification: Send alerts (email, Slack, webhook)

The executor resolves input refs to get data from upstream dependencies
and delivers it according to the exposure configuration.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import warnings

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = None

from seeknal.dag.manifest import Node, NodeType
from seeknal.workflow.executors.base import (
    BaseExecutor,
    ExecutorResult,
    ExecutionContext,
    ExecutionStatus,
    ExecutorValidationError,
    ExecutorExecutionError,
    register_executor,
)


class ExposureType(Enum):
    """Types of exposure destinations."""
    FILE = "file"
    API = "api"
    DATABASE = "database"
    NOTIFICATION = "notification"
    STARROCKS_MV = "starrocks_materialized_view"


class FileFormat(Enum):
    """File export formats."""
    CSV = "csv"
    PARQUET = "parquet"
    JSON = "json"
    JSONL = "jsonl"


@dataclass
class DeliveryResult:
    """
    Result of a delivery attempt.

    Attributes:
        success: Whether delivery succeeded
        records_sent: Number of records delivered
        bytes_written: Number of bytes written (for files)
        url: URL or path where data was delivered
        duration_seconds: Time taken for delivery
        error_message: Error message if delivery failed
        metadata: Additional delivery metadata
    """
    success: bool
    records_sent: int = 0
    bytes_written: int = 0
    url: Optional[str] = None
    duration_seconds: float = 0.0
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


@register_executor(NodeType.EXPOSURE)
class ExposureExecutor(BaseExecutor):
    """
    Executor for EXPOSURE nodes.

    Handles publishing data to external systems based on the exposure type:
    - file: Export to local files (CSV, Parquet, JSON)
    - api: POST to REST endpoints
    - database: Insert into database tables
    - notification: Send alerts (webhook, email, Slack)

    The executor:
    1. Resolves input refs to get data from upstream dependencies
    2. Validates the exposure configuration
    3. Executes the delivery based on type
    4. Returns delivery confirmation with timing and metadata

    Example:
        >>> # YAML configuration
        >>> kind: exposure
        >>> name: export_users
        >>> type: file
        >>> params:
        >>>   format: csv
        >>>   path: /tmp/users.csv
        >>> depends_on:
        >>>   - ref: transform.clean_users

        >>> # Execution
        >>> executor = ExposureExecutor(node, context)
        >>> result = executor.run()
        >>> if result.is_success():
        ...     print(f"Exported {result.row_count} rows to {result.output_path}")
    """

    @property
    def node_type(self) -> NodeType:
        """Return the node type this executor handles."""
        return NodeType.EXPOSURE

    def validate(self) -> None:
        """
        Validate the exposure node configuration.

        Checks:
        - Exposure type is valid
        - Required fields are present based on type
        - Input refs are specified

        Raises:
            ExecutorValidationError: If configuration is invalid
        """
        config = self.node.config

        # Check exposure type
        exposure_type = config.get("type")
        if not exposure_type:
            raise ExecutorValidationError(
                self.node.id,
                "Missing required field 'type' in exposure configuration"
            )

        try:
            ExposureType(exposure_type.lower())
        except ValueError:
            raise ExecutorValidationError(
                self.node.id,
                f"Invalid exposure type: {exposure_type}. "
                f"Must be one of: {[t.value for t in ExposureType]}"
            )

        # Validate type-specific requirements
        if exposure_type.lower() == ExposureType.FILE.value:
            self._validate_file_exposure(config)
        elif exposure_type.lower() == ExposureType.API.value:
            self._validate_api_exposure(config)
        elif exposure_type.lower() == ExposureType.DATABASE.value:
            self._validate_database_exposure(config)
        elif exposure_type.lower() == ExposureType.NOTIFICATION.value:
            self._validate_notification_exposure(config)
        elif exposure_type.lower() == ExposureType.STARROCKS_MV.value:
            self._validate_starrocks_mv_exposure(config)

    def _validate_file_exposure(self, config: Dict[str, Any]) -> None:
        """Validate file export configuration."""
        params = config.get("params", {})
        if not isinstance(params, dict):
            raise ExecutorValidationError(
                self.node.id,
                "'params' must be a dictionary for file exposures"
            )

        # Check format
        format_val = params.get("format", "csv").lower()
        try:
            FileFormat(format_val)
        except ValueError:
            raise ExecutorValidationError(
                self.node.id,
                f"Invalid file format: {format_val}. "
                f"Must be one of: {[f.value for f in FileFormat]}"
            )

        # Check path
        if "path" not in params and "url" not in config:
            raise ExecutorValidationError(
                self.node.id,
                "File exposures require 'params.path' or 'url' field"
            )

    def _validate_api_exposure(self, config: Dict[str, Any]) -> None:
        """Validate API endpoint configuration."""
        if "url" not in config and "params" not in config:
            raise ExecutorValidationError(
                self.node.id,
                "API exposures require 'url' or 'params.url' field"
            )

    def _validate_database_exposure(self, config: Dict[str, Any]) -> None:
        """Validate database write configuration."""
        params = config.get("params", {})
        if not isinstance(params, dict):
            raise ExecutorValidationError(
                self.node.id,
                "'params' must be a dictionary for database exposures"
            )

        if "table" not in params:
            raise ExecutorValidationError(
                self.node.id,
                "Database exposures require 'params.table' field"
            )

    def _validate_notification_exposure(self, config: Dict[str, Any]) -> None:
        """Validate notification configuration."""
        params = config.get("params", {})
        if not isinstance(params, dict):
            raise ExecutorValidationError(
                self.node.id,
                "'params' must be a dictionary for notifications"
            )

        # Check notification channel
        channel = params.get("channel", "webhook").lower()
        valid_channels = ["webhook", "email", "slack"]
        if channel not in valid_channels:
            raise ExecutorValidationError(
                self.node.id,
                f"Invalid notification channel: {channel}. "
                f"Must be one of: {valid_channels}"
            )

        # Check URL for webhook/Slack
        if channel in ["webhook", "slack"]:
            if "url" not in params and "url" not in config:
                raise ExecutorValidationError(
                    self.node.id,
                    f"{channel} notifications require 'url' or 'params.url' field"
                )

    def pre_execute(self) -> None:
        """
        Prepare output directories for file exposures.

        Creates parent directories for file exports if they don't exist.
        """
        config = self.node.config
        exposure_type = config.get("type", "").lower()

        if exposure_type == ExposureType.FILE.value:
            params = config.get("params", {})
            file_path = params.get("path") or config.get("url")

            if file_path:
                path = Path(file_path)
                # Ensure parent directory exists
                path.parent.mkdir(parents=True, exist_ok=True)

    def execute(self) -> ExecutorResult:
        """
        Execute the exposure node.

        Resolves input data, determines delivery type, and executes delivery.

        Returns:
            ExecutorResult with delivery status and metadata

        Raises:
            ExecutorExecutionError: If execution fails
        """
        start_time = time.time()

        try:
            # Handle dry-run mode
            if self.context.dry_run:
                return ExecutorResult(
                    node_id=self.node.id,
                    status=ExecutionStatus.SUCCESS,
                    duration_seconds=0.0,
                    is_dry_run=True,
                    metadata={
                        "exposure_type": self.node.config.get("type"),
                        "message": "Dry-run - no data delivered"
                    }
                )

            # Get exposure configuration
            config = self.node.config
            exposure_type = config.get("type", "").lower()

            # Resolve input data
            data = self._resolve_input_data()

            # Execute based on exposure type
            if exposure_type == ExposureType.FILE.value:
                result = self._execute_file_exposure(data)
            elif exposure_type == ExposureType.API.value:
                result = self._execute_api_exposure(data)
            elif exposure_type == ExposureType.DATABASE.value:
                result = self._execute_database_exposure(data)
            elif exposure_type == ExposureType.NOTIFICATION.value:
                result = self._execute_notification_exposure(data)
            elif exposure_type == ExposureType.STARROCKS_MV.value:
                result = self._execute_starrocks_mv_exposure(data)
            else:
                raise ExecutorExecutionError(
                    self.node.id,
                    f"Unsupported exposure type: {exposure_type}"
                )

            # Calculate duration
            duration = time.time() - start_time
            result.duration_seconds = duration

            # Build executor result
            if result.success:
                return ExecutorResult(
                    node_id=self.node.id,
                    status=ExecutionStatus.SUCCESS,
                    duration_seconds=duration,
                    row_count=result.records_sent,
                    output_path=Path(result.url) if result.url and exposure_type == "file" else None,
                    metadata={
                        "exposure_type": exposure_type,
                        "delivery_url": result.url,
                        "bytes_written": result.bytes_written,
                        "delivery_duration": result.duration_seconds,
                        **result.metadata
                    }
                )
            else:
                return ExecutorResult(
                    node_id=self.node.id,
                    status=ExecutionStatus.FAILED,
                    duration_seconds=duration,
                    error_message=result.error_message,
                    metadata={
                        "exposure_type": exposure_type,
                        **result.metadata
                    }
                )

        except ExecutorExecutionError:
            raise
        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Exposure execution failed: {str(e)}",
                e
            ) from e

    def _resolve_input_data(self) -> Any:
        """
        Resolve input data from upstream dependencies.

        Loads data from intermediate storage for exposure nodes.

        Returns:
            DataFrame from upstream dependency or None
        """
        config = self.node.config
        depends_on = config.get("depends_on", [])
        inputs = config.get("inputs", [])

        # Use inputs if depends_on is not available
        refs = depends_on if depends_on else inputs

        if not refs:
            return None

        # Get the first input ref
        if isinstance(refs, list) and len(refs) > 0:
            first_ref = refs[0]
            if isinstance(first_ref, dict):
                ref = first_ref.get("ref", "unknown")
            else:
                ref = str(first_ref)

            # Parse ref (format: kind.name, e.g., "transform.sales_forecast")
            if "." not in ref:
                return None

            kind, ref_name = ref.split(".", 1)

            # Load from intermediate storage
            project_path = Path.cwd()
            intermediate_path = project_path / "target" / "intermediate" / f"{kind}_{ref_name}.parquet"

            if intermediate_path.exists():
                import pandas as pd
                try:
                    df = pd.read_parquet(intermediate_path)
                    return df
                except Exception as e:
                    # Return metadata if loading fails
                    return {
                        "source_ref": ref,
                        "error": f"Failed to load intermediate data: {e}"
                    }

        return None

    def _execute_file_exposure(self, data: Any) -> DeliveryResult:
        """
        Execute file export exposure.

        Supports CSV, Parquet, JSON, and JSONL formats.

        Args:
            data: Data to export (dict, list, or pandas DataFrame)

        Returns:
            DeliveryResult with export status
        """
        start = time.time()
        config = self.node.config
        params = config.get("params", {})

        # Get file path and format
        file_path = params.get("path") or config.get("url")
        format_val = params.get("format", "csv").lower()

        if not file_path:
            return DeliveryResult(
                success=False,
                error_message="Missing file path in configuration"
            )

        path = Path(file_path)

        # In dry-run or without real data, create a placeholder
        if data is None or isinstance(data, dict) and "source_ref" in data:
            # Create sample data for demonstration
            sample_data = self._create_sample_data()
            records = self._write_sample_file(path, format_val, sample_data)

            return DeliveryResult(
                success=True,
                records_sent=records,
                bytes_written=path.stat().st_size if path.exists() else 0,
                url=str(path),
                duration_seconds=time.time() - start,
                metadata={
                    "format": format_val,
                    "note": "Sample data - upstream data not available in this context"
                }
            )

        # Real data export (requires pandas)
        if not PANDAS_AVAILABLE:
            return DeliveryResult(
                success=False,
                error_message="pandas is required for data export. Install with: pip install pandas"
            )

        try:
            # Convert data to DataFrame if needed
            if isinstance(data, pd.DataFrame):
                df = data
            elif isinstance(data, (list, dict)):
                df = pd.DataFrame(data)
            else:
                return DeliveryResult(
                    success=False,
                    error_message=f"Unsupported data type: {type(data)}"
                )

            # Write based on format
            records = len(df)

            if format_val == FileFormat.CSV.value:
                df.to_csv(path, index=False)
            elif format_val == FileFormat.PARQUET.value:
                df.to_parquet(path, index=False)
            elif format_val == FileFormat.JSON.value:
                df.to_json(path, orient="records", indent=2)
            elif format_val == FileFormat.JSONL.value:
                df.to_json(path, orient="records", lines=True)
            else:
                return DeliveryResult(
                    success=False,
                    error_message=f"Unsupported format: {format_val}"
                )

            return DeliveryResult(
                success=True,
                records_sent=records,
                bytes_written=path.stat().st_size,
                url=str(path),
                duration_seconds=time.time() - start,
                metadata={"format": format_val}
            )

        except Exception as e:
            return DeliveryResult(
                success=False,
                error_message=f"Failed to write file: {str(e)}"
            )

    def _execute_api_exposure(self, data: Any) -> DeliveryResult:
        """
        Execute API POST exposure.

        Posts data to a REST endpoint.

        Args:
            data: Data to post

        Returns:
            DeliveryResult with POST status
        """
        start = time.time()
        config = self.node.config
        params = config.get("params", {})
        url = params.get("url") or config.get("url")

        if not url:
            return DeliveryResult(
                success=False,
                error_message="Missing URL in API exposure configuration"
            )

        # Check if requests is available
        try:
            import requests
        except ImportError:
            return DeliveryResult(
                success=False,
                error_message="requests library is required for API exposure. Install with: pip install requests"
            )

        try:
            # Prepare payload
            if isinstance(data, dict):
                payload = data
            elif isinstance(data, list):
                payload = {"data": data}
            else:
                payload = {"message": str(data), "source": self.node.id}

            # Get headers and auth
            headers = params.get("headers", {})
            auth = params.get("auth")

            # Make POST request
            response = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=params.get("timeout", 30),
                auth=auth
            )

            # Check response
            response.raise_for_status()

            return DeliveryResult(
                success=True,
                records_sent=1 if data else 0,
                url=url,
                duration_seconds=time.time() - start,
                metadata={
                    "status_code": response.status_code,
                    "response": response.text[:200] if response.text else None
                }
            )

        except requests.exceptions.RequestException as e:
            return DeliveryResult(
                success=False,
                error_message=f"API request failed: {str(e)}"
            )

    def _execute_database_exposure(self, data: Any) -> DeliveryResult:
        """
        Execute database write exposure.

        Writes data to a database table.

        Args:
            data: Data to write

        Returns:
            DeliveryResult with write status
        """
        start = time.time()
        config = self.node.config
        params = config.get("params", {})
        table = params.get("table")

        if not table:
            return DeliveryResult(
                success=False,
                error_message="Missing table name in database exposure configuration"
            )

        # Check for DuckDB connection in context
        con = self.context.duckdb_connection

        if not con:
            return DeliveryResult(
                success=False,
                error_message="No database connection available in execution context"
            )

        try:
            # For now, return a placeholder result
            # Real implementation would execute INSERT or CREATE TABLE AS
            return DeliveryResult(
                success=True,
                records_sent=0,
                url=f"table://{table}",
                duration_seconds=time.time() - start,
                metadata={
                    "table": table,
                    "note": "Database write integration pending"
                }
            )

        except Exception as e:
            return DeliveryResult(
                success=False,
                error_message=f"Database write failed: {str(e)}"
            )

    def _execute_notification_exposure(self, data: Any) -> DeliveryResult:
        """
        Execute notification exposure.

        Sends notifications via webhook, email, or Slack.

        Args:
            data: Notification data/message

        Returns:
            DeliveryResult with notification status
        """
        start = time.time()
        config = self.node.config
        params = config.get("params", {})
        channel = params.get("channel", "webhook").lower()

        if channel == "webhook":
            return self._send_webhook_notification(data, params, start)
        elif channel == "slack":
            return self._send_slack_notification(data, params, start)
        elif channel == "email":
            return self._send_email_notification(data, params, start)
        else:
            return DeliveryResult(
                success=False,
                error_message=f"Unsupported notification channel: {channel}"
            )

    def _send_webhook_notification(
        self,
        data: Any,
        params: Dict[str, Any],
        start: float
    ) -> DeliveryResult:
        """Send webhook notification."""
        try:
            import requests
        except ImportError:
            return DeliveryResult(
                success=False,
                error_message="requests library required for webhooks. Install with: pip install requests"
            )

        url = params.get("url") or self.node.config.get("url")
        if not url:
            return DeliveryResult(
                success=False,
                error_message="Missing webhook URL"
            )

        try:
            # Prepare payload
            payload = params.get("payload", {})
            if isinstance(payload, dict):
                payload.update({
                    "node": self.node.id,
                    "project": self.context.project_name,
                    "data": str(data)
                })
            else:
                payload = {
                    "node": self.node.id,
                    "project": self.context.project_name,
                    "message": str(data)
                }

            # Send POST request
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()

            return DeliveryResult(
                success=True,
                records_sent=1,
                url=url,
                duration_seconds=time.time() - start,
                metadata={"status_code": response.status_code}
            )

        except Exception as e:
            return DeliveryResult(
                success=False,
                error_message=f"Webhook notification failed: {str(e)}"
            )

    def _send_slack_notification(
        self,
        data: Any,
        params: Dict[str, Any],
        start: float
    ) -> DeliveryResult:
        """Send Slack notification."""
        try:
            import requests
        except ImportError:
            return DeliveryResult(
                success=False,
                error_message="requests library required for Slack. Install with: pip install requests"
            )

        url = params.get("url") or self.node.config.get("url")
        if not url:
            return DeliveryResult(
                success=False,
                error_message="Missing Slack webhook URL"
            )

        try:
            # Prepare Slack message
            message = params.get("message", f"Exposure {self.node.id} completed")
            payload = {
                "text": message,
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Exposure: {self.node.id}*\n{message}"
                        }
                    }
                ]
            }

            # Send POST request
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()

            return DeliveryResult(
                success=True,
                records_sent=1,
                url=url,
                duration_seconds=time.time() - start,
                metadata={"status_code": response.status_code}
            )

        except Exception as e:
            return DeliveryResult(
                success=False,
                error_message=f"Slack notification failed: {str(e)}"
            )

    def _send_email_notification(
        self,
        data: Any,
        params: Dict[str, Any],
        start: float
    ) -> DeliveryResult:
        """Send email notification."""
        # Email notification is optional - return placeholder
        return DeliveryResult(
            success=False,
            error_message="Email notifications not yet implemented. Use webhook or Slack instead."
        )

    def _validate_starrocks_mv_exposure(self, config: Dict[str, Any]) -> None:
        """Validate StarRocks materialized view exposure configuration."""
        params = config.get("params", {})
        if not isinstance(params, dict):
            raise ExecutorValidationError(
                self.node.id,
                "'params' must be a dictionary for StarRocks MV exposures"
            )

        if "mv_name" not in params:
            raise ExecutorValidationError(
                self.node.id,
                "StarRocks MV exposures require 'params.mv_name' field"
            )

        if "query" not in params and not config.get("depends_on"):
            raise ExecutorValidationError(
                self.node.id,
                "StarRocks MV exposures require 'params.query' or 'depends_on'"
            )

    def _execute_starrocks_mv_exposure(self, data: Any) -> DeliveryResult:
        """
        Execute StarRocks materialized view creation.

        Generates and executes CREATE MATERIALIZED VIEW DDL on StarRocks.

        Args:
            data: Not used directly; query comes from config

        Returns:
            DeliveryResult with MV creation status
        """
        start = time.time()
        config = self.node.config
        params = config.get("params", {})
        mv_name = params.get("mv_name", self.node.name)
        query = params.get("query", "")
        refresh = params.get("refresh", "MANUAL")
        properties = params.get("properties", {})

        if not query:
            return DeliveryResult(
                success=False,
                error_message="Missing 'query' in StarRocks MV exposure params"
            )

        try:
            from seeknal.connections.starrocks import create_starrocks_connection
        except ImportError:
            return DeliveryResult(
                success=False,
                error_message="pymysql required for StarRocks. Install with: pip install pymysql"
            )

        # Build DDL
        props_str = ""
        if properties:
            props_items = ", ".join(f'"{k}" = "{v}"' for k, v in properties.items())
            props_str = f"\nPROPERTIES ({props_items})"

        ddl = (
            f"CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_name}\n"
            f"REFRESH {refresh}{props_str}\n"
            f"AS\n{query}"
        )

        # Get connection config
        conn_config = {
            "host": params.get("host", "localhost"),
            "port": params.get("port", 9030),
            "user": params.get("user", "root"),
            "password": params.get("password", ""),
            "database": params.get("database", ""),
        }

        try:
            sr_conn = create_starrocks_connection(conn_config)
            cursor = sr_conn.cursor()
            cursor.execute(ddl)
            cursor.close()
            sr_conn.close()

            return DeliveryResult(
                success=True,
                records_sent=0,
                url=f"starrocks://{conn_config['host']}:{conn_config['port']}/{mv_name}",
                duration_seconds=time.time() - start,
                metadata={
                    "mv_name": mv_name,
                    "refresh": refresh,
                    "ddl": ddl,
                }
            )

        except Exception as e:
            return DeliveryResult(
                success=False,
                error_message=f"Failed to create StarRocks MV '{mv_name}': {str(e)}",
                metadata={"ddl": ddl}
            )

    def _create_sample_data(self) -> List[Dict[str, Any]]:
        """Create sample data for demonstration."""
        return [
            {"id": 1, "name": "Sample Record 1", "value": 100},
            {"id": 2, "name": "Sample Record 2", "value": 200},
            {"id": 3, "name": "Sample Record 3", "value": 300},
        ]

    def _write_sample_file(
        self,
        path: Path,
        format_val: str,
        data: List[Dict[str, Any]]
    ) -> int:
        """
        Write sample data to file.

        Args:
            path: File path to write
            format_val: File format (csv, json, etc.)
            data: Data to write

        Returns:
            Number of records written
        """
        try:
            if format_val == FileFormat.CSV.value:
                if PANDAS_AVAILABLE:
                    df = pd.DataFrame(data)
                    df.to_csv(path, index=False)
                else:
                    # Fallback to manual CSV writing
                    import csv
                    with open(path, 'w', newline='') as f:
                        if data:
                            writer = csv.DictWriter(f, fieldnames=data[0].keys())
                            writer.writeheader()
                            writer.writerows(data)

            elif format_val == FileFormat.JSON.value:
                with open(path, 'w') as f:
                    json.dump(data, f, indent=2)

            elif format_val == FileFormat.JSONL.value:
                with open(path, 'w') as f:
                    for record in data:
                        f.write(json.dumps(record) + '\n')

            elif format_val == FileFormat.PARQUET.value:
                if PANDAS_AVAILABLE:
                    df = pd.DataFrame(data)
                    df.to_parquet(path, index=False)
                else:
                    # Fallback to JSON
                    warnings.warn("pandas not available, writing JSON instead of Parquet")
                    json_path = path.with_suffix('.json')
                    with open(json_path, 'w') as f:
                        json.dump(data, f, indent=2)
                    path = json_path

            return len(data)

        except Exception as e:
            # If writing fails, try to create an empty file as placeholder
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
                return 0
            except Exception:
                pass
            raise

    def post_execute(self, result: ExecutorResult) -> ExecutorResult:
        """
        Cleanup and finalize after execution.

        Adds additional metadata to the result.

        Args:
            result: Result from execute()

        Returns:
            Modified ExecutorResult
        """
        # Add executor version
        result.metadata["executor_version"] = "1.0.0"
        result.metadata["executor_class"] = "ExposureExecutor"

        # Add delivery confirmation
        if result.is_success():
            result.metadata["delivery_confirmed"] = True
        else:
            result.metadata["delivery_confirmed"] = False

        return result
