"""
Python executor for running Python pipeline nodes via uv.

This module provides the PythonExecutor class that executes Python pipeline
files using Astral's uv tool with PEP 723 inline script metadata support.

Environment Variable Type Handling Limitations
----------------------------------------------

All parameter values passed to Python scripts are converted to environment
variables. Due to environment variable limitations in all major operating systems,
**all parameter values are converted to strings** before being passed to the
Python execution context.

This means that if you pass parameters like:
```python
context.params = {
    "count": 100,           # int
    "ratio": 0.95,          # float
    "active": True,         # bool
    "tags": ["a", "b"]     # list
}
```

They will arrive in your Python script as strings:
```python
count = "100"           # String, not int
ratio = "0.95"          # String, not float
active = "True"         # String, not bool
tags = "['a', 'b']"     # String representation of list
```

Type Preservation Alternatives
-----------------------------

To work around this limitation, you have several options:

1. **JSON-Encoded Values (Recommended)**
   When setting parameters, encode them as JSON strings:
   ```python
   import json

   context.params = {
       "count": json.dumps(100),
       "ratio": json.dumps(0.95),
       "active": json.dumps(True),
       "tags": json.dumps(["a", "b"])
   }
   ```

   In your Python script, decode them:
   ```python
   import json
   import os

   def my_function(ctx):
       count = json.loads(os.environ.get("SEEKNAL_PARAM_COUNT"))
       ratio = float(os.environ.get("SEEKNAL_PARAM_RATIO"))
       active = json.loads(os.environ.get("SEEKNAL_PARAM_ACTIVE"))
       tags = json.loads(os.environ.get("SEEKNAL_PARAM_TAGS"))
   ```

2. **Type-Prefixed Variables**
   Use environment variable names that indicate their type:
   ```python
   context.params = {
       "count__type=int": 100,
       "ratio__type=float": 0.95,
       "active__type=bool": True,
       "tags__type=json": ["a", "b"]
   }
   ```

   In your Python script, parse them accordingly:
   ```python
   import os

   def my_function(ctx):
       def parse_value(value, type_hint):
           if type_hint == "int":
               return int(value)
           elif type_hint == "float":
               return float(value)
           elif type_hint == "bool":
               return value.lower() in ("true", "1", "yes")
           elif type_hint == "json":
               import json
               return json.loads(value)
           return value

       # Extract all environment variables
       for key, value in os.environ.items():
           if key.startswith("SEEKNAL_PARAM_"):
               param_name = key.replace("SEEKNAL_PARAM_", "")
               if "__type=" in param_name:
                   name, type_info = param_name.split("__type=")
                   parsed_value = parse_value(value, type_info)
   ```

3. **Use Context Parameters Directly**
   If you need the original parameter types, access them directly from the context
   before they're converted to environment variables:
   ```python
   def my_function(ctx):
       # Access parameters directly without using get_param()
       count = ctx.params.get("count")  # This will be the original int value
       ratio = ctx.params.get("ratio")  # This will be the original float value
   ```

Best Practices
-------------

- Document parameter types in your pipeline definitions
- Use JSON encoding for complex data structures
- Provide default values in case environment variables are missing
- Create helper functions for common type conversions
"""

import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

from seeknal.workflow.executors.base import (
    BaseExecutor,
    ExecutionContext,
    ExecutorResult,
    ExecutionStatus,
    ExecutorValidationError,
    register_executor,
)
from seeknal.dag.manifest import Node, NodeType


def check_uv_installed() -> bool:
    """Check if uv is available in PATH.

    Returns:
        True if uv command is available, False otherwise
    """
    return shutil.which("uv") is not None


@register_executor(NodeType.PYTHON)
class PythonExecutor(BaseExecutor):
    """
    Execute Python pipeline nodes via uv run.

    This executor handles Python pipeline files (*.py) that use the
    @source, @transform, and @feature_group decorators. It executes
    them using Astral's uv tool, which supports PEP 723 inline
    script metadata for dependency management.

    Key Features:
    - Executes Python files via `uv run`
    - Preserves PEP 723 inline dependencies
    - Provides helpful error message if uv is missing
    - 30-minute subprocess timeout
    - Output truncation for large logs

    Example:
        @transform(name="clean_data")
        def clean_data(ctx):
            df = ctx.ref("source.raw_data")
            return ctx.duckdb.sql("SELECT * FROM df WHERE active").df()
    """

    @property
    def node_type(self) -> NodeType:
        """Return PYTHON node type."""
        return NodeType.PYTHON

    def validate(self) -> None:
        """Validate Python execution requirements.

        Raises:
            ExecutorValidationError: If uv is not installed or file doesn't exist
        """
        if not check_uv_installed():
            raise ExecutorValidationError(
                self.node.id,
                "uv is required for Python pipelines but was not found.\n"
                "Install with: curl -LsSf https://astral.sh/uv/install.sh | sh"
            )

        file_path = Path(self.node.file_path)
        if not file_path.exists():
            raise ExecutorValidationError(
                self.node.id,
                f"Pipeline file not found: {file_path}"
            )

    def execute(self) -> ExecutorResult:
        """Execute Python pipeline via uv run.

        Returns:
            ExecutorResult with execution outcome

        Raises:
            ExecutorValidationError: If validation fails
        """
        import time

        file_path = Path(self.node.file_path)
        start_time = time.time()

        # Build execution script that runs the specific function
        runner_script = self._generate_runner_script()
        runner_path = self.context.target_path / f"_runner_{self.node.name}.py"
        runner_path.parent.mkdir(parents=True, exist_ok=True)
        runner_path.write_text(runner_script)

        # Execute via uv run
        cmd = ["uv", "run", str(runner_path)]

        try:
            result = subprocess.run(
                cmd,
                cwd=str(self.context.workspace_path),
                capture_output=True,
                text=True,
                timeout=1800,  # 30 minute timeout
                env=self._get_subprocess_env(),
            )

            duration = time.time() - start_time

            if result.returncode != 0:
                # Extract last meaningful line from stderr for summary
                stderr_lines = [l for l in result.stderr.strip().splitlines() if l.strip()]
                last_stderr = stderr_lines[-1] if stderr_lines else "Unknown error"
                error_msg = f"Process exited with code {result.returncode}: {last_stderr}"

                # Build untruncated output for log files
                output_parts = []
                if result.stdout:
                    output_parts.append(f"stdout:\n{result.stdout}")
                if result.stderr:
                    output_parts.append(f"stderr:\n{result.stderr}")
                full_output = "\n\n".join(output_parts) if output_parts else None

                return ExecutorResult(
                    node_id=self.node.id,
                    status=ExecutionStatus.FAILED,
                    duration_seconds=duration,
                    error_message=error_msg,
                    output_log=full_output,
                    metadata={
                        "stdout": result.stdout[-10000:] if len(result.stdout) > 10000 else result.stdout,
                        "stderr": result.stderr[-10000:] if len(result.stderr) > 10000 else result.stderr,
                        "returncode": result.returncode,
                    },
                )

            # Read row count from output
            row_count = 0
            output_path = self.context.target_path / "intermediate" / f"{self.node.id.replace('.', '_')}.parquet"
            if output_path.exists():
                try:
                    import pandas as pd
                    row_count = len(pd.read_parquet(output_path))
                except Exception:
                    pass  # Row count not critical

            return ExecutorResult(
                node_id=self.node.id,
                status=ExecutionStatus.SUCCESS,
                duration_seconds=duration,
                row_count=row_count,
                output_path=output_path if output_path.exists() else None,
                metadata={"stdout": result.stdout[-1000:] if len(result.stdout) > 1000 else result.stdout},
            )

        except subprocess.TimeoutExpired:
            duration = time.time() - start_time
            return ExecutorResult(
                node_id=self.node.id,
                status=ExecutionStatus.FAILED,
                duration_seconds=duration,
                error_message="Execution timed out after 30 minutes",
                metadata={"error": "Execution timed out after 30 minutes"},
            )

    def post_execute(self, result: ExecutorResult) -> ExecutorResult:
        """Post-execution: handle materialization for Python transforms.

        Loads intermediate parquet into a DuckDB view and dispatches
        materialization to configured targets.
        """
        import logging

        logger = logging.getLogger(__name__)

        if result.status != ExecutionStatus.SUCCESS or result.is_dry_run:
            return result

        mat_targets = self.node.config.get("materializations", [])
        if not mat_targets:
            return result

        if getattr(self.context, "materialize_enabled", None) is False:
            return result

        # Load intermediate parquet into DuckDB view for materialization
        intermediate_path = (
            self.context.target_path
            / "intermediate"
            / f"{self.node.id.replace('.', '_')}.parquet"
        )
        if not intermediate_path.exists():
            logger.warning(
                f"Intermediate file not found for '{self.node.id}', skipping materialization"
            )
            return result

        try:
            con = self.context.get_duckdb_connection()
            view_name = f"transform.{self.node.name}"

            # Create schema and view from parquet
            con.execute("CREATE SCHEMA IF NOT EXISTS transform")
            con.execute(
                f"CREATE OR REPLACE VIEW {view_name} AS "
                f"SELECT * FROM read_parquet('{intermediate_path}')"
            )

            from seeknal.workflow.materialization.dispatcher import MaterializationDispatcher
            from seeknal.workflow.materialization.profile_loader import ProfileLoader

            profile_path = getattr(self.context, 'profile_path', None)
            loader = ProfileLoader(profile_path=profile_path) if profile_path else None
            dispatcher = MaterializationDispatcher(profile_loader=loader)
            dispatch_result = dispatcher.dispatch(
                con=con,
                view_name=view_name,
                targets=mat_targets,
                node_id=self.node.id,
                env_name=getattr(self.context, 'env_name', None),
            )

            result.metadata["materialization"] = {
                "enabled": True,
                "success": dispatch_result.all_succeeded,
                "total": dispatch_result.total,
                "succeeded": dispatch_result.succeeded,
                "failed": dispatch_result.failed,
                "results": dispatch_result.results,
            }

            if dispatch_result.all_succeeded:
                logger.info(
                    f"Materialized node '{self.node.id}' to "
                    f"{dispatch_result.succeeded}/{dispatch_result.total} targets"
                )
            else:
                logger.warning(
                    f"Materialized node '{self.node.id}': "
                    f"{dispatch_result.succeeded}/{dispatch_result.total} succeeded"
                )
        except Exception as e:
            logger.warning(f"Failed to materialize node '{self.node.id}': {e}")
            result.metadata["materialization"] = {
                "enabled": True,
                "success": False,
                "error": str(e),
            }

        return result

    def _generate_runner_script(self) -> str:
        """Generate wrapper script to execute specific function.

        Returns:
            Python script content as string
        """
        import seeknal
        seeknal_path = Path(seeknal.__file__).parent.parent

        file_path = Path(self.node.file_path)
        func_name = self.node.name

        # Read original file to preserve PEP 723 header, but remove seeknal from deps
        # since we're adding sys.path and seeknal is a local package
        original_content = file_path.read_text()
        pep723_header = self._extract_pep723_header(original_content)
        # Remove seeknal from PEP 723 deps as it's a local package
        pep723_header = pep723_header.replace('#     "seeknal",\n', '')
        pep723_header = pep723_header.replace('#     "seeknal"', '')

        return f'''{pep723_header}
import sys
sys.path.insert(0, "{seeknal_path}")
sys.path.insert(0, "{self.context.workspace_path}")

from pathlib import Path
from seeknal.pipeline.context import PipelineContext

# Import the pipeline module
import importlib.util
spec = importlib.util.spec_from_file_location("pipeline", "{file_path}")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

# Create execution context
ctx = PipelineContext(
    project_path=Path("{self.context.workspace_path}"),
    target_dir=Path("{self.context.target_path}"),
    config={self.node.config.get("profile_config", {})},
)

try:
    # Execute the function
    func = getattr(module, "{func_name}")
    result = func(ctx)
    print(f"Executed {func_name} successfully")
finally:
    ctx.close()
'''

    def _extract_pep723_header(self, content: str) -> str:
        """Extract PEP 723 inline script metadata.

        Args:
            content: Python file content

        Returns:
            PEP 723 header string or empty string if not found
        """
        lines = content.split("\n")
        header_lines = []
        in_header = False

        for line in lines:
            if line.strip() == "# /// script":
                in_header = True
            if in_header:
                header_lines.append(line)
            if line.strip() == "# ///" and in_header:
                break

        return "\n".join(header_lines) if header_lines else ""

    def _get_subprocess_env(self) -> dict[str, str]:
        """Get environment variables for subprocess.

        Returns:
            Environment variables dictionary
        """
        env = os.environ.copy()
        # Add seeknal-specific env vars
        env["SEEKNAL_PROJECT_PATH"] = str(self.context.workspace_path)

        # Add resolved parameters for get_param() helper
        if self.context.params:
            for key, value in self.context.params.items():
                env_key = f"SEEKNAL_PARAM_{key.upper()}"
                env[env_key] = str(value)

        return env
