"""
Pipeline execution context for Python pipeline functions.

The PipelineContext (ctx) object is passed to decorated pipeline functions
and provides access to:
- Upstream node outputs via ctx.ref()
- DuckDB connection via ctx.duckdb
- Profile configuration via ctx.config
"""

from dataclasses import dataclass, field
from typing import Any, Optional
from pathlib import Path

# Import duckdb eagerly as it's needed for the connection
try:
    import duckdb
except ImportError:
    duckdb = None

# Import pandas lazily when needed (in _store_output and ref methods)
# to avoid top-level import errors when seeknal is imported from system path


@dataclass
class PipelineContext:
    """Execution context passed to decorated pipeline functions.

    Attributes:
        project_path: Path to the project root directory
        target_dir: Path to the target directory for outputs
        config: Profile configuration dict (from profiles.yml)
        _duckdb_con: Internal DuckDB connection (lazy initialization)
        _node_outputs: Internal cache of node outputs

    Example:
        @transform(name="clean_users")
        def clean_users(ctx):
            df = ctx.ref("source.raw_users")
            result = ctx.duckdb.sql("SELECT * FROM df WHERE active").df()
            return result
    """
    project_path: Path
    target_dir: Path
    config: dict
    _duckdb_con: Optional[Any] = field(default=None, repr=False)
    _node_outputs: dict = field(default_factory=dict, repr=False)

    @property
    def duckdb(self) -> Any:
        """Lazily create and return DuckDB connection.

        Returns:
            DuckDB connection object

        Raises:
            ImportError: If duckdb is not installed
        """
        if duckdb is None:
            raise ImportError(
                "duckdb is required for Python pipelines. "
                "Install with: pip install duckdb"
            )

        if self._duckdb_con is None:
            db_path = self.config.get("path", ":memory:")
            self._duckdb_con = duckdb.connect(db_path)
        return self._duckdb_con

    def ref(self, node_id: str) -> Any:
        """Reference another node's output.

        Retrieves the output DataFrame from an upstream node.
        First checks the in-memory cache, then loads from intermediate storage.

        Args:
            node_id: Node identifier like "source.raw_users" or "transform.clean_users"

        Returns:
            DataFrame from the referenced node

        Raises:
            ValueError: If node output is not found
            ImportError: If pandas is not installed
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for Python pipelines. "
                "Ensure it is in the PEP 723 dependencies."
            )

        # Check in-memory cache first
        if node_id in self._node_outputs:
            return self._node_outputs[node_id]

        # Load from intermediate storage
        intermediate_path = self.target_dir / "intermediate" / f"{node_id.replace('.', '_')}.parquet"
        if intermediate_path.exists():
            df = pd.read_parquet(intermediate_path)
            self._node_outputs[node_id] = df
            return df

        raise ValueError(
            f"Node '{node_id}' not found. Ensure it is executed before this node. "
            f"Available nodes: {list(self._node_outputs.keys())}"
        )

    def _store_output(self, node_id: str, df: Any) -> Path:
        """Store node output for cross-references.

        Stores the DataFrame in both memory cache and intermediate storage
        for use by downstream nodes.

        Args:
            node_id: Node identifier
            df: DataFrame to store

        Returns:
            Path where the output was stored
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for Python pipelines. "
                "Ensure it is in the PEP 723 dependencies."
            )

        # Store in memory
        self._node_outputs[node_id] = df

        # Store to disk
        output_path = self.target_dir / "intermediate" / f"{node_id.replace('.', '_')}.parquet"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Use pandas to_parquet if available, otherwise fallback
        if hasattr(df, "to_parquet"):
            df.to_parquet(output_path, index=False)
        else:
            # Fallback for non-DataFrame objects
            import pickle
            with open(output_path.with_suffix(".pkl"), "wb") as f:
                pickle.dump(df, f)

        return output_path

    def close(self) -> None:
        """Clean up resources.

        Closes the DuckDB connection if it was opened.
        """
        if self._duckdb_con is not None:
            self._duckdb_con.close()
            self._duckdb_con = None
