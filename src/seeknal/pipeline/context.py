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
            # Convert Arrow-backed string columns to object dtype so DuckDB
            # replacement scan can handle them (DuckDB doesn't recognize pd.StringDtype)
            str_cols = df.select_dtypes(include=["string"]).columns
            if len(str_cols) > 0:
                df[str_cols] = df[str_cols].astype(object)
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

    def features(
        self,
        entity_name: str,
        feature_list: list[str],
        as_of: Optional[str] = None,
    ) -> Any:
        """Retrieve selected features from a consolidated entity view.

        Read-only retrieval API for cross-FG feature selection. Each item
        in feature_list should be formatted as "fg_name.feature_name".

        Args:
            entity_name: Entity name (e.g. "customer")
            feature_list: List of "fg_name.feature_name" strings
            as_of: Optional ISO date/datetime string for point-in-time filter.
                Returns latest row per entity key where event_time <= as_of.

        Returns:
            pandas DataFrame with join_keys, event_time, and selected features
            as flat columns named "fg_name__feature_name"

        Raises:
            FileNotFoundError: If consolidated parquet doesn't exist
            ValueError: If feature_list contains invalid references
        """
        import duckdb as _duckdb

        parquet_path = self.target_dir / "feature_store" / entity_name / "features.parquet"
        if not parquet_path.exists():
            raise FileNotFoundError(
                f"No consolidated features found for entity '{entity_name}'. "
                f"Run `seeknal run` first, or check entity name."
            )

        # Parse feature references
        struct_selects = []
        for ref in feature_list:
            if "." not in ref:
                raise ValueError(
                    f"Invalid feature reference '{ref}'. "
                    f"Expected format: 'fg_name.feature_name'"
                )
            fg_name, feat_name = ref.split(".", 1)
            struct_selects.append(
                f"{fg_name}.{feat_name} AS {fg_name}__{feat_name}"
            )

        # Load catalog to get join_keys
        catalog_path = parquet_path.parent / "_entity_catalog.json"
        join_keys = []
        if catalog_path.exists():
            from seeknal.workflow.consolidation.catalog import EntityCatalog  # ty: ignore[unresolved-import]
            catalog = EntityCatalog.load(catalog_path)
            if catalog:
                join_keys = catalog.join_keys

        # Build query
        key_cols = ", ".join(join_keys) + ", " if join_keys else ""
        select_clause = f"{key_cols}event_time, " + ", ".join(struct_selects)

        con = _duckdb.connect()
        try:
            if as_of:
                # Point-in-time: filter + dedup latest row per entity key
                if join_keys:
                    partition_by = ", ".join(join_keys)
                    query = (
                        f"SELECT {select_clause} FROM ("
                        f"  SELECT *, ROW_NUMBER() OVER ("
                        f"    PARTITION BY {partition_by} "
                        f"    ORDER BY event_time DESC"
                        f"  ) AS _rn "
                        f"  FROM '{parquet_path}' "
                        f"  WHERE event_time <= CAST('{as_of}' AS TIMESTAMP)"
                        f") WHERE _rn = 1"
                    )
                else:
                    query = (
                        f"SELECT {select_clause} "
                        f"FROM '{parquet_path}' "
                        f"WHERE event_time <= CAST('{as_of}' AS TIMESTAMP)"
                    )
            else:
                query = f"SELECT {select_clause} FROM '{parquet_path}'"

            return con.execute(query).df()
        finally:
            con.close()

    def entity(
        self,
        entity_name: str,
        as_of: Optional[str] = None,
    ) -> Any:
        """Retrieve all features for an entity from the consolidated view.

        Returns the full consolidated parquet with struct columns intact.
        Users can access struct fields via DuckDB dot notation.

        Args:
            entity_name: Entity name (e.g. "customer")
            as_of: Optional ISO date/datetime string for point-in-time filter

        Returns:
            pandas DataFrame with join_keys, event_time, and struct columns

        Raises:
            FileNotFoundError: If consolidated parquet doesn't exist
        """
        import duckdb as _duckdb

        parquet_path = self.target_dir / "feature_store" / entity_name / "features.parquet"
        if not parquet_path.exists():
            raise FileNotFoundError(
                f"No consolidated features found for entity '{entity_name}'. "
                f"Run `seeknal run` first, or check entity name."
            )

        # Load catalog to get join_keys for point-in-time dedup
        catalog_path = parquet_path.parent / "_entity_catalog.json"
        join_keys = []
        if catalog_path.exists():
            from seeknal.workflow.consolidation.catalog import EntityCatalog  # ty: ignore[unresolved-import]
            catalog = EntityCatalog.load(catalog_path)
            if catalog:
                join_keys = catalog.join_keys

        con = _duckdb.connect()
        try:
            if as_of:
                if join_keys:
                    partition_by = ", ".join(join_keys)
                    query = (
                        f"SELECT * FROM ("
                        f"  SELECT *, ROW_NUMBER() OVER ("
                        f"    PARTITION BY {partition_by} "
                        f"    ORDER BY event_time DESC"
                        f"  ) AS _rn "
                        f"  FROM '{parquet_path}' "
                        f"  WHERE event_time <= CAST('{as_of}' AS TIMESTAMP)"
                        f") WHERE _rn = 1"
                    )
                else:
                    query = (
                        f"SELECT * FROM '{parquet_path}' "
                        f"WHERE event_time <= CAST('{as_of}' AS TIMESTAMP)"
                    )
            else:
                query = f"SELECT * FROM '{parquet_path}'"

            return con.execute(query).df()
        finally:
            con.close()

    def close(self) -> None:
        """Clean up resources.

        Closes the DuckDB connection if it was opened.
        """
        if self._duckdb_con is not None:
            self._duckdb_con.close()
            self._duckdb_con = None
