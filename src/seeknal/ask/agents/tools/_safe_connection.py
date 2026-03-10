"""SafeConnection wrapper for DuckDB connections used by execute_python.

Routes all SQL queries through validate_sql_for_agent() so that
Python code execution cannot bypass the SQL security blocklist.
"""

from typing import Any


class SafeConnection:
    """DuckDB connection wrapper that validates all SQL before execution.

    Uses name-mangling (__conn) to prevent LLM-generated code from
    accessing the underlying connection directly via conn._conn.
    """

    def __init__(self, conn: Any) -> None:
        self.__conn = conn

    def _validate(self, query: str) -> None:
        from seeknal.ask.security import validate_sql_for_agent

        # Block multi-statement queries (semicolons)
        stripped = query.strip().rstrip(";")
        if ";" in stripped:
            raise ValueError(
                "Multi-statement queries are not allowed. "
                "Execute one statement at a time."
            )
        validate_sql_for_agent(query)

    def sql(self, query: str, *args: Any, **kwargs: Any) -> Any:
        """Execute SQL after validation."""
        self._validate(query)
        return self.__conn.sql(query, *args, **kwargs)

    def execute(self, query: str, *args: Any, **kwargs: Any) -> Any:
        """Execute SQL after validation."""
        self._validate(query)
        return self.__conn.execute(query, *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        """Delegate non-SQL methods to the underlying connection."""
        return getattr(self.__conn, name)
