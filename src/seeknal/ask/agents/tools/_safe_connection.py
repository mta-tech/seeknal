"""SafeConnection wrapper for DuckDB connections used by execute_python.

Routes all SQL queries through validate_sql_for_agent() so that
Python code execution cannot bypass the SQL security blocklist.
"""


class SafeConnection:
    """DuckDB connection wrapper that validates all SQL before execution."""

    def __init__(self, conn):
        self._conn = conn

    def sql(self, query: str, *args, **kwargs):
        """Execute SQL after validation."""
        from seeknal.ask.security import validate_sql_for_agent

        validate_sql_for_agent(query)
        return self._conn.sql(query, *args, **kwargs)

    def execute(self, query: str, *args, **kwargs):
        """Execute SQL after validation."""
        from seeknal.ask.security import validate_sql_for_agent

        validate_sql_for_agent(query)
        return self._conn.execute(query, *args, **kwargs)

    def __getattr__(self, name):
        """Delegate non-SQL methods to the underlying connection."""
        return getattr(self._conn, name)
