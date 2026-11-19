"""DuckDB 1.4.x / 2.0.x Arrow result conversion helpers.

DuckDB 2.0 removed ``fetch_arrow_table()`` and ``arrow()`` from
``DuckDBPyConnection`` in favor of ``to_arrow_table()`` and
``to_arrow_reader()``, while DuckDB 1.4.x connections and relations expose a
mix of ``to_arrow_*``, ``fetch_arrow_*``, and ``arrow()`` (returning either a
``pyarrow.Table`` or a ``pyarrow.RecordBatchReader`` depending on object and
version).

These helpers normalize both connection results (``con.execute(...)``) and
relations (``con.sql(...)``, ``con.table(...)``) across DuckDB 1.4.x and 2.0.x.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa

__all__ = [
    "result_to_arrow_table",
    "result_to_arrow_reader",
]


def _coerce_to_arrow_table(arrow_obj: Any) -> pa.Table:
    import pyarrow as pa

    if isinstance(arrow_obj, pa.Table):
        return arrow_obj
    read_all = getattr(arrow_obj, "read_all", None)
    if isinstance(arrow_obj, pa.RecordBatchReader) or callable(read_all):
        table = arrow_obj.read_all()
        if isinstance(table, pa.Table):
            return table
        raise TypeError(
            f"Expected read_all() to return pyarrow.Table, got {type(table).__name__}"
        )
    raise TypeError(
        f"Expected pyarrow.Table or pyarrow.RecordBatchReader, got {type(arrow_obj).__name__}"
    )


def _coerce_to_arrow_reader(arrow_obj: Any) -> pa.RecordBatchReader:
    import pyarrow as pa

    if isinstance(arrow_obj, pa.RecordBatchReader):
        return arrow_obj
    if isinstance(arrow_obj, pa.Table):
        return pa.RecordBatchReader.from_batches(
            arrow_obj.schema, arrow_obj.to_batches()
        )
    read_all = getattr(arrow_obj, "read_all", None)
    if callable(read_all):
        table = read_all()
        if isinstance(table, pa.Table):
            return pa.RecordBatchReader.from_batches(
                table.schema, table.to_batches()
            )
    raise TypeError(
        f"Expected pyarrow.RecordBatchReader or pyarrow.Table, got {type(arrow_obj).__name__}"
    )


def result_to_arrow_table(result: Any) -> pa.Table:
    """Convert a DuckDB execution result or relation into a ``pyarrow.Table``.

    Prefers ``to_arrow_table()`` when available, falling back to
    ``fetch_arrow_table()``, ``arrow()``, ``to_arrow_reader()``, or
    ``fetch_arrow_reader()``. If the underlying call returns a
    ``pyarrow.RecordBatchReader``, ``.read_all()`` is called so a
    ``pyarrow.Table`` is always returned.
    """
    for method_name in (
        "to_arrow_table",
        "fetch_arrow_table",
        "arrow",
        "to_arrow_reader",
        "fetch_arrow_reader",
    ):
        method = getattr(result, method_name, None)
        if callable(method):
            return _coerce_to_arrow_table(method())

    raise TypeError(
        f"Cannot convert {type(result).__name__} to pyarrow.Table: "
        "no supported DuckDB Arrow conversion method found"
    )


def result_to_arrow_reader(result: Any) -> pa.RecordBatchReader:
    """Convert a DuckDB execution result or relation into a ``pyarrow.RecordBatchReader``.

    Prefers ``to_arrow_reader()`` when available, falling back to
    ``fetch_arrow_reader()``, ``arrow()``, ``fetch_arrow_table()``, or
    ``to_arrow_table()``. If the underlying call returns a ``pyarrow.Table``,
    it is wrapped via ``pa.RecordBatchReader.from_batches(tbl.schema, tbl.to_batches())``
    so a ``pyarrow.RecordBatchReader`` is always returned.
    """
    for method_name in (
        "to_arrow_reader",
        "fetch_arrow_reader",
        "arrow",
        "fetch_arrow_table",
        "to_arrow_table",
    ):
        method = getattr(result, method_name, None)
        if callable(method):
            return _coerce_to_arrow_reader(method())

    raise TypeError(
        f"Cannot convert {type(result).__name__} to pyarrow.RecordBatchReader: "
        "no supported DuckDB Arrow conversion method found"
    )
