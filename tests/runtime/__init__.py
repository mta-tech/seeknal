import sys
import types

# Stub missing unsealed seeknal modules if not present
if "seeknal.validation" not in sys.modules:
    val_mod = types.ModuleType("seeknal.validation")
    val_mod.validate_sql_identifier = lambda *a, **kw: None
    val_mod.validate_table_name = lambda *a, **kw: None
    val_mod.validate_column_name = lambda *a, **kw: None
    val_mod.validate_column_names = lambda *a, **kw: None
    val_mod.validate_database_name = lambda *a, **kw: None
    val_mod.validate_schema_name = lambda *a, **kw: None
    val_mod.validate_file_path = lambda *a, **kw: None
    val_mod.validate_sql_value = lambda *a, **kw: None
    sys.modules["seeknal.validation"] = val_mod

if "seeknal.exceptions" not in sys.modules:
    exc_mod = types.ModuleType("seeknal.exceptions")

    class InvalidIdentifierError(Exception):
        pass

    class InvalidPathError(Exception):
        pass

    exc_mod.InvalidIdentifierError = InvalidIdentifierError
    exc_mod.InvalidPathError = InvalidPathError
    sys.modules["seeknal.exceptions"] = exc_mod
