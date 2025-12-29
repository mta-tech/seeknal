"""Quick integration check for validation module."""
import pytest
from seeknal.validation import (
    validate_sql_identifier,
    validate_table_name,
    validate_column_name,
    validate_database_name,
    validate_schema_name,
    validate_file_path,
    validate_sql_value,
    validate_column_names,
)
from seeknal.exceptions import InvalidIdentifierError, InvalidPathError
from seeknal.tasks.duckdb import DuckDBTask


def test_validation_module_imports():
    """Test that validation functions are properly exposed."""
    assert validate_table_name('users') == 'users'
    assert validate_column_name('id') == 'id'
    assert validate_database_name('test_db') == 'test_db'
    assert validate_schema_name('public') == 'public'
    assert validate_file_path('/path/to/file.parquet') == '/path/to/file.parquet'
    assert validate_sql_value('test_value') == 'test_value'
    assert validate_column_names(['col1', 'col2']) == ['col1', 'col2']


def test_duckdb_validation_integration():
    """Test DuckDB task uses validation."""
    task = DuckDBTask()
    task.add_input(path='/data/test.parquet')
    assert task.input['path'] == '/data/test.parquet'


def test_duckdb_rejects_sql_injection():
    """Test DuckDB task rejects SQL injection in path."""
    task = DuckDBTask()
    with pytest.raises(InvalidPathError):
        task.add_input(path="/data/file'; DROP TABLE users; --")


if __name__ == '__main__':
    test_validation_module_imports()
    test_duckdb_validation_integration()
    print("All integration checks passed!")
