import pyarrow.parquet as pq
import pytest
from seeknal.tasks.duckdb import DuckDBTask
from seeknal.exceptions import InvalidPathError
import os


def test_duckdb_task():

    arrow_df = pq.read_table("tests/data/poi_sample.parquet")
    my_duckdb = (
        DuckDBTask()
        .add_input(dataframe=arrow_df)
        .add_sql("SELECT poi_name, lat, long FROM __THIS__")
        .add_sql("SELECT poi_name, lat FROM __THIS__")
    )
    res = my_duckdb.transform()

    assert res is not None


def test_duckdb_task_with_path():

    my_duckdb = (
        DuckDBTask()
        .add_input(path="tests/data/poi_sample.parquet/part-0.parquet")
        .add_sql("SELECT poi_name, lat, long FROM __THIS__")
        .add_sql("SELECT poi_name, lat FROM __THIS__")
    )
    res = my_duckdb.transform()

    assert res is not None


class TestDuckDBPathValidation:
    """Tests for path validation in DuckDBTask.add_input()."""

    # Valid path tests
    def test_valid_simple_path(self):
        """Test that simple valid paths are accepted."""
        task = DuckDBTask()
        task.add_input(path="/data/file.parquet")
        assert task.input["path"] == "/data/file.parquet"

    def test_valid_relative_path(self):
        """Test that relative paths are accepted."""
        task = DuckDBTask()
        task.add_input(path="./data/file.parquet")
        assert task.input["path"] == "./data/file.parquet"

    def test_valid_path_with_underscores(self):
        """Test that paths with underscores are accepted."""
        task = DuckDBTask()
        task.add_input(path="/data/my_file_name.parquet")
        assert task.input["path"] == "/data/my_file_name.parquet"

    def test_valid_path_with_hyphens(self):
        """Test that paths with hyphens are accepted."""
        task = DuckDBTask()
        task.add_input(path="/data/my-file-name.parquet")
        assert task.input["path"] == "/data/my-file-name.parquet"

    def test_valid_path_with_spaces(self):
        """Test that paths with spaces are accepted (valid for paths)."""
        task = DuckDBTask()
        task.add_input(path="/data/my file name.parquet")
        assert task.input["path"] == "/data/my file name.parquet"

    def test_valid_s3_path(self):
        """Test that S3 paths are accepted."""
        task = DuckDBTask()
        task.add_input(path="s3://bucket/path/to/file.parquet")
        assert task.input["path"] == "s3://bucket/path/to/file.parquet"

    # Invalid path tests - SQL injection attempts with single quote
    def test_invalid_path_with_single_quote_raises_error(self):
        """Test that path with single quote raises InvalidPathError."""
        task = DuckDBTask()
        with pytest.raises(InvalidPathError) as exc_info:
            task.add_input(path="/data/file'name.parquet")
        assert "forbidden character" in str(exc_info.value)
        assert "'" in str(exc_info.value)

    def test_invalid_path_sql_injection_single_quote(self):
        """Test SQL injection with single quote is blocked."""
        task = DuckDBTask()
        with pytest.raises(InvalidPathError):
            task.add_input(path="/data/'; DROP TABLE users; --")

    # Invalid path tests - SQL injection attempts with double quote
    def test_invalid_path_with_double_quote_raises_error(self):
        """Test that path with double quote raises InvalidPathError."""
        task = DuckDBTask()
        with pytest.raises(InvalidPathError) as exc_info:
            task.add_input(path='/data/file"name.parquet')
        assert "forbidden character" in str(exc_info.value)

    def test_invalid_path_sql_injection_double_quote(self):
        """Test SQL injection with double quote is blocked."""
        task = DuckDBTask()
        with pytest.raises(InvalidPathError):
            task.add_input(path='/data/file.parquet"; DELETE FROM data;')

    # Invalid path tests - SQL injection attempts with semicolon
    def test_invalid_path_with_semicolon_raises_error(self):
        """Test that path with semicolon raises InvalidPathError."""
        task = DuckDBTask()
        with pytest.raises(InvalidPathError) as exc_info:
            task.add_input(path="/data/file;name.parquet")
        assert "forbidden character" in str(exc_info.value)
        assert ";" in str(exc_info.value)

    def test_invalid_path_stacked_query_injection(self):
        """Test stacked query SQL injection is blocked."""
        task = DuckDBTask()
        with pytest.raises(InvalidPathError):
            task.add_input(path="/data/file.parquet; DROP TABLE users")

    # Invalid path tests - SQL comment injection
    def test_invalid_path_with_sql_comment_raises_error(self):
        """Test that path with SQL comment (--) raises InvalidPathError."""
        task = DuckDBTask()
        with pytest.raises(InvalidPathError) as exc_info:
            task.add_input(path="/data/file--comment.parquet")
        assert "forbidden character" in str(exc_info.value)
        assert "--" in str(exc_info.value)

    def test_invalid_path_comment_injection(self):
        """Test SQL comment injection is blocked."""
        task = DuckDBTask()
        with pytest.raises(InvalidPathError):
            task.add_input(path="/data/file.parquet--comment to ignore")

    # Invalid path tests - SQL block comment injection
    def test_invalid_path_with_block_comment_start_raises_error(self):
        """Test that path with block comment start (/*) raises InvalidPathError."""
        task = DuckDBTask()
        with pytest.raises(InvalidPathError) as exc_info:
            task.add_input(path="/data/file/*comment.parquet")
        assert "forbidden character" in str(exc_info.value)

    def test_invalid_path_with_block_comment_end_raises_error(self):
        """Test that path with block comment end (*/) raises InvalidPathError."""
        task = DuckDBTask()
        with pytest.raises(InvalidPathError) as exc_info:
            task.add_input(path="/data/file*/comment.parquet")
        assert "forbidden character" in str(exc_info.value)

    def test_invalid_path_block_comment_injection(self):
        """Test block comment SQL injection is blocked."""
        task = DuckDBTask()
        with pytest.raises(InvalidPathError):
            task.add_input(path="/data/file.parquet/**/")

    # Complex SQL injection attempts
    def test_invalid_path_union_injection(self):
        """Test UNION-based SQL injection via path is blocked."""
        task = DuckDBTask()
        with pytest.raises(InvalidPathError):
            task.add_input(path="/data/'; UNION SELECT * FROM passwords --")

    def test_invalid_path_drop_table_injection(self):
        """Test DROP TABLE SQL injection via path is blocked."""
        task = DuckDBTask()
        with pytest.raises(InvalidPathError):
            task.add_input(path="/data/file'; DROP TABLE users; --")

    def test_invalid_path_delete_injection(self):
        """Test DELETE SQL injection via path is blocked."""
        task = DuckDBTask()
        with pytest.raises(InvalidPathError):
            task.add_input(path="/data/file'; DELETE FROM users WHERE 1=1; --")

    def test_invalid_path_multiple_injection_chars(self):
        """Test path with multiple injection characters is blocked."""
        task = DuckDBTask()
        with pytest.raises(InvalidPathError):
            task.add_input(path="/data/'; --/**/\"")

    # Empty path test
    def test_empty_path_raises_error(self):
        """Test that empty path raises InvalidPathError."""
        task = DuckDBTask()
        with pytest.raises(InvalidPathError) as exc_info:
            task.add_input(path="")
        assert "cannot be empty" in str(exc_info.value)

    # Dataframe input should work regardless
    def test_dataframe_input_bypasses_path_validation(self):
        """Test that dataframe input doesn't trigger path validation."""
        arrow_df = pq.read_table("tests/data/poi_sample.parquet")
        task = DuckDBTask()
        task.add_input(dataframe=arrow_df)
        assert "dataframe" in task.input
        assert "path" not in task.input
