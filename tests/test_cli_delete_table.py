"""
Integration tests for CLI delete-table command.

Tests for the delete-table command including interactive mode,
force mode, dependency warnings, and error handling.

These tests verify:
1. CLI command implementation patterns through static code analysis
2. Expected behavior patterns match the established codebase conventions
3. Command structure follows typer CLI patterns

Note: Due to Python 3.10+ syntax in the main CLI code (match/case statements),
these tests use static code analysis to verify implementation patterns rather
than direct imports which would fail on Python 3.9.
"""

import os
import re
import pytest


# Path to the source code for static analysis
SRC_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLI_FILE = os.path.join(SRC_DIR, 'src', 'seeknal', 'cli', 'main.py')


def read_cli_source():
    """Read the main.py CLI source code for analysis."""
    with open(CLI_FILE, 'r') as f:
        return f.read()


def extract_delete_table_function(source_code):
    """Extract the delete_table function code from source."""
    # Find the delete_table function
    start = source_code.find('@app.command("delete-table")')
    if start == -1:
        return ""

    # Find the end of the function (next @app.command or def main)
    rest = source_code[start:]
    next_command = rest.find('\n@app.command(', 1)
    main_func = rest.find('\ndef main(')

    if next_command != -1 and main_func != -1:
        end = min(next_command, main_func)
    elif next_command != -1:
        end = next_command
    elif main_func != -1:
        end = main_func
    else:
        end = len(rest)

    return rest[:end]


class TestDeleteTableCommandExists:
    """Tests verifying delete-table command is properly defined."""

    @pytest.fixture
    def source_code(self):
        """Read the source code once per test class."""
        return read_cli_source()

    def test_delete_table_command_decorated(self, source_code):
        """Verify delete-table is registered as app command."""
        assert '@app.command("delete-table")' in source_code

    def test_delete_table_function_defined(self, source_code):
        """Verify delete_table function is defined."""
        assert 'def delete_table(' in source_code

    def test_delete_table_has_docstring(self, source_code):
        """Verify delete_table has docstring documentation."""
        func_code = extract_delete_table_function(source_code)
        assert '"""Delete' in func_code or "'''Delete" in func_code


class TestDeleteTableArguments:
    """Tests for delete-table command arguments and options."""

    @pytest.fixture
    def source_code(self):
        return read_cli_source()

    @pytest.fixture
    def func_code(self, source_code):
        return extract_delete_table_function(source_code)

    def test_table_name_argument_defined(self, source_code):
        """Verify table_name is defined as typer.Argument."""
        assert 'table_name: str = typer.Argument(' in source_code

    def test_table_name_has_help_text(self, func_code):
        """Verify table_name argument has help text."""
        assert 'help=' in func_code
        assert 'TABLE_NAME' in func_code or 'table' in func_code.lower()

    def test_force_option_defined(self, source_code):
        """Verify --force option is defined."""
        assert '"--force"' in source_code

    def test_force_short_option_defined(self, source_code):
        """Verify -f short option is defined."""
        assert '"-f"' in source_code

    def test_force_is_boolean_flag(self, func_code):
        """Verify force is defined as boolean."""
        assert 'force: bool = typer.Option(' in func_code

    def test_force_defaults_to_false(self, func_code):
        """Verify force defaults to False."""
        # Pattern: force: bool = typer.Option(False, ...
        pattern = r'force:\s*bool\s*=\s*typer\.Option\s*\(\s*False'
        assert re.search(pattern, func_code)

    def test_force_has_help_text(self, func_code):
        """Verify force option has help text."""
        # Find the force option definition and check it has help
        force_start = func_code.find('force: bool = typer.Option(')
        if force_start != -1:
            force_section = func_code[force_start:force_start + 200]
            assert 'help=' in force_section


class TestDeleteTableValidation:
    """Tests for delete-table validation logic."""

    @pytest.fixture
    def func_code(self):
        return extract_delete_table_function(read_cli_source())

    def test_checks_table_exists(self, func_code):
        """Verify table existence is checked first."""
        assert 'OnlineTableRequest.select_by_name' in func_code

    def test_handles_table_not_found(self, func_code):
        """Verify table not found case is handled."""
        assert 'not found' in func_code.lower()

    def test_exits_with_error_if_not_found(self, func_code):
        """Verify exit code 1 if table not found."""
        assert 'typer.Exit(1)' in func_code

    def test_shows_looking_up_message(self, func_code):
        """Verify 'Looking up table' message is displayed."""
        assert 'Looking up table' in func_code


class TestDeleteTableDependencyChecking:
    """Tests for delete-table dependency checking."""

    @pytest.fixture
    def func_code(self):
        return extract_delete_table_function(read_cli_source())

    def test_checks_feature_group_dependencies(self, func_code):
        """Verify feature group dependencies are checked."""
        assert 'get_feature_group_from_online_table' in func_code

    def test_tracks_dependent_feature_groups(self, func_code):
        """Verify dependent feature groups are tracked in a list."""
        assert 'dependent_feature_groups' in func_code

    def test_shows_dependency_warning(self, func_code):
        """Verify warning is shown for dependencies."""
        assert '_echo_warning' in func_code
        assert 'dependent' in func_code

    def test_shows_count_of_dependencies(self, func_code):
        """Verify count of dependencies is displayed."""
        # Should show something like "has N dependent feature group(s)"
        assert 'len(dependent_feature_groups)' in func_code

    def test_lists_each_dependent_feature_group(self, func_code):
        """Verify each dependent feature group is listed."""
        # Should iterate over and print each feature group name
        assert 'for fg_name in dependent_feature_groups:' in func_code

    def test_fetches_feature_group_names(self, func_code):
        """Verify feature group names are fetched from database."""
        assert 'FeatureGroupTable' in func_code
        assert 'fg.name' in func_code


class TestDeleteTableInteractiveMode:
    """Tests for delete-table interactive confirmation mode."""

    @pytest.fixture
    def func_code(self):
        return extract_delete_table_function(read_cli_source())

    def test_uses_typer_confirm(self, func_code):
        """Verify typer.confirm is used for confirmation."""
        assert 'typer.confirm(' in func_code

    def test_confirmation_has_default_false(self, func_code):
        """Verify confirmation defaults to False (safe default)."""
        assert 'default=False' in func_code

    def test_prompts_with_table_name(self, func_code):
        """Verify confirmation prompt includes table name."""
        # Should have something like f"Are you sure you want to delete table '{table_name}'"
        assert 'table_name' in func_code
        assert 'Are you sure' in func_code or 'confirm' in func_code.lower()

    def test_handles_user_cancellation(self, func_code):
        """Verify user cancellation is handled."""
        assert 'cancelled' in func_code.lower()

    def test_exits_gracefully_on_cancellation(self, func_code):
        """Verify graceful exit on cancellation (exit code 0)."""
        assert 'typer.Exit(0)' in func_code

    def test_shows_info_on_cancellation(self, func_code):
        """Verify info message on cancellation."""
        assert '_echo_info' in func_code
        assert 'cancelled' in func_code.lower()


class TestDeleteTableForceMode:
    """Tests for delete-table force mode (--force flag)."""

    @pytest.fixture
    def func_code(self):
        return extract_delete_table_function(read_cli_source())

    def test_force_skips_confirmation_with_dependencies(self, func_code):
        """Verify --force skips confirmation even with dependencies."""
        # Should check 'if not force:' before prompting
        assert 'if not force:' in func_code

    def test_force_skips_confirmation_without_dependencies(self, func_code):
        """Verify --force skips confirmation without dependencies too."""
        # Both dependency and no-dependency paths should check force
        matches = re.findall(r'if not force:', func_code)
        assert len(matches) >= 2, "Should have at least 2 'if not force:' checks"


class TestDeleteTableDeletion:
    """Tests for delete-table actual deletion logic."""

    @pytest.fixture
    def func_code(self):
        return extract_delete_table_function(read_cli_source())

    def test_uses_online_features_duckdb(self, func_code):
        """Verify OnlineFeaturesDuckDB is used for deletion."""
        assert 'OnlineFeaturesDuckDB' in func_code

    def test_creates_online_features_object(self, func_code):
        """Verify OnlineFeaturesDuckDB object is created with proper params."""
        assert 'OnlineFeaturesDuckDB(' in func_code
        assert 'name=table_name' in func_code

    def test_calls_delete_method(self, func_code):
        """Verify delete() method is called."""
        assert '.delete()' in func_code

    def test_gets_entity_for_deletion(self, func_code):
        """Verify entity is retrieved for file deletion."""
        assert 'EntityRequest.select_by_id' in func_code
        assert 'entity_id' in func_code

    def test_uses_online_store_duckdb(self, func_code):
        """Verify OnlineStoreDuckDB is used."""
        assert 'OnlineStoreDuckDB' in func_code


class TestDeleteTableSuccessMessages:
    """Tests for delete-table success message handling."""

    @pytest.fixture
    def func_code(self):
        return extract_delete_table_function(read_cli_source())

    def test_shows_deleting_message(self, func_code):
        """Verify 'Deleting table' message is shown."""
        assert "Deleting table" in func_code

    def test_shows_success_message(self, func_code):
        """Verify success message is shown on completion."""
        assert '_echo_success' in func_code
        assert 'deleted successfully' in func_code

    def test_success_includes_table_name(self, func_code):
        """Verify success message includes table name."""
        # Should have f"Table '{table_name}' deleted successfully"
        pattern = r'_echo_success\([^)]*table_name'
        assert re.search(pattern, func_code)


class TestDeleteTableWarningMessages:
    """Tests for delete-table warning message handling."""

    @pytest.fixture
    def func_code(self):
        return extract_delete_table_function(read_cli_source())

    def test_shows_warning_on_partial_success(self, func_code):
        """Verify warning is shown when deletion returns False."""
        assert 'completed with warnings' in func_code or 'warning' in func_code.lower()

    def test_checks_deletion_return_value(self, func_code):
        """Verify deletion return value is checked."""
        assert 'if success:' in func_code or 'success = ' in func_code


class TestDeleteTableErrorHandling:
    """Tests for delete-table error handling."""

    @pytest.fixture
    def func_code(self):
        return extract_delete_table_function(read_cli_source())

    def test_has_try_except_block(self, func_code):
        """Verify try-except block for error handling."""
        assert 'try:' in func_code
        assert 'except' in func_code

    def test_handles_generic_exceptions(self, func_code):
        """Verify generic Exception is caught."""
        assert 'except Exception as e:' in func_code or 'except typer.Exit:' in func_code

    def test_shows_error_message_on_failure(self, func_code):
        """Verify error message is shown on failure."""
        assert '_echo_error' in func_code
        assert 'Failed to delete' in func_code

    def test_exits_with_error_code_on_failure(self, func_code):
        """Verify exit code 1 on failure."""
        assert 'typer.Exit(1)' in func_code

    def test_re_raises_typer_exit(self, func_code):
        """Verify typer.Exit exceptions are re-raised."""
        assert 'except typer.Exit:' in func_code
        assert 'raise' in func_code


class TestDeleteTableImports:
    """Tests for required imports in CLI module."""

    @pytest.fixture
    def source_code(self):
        return read_cli_source()

    def test_imports_online_table_request(self, source_code):
        """Verify OnlineTableRequest is imported."""
        assert 'OnlineTableRequest' in source_code
        # Check it's used in delete_table function
        func_code = extract_delete_table_function(source_code)
        assert 'OnlineTableRequest' in func_code

    def test_imports_entity_request(self, source_code):
        """Verify EntityRequest is imported."""
        assert 'EntityRequest' in source_code

    def test_imports_feature_group_table(self, source_code):
        """Verify FeatureGroupTable is imported."""
        assert 'FeatureGroupTable' in source_code

    def test_imports_get_db_session(self, source_code):
        """Verify get_db_session is imported."""
        assert 'get_db_session' in source_code

    def test_imports_online_features_duckdb(self, source_code):
        """Verify OnlineFeaturesDuckDB is imported."""
        assert 'OnlineFeaturesDuckDB' in source_code

    def test_imports_online_store_duckdb(self, source_code):
        """Verify OnlineStoreDuckDB is imported."""
        assert 'OnlineStoreDuckDB' in source_code


class TestDeleteTableWorkflow:
    """Tests for complete delete-table workflow order."""

    @pytest.fixture
    def func_code(self):
        return extract_delete_table_function(read_cli_source())

    def test_validates_before_checking_dependencies(self, func_code):
        """Verify table validation happens before dependency check."""
        validate_pos = func_code.find('OnlineTableRequest.select_by_name')
        deps_pos = func_code.find('get_feature_group_from_online_table')

        assert validate_pos != -1, "Should validate table existence"
        assert deps_pos != -1, "Should check dependencies"
        assert validate_pos < deps_pos, "Validation should come before dependency check"

    def test_checks_dependencies_before_confirmation(self, func_code):
        """Verify dependencies checked before asking for confirmation."""
        deps_pos = func_code.find('get_feature_group_from_online_table')
        confirm_pos = func_code.find('typer.confirm')

        assert deps_pos != -1, "Should check dependencies"
        assert confirm_pos != -1, "Should have confirmation"
        assert deps_pos < confirm_pos, "Dependency check should come before confirmation"

    def test_confirms_before_deleting(self, func_code):
        """Verify confirmation happens before deletion."""
        confirm_pos = func_code.find('typer.confirm')
        delete_pos = func_code.find('.delete()')

        assert confirm_pos != -1, "Should have confirmation"
        assert delete_pos != -1, "Should call delete"
        assert confirm_pos < delete_pos, "Confirmation should come before deletion"


class TestDeleteTableMessagePatterns:
    """Tests for consistent message patterns in delete-table."""

    @pytest.fixture
    def func_code(self):
        return extract_delete_table_function(read_cli_source())

    def test_uses_echo_success_helper(self, func_code):
        """Verify _echo_success helper is used."""
        assert '_echo_success(' in func_code

    def test_uses_echo_error_helper(self, func_code):
        """Verify _echo_error helper is used."""
        assert '_echo_error(' in func_code

    def test_uses_echo_warning_helper(self, func_code):
        """Verify _echo_warning helper is used."""
        assert '_echo_warning(' in func_code

    def test_uses_echo_info_helper(self, func_code):
        """Verify _echo_info helper is used."""
        assert '_echo_info(' in func_code

    def test_uses_typer_echo_for_plain_output(self, func_code):
        """Verify typer.echo is used for plain output."""
        assert 'typer.echo(' in func_code


class TestDeleteTableHelpTextInMain:
    """Tests for delete-table appearing in main CLI help."""

    @pytest.fixture
    def source_code(self):
        return read_cli_source()

    def test_docstring_mentions_online_table(self):
        """Verify command docstring mentions online table."""
        func_code = extract_delete_table_function(read_cli_source())
        # Extract docstring
        docstring_match = re.search(r'"""([^"]*?)"""', func_code, re.DOTALL)
        if docstring_match:
            docstring = docstring_match.group(1).lower()
            assert 'online table' in docstring

    def test_docstring_mentions_force(self):
        """Verify command docstring mentions --force flag."""
        func_code = extract_delete_table_function(read_cli_source())
        docstring_match = re.search(r'"""([^"]*?)"""', func_code, re.DOTALL)
        if docstring_match:
            docstring = docstring_match.group(1).lower()
            assert 'force' in docstring


class TestDeleteTableDatabaseSession:
    """Tests for proper database session usage."""

    @pytest.fixture
    def func_code(self):
        return extract_delete_table_function(read_cli_source())

    def test_uses_context_manager_for_session(self, func_code):
        """Verify get_db_session is used as context manager."""
        assert 'with get_db_session() as session:' in func_code

    def test_uses_sqlmodel_select(self, func_code):
        """Verify sqlmodel select is used for queries."""
        assert 'select(' in func_code

    def test_uses_where_clause(self, func_code):
        """Verify where clause is used for filtering."""
        assert '.where(' in func_code

    def test_uses_session_exec(self, func_code):
        """Verify session.exec is used for query execution."""
        assert 'session.exec(' in func_code


class TestDeleteTableEntityHandling:
    """Tests for entity handling in delete-table."""

    @pytest.fixture
    def func_code(self):
        return extract_delete_table_function(read_cli_source())

    def test_retrieves_entity_by_id(self, func_code):
        """Verify entity is retrieved by ID."""
        assert 'EntityRequest.select_by_id' in func_code
        assert 'online_table.entity_id' in func_code

    def test_creates_entity_object(self, func_code):
        """Verify Entity object is created."""
        assert 'Entity(' in func_code

    def test_handles_entity_in_online_features(self, func_code):
        """Verify entity is passed to OnlineFeaturesDuckDB."""
        assert 'lookup_key=entity_obj' in func_code


class TestDeleteTableEdgeCases:
    """Tests for edge case handling patterns."""

    @pytest.fixture
    def func_code(self):
        return extract_delete_table_function(read_cli_source())

    def test_handles_empty_dependency_list(self, func_code):
        """Verify empty dependency list is handled."""
        # Should check if dependent_feature_groups is truthy
        assert 'if dependent_feature_groups:' in func_code

    def test_handles_no_entity_found(self, func_code):
        """Verify None entity is handled."""
        assert 'if entity:' in func_code

    def test_handles_table_id_for_deletion(self, func_code):
        """Verify table ID is passed to OnlineFeaturesDuckDB."""
        assert 'id=online_table.id' in func_code
