import pytest
from unittest import mock
import typer # For mocking typer.echo

from seeknal.workspace import Workspace, require_workspace
from seeknal.context import Context, SETTINGS
from seeknal.exceptions import ProjectNotSetError
from seeknal.account import Account
from seeknal.featurestore.featurestore import OfflineStore # Assuming this is the correct import path
from seeknal.api_request.rest_api_request import WorkspaceRequest # Assuming this is the correct import path


# Fixtures
@pytest.fixture
def mock_settings():
    """Fixture to manage SETTINGS, ensuring a clean state for each test."""
    original_settings = SETTINGS.copy()
    yield SETTINGS # Provide the global SETTINGS object for modification
    SETTINGS.clear()
    SETTINGS.update(original_settings)

@pytest.fixture
def mock_account_get_username(mocker):
    return mocker.patch.object(Account, 'get_username')

@pytest.fixture
def mock_workspace_request_select_by_name(mocker):
    return mocker.patch.object(WorkspaceRequest, 'select_by_name_and_project_id')

@pytest.fixture
def mock_workspace_request_select_by_id(mocker):
    return mocker.patch.object(WorkspaceRequest, 'select_by_id')

@pytest.fixture
def mock_workspace_request_save(mocker):
    # Mocking the instance method save
    return mocker.patch.object(WorkspaceRequest, 'save')


@pytest.fixture
def mock_offline_store_get_or_create(mocker):
    return mocker.patch.object(OfflineStore, 'get_or_create')

@pytest.fixture
def mock_typer_echo(mocker):
    return mocker.patch('typer.echo')


# Tests for Workspace Class
class TestWorkspace:

    def test_post_init_default_name(self, mock_account_get_username, mock_settings):
        """Test that name is set to Account.get_username() if not provided and snake_cased."""
        mock_account_get_username.return_value = "TestUser"
        SETTINGS['project_id'] = 'test_project' # Required for Workspace initialization
        ws = Workspace()
        assert ws.name == "test_user"
        mock_account_get_username.assert_called_once()

    def test_post_init_provided_name_snake_case(self, mock_settings):
        """Test that a provided name is converted to snake_case."""
        SETTINGS['project_id'] = 'test_project' # Required for Workspace initialization
        ws = Workspace(name="MyCustomWorkspace")
        assert ws.name == "my_custom_workspace"

    def test_post_init_provided_name_already_snake_case(self, mock_settings):
        """Test that a provided name already in snake_case remains unchanged."""
        SETTINGS['project_id'] = 'test_project' # Required for Workspace initialization
        ws = Workspace(name="my_custom_workspace")
        assert ws.name == "my_custom_workspace"

    def test_get_or_create_workspace_does_not_exist(self, mock_settings, mock_workspace_request_select_by_name, mock_workspace_request_save, mock_typer_echo):
        """Test get_or_create when workspace does not exist."""
        SETTINGS['project_id'] = 'test_project'
        ws = Workspace(name="new_workspace")

        mock_workspace_request_select_by_name.return_value = None
        # Mock the save method on the instance of WorkspaceRequest that will be created
        mock_save_instance = mock.MagicMock()
        mock_save_instance.id = "new_ws_id_from_save"
        mock_workspace_request_save.return_value = mock_save_instance
        
        # mock the WorkspaceRequest constructor to return an object that has a save method
        with mock.patch('seeknal.workspace.WorkspaceRequest') as mock_wr_constructor:
            # Configure the instance returned by the constructor
            mock_wr_instance = mock.MagicMock()
            mock_wr_instance.save.return_value = mock_save_instance
            mock_wr_constructor.return_value = mock_wr_instance

            result_ws = ws.get_or_create()

            mock_workspace_request_select_by_name.assert_called_once_with(name="new_workspace", project_id="test_project")
            
            # Check that WorkspaceRequest was constructed with the correct parameters
            mock_wr_constructor.assert_called_once_with(
                name="new_workspace",
                project_id="test_project",
                description=None, # Assuming default description
                creator_id=None # Assuming creator_id is not set in this flow or handled by Account
            )
            mock_wr_instance.save.assert_called_once() # Check that save was called on the instance
            mock_typer_echo.assert_any_call("Workspace 'new_workspace' created successfully.") # Using any_call for flexibility
            assert SETTINGS.get("workspace_id") == "new_ws_id_from_save"
            assert result_ws.id == "new_ws_id_from_save"
            assert result_ws is ws # Should return the same instance

    def test_get_or_create_workspace_exists(self, mock_settings, mock_workspace_request_select_by_name, mock_workspace_request_save, mock_typer_echo):
        """Test get_or_create when workspace already exists."""
        SETTINGS['project_id'] = 'test_project'
        ws = Workspace(name="existing_workspace")

        mock_existing_ws_data = mock.MagicMock()
        mock_existing_ws_data.id = "existing_ws_id"
        mock_existing_ws_data.name = "existing_workspace" # Name from DB
        mock_existing_ws_data.description = "Existing Description"
        mock_existing_ws_data.creator_id = "creator"
        mock_existing_ws_data.created_at = "timestamp"
        mock_existing_ws_data.offline_store_id = "offline_id"


        mock_workspace_request_select_by_name.return_value = mock_existing_ws_data
        
        result_ws = ws.get_or_create()

        mock_workspace_request_select_by_name.assert_called_once_with(name="existing_workspace", project_id="test_project")
        mock_workspace_request_save.assert_not_called() # Save should not be called on the class itself
        
        # Also check that the instance of WorkspaceRequest, if created, its save is not called
        # This is implicitly tested by not mocking the constructor and its instance's save method to be called.

        mock_typer_echo.assert_any_call("Workspace 'existing_workspace' already exists.")
        assert SETTINGS.get("workspace_id") == "existing_ws_id"
        assert result_ws.id == "existing_ws_id"
        assert result_ws.name == "existing_workspace" # Name should be updated from fetched data
        assert result_ws.description == "Existing Description"
        assert result_ws.creator_id == "creator"
        assert result_ws.created_at == "timestamp"
        assert result_ws.offline_store_id == "offline_id"
        assert result_ws is ws # Should return the same instance

    def test_attach_offline_store(self, mock_settings, mock_offline_store_get_or_create, mock_typer_echo):
        """Test attach_offline_store successfully attaches an offline store."""
        SETTINGS['project_id'] = 'test_project'
        ws = Workspace(name="my_workspace")
        ws.id = "ws_id_123" # Simulate that get_or_create has been called

        mock_offline_store_instance = mock.MagicMock(spec=OfflineStore)
        mock_offline_store_instance.id = "offline_store_id_456"
        mock_offline_store_get_or_create.return_value = mock_offline_store_instance
        
        # Mock the WorkspaceRequest constructor and its save method
        with mock.patch('seeknal.workspace.WorkspaceRequest') as mock_wr_constructor:
            mock_wr_instance = mock.MagicMock()
            mock_wr_constructor.return_value = mock_wr_instance

            # Create a dummy offline_store object to pass to the method
            dummy_offline_store = OfflineStore(name="test_store", type="test_type", options={}) 
            
            ws.attach_offline_store(dummy_offline_store)

            # Verify OfflineStore.get_or_create was called on the passed object
            mock_offline_store_get_or_create.assert_called_once_with()
            
            # Verify WorkspaceRequest was initialized with the correct body
            # The actual body construction is inside WorkspaceRequest, so we check the args to constructor
            # and that save was called.
            expected_wr_call_args = {
                "id": "ws_id_123", # id should be passed for update
                "name": "my_workspace",
                "project_id": "test_project",
                "offline_store_id": "offline_store_id_456",
                # description and creator_id are not explicitly set here,
                # so they should be their defaults or whatever Workspace sets.
            }
            
            # Check that the constructor was called with arguments that include the expected ones
            called_args, called_kwargs = mock_wr_constructor.call_args
            assert called_kwargs['id'] == expected_wr_call_args['id']
            assert called_kwargs['name'] == expected_wr_call_args['name']
            assert called_kwargs['project_id'] == expected_wr_call_args['project_id']
            assert called_kwargs['offline_store_id'] == expected_wr_call_args['offline_store_id']

            mock_wr_instance.save.assert_called_once()
            mock_typer_echo.assert_any_call(
                "Offline store 'test_store' attached to workspace 'my_workspace' successfully."
            )
            assert ws.offline_store_id == "offline_store_id_456"

    @staticmethod
    def test_current_workspace_id_set(mock_settings, mock_workspace_request_select_by_id):
        """Test Workspace.current() when workspace_id is set in context."""
        SETTINGS['workspace_id'] = "current_ws_id"
        mock_ws_data = mock.MagicMock()
        mock_ws_data.id = "current_ws_id"
        mock_ws_data.name = "Current Workspace"
        # Add other attributes as necessary if Workspace class expects them upon construction from data
        mock_workspace_request_select_by_id.return_value = mock_ws_data

        # Mock the constructor of Workspace to verify it's called with the fetched data
        with mock.patch('seeknal.workspace.Workspace') as mock_ws_constructor:
            mock_ws_instance = mock.MagicMock(spec=Workspace) # This will be the instance returned by Workspace()
            mock_ws_constructor.return_value = mock_ws_instance # Workspace() will return this
            
            current_ws_instance = Workspace.current()

            mock_workspace_request_select_by_id.assert_called_once_with(id="current_ws_id")
            # Verify that the Workspace constructor was called with the attributes from mock_ws_data
            # This assumes Workspace constructor can take these attributes as kwargs
            # or has a from_orm/from_dict style method that current() would use.
            # Based on the provided Workspace snippet, it takes them as direct kwargs.
            mock_ws_constructor.assert_called_once_with(
                id="current_ws_id",
                name="Current Workspace",
                # ensure other attributes that would be returned by select_by_id and passed to constructor are listed
            )
            assert current_ws_instance is mock_ws_instance


    @staticmethod
    def test_current_workspace_id_not_set(mock_settings):
        """Test Workspace.current() returns None if workspace_id is not in context."""
        SETTINGS.pop('workspace_id', None) # Ensure workspace_id is not set
        
        current_ws = Workspace.current()
        assert current_ws is None


# Tests for @require_workspace Decorator

# Dummy function to be decorated
@require_workspace
def dummy_decorated_function(arg1, kwarg1=None):
    """A dummy function to test the @require_workspace decorator."""
    return f"Executed with {arg1} and {kwarg1}"

class TestRequireWorkspaceDecorator:

    def test_require_workspace_project_not_set(self, mock_settings):
        """Test @require_workspace when project_id is not set."""
        SETTINGS.pop('project_id', None) # Ensure project_id is not set
        SETTINGS.pop('workspace_id', None)

        with pytest.raises(ProjectNotSetError):
            dummy_decorated_function("test_arg")

    def test_require_workspace_project_set_workspace_not_set_creates_default(
        self, mock_settings, mock_account_get_username, 
        mock_workspace_request_select_by_name, mock_workspace_request_save,
        mock_typer_echo # For workspace creation messages
    ):
        """Test @require_workspace creates a default workspace if none is set."""
        SETTINGS['project_id'] = "decorator_project"
        SETTINGS.pop('workspace_id', None) # Ensure workspace_id is not set

        mock_account_get_username.return_value = "default_user"
        
        # Scenario: Workspace "default_user" (snake_cased from "DefaultUser") does not exist
        mock_workspace_request_select_by_name.return_value = None
        
        # Mock the save method on the instance of WorkspaceRequest that will be created
        mock_save_instance = mock.MagicMock()
        mock_save_instance.id = "default_ws_id_from_decorator"
        # mock_workspace_request_save.return_value = mock_save_instance # This mocks the class method, not instance

        with mock.patch('seeknal.workspace.WorkspaceRequest') as mock_wr_constructor:
            mock_wr_instance = mock.MagicMock()
            mock_wr_instance.save.return_value = mock_save_instance # Instance's save returns the mock with id
            mock_wr_constructor.return_value = mock_wr_instance # Constructor returns this instance

            result = dummy_decorated_function("arg_val", kwarg1="kwarg_val")

            # Verify default workspace creation flow
            mock_account_get_username.assert_called_once()
            # Workspace name will be snake_cased "default_user"
            mock_workspace_request_select_by_name.assert_called_once_with(name="default_user", project_id="decorator_project")
            
            mock_wr_constructor.assert_called_once_with(
                name="default_user",
                project_id="decorator_project",
                description=None, # Default
                creator_id=None # Default
            )
            mock_wr_instance.save.assert_called_once()
            mock_typer_echo.assert_any_call("Workspace 'default_user' created successfully.")
            
            assert SETTINGS['workspace_id'] == "default_ws_id_from_decorator"
            assert result == "Executed with arg_val and kwarg_val"


    def test_require_workspace_project_and_workspace_set(
        self, mock_settings, mock_account_get_username, 
        mock_workspace_request_select_by_name, mock_workspace_request_save
    ):
        """Test @require_workspace when both project_id and workspace_id are set."""
        SETTINGS['project_id'] = "decorator_project_set"
        SETTINGS['workspace_id'] = "decorator_workspace_set"

        result = dummy_decorated_function("another_arg")

        # Ensure no calls were made to create/fetch workspace as it's already set
        mock_account_get_username.assert_not_called()
        mock_workspace_request_select_by_name.assert_not_called()
        mock_workspace_request_save.assert_not_called() # Check class method mock
        # Also ensure no WorkspaceRequest instance was created and save called on it
        # (This is implicitly checked by not mocking WorkspaceRequest constructor here)

        assert result == "Executed with another_arg and None"
        # Ensure context was not changed
        assert SETTINGS['project_id'] == "decorator_project_set"
        assert SETTINGS['workspace_id'] == "decorator_workspace_set"
