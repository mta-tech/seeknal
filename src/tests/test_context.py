import pytest
import os
import logging
from unittest import mock

from seeknal.context import (
    load_default_config,
    load_session_config,
    Context,
    get_project_id,
    get_workspace_id,
    check_project_id,
    require_project,
    configure_logging,
    get_logger,
    SETTINGS,
    DEFAULT_CONFIG,
)
from seeknal.exceptions import ProjectNotSetError


# Fixtures
@pytest.fixture
def mock_load_config():
    with mock.patch("seeknal.context.load_configuration") as mc:
        yield mc

@pytest.fixture
def mock_process_defaults():
    with mock.patch("seeknal.context.process_task_defaults") as mp:
        yield mp

@pytest.fixture
def mock_isfile():
    with mock.patch("os.path.isfile") as mi:
        yield mi

@pytest.fixture
def mock_load_toml():
    with mock.patch("seeknal.context.load_toml") as ml:
        yield ml

# Tests for Configuration Loading
def test_load_default_config(mock_load_config, mock_process_defaults):
    """Test that load_default_config loads default configurations correctly."""
    mock_load_config.return_value = {"default_key": "default_value"}
    mock_process_defaults.return_value = {"processed_key": "processed_value"}

    config = load_default_config()

    mock_load_config.assert_called_once_with(DEFAULT_CONFIG)
    mock_process_defaults.assert_called_once_with({"default_key": "default_value"})
    assert config == {"processed_key": "processed_value"}

def test_load_session_config_exists(mock_isfile, mock_load_toml):
    """Test that load_session_config loads a session configuration file if it exists."""
    mock_isfile.return_value = True
    mock_load_toml.return_value = {"session_key": "session_value"}
    SETTINGS["config_file"] = "dummy_config.toml"

    config = load_session_config()

    mock_isfile.assert_called_once_with("dummy_config.toml")
    mock_load_toml.assert_called_once_with("dummy_config.toml")
    assert config == {"session_key": "session_value"}

def test_load_session_config_not_exists(mock_isfile, mock_load_toml):
    """Test that load_session_config returns an empty dict if the session file does not exist."""
    mock_isfile.return_value = False
    SETTINGS["config_file"] = "dummy_config.toml"

    config = load_session_config()

    mock_isfile.assert_called_once_with("dummy_config.toml")
    mock_load_toml.assert_not_called()
    assert config == {}

# Tests for Context Class
def test_context_initialization():
    """Test Context initialization with a dictionary and keyword arguments."""
    default_config = {"default_key": "default_value"}
    context_settings = {"setting_key": "setting_value"}
    
    with mock.patch("seeknal.context.load_default_config", return_value=default_config):
        ctx = Context(context_settings, extra_key="extra_value")

    assert ctx.default_key == "default_value"
    assert ctx.setting_key == "setting_value"
    assert ctx.extra_key == "extra_value"

def test_context_context_manager():
    """Test that context variables are correctly set within the with block and restored afterwards."""
    SETTINGS["initial_key"] = "initial_value"
    ctx = Context(new_key="new_value")

    assert SETTINGS.get("new_key") is None
    assert SETTINGS.get("initial_key") == "initial_value"

    with ctx:
        assert SETTINGS.get("new_key") == "new_value"
        assert SETTINGS.get("initial_key") == "initial_value" # It should not be affected
        SETTINGS["another_key"] = "another_value" # settings made inside with block
    
    assert SETTINGS.get("new_key") is None # Restored
    assert SETTINGS.get("initial_key") == "initial_value" # Restored
    assert SETTINGS.get("another_key") is None # Restored

    # cleanup global SETTINGS
    del SETTINGS["initial_key"]


def test_context_getstate():
    """Test that Context.__getstate__ raises a TypeError when an attempt is made to pickle the context."""
    ctx = Context()
    with pytest.raises(TypeError):
        pickle.dumps(ctx) # Removed "import pickle" as it is not used elsewhere. Will add if necessary.

def test_context_repr():
    """Test that Context.__repr__ returns '<Context>'."""
    ctx = Context()
    assert repr(ctx) == "<Context>"


# Tests for Context Utility Functions
def test_get_project_id():
    """Test that get_project_id returns project_id from context or None."""
    SETTINGS["project_id"] = "test_project"
    assert get_project_id() == "test_project"
    del SETTINGS["project_id"] # cleanup
    assert get_project_id() is None

def test_get_workspace_id():
    """Test that get_workspace_id returns workspace_id from context or None."""
    SETTINGS["workspace_id"] = "test_workspace"
    assert get_workspace_id() == "test_workspace"
    del SETTINGS["workspace_id"] # cleanup
    assert get_workspace_id() is None

def test_check_project_id_raises_error():
    """Test that check_project_id raises ProjectNotSetError if project_id is not set."""
    # Ensure project_id is not set
    if "project_id" in SETTINGS:
        del SETTINGS["project_id"]
    with pytest.raises(ProjectNotSetError):
        check_project_id()

def test_check_project_id_no_error():
    """Test that check_project_id does nothing if project_id is set."""
    SETTINGS["project_id"] = "test_project"
    try:
        check_project_id()
    except ProjectNotSetError:
        pytest.fail("check_project_id raised ProjectNotSetError unexpectedly")
    finally:
        del SETTINGS["project_id"] # cleanup

@require_project
def dummy_function_require_project():
    return "Project is set"

def test_require_project_decorator_raises_error():
    """Test that @require_project decorated function raises ProjectNotSetError if project_id is not set."""
    # Ensure project_id is not set
    if "project_id" in SETTINGS:
        del SETTINGS["project_id"]
    with pytest.raises(ProjectNotSetError):
        dummy_function_require_project()

def test_require_project_decorator_no_error():
    """Test that @require_project decorated function executes if project_id is set."""
    SETTINGS["project_id"] = "test_project"
    try:
        result = dummy_function_require_project()
        assert result == "Project is set"
    except ProjectNotSetError:
        pytest.fail("@require_project raised ProjectNotSetError unexpectedly")
    finally:
        del SETTINGS["project_id"] # cleanup


# Tests for Logging
def test_configure_logging_default():
    """Test that configure_logging returns a logger and configures it."""
    # Reset logging to a known state
    logging.getLogger("seeknal").handlers = []
    
    logger = configure_logging(log_level="INFO", testing=False)
    assert isinstance(logger, logging.Logger)
    assert logger.name == "seeknal"
    assert logger.level == logging.INFO
    assert len(logger.handlers) > 0 # Check that some handler is configured

def test_configure_logging_testing_mode():
    """Test that configure_logging in testing mode configures 'seeknal-test-logger'."""
    # Reset logging to a known state
    logging.getLogger("seeknal-test-logger").handlers = []
    
    logger = configure_logging(log_level="DEBUG", testing=True)
    assert isinstance(logger, logging.Logger)
    assert logger.name == "seeknal-test-logger"
    assert logger.level == logging.DEBUG
    assert len(logger.handlers) > 0 # Check that some handler is configured

def test_get_logger_root():
    """Test that get_logger() returns the root 'seeknal' logger."""
    # Ensure configure_logging has been called at least once (e.g., by other tests or globally)
    # If not, call it to set up the base logger.
    if not logging.getLogger("seeknal").handlers:
        configure_logging() 
        
    logger = get_logger()
    assert isinstance(logger, logging.Logger)
    assert logger.name == "seeknal"

def test_get_logger_child():
    """Test that get_logger(name) returns a child logger of 'seeknal'."""
     # Ensure configure_logging has been called at least once
    if not logging.getLogger("seeknal").handlers:
        configure_logging()

    child_logger_name = "my_module"
    logger = get_logger(child_logger_name)
    assert isinstance(logger, logging.Logger)
    assert logger.name == f"seeknal.{child_logger_name}"
    # Check that it's a child of the 'seeknal' logger
    assert logger.parent.name == "seeknal"
