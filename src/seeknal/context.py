import contextlib
import logging
import os
import sys
import threading
from typing import Any, Iterator, MutableMapping

from decouple import AutoConfig

from .configuration import (
    Config,
    DotDict,
    interpolate_env_vars,
    load_configuration,
    load_toml,
    merge_dicts,
    process_task_defaults,
)
from .exceptions import ProjectNotSetError

DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), "config.toml")
CONFIG_BASE_URL = os.getenv(
    "SEEKNAL_BASE_CONFIG_PATH", os.path.join(os.path.expanduser("~"), ".seeknal")
)
decouple_config = AutoConfig(search_path=CONFIG_BASE_URL)
USER_CONFIG = decouple_config("SEEKNAL_USER_CONFIG_PATH", default="~/.seeknal/config.toml")
BACKEND_CONFIG = decouple_config(
    "SEEKNAL_BACKEND_CONFIG_PATH", default="~/.seeknal/backend.toml"
)
ENV_VAR_PREFIX = "SEEKNAL"
DEFAULT_SEEKNAL_DB_PATH = os.getenv("DEFAULT_SEEKNAL_DB_PATH", os.path.join(CONFIG_BASE_URL, "seeknal.db"))


def load_default_config() -> "Config":
    # load seeknal configuration
    config = load_configuration(
        path=DEFAULT_CONFIG,
        user_config_path=USER_CONFIG,
        backend_config_path=BACKEND_CONFIG,
        env_var_prefix=ENV_VAR_PREFIX,
    )

    # add task defaults
    config = process_task_defaults(config)

    return config


def load_session_config(base_config_path: str):
    user_config_path = os.path.join(base_config_path, "session.toml")

    if user_config_path and os.path.isfile(str(interpolate_env_vars(user_config_path))):
        user_config = load_toml(user_config_path)
    else:
        user_config = {}

    return user_config


def _create_logger(name: str) -> logging.Logger:
    """
    Creates a logger with a `StreamHandler` that has level and formatting
    set from `seeknal.config`.

    Args:
        - name (str): Name to use for logger.

    Returns:
        - logging.Logger: a configured logging object
    """
    logger = logging.getLogger(name)

    # Set the format from the config for stdout
    formatter = logging.Formatter(
        context.config.logging.format, context.config.logging.datefmt
    )
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # Set the level
    logger.setLevel(context.config.logging.level)

    return logger


class Context(DotDict, threading.local):
    """
    A thread safe context store for seeknal data.

    The `Context` is a `DotDict` subclass, and can be instantiated the same way.

    Args:
        - *args (Any): arguments to provide to the `DotDict` constructor (e.g.,
            an initial dictionary)
        - **kwargs (Any): any key / value pairs to initialize this context with
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        init = {}

        # Initialize with config context
        init.update(config.get("context", {}))
        # Overwrite with explicit args
        init.update(dict(*args, **kwargs))
        # Merge in config (with explicit args overwriting)
        init["config"] = merge_dicts(config, init.get("config", {}))
        super().__init__(init)

    def __getstate__(self) -> None:
        """
        Because we dynamically update context during runs, we don't ever want to pickle
        or "freeze" the contents of context.  Consequently it should always be accessed
        as an attribute of the seeknal module.
        """
        raise TypeError(
            "Pickling context objects is explicitly not supported. You should always "
            "access context as an attribute of the `seeknal` module, as in `seeknal.context`"
        )

    def __repr__(self) -> str:
        return "<Context>"

    @contextlib.contextmanager
    def __call__(self, *args: MutableMapping, **kwargs: Any) -> Iterator["Context"]:
        """
        A context manager for setting / resetting the seeknal context

        Example:
            import seeknal.context
            with seeknal.context(dict(a=1, b=2), c=3):
                print(seeknal.context.a) # 1
        """
        # Avoid creating new `Context` object, copy as `dict` instead.
        previous_context = self.__dict__.copy()
        try:
            new_context = dict(*args, **kwargs)
            if "config" in new_context:
                new_context["config"] = merge_dicts(
                    self.get("config", {}), new_context["config"]
                )
            self.update(new_context)  # type: ignore
            yield self
        finally:
            self.clear()
            self.update(previous_context)


def configure_logging(testing: bool = False) -> logging.Logger:
    """
    Creates a "seeknal" root logger with a `StreamHandler` that has level and formatting
    set from `seeknal.config`.

    Args:
        - testing (bool, optional): a boolean specifying whether this configuration
            is for testing purposes only; this helps us isolate any global state during testing
            by configuring a "seeknal-test-logger" instead of the standard "seeknal" logger

    Returns:
        - logging.Logger: a configured logging object
    """
    name = "seeknal-test-logger" if testing else "seeknal"

    return _create_logger(name)


def get_logger(name: str = None) -> logging.Logger:
    """
    Returns a logger.

    Args:
        - name (str): if `None`, the root seeknal logger is returned. If provided, a child
            logger of the name `{name}"` is returned. The child logger inherits
            the root logger's settings.

    Returns:
        - logging.Logger: a configured logging object with the appropriate name
    """

    if name is None:
        return seeknal_logger
    else:
        return seeknal_logger.getChild(name)


config = load_default_config()
context = Context(
    {
        "user_config_path": USER_CONFIG,
        "backend_config_path": BACKEND_CONFIG,
        "base_config_path": CONFIG_BASE_URL,
    }
)
session = load_session_config(CONFIG_BASE_URL)
context.session = session
context.logger = seeknal_logger = configure_logging()

logger = get_logger()
logger.propagate = False


def get_project_id():
    try:
        return context.project_id
    except AttributeError as exc:
        return None


def get_workspace_id():
    try:
        return context.workspace_id
    except AttributeError as exc:
        return None


def get_run_log():
    try:
        return context.run_log
    except AttributeError as exc:
        return None


def check_project_id():
    if get_project_id() is None:
        raise ProjectNotSetError(
            "Project hasn't been set in your environment. Please set it with Project(name='...')."
        )


def require_project(func):
    def wrapper(*args, **kwargs):
        check_project_id()
        return func(*args, **kwargs)

    return wrapper
