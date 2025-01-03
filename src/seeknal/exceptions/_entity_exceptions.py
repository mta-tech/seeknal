class EntityNotFoundError(Exception):
    """Custom error that is raised when entity not found in seeknal (possibly not saved yet)"""

    pass


class EntityNotSavedError(Exception):
    """Custom error that is raised when entity not saved in seeknal"""

    pass
