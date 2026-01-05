# Error Handling

This guide covers the common exceptions in Seeknal and demonstrates best practices for handling them in your applications.

## Overview

Seeknal provides a set of custom exceptions to help you identify and handle specific error conditions. All exceptions can be imported from `seeknal.exceptions`:

```python
from seeknal.exceptions import (
    # Project errors
    ProjectNotSetError,
    ProjectNotFoundError,
    # Entity errors
    EntityNotFoundError,
    EntityNotSavedError,
    # Feature store errors
    FeatureGroupNotFoundError,
    FeatureServingNotFoundError,
    # Validation errors
    InvalidIdentifierError,
    InvalidPathError,
)
```

## Exception Categories

### Project Exceptions

#### ProjectNotSetError

Raised when an operation requires a project context, but no project has been set.

```python
from seeknal import Project
from seeknal.exceptions import ProjectNotSetError

# Example: Handling missing project context
try:
    # Operations that require a project will fail if none is set
    from seeknal.context import check_project_id
    check_project_id()
except ProjectNotSetError as e:
    print(f"Error: {e}")
    # Solution: Set up a project first
    project = Project(name="my_project", description="My feature store")
    project.get_or_create()
```

**Best Practice:** Always initialize your project at the start of your application:

```python
from seeknal import Project

def initialize_seeknal():
    """Initialize Seeknal with the required project context."""
    project = Project(name="my_feature_store")
    project = project.get_or_create()
    return project

# Call at application startup
project = initialize_seeknal()
```

#### ProjectNotFoundError

Raised when attempting to access a project that doesn't exist in the database.

```python
from seeknal import Project
from seeknal.exceptions import ProjectNotFoundError

def get_project_safely(project_id: int):
    """Safely retrieve a project by ID with error handling."""
    try:
        return Project.get_by_id(project_id)
    except ProjectNotFoundError:
        print(f"Project with ID {project_id} was not found.")
        return None

# Usage
project = get_project_safely(999)  # Returns None if not found
```

### Entity Exceptions

#### EntityNotSavedError

Raised when attempting to perform operations on an entity that hasn't been persisted.

```python
from seeknal import Entity
from seeknal.exceptions import EntityNotSavedError

def update_entity_safely(entity: Entity, **updates):
    """Update an entity with proper error handling."""
    try:
        entity.update(**updates)
    except EntityNotSavedError:
        print("Entity must be saved before updating.")
        print("Call entity.get_or_create() first.")
        # Optionally, save it automatically
        entity.get_or_create()
        entity.update(**updates)

# Usage
entity = Entity(name="customer", join_keys=["customer_id"])
# This will handle the case where entity hasn't been saved
update_entity_safely(entity, description="Customer entity for retail")
```

**Best Practice:** Always call `get_or_create()` before performing operations:

```python
from seeknal import Entity

# Correct pattern
entity = Entity(
    name="customer",
    join_keys=["customer_id"],
    pii_keys=["email", "phone"],
    description="Customer entity"
)
entity = entity.get_or_create()  # Always call this first
entity.update(description="Updated description")  # Now safe to update
```

#### EntityNotFoundError

Raised when an entity cannot be found in the feature store.

```python
from seeknal import Entity
from seeknal.exceptions import EntityNotFoundError

def get_or_create_entity(name: str, join_keys: list) -> Entity:
    """Get an existing entity or create a new one."""
    entity = Entity(name=name, join_keys=join_keys)
    entity = entity.get_or_create()
    return entity

# The get_or_create() pattern handles both cases automatically
customer_entity = get_or_create_entity("customer", ["customer_id"])
```

### Feature Store Exceptions

#### FeatureGroupNotFoundError

Raised when a feature group cannot be found in the feature store.

```python
from seeknal.exceptions import FeatureGroupNotFoundError

def load_feature_group(name: str):
    """Load a feature group with error handling."""
    try:
        # Feature group loading logic here
        pass
    except FeatureGroupNotFoundError:
        print(f"Feature group '{name}' not found.")
        print("Make sure the feature group has been created and saved.")
        return None
```

#### FeatureServingNotFoundError

Raised when a feature serving configuration cannot be found.

```python
from seeknal.exceptions import FeatureServingNotFoundError

def get_feature_serving(name: str):
    """Get feature serving configuration with error handling."""
    try:
        # Feature serving lookup logic here
        pass
    except FeatureServingNotFoundError:
        print(f"Feature serving '{name}' not found.")
        print("Please ensure the feature serving has been configured.")
        return None
```

### Validation Exceptions

#### InvalidIdentifierError

Raised when a SQL identifier (table name, column name, database name) contains invalid characters or exceeds length limits.

```python
from seeknal.validation import validate_table_name, validate_column_name
from seeknal.exceptions import InvalidIdentifierError

def create_table_safely(table_name: str, columns: list):
    """Create a table with validated identifiers."""
    try:
        # Validate table name
        validated_name = validate_table_name(table_name)

        # Validate column names
        for col in columns:
            validate_column_name(col)

        print(f"Creating table: {validated_name}")
        # Proceed with table creation...

    except InvalidIdentifierError as e:
        print(f"Invalid identifier: {e}")
        print("Identifiers must:")
        print("  - Start with a letter or underscore")
        print("  - Contain only alphanumeric characters and underscores")
        print("  - Be no longer than 128 characters")

# Valid usage
create_table_safely("user_features", ["user_id", "feature_1", "feature_2"])

# Invalid usage - will raise error
create_table_safely("user-features", ["user_id"])  # Hyphens not allowed
create_table_safely("123_table", ["user_id"])  # Cannot start with number
```

**Common Invalid Identifier Patterns:**

```python
from seeknal.validation import validate_sql_identifier
from seeknal.exceptions import InvalidIdentifierError

# Examples of invalid identifiers
invalid_identifiers = [
    "",              # Empty string
    "123table",      # Starts with number
    "table-name",    # Contains hyphen
    "table name",    # Contains space
    "table;drop",    # Contains semicolon (SQL injection attempt)
]

for identifier in invalid_identifiers:
    try:
        validate_sql_identifier(identifier)
    except InvalidIdentifierError as e:
        print(f"Invalid: '{identifier}' - {e}")
```

#### InvalidPathError

Raised when a file path contains potentially dangerous characters that could be used for SQL injection.

```python
from seeknal.validation import validate_file_path
from seeknal.exceptions import InvalidPathError

def load_data_safely(file_path: str):
    """Load data from a file path with validation."""
    try:
        validated_path = validate_file_path(file_path)
        print(f"Loading data from: {validated_path}")
        # Proceed with file loading...

    except InvalidPathError as e:
        print(f"Invalid file path: {e}")
        print("File paths cannot contain SQL injection characters like:")
        print("  ', \", ;, --, /*, */")

# Valid usage
load_data_safely("/data/features/customer_data.parquet")

# Invalid usage - will raise error
load_data_safely("/data/features'; DROP TABLE users; --")  # SQL injection attempt
```

## Comprehensive Error Handling Pattern

Here's a complete example showing how to handle multiple exception types in a production application:

```python
from seeknal import Project, Entity
from seeknal.exceptions import (
    ProjectNotSetError,
    ProjectNotFoundError,
    EntityNotFoundError,
    EntityNotSavedError,
    InvalidIdentifierError,
    InvalidPathError,
)
from seeknal.validation import validate_table_name

class SeeknalClient:
    """A client wrapper with comprehensive error handling."""

    def __init__(self, project_name: str):
        self.project = None
        self.entities = {}
        self._initialize_project(project_name)

    def _initialize_project(self, project_name: str):
        """Initialize the Seeknal project."""
        try:
            validate_table_name(project_name)
            self.project = Project(name=project_name)
            self.project = self.project.get_or_create()
            print(f"Project '{project_name}' initialized successfully.")
        except InvalidIdentifierError as e:
            raise ValueError(f"Invalid project name: {e}")

    def create_entity(
        self,
        name: str,
        join_keys: list,
        description: str = None
    ) -> Entity:
        """Create or retrieve an entity with error handling."""
        try:
            # Validate the entity name
            validate_table_name(name)

            # Create and save the entity
            entity = Entity(
                name=name,
                join_keys=join_keys,
                description=description
            )
            entity = entity.get_or_create()
            self.entities[name] = entity
            return entity

        except InvalidIdentifierError as e:
            print(f"Invalid entity name: {e}")
            raise
        except Exception as e:
            print(f"Unexpected error creating entity: {e}")
            raise

    def update_entity(self, name: str, **updates) -> Entity:
        """Update an entity with proper error handling."""
        try:
            if name not in self.entities:
                raise EntityNotFoundError(f"Entity '{name}' not in cache")

            entity = self.entities[name]
            entity.update(**updates)
            return entity

        except EntityNotSavedError:
            print(f"Entity '{name}' hasn't been saved. Saving now...")
            entity = self.entities[name].get_or_create()
            entity.update(**updates)
            return entity
        except EntityNotFoundError as e:
            print(f"Entity not found: {e}")
            raise


# Usage
try:
    client = SeeknalClient("my_feature_store")

    # Create entities
    customer = client.create_entity(
        name="customer",
        join_keys=["customer_id"],
        description="Customer entity"
    )

    # Update entity
    client.update_entity("customer", description="Updated customer entity")

except ValueError as e:
    print(f"Configuration error: {e}")
except (EntityNotFoundError, EntityNotSavedError) as e:
    print(f"Entity error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

## Logging Errors

For production applications, use Python's logging module:

```python
import logging
from seeknal.exceptions import (
    ProjectNotSetError,
    EntityNotFoundError,
    InvalidIdentifierError,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("seeknal_app")

def process_with_logging():
    """Example with proper logging."""
    try:
        # Your Seeknal operations here
        pass
    except ProjectNotSetError as e:
        logger.error("Project not set: %s", e)
        raise
    except EntityNotFoundError as e:
        logger.warning("Entity not found: %s", e)
        return None
    except InvalidIdentifierError as e:
        logger.error("Validation error: %s", e)
        raise ValueError(f"Invalid input: {e}")
    except Exception as e:
        logger.exception("Unexpected error occurred")
        raise
```

## Summary

| Exception | When Raised | How to Handle |
|-----------|-------------|---------------|
| `ProjectNotSetError` | No project context set | Initialize project with `Project(...).get_or_create()` |
| `ProjectNotFoundError` | Project doesn't exist | Use `get_or_create()` or verify project ID |
| `EntityNotSavedError` | Entity not persisted | Call `entity.get_or_create()` first |
| `EntityNotFoundError` | Entity doesn't exist | Use `get_or_create()` pattern |
| `FeatureGroupNotFoundError` | Feature group missing | Verify feature group is created and saved |
| `FeatureServingNotFoundError` | Feature serving missing | Verify serving configuration exists |
| `InvalidIdentifierError` | Invalid SQL identifier | Use alphanumeric characters and underscores only |
| `InvalidPathError` | Dangerous file path | Remove SQL injection characters from paths |
