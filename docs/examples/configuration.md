# Configuration

This guide demonstrates how to configure Seeknal using configuration files and environment variables. Proper configuration is essential for connecting to your data infrastructure and customizing Seeknal's behavior.

## Overview

Seeknal uses a hierarchical configuration system that supports:

- **TOML configuration files** - For persistent settings
- **Environment variables** - For sensitive values and deployment flexibility
- **Variable interpolation** - Reference other config values or environment variables
- **Programmatic access** - Dot notation for easy access to nested settings

## Configuration Files

Seeknal uses TOML format for configuration files. By default, it looks for:

- User config: `~/config.toml` (customizable via `__USER_CONFIG_PATH` env var)
- Backend config: `~/backend.toml` (customizable via `__BACKEND_CONFIG_PATH` env var)

### Basic Configuration File

Create a configuration file at `~/config.toml`:

```toml
# Seeknal Configuration File

[logging]
level = "INFO"
format = "%(asctime)s - %(levelname)s - %(message)s"
datefmt = "%Y-%m-%d %H:%M:%S"

[tasks]
[tasks.defaults]
max_retries = 3
retry_delay = 60  # seconds
timeout = 3600    # 1 hour in seconds

[storage]
base_path = "/data/seeknal"
offline_store = "${storage.base_path}/offline"
online_store = "${storage.base_path}/online"

[database]
host = "localhost"
port = 5432
name = "seeknal_db"
```

### Loading Configuration

```python
from seeknal.configuration import load_configuration, config

# The default configuration is loaded automatically
# Access it via the global `config` object
print(f"Log level: {config.logging.level}")

# Or load a custom configuration
custom_config = load_configuration(
    user_config_path="/path/to/custom/config.toml",
    backend_config_path="/path/to/backend.toml",
    env_var_prefix="SEEKNAL"
)
```

## Environment Variables

Environment variables provide a flexible way to configure Seeknal, especially for sensitive values like credentials.

### Setting Configuration via Environment Variables

Environment variables follow the pattern: `PREFIX__SECTION__KEY`

```bash
# Set configuration via environment variables
export SEEKNAL__LOGGING__LEVEL="DEBUG"
export SEEKNAL__DATABASE__HOST="production-db.example.com"
export SEEKNAL__DATABASE__PORT="5432"
export SEEKNAL__STORAGE__BASE_PATH="/mnt/data/seeknal"
```

### Loading Configuration with Environment Variable Prefix

```python
from seeknal.configuration import load_configuration

# Load configuration with environment variable override support
config = load_configuration(
    user_config_path="~/.config.toml",
    env_var_prefix="SEEKNAL"
)

# Environment variables override file settings
print(f"Database host: {config.database.host}")
# Output: production-db.example.com (from environment)
```

### Type Conversion

Environment variable values are automatically converted to appropriate Python types:

```bash
# Boolean values (case-insensitive)
export SEEKNAL__DEBUG="true"
export SEEKNAL__VERBOSE="FALSE"

# Numeric values
export SEEKNAL__MAX_WORKERS="4"
export SEEKNAL__TIMEOUT="3.5"

# Lists and dicts (Python literal syntax)
export SEEKNAL__ALLOWED_HOSTS='["localhost", "127.0.0.1"]'
```

```python
from seeknal.configuration import string_to_type

# Type conversion examples
print(string_to_type("true"))      # True (bool)
print(string_to_type("FALSE"))     # False (bool)
print(string_to_type("42"))        # 42 (int)
print(string_to_type("3.14"))      # 3.14 (float)
print(string_to_type('["a", "b"]')) # ["a", "b"] (list)
print(string_to_type("hello"))     # "hello" (str)
```

## Variable Interpolation

Seeknal supports variable interpolation using the `${section.key}` syntax.

### Referencing Other Config Values

```toml
# config.toml

[storage]
base_path = "/data/seeknal"
# Reference other config values with ${section.key}
offline_store = "${storage.base_path}/offline"
online_store = "${storage.base_path}/online"
archive_store = "${storage.base_path}/archive"

[project]
name = "my_project"
feature_path = "${storage.offline_store}/${project.name}/features"
```

### Referencing Environment Variables

```toml
# config.toml

[database]
# Reference environment variables with standard shell syntax
host = "$DB_HOST"
port = "$DB_PORT"
username = "$DB_USER"
password = "$DB_PASSWORD"

[storage]
# Supports home directory expansion
base_path = "~/seeknal_data"
```

```python
import os
from seeknal.configuration import load_configuration

# Set environment variables
os.environ["DB_HOST"] = "db.example.com"
os.environ["DB_PORT"] = "5432"
os.environ["DB_USER"] = "seeknal_user"
os.environ["DB_PASSWORD"] = "secret"

# Load configuration - environment variables are interpolated
config = load_configuration(user_config_path="config.toml")

print(f"Database: {config.database.username}@{config.database.host}")
# Output: seeknal_user@db.example.com
```

### Nested Interpolation

```python
from seeknal.configuration import interpolate_env_vars

# Supports nested environment variable expansion
os.environ["BASE"] = "$HOME/projects"
os.environ["PROJECT"] = "seeknal"

result = interpolate_env_vars("$BASE/$PROJECT/data")
print(result)  # /home/user/projects/seeknal/data
```

## DotDict Access

Configuration values can be accessed using dot notation for cleaner code.

### Accessing Configuration Values

```python
from seeknal.configuration import config

# Dot notation access
log_level = config.logging.level
db_host = config.database.host

# Dictionary-style access also works
log_level = config["logging"]["level"]
db_host = config.get("database", {}).get("host", "localhost")

# Mixed access patterns
storage_config = config.storage
offline_path = storage_config["offline_store"]
```

### Working with DotDict

```python
from seeknal.configuration import DotDict

# Create a DotDict from a regular dictionary
settings = DotDict({
    "database": {
        "host": "localhost",
        "port": 5432
    },
    "logging": {
        "level": "INFO"
    }
})

# Access with dot notation
print(settings.database.host)  # localhost
print(settings.logging.level)  # INFO

# Modify values
settings.database.host = "production.example.com"
settings.database.ssl = True  # Add new keys

# Convert back to regular dict
regular_dict = settings.to_dict()
```

### Creating Nested Configurations

```python
from seeknal.configuration import DotDict, as_nested_dict

# Convert nested dictionaries to DotDict for dot access
raw_config = {
    "spark": {
        "master": "local[*]",
        "app_name": "seeknal",
        "config": {
            "spark.sql.shuffle.partitions": "200"
        }
    }
}

# Convert to nested DotDict
config = as_nested_dict(raw_config, dct_class=DotDict)

# Now accessible with dots
print(config.spark.master)  # local[*]
print(config.spark.config["spark.sql.shuffle.partitions"])  # 200
```

## Complete Configuration Example

Here's a comprehensive configuration setup for a production environment:

### Configuration File (`config.toml`)

```toml
# Seeknal Production Configuration

[logging]
level = "INFO"
format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
datefmt = "%Y-%m-%d %H:%M:%S"
log_attributes = "[]"

[tasks]
[tasks.defaults]
max_retries = 3
retry_delay = 60
timeout = 7200

[storage]
base_path = "$SEEKNAL_DATA_PATH"
offline_store = "${storage.base_path}/offline"
online_store = "${storage.base_path}/online"

[database]
host = "$SEEKNAL_DB_HOST"
port = "$SEEKNAL_DB_PORT"
name = "$SEEKNAL_DB_NAME"
username = "$SEEKNAL_DB_USER"
password = "$SEEKNAL_DB_PASSWORD"

[spark]
master = "$SPARK_MASTER"
app_name = "seeknal-production"
executor_memory = "4g"
driver_memory = "2g"
```

### Application Setup

```python
import os
from seeknal.configuration import load_configuration, Config

def setup_environment():
    """Set up environment variables for Seeknal."""
    # These would typically come from a secrets manager or deployment config
    os.environ["SEEKNAL_DATA_PATH"] = "/mnt/data/seeknal"
    os.environ["SEEKNAL_DB_HOST"] = "db.production.example.com"
    os.environ["SEEKNAL_DB_PORT"] = "5432"
    os.environ["SEEKNAL_DB_NAME"] = "seeknal_prod"
    os.environ["SEEKNAL_DB_USER"] = "seeknal_app"
    os.environ["SEEKNAL_DB_PASSWORD"] = os.getenv("DB_SECRET", "")
    os.environ["SPARK_MASTER"] = "spark://spark-master:7077"

def load_app_config() -> Config:
    """Load the application configuration."""
    config = load_configuration(
        user_config_path="config.toml",
        env_var_prefix="SEEKNAL"
    )
    return config

def main():
    # Setup environment
    setup_environment()

    # Load configuration
    config = load_app_config()

    # Use configuration
    print(f"Storage path: {config.storage.base_path}")
    print(f"Database: {config.database.host}:{config.database.port}")
    print(f"Spark master: {config.spark.master}")
    print(f"Task timeout: {config.tasks.defaults.timeout} seconds")

if __name__ == "__main__":
    main()
```

## Converting Configuration to Environment Variables

Export configuration values as environment variables for subprocess calls:

```python
from seeknal.configuration import config, to_environment_variables

# Convert entire configuration to environment variables
env_vars = to_environment_variables(config, prefix="SEEKNAL")
print(env_vars)
# Output: {'SEEKNAL__LOGGING__LEVEL': 'INFO', 'SEEKNAL__DATABASE__HOST': '...', ...}

# Convert only specific keys
env_vars = to_environment_variables(
    config,
    include=["logging.level", "database.host"],
    prefix="APP"
)
print(env_vars)
# Output: {'APP__LOGGING__LEVEL': 'INFO', 'APP__DATABASE__HOST': 'localhost'}
```

## Best Practices

!!! tip "Configuration Best Practices"
    - **Never hardcode credentials** - Use environment variables for sensitive data
    - **Use interpolation** - Avoid repeating paths and values
    - **Environment-specific files** - Maintain separate configs for dev/staging/prod
    - **Validate early** - Load and validate configuration at application startup

!!! warning "Security Considerations"
    - Store sensitive values in environment variables or a secrets manager
    - Never commit configuration files with credentials to version control
    - Use file permissions to protect configuration files
    - Rotate credentials regularly

### Development vs Production

```python
import os
from seeknal.configuration import load_configuration

def get_config():
    """Load environment-appropriate configuration."""
    env = os.getenv("SEEKNAL_ENV", "development")

    if env == "production":
        config_path = "/etc/seeknal/config.toml"
    elif env == "staging":
        config_path = "~/.seeknal/staging.toml"
    else:
        config_path = "~/.seeknal/dev.toml"

    return load_configuration(
        user_config_path=config_path,
        env_var_prefix="SEEKNAL"
    )
```

### Configuration Validation

```python
from seeknal.configuration import load_configuration, validate_config

def load_and_validate():
    """Load configuration with validation."""
    config = load_configuration(user_config_path="config.toml")

    # Built-in validation is called automatically
    # Add custom validation as needed
    required_keys = ["database.host", "storage.base_path"]

    for key in required_keys:
        parts = key.split(".")
        current = config
        for part in parts:
            if not hasattr(current, part) or getattr(current, part) is None:
                raise ValueError(f"Required configuration missing: {key}")
            current = getattr(current, part)

    return config
```

## Next Steps

After configuring Seeknal, you can:

1. **Initialize Projects** - Set up your feature store ([Initialization](initialization.md))
2. **Create Pipelines** - Build data pipelines ([Flows & Pipelines](flows.md))
3. **Feature Store** - Manage ML features ([FeatureStore](featurestore.md))
4. **Handle Errors** - Implement robust error handling ([Error Handling](error_handling.md))
