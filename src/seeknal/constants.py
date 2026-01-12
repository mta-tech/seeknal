"""
Constants module for Seeknal.

This module contains all magic numbers, string constants, and configuration
defaults used throughout the Seeknal codebase. Having these values in one
place improves maintainability and makes it easier to update configuration
defaults.

Constants are organized by category:
- Time-related constants (TTLs, timeouts, intervals)
- Format patterns (date formats, string patterns)
- Validation defaults (thresholds, limits)
- System defaults (retries, batch sizes)
"""

from datetime import timedelta

# =============================================================================
# Time-related Constants
# =============================================================================

# Default TTL (Time To Live) for cached data
DEFAULT_TTL_SECONDS = 86400  # 24 hours
DEFAULT_TTL_TIMELTA = timedelta(seconds=DEFAULT_TTL_SECONDS)

# Default freshness validator maximum age
DEFAULT_FRESHNESS_MAX_AGE_SECONDS = 86400  # 24 hours

# Retry delays and timeouts
DEFAULT_RETRY_DELAY_SECONDS = 1
DEFAULT_MAX_RETRIES = 3
DEFAULT_CONNECTION_TIMEOUT_SECONDS = 30
DEFAULT_QUERY_TIMEOUT_SECONDS = 120

# =============================================================================
# Date/Time Format Patterns
# =============================================================================

# ISO 8601 date format
ISO_DATE_FORMAT = "%Y-%m-%d"
ISO_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
ISO_DATETIME_FORMAT_WITH_TZ = "%Y-%m-%dT%H:%M:%SZ"

# Common date patterns for data parsing
DEFAULT_DATE_PATTERN = "yyyy-MM-dd"
DEFAULT_DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss"
DEFAULT_DATE_PATTERN_SIMPLE = "yyyyMMdd"

# =============================================================================
# Validation Defaults
# =============================================================================

# Default thresholds for validators
DEFAULT_MAX_NULL_PERCENTAGE = 0.0  # 0% null values allowed
DEFAULT_MAX_DUPLICATE_PERCENTAGE = 0.0  # 0% duplicates allowed
DEFAULT_MIN_FRESHNESS_SECONDS = 86400  # 24 hours

# Validation limits
DEFAULT_VALIDATION_TIMEOUT_SECONDS = 300  # 5 minutes
MAX_VALIDATION_ERRORS_TO_DISPLAY = 100

# =============================================================================
# System Defaults
# =============================================================================

# Pagination
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 1000

# Batch processing
DEFAULT_BATCH_SIZE = 1000
DEFAULT_MAX_BATCH_SIZE = 10000

# File sizes and limits
DEFAULT_MAX_FILE_SIZE_MB = 100
DEFAULT_MAX_MEMORY_MB = 512

# =============================================================================
# Feature Store Defaults
# =============================================================================

# Materialization defaults
DEFAULT_OFFLINE_STORE_PATH = "data/offline"
DEFAULT_ONLINE_STORE_PATH = "data/online"

# Feature group defaults
DEFAULT_FEATURE_GROUP_DESCRIPTION = ""
DEFAULT_ENTITY_JOIN_KEYS = []

# =============================================================================
# CLI Defaults
# =============================================================================

# Output formats
DEFAULT_OUTPUT_FORMAT = "table"
DEFAULT_TABLE_FORMAT = "simple"

# Color/style settings
CLI_SUCCESS_SYMBOL = "✓"
CLI_ERROR_SYMBOL = "✗"
CLI_WARNING_SYMBOL = "⚠"
CLI_INFO_SYMBOL = "ℹ"

# =============================================================================
# Database Defaults
# =============================================================================

# Connection pool settings
DEFAULT_DB_CONNECTION_POOL_SIZE = 5
DEFAULT_DB_MAX_OVERFLOW = 10
DEFAULT_DB_POOL_TIMEOUT_SECONDS = 30

# Query limits
DEFAULT_QUERY_LIMIT = 1000
MAX_QUERY_LIMIT = 10000

# =============================================================================
# Version Management Defaults
# =============================================================================

# Version history
DEFAULT_VERSION_LIMIT = 10
MAX_VERSION_HISTORY = 100

# Version comparison
DEFAULT_BASE_VERSION = 1

# =============================================================================
# Error Messages
# =============================================================================

# Common error message templates
ERROR_RESOURCE_NOT_FOUND = "Resource '{name}' of type '{type}' not found"
ERROR_VALIDATION_FAILED = "Validation failed: {reason}"
ERROR_DATABASE_CONNECTION = "Failed to connect to database: {reason}"
ERROR_INVALID_CONFIGURATION = "Invalid configuration: {reason}"

# =============================================================================
# File Extensions
# =============================================================================

# Supported file extensions
PARQUET_EXTENSION = ".parquet"
CSV_EXTENSION = ".csv"
JSON_EXTENSION = ".json"
YAML_EXTENSION = ".yaml"
YML_EXTENSION = ".yml"

# =============================================================================
# HTTP/API Defaults
# =============================================================================

# API timeouts
DEFAULT_API_TIMEOUT_SECONDS = 30
DEFAULT_API_RETRY_COUNT = 3

# HTTP status codes
HTTP_OK = 200
HTTP_CREATED = 201
HTTP_BAD_REQUEST = 400
HTTP_UNAUTHORIZED = 401
HTTP_NOT_FOUND = 404
HTTP_INTERNAL_SERVER_ERROR = 500
