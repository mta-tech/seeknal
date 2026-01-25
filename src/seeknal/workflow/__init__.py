"""
Workflow module for Seeknal 2.0 dbt-inspired YAML development.

This module provides three-phase workflow:
1. draft - Generate YAML templates from Jinja2
2. dry-run - Validate and preview execution
3. apply - Move file to production and update manifest
"""

# The workflow commands are imported directly in main.py
# This file serves as the package marker

