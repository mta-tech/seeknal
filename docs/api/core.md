# Core API Reference

This page documents the core Seeknal classes that form the foundation of the library. These classes handle project management, entity definitions, data flows, and execution context.

## Overview

The core module provides the essential building blocks for working with Seeknal:

| Class | Purpose |
|-------|---------|
| [`Project`](#seeknal.project.Project) | Manage Seeknal projects and their lifecycle |
| [`Entity`](#seeknal.entity.Entity) | Define entities with join keys for feature stores |
| [`Flow`](#seeknal.flow.Flow) | Create and manage data transformation pipelines |
| [`Context`](#seeknal.context.Context) | Execution context and session management |

## Project

The `Project` class is the top-level container for organizing Seeknal resources. Projects provide namespace isolation and resource management.

:::seeknal.project
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## Entity

The `Entity` class defines entities with join keys that serve as the primary identifiers for feature lookups in the feature store.

:::seeknal.entity
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## Flow

The `Flow` class enables the creation and execution of data transformation pipelines. Flows connect inputs, tasks, and outputs to build complete data processing workflows.

:::seeknal.flow
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## Context

The `Context` class manages the execution context and session state for Seeknal operations.

:::seeknal.context
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## Configuration

The `Configuration` module handles configuration management, including loading and validating configuration files.

:::seeknal.configuration
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## Validation Utilities

Seeknal provides comprehensive validation utilities for SQL identifiers, table names, column names, and file paths to prevent injection attacks and ensure data integrity.

:::seeknal.validation
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## Exceptions

Core exception classes for error handling throughout the Seeknal library.

:::seeknal.exceptions
    options:
      show_root_heading: false
      show_source: true
      members_order: source
