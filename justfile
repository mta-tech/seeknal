# Seeknal Project Commands
# https://github.com/casey/just

# Default recipe: list all available commands
default:
    @just --list

# --- Development ---

# Install project in development mode
install:
    pip install -e ".[dev]"

# Install documentation dependencies
install-docs:
    pip install -r requirements-docs.txt

# Run the CLI
run *ARGS:
    seeknal {{ARGS}}

# --- Testing ---

# Run all tests
test:
    pytest

# Run tests with coverage
test-cov:
    pytest --cov=seeknal --cov-report=term-missing

# Run specific test file
test-file FILE:
    pytest {{FILE}} -v

# Run CLI tests
test-cli:
    pytest tests/cli/ -v

# Run e2e tests
test-e2e:
    pytest tests/e2e/ -v

# --- Documentation ---

# Serve docs locally (live reload)
docs-serve PORT="8080":
    mkdocs serve --dev-addr 127.0.0.1:{{PORT}}

# Build docs
docs-build:
    mkdocs build

# Deploy docs to GitHub Pages
docs-deploy:
    mkdocs gh-deploy --force

# --- Code Quality ---

# Format code with black
fmt:
    black src/ tests/ --line-length 100

# Check formatting without modifying
fmt-check:
    black src/ tests/ --line-length 100 --check

# Type check with pyright
typecheck:
    pyright src/seeknal/

# --- Git ---

# Show current branch status
status:
    @git status --short
    @echo ""
    @git log --oneline -5

# --- Seeknal Workflow ---

# Initialize a new seeknal project
init NAME:
    seeknal init --name {{NAME}}

# Run a seeknal pipeline
pipeline *ARGS:
    seeknal run {{ARGS}}

# Show execution plan
plan:
    seeknal plan

# Start SQL REPL
repl *ARGS:
    seeknal repl {{ARGS}}
