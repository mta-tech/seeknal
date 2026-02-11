---
last_updated: 2026-02-11
environment: development
platform: cross-platform
---

# Deployment Guide

Documentation for deploying Seeknal.

## Quick Start

This system runs locally using Claude Code with custom commands and agents. No traditional deployment needed.

## Environment

- **Platform**: macOS/Linux/Windows (Claude Code supported)
- **Runtime**: Python 3.11+
- **Configuration**: `.claude/` directory

## Configuration

Located in `.claude/`:
- `commands/` - Slash command implementations
- `agents/` - Sub-agent definitions
- `skills/` - Reusable prompt templates
- `docs/` - Learnings and compounded knowledge

## Changelog

### 2026-02-11 - Fix Integration Security and Functionality Issues

**Environment:** development
**Platform:** cross-platform (Linux, macOS, Windows)

**Changes:**
- [Security] Path traversal vulnerability fixed in FileStateBackend
- [Validation] Parameter name validation enforced
- [Validation] Date range validation (2000-2100, max 1 year future)
- [Breaking Change] FileStateBackend raises ValueError for insecure paths
- [Breaking Change] Invalid parameter names now raise ValueError
- [Code Quality] `type` parameter renamed to `param_type`
- [Code Quality] Centralized type conversion module created

**Migration Notes:**
- Set `SEEKNAL_BASE_CONFIG_PATH` to secure directory (not /tmp)
- Rename parameters with special characters to alphanumeric only

**Lessons Learned:**
- Path sanitization AND validation provides defense in depth against traversal attacks
- Warning-based parameter collision handling maintains backward compatibility
- Centralized type conversion ensures consistent behavior across codebase
- Date validation prevents unreasonable dates (year range: 2000-2100, max 1 year future)
- Parameter names must follow Python identifier conventions (alphanumeric + underscores)
