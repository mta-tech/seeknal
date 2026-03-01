---
topic: Seeknal Electron Desktop Application
date: 2026-03-01
status: complete
participants: user, claude
---

# Brainstorm: Seeknal Electron Desktop Application

## Context

Seeknal is a CLI-first data and AI/ML engineering platform (Python 3.11+, Typer, DuckDB, PySpark). It has ~35+ CLI commands, no existing UI/frontend, and no REST API (Atlas API is commented out). The goal is to build a cross-platform Electron desktop application that provides visual advantages over the CLI.

## Key Decisions

### 1. Purpose: All-in-One Workspace
A unified IDE-like experience combining pipeline design, feature management, REPL, and monitoring for both data and ML engineers.

### 2. MVP Scope: Full Suite
- Project management (init, select, configure)
- Pipeline DAG viewer with visual node editing
- Feature group browser with version history
- Integrated DuckDB REPL/query editor
- Real-time pipeline execution monitoring
- Environment management (plan/apply/promote)

### 3. Architecture: FastAPI Bridge
Add a thin FastAPI server to seeknal that wraps CLI operations as REST endpoints. Electron frontend (React/Vue) talks to it via HTTP.

**Why this approach:**
- Clean separation between frontend and backend
- Testable independently (API tests + E2E tests)
- Reusable for a web version later
- The Atlas API pattern already exists in the codebase (commented out) as prior art
- Standard tooling (OpenAPI docs, type safety with Pydantic)

**Rejected alternatives:**
- IPC + Child Process: Tighter coupling, harder to test, no web reuse
- Python-in-WASM (Pyodide): Too experimental, limited Python ecosystem support

### 4. Platform: Cross-Platform (macOS, Windows, Linux)
Must work on all three from day one via Electron.

### 5. Success Criteria: Visual Advantage
Focus on things GUI does better than CLI:
- DAG visualization (interactive node graph)
- Data previews (table views, charts)
- Drag-and-drop pipeline editing
- Real-time execution monitoring with progress bars
- Feature group schema diff viewer

### 6. Distribution: Hybrid
Auto-detect existing seeknal installation; offer to install via uv/pip if missing. Keeps download size small while ensuring zero-friction onboarding.

## Chosen Approach: FastAPI Bridge

```
┌─────────────────────────────────────────┐
│           Electron App                   │
│  ┌───────────────────────────────────┐  │
│  │   React Frontend (Renderer)       │  │
│  │   - DAG Viewer (React Flow)       │  │
│  │   - Feature Browser               │  │
│  │   - REPL Editor (Monaco/CodeMirror)│  │
│  │   - Pipeline Monitor              │  │
│  │   - Environment Manager           │  │
│  └───────────────┬───────────────────┘  │
│                   │ HTTP (localhost)      │
│  ┌───────────────┴───────────────────┐  │
│  │   Electron Main Process           │  │
│  │   - Spawns FastAPI server         │  │
│  │   - Manages Python lifecycle      │  │
│  │   - File system access            │  │
│  │   - Native menus/dialogs          │  │
│  └───────────────┬───────────────────┘  │
└───────────────────┼─────────────────────┘
                    │ child_process
┌───────────────────┴─────────────────────┐
│   FastAPI Server (Python)                │
│   - /api/projects/*                      │
│   - /api/pipelines/*                     │
│   - /api/feature-groups/*                │
│   - /api/repl/execute                    │
│   - /api/environments/*                  │
│   - /api/materialization/*               │
│   - WebSocket: /ws/pipeline-status       │
│                                          │
│   Wraps: seeknal Python library          │
│   (context, request, flow, featurestore) │
└──────────────────────────────────────────┘
```

## Technical Notes

### Existing Code to Leverage
- `REPL.execute_oneshot(sql)` returns `(columns, rows)` — perfect for the query editor endpoint
- `DAGBuilder` + `DAGRunner` — can expose DAG structure as JSON for visualization
- `RunState` at `target/run_state.json` — pipeline execution status for monitoring
- `FeatureGroup.list_versions()`, `.get_version()`, `.compare_versions()` — version browser
- `seeknal_project.yml` — project detection for file picker
- Parquet files in `target/intermediate/` and `target/feature_store/` — data preview

### Frontend Technology Stack
- **React** with TypeScript (largest ecosystem for Electron)
- **React Flow** for DAG visualization
- **Monaco Editor** or **CodeMirror 6** for SQL REPL
- **TanStack Table** for data grid/preview
- **Tailwind CSS** for styling
- **electron-builder** for packaging

### Backend Additions Needed
- New `src/seeknal/api/` module with FastAPI routes
- WebSocket support for real-time pipeline monitoring
- JSON serialization for all core models (many already have Pydantic)
- CORS middleware for localhost communication

## Constraints
- Python 3.11+ required (matches seeknal requirement)
- Node.js 18+ for Electron
- DuckDB runs in-process (no separate server)
- SQLite metadata DB is file-locked (single writer)

## Open Questions

_All resolved during brainstorm._

## Resolved Questions

1. **Q: Should we build a web app instead?** A: No — desktop app gives native file system access, can spawn Python processes, and works offline. FastAPI bridge architecture allows web version later.
2. **Q: React vs Vue vs Svelte?** A: React — largest ecosystem for Electron, best DAG visualization libraries (React Flow), Monaco Editor integration is mature.
3. **Q: How to handle Python dependency management?** A: Hybrid — detect existing seeknal, offer to install via uv/pip if missing. Avoids bundling a full Python runtime.
