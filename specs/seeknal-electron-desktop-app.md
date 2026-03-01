---
title: "Seeknal Electron Desktop Application"
type: feat
date: 2026-03-01
status: ready
complexity: complex
origin: docs/brainstorms/2026-03-01-seeknal-electron-desktop-app-brainstorm.md
---

# Plan: Seeknal Electron Desktop Application

## Task Description

Build a cross-platform Electron desktop application for Seeknal that provides an all-in-one workspace for data and ML engineers. The app uses a **FastAPI bridge** architecture: a thin Python HTTP server wraps existing seeknal library functions as REST endpoints, while a React frontend in Electron's renderer process provides visual pipeline design, feature store management, an integrated DuckDB REPL, real-time pipeline monitoring, and environment management.

The app focuses on **visual advantages** over the CLI — interactive DAG graphs, data previews, schema diff viewers, and drag-and-drop editing — rather than simply mirroring CLI commands in a GUI. Distribution uses a hybrid model: auto-detect existing seeknal installation or offer to install via uv/pip.

Based on brainstorm: `docs/brainstorms/2026-03-01-seeknal-electron-desktop-app-brainstorm.md`

## Objective

1. **FastAPI Backend** — Create `src/seeknal/api/` module exposing core seeknal operations as REST + WebSocket endpoints
2. **Electron Shell** — Scaffold Electron app with main process managing Python server lifecycle and native OS integration
3. **React Frontend** — Build 6 core views: Dashboard, Pipeline DAG Editor, Feature Store Browser, SQL REPL, Pipeline Monitor, Environment Manager
4. **Packaging & Distribution** — Cross-platform builds (macOS, Windows, Linux) with hybrid Python detection

## Relevant Files

### Existing Files (Backend Integration Points)
- `src/seeknal/cli/main.py` — CLI commands to mirror as API endpoints (5400+ lines, ~35 commands)
- `src/seeknal/cli/repl.py` — `REPL.execute_oneshot(sql)` for SQL query execution
- `src/seeknal/workflow/dag.py` — `DAGBuilder`, `DAGNode` for pipeline graph structure
- `src/seeknal/workflow/runner.py` — `DAGRunner` for pipeline execution
- `src/seeknal/workflow/state.py` — `RunState`, `NodeStatus` for execution monitoring
- `src/seeknal/featurestore/feature_group.py` — Feature group CRUD + versioning
- `src/seeknal/featurestore/duckdbengine/feature_group.py` — DuckDB feature groups
- `src/seeknal/dag/manifest.py` — `Manifest`, `ManifestNode`, `NodeType` for DAG serialization
- `src/seeknal/pipeline/discoverer.py` — Python pipeline discovery and AST parsing
- `src/seeknal/models.py` — SQLModel database models (Project, FeatureGroup, etc.)
- `src/seeknal/context.py` — Global context management
- `src/seeknal/project.py` — Project management
- `src/seeknal/workspace.py` — Workspace context
- `src/seeknal/workflow/materialization/dispatcher.py` — Multi-target materialization
- `src/seeknal/workflow/consolidation/consolidator.py` — Entity consolidation
- `pyproject.toml` — Package configuration, dependencies

### New Files to Create

**Python API Layer:**
- `src/seeknal/api/__init__.py` — API module init
- `src/seeknal/api/app.py` — FastAPI application factory
- `src/seeknal/api/routes/projects.py` — Project CRUD endpoints
- `src/seeknal/api/routes/pipelines.py` — Pipeline parse, run, status endpoints
- `src/seeknal/api/routes/feature_groups.py` — Feature group browse, version, validate endpoints
- `src/seeknal/api/routes/repl.py` — SQL REPL execution endpoint
- `src/seeknal/api/routes/environments.py` — Env plan/apply/promote endpoints
- `src/seeknal/api/routes/materialization.py` — Materialization status and trigger endpoints
- `src/seeknal/api/routes/entities.py` — Entity consolidation endpoints
- `src/seeknal/api/ws.py` — WebSocket handler for real-time pipeline events
- `src/seeknal/api/schemas.py` — Pydantic response/request models
- `src/seeknal/api/deps.py` — FastAPI dependency injection (context, db session)

**Electron App:**
- `desktop/` — Root directory for Electron app
- `desktop/package.json` — Electron + React dependencies
- `desktop/electron/main.ts` — Main process (window, Python lifecycle, IPC)
- `desktop/electron/preload.ts` — Preload script for secure IPC
- `desktop/electron/python-bridge.ts` — Python server spawn/detect/manage
- `desktop/src/` — React frontend source
- `desktop/src/App.tsx` — Root component with router
- `desktop/src/pages/Dashboard.tsx` — Project overview dashboard
- `desktop/src/pages/PipelineEditor.tsx` — DAG viewer/editor with React Flow
- `desktop/src/pages/FeatureStore.tsx` — Feature group browser
- `desktop/src/pages/SqlRepl.tsx` — DuckDB query editor
- `desktop/src/pages/PipelineMonitor.tsx` — Real-time execution monitor
- `desktop/src/pages/EnvironmentManager.tsx` — Env plan/apply/promote UI
- `desktop/src/components/dag/` — DAG node components, custom edges
- `desktop/src/components/data-preview/` — Table viewer, schema display
- `desktop/src/components/diff-viewer/` — Schema diff component
- `desktop/src/hooks/useApi.ts` — API client hooks
- `desktop/src/hooks/useWebSocket.ts` — WebSocket connection hook
- `desktop/electron-builder.yml` — Build configuration for all platforms

**Tests:**
- `tests/api/` — FastAPI endpoint tests
- `desktop/src/__tests__/` — React component tests

## Step by Step Tasks

### Phase 1: Foundation — FastAPI Backend

#### 1. Scaffold FastAPI API module
- **Depends On:** none
- **Assigned To:** backend-builder
- **Agent Type:** backend-agent
- **Parallel:** false
- Create `src/seeknal/api/` package with FastAPI app factory
- Add `fastapi`, `uvicorn[standard]` to `pyproject.toml` optional dependencies under `[project.optional-dependencies] desktop`
- Implement dependency injection for seeknal context and DB session in `deps.py`
- Define Pydantic response schemas in `schemas.py` (ProjectResponse, FeatureGroupResponse, PipelineResponse, etc.)
- Add CORS middleware for `http://localhost:*` origins
- Add CLI command `seeknal api start --port 8765` to launch the server
- **Acceptance Criteria:**
  - [ ] `seeknal api start` launches FastAPI on configurable port
  - [ ] OpenAPI docs accessible at `/docs`
  - [ ] Health endpoint at `/api/health` returns 200

#### 2. Implement project management endpoints
- **Depends On:** 1
- **Assigned To:** backend-builder
- **Agent Type:** backend-agent
- **Parallel:** false
- `GET /api/projects` — list all projects
- `POST /api/projects` — create/init project
- `GET /api/projects/{id}` — project details (feature groups, flows, entities)
- `POST /api/projects/{id}/select` — set active project
- `GET /api/projects/current` — get current project context
- Wraps logic from `cli/main.py` init, list, show commands and `project.py`
- **Acceptance Criteria:**
  - [ ] All CRUD operations work via HTTP
  - [ ] Returns JSON with project metadata, feature group count, last run date

#### 3. Implement pipeline endpoints
- **Depends On:** 1
- **Assigned To:** backend-builder
- **Agent Type:** backend-agent
- **Parallel:** true (with task 4)
- `POST /api/pipelines/parse` — parse project, return manifest JSON (DAG nodes + edges)
- `POST /api/pipelines/run` — trigger pipeline execution (async)
- `GET /api/pipelines/status` — read `target/run_state.json` for execution state
- `GET /api/pipelines/nodes/{node_id}/preview` — preview intermediate parquet output (first 100 rows)
- `GET /api/pipelines/lineage` — return lineage graph
- WebSocket `/ws/pipeline-events` — stream node status changes during execution
- Wraps `DAGBuilder`, `DAGRunner`, `RunState`, and `discoverer.py`
- **Acceptance Criteria:**
  - [ ] Parse returns full DAG as JSON with nodes, edges, and metadata
  - [ ] Run triggers async execution and streams status via WebSocket
  - [ ] Preview returns columnar data for any intermediate parquet

#### 4. Implement feature store endpoints
- **Depends On:** 1
- **Assigned To:** backend-builder
- **Agent Type:** backend-agent
- **Parallel:** true (with task 3)
- `GET /api/feature-groups` — list all feature groups with metadata
- `GET /api/feature-groups/{name}` — detail view (schema, entity, materialization config)
- `GET /api/feature-groups/{name}/versions` — version history
- `GET /api/feature-groups/{name}/versions/{v1}/diff/{v2}` — schema diff between versions
- `POST /api/feature-groups/{name}/validate` — run feature validation
- `POST /api/feature-groups/{name}/materialize` — trigger materialization
- `GET /api/feature-groups/{name}/preview` — sample data from latest parquet
- `GET /api/entities` — list consolidated entities
- `GET /api/entities/{name}` — entity detail with catalog info
- Wraps `FeatureGroup`, `FeatureGroupDuckDB`, validation framework, entity consolidation
- **Acceptance Criteria:**
  - [ ] Version diff returns structured schema comparison
  - [ ] Preview returns sample rows from feature store parquets
  - [ ] Validation returns structured results (pass/warn/fail per rule)

#### 5. Implement REPL and environment endpoints
- **Depends On:** 1
- **Assigned To:** backend-builder
- **Agent Type:** backend-agent
- **Parallel:** true (with tasks 3, 4)
- `POST /api/repl/execute` — execute SQL via `REPL.execute_oneshot()`, return `{columns, rows, duration_ms}`
- `GET /api/repl/tables` — list registered tables/views in REPL
- `GET /api/environments` — list virtual environments
- `POST /api/environments/plan` — create env plan
- `POST /api/environments/{name}/apply` — execute env plan
- `POST /api/environments/promote` — promote environment
- `DELETE /api/environments/{name}` — delete environment
- `GET /api/materialization/status` — materialization targets and status
- **Acceptance Criteria:**
  - [ ] SQL execution returns structured results with timing
  - [ ] Read-only SQL allowlist enforced (same as CLI REPL)
  - [ ] Environment operations mirror CLI behavior

### Phase 2: Electron Shell

#### 6. Scaffold Electron application
- **Depends On:** none
- **Assigned To:** frontend-builder
- **Agent Type:** frontend-agent
- **Parallel:** true (with Phase 1)
- Initialize `desktop/` directory with `electron-vite` or `electron-forge` + React + TypeScript
- Configure `electron-builder.yml` for macOS (dmg), Windows (nsis), Linux (AppImage) targets
- Set up main process in `desktop/electron/main.ts` with BrowserWindow
- Create preload script with contextBridge for secure IPC
- Configure hot-reload for development
- **Acceptance Criteria:**
  - [ ] `npm run dev` launches Electron window with React
  - [ ] Preload script exposes typed IPC API
  - [ ] Build produces platform-specific artifacts

#### 7. Implement Python bridge in main process
- **Depends On:** 1, 6
- **Assigned To:** frontend-builder
- **Agent Type:** frontend-agent
- **Parallel:** false
- `desktop/electron/python-bridge.ts`:
  - Auto-detect seeknal: check `which seeknal`, validate version
  - If missing, prompt user and offer install via `uv pip install seeknal` or `pip install seeknal`
  - Spawn `seeknal api start --port <random-available-port>` as child process
  - Health check loop until `/api/health` responds
  - Pass port to renderer via IPC
  - Graceful shutdown on app quit (SIGTERM → wait → SIGKILL)
  - Restart on crash with exponential backoff (max 3 retries)
- **Acceptance Criteria:**
  - [ ] App launches and auto-starts Python API server
  - [ ] Missing seeknal detected and install offered
  - [ ] Server crashes are recovered automatically
  - [ ] Clean shutdown on app close

### Phase 3: React Frontend — Core Views

#### 8. Build navigation shell and API client
- **Depends On:** 6, 7
- **Assigned To:** frontend-builder
- **Agent Type:** frontend-agent
- **Parallel:** false
- Sidebar navigation with icons for each view (Dashboard, Pipelines, Features, REPL, Monitor, Environments)
- `useApi` hook wrapping fetch with base URL from IPC, error handling, loading states
- `useWebSocket` hook for pipeline event streaming
- Shared layout components: PageHeader, DataTable, StatusBadge, LoadingSkeleton
- Tailwind CSS setup with seeknal brand colors
- **Acceptance Criteria:**
  - [ ] Navigation between all 6 pages works
  - [ ] API calls succeed through the FastAPI bridge
  - [ ] Loading and error states display correctly

#### 9. Build Dashboard page
- **Depends On:** 2, 8
- **Assigned To:** frontend-builder
- **Agent Type:** frontend-agent
- **Parallel:** true (with tasks 10-13)
- Project selector (dropdown of all projects)
- Overview cards: feature group count, pipeline node count, last run status, entity count
- Recent activity feed (from run state history)
- Quick actions: Run Pipeline, Open REPL, New Feature Group
- **Acceptance Criteria:**
  - [ ] Project switching works and refreshes all data
  - [ ] Stats cards populated from API responses

#### 10. Build Pipeline DAG Editor
- **Depends On:** 3, 8
- **Assigned To:** frontend-builder
- **Agent Type:** frontend-agent
- **Parallel:** true (with tasks 9, 11-13)
- Interactive DAG visualization using **React Flow**
- Custom node components per NodeType (source, transform, aggregation, feature_group, etc.)
- Edge rendering showing data flow direction
- Node click → side panel with: config, SQL/Python code, materialization targets, last run status
- Minimap for large pipelines
- Layout algorithm (dagre) for auto-positioning
- Node data preview: click "Preview" on any node to see intermediate parquet data
- **Acceptance Criteria:**
  - [ ] DAG renders from `/api/pipelines/parse` response
  - [ ] Nodes are color-coded by type
  - [ ] Side panel shows node details and data preview
  - [ ] Minimap works for pipelines with 20+ nodes

#### 11. Build Feature Store Browser
- **Depends On:** 4, 8
- **Assigned To:** frontend-builder
- **Agent Type:** frontend-agent
- **Parallel:** true (with tasks 9, 10, 12, 13)
- Searchable list of feature groups with filters (entity, materialization type)
- Detail view: schema table, entity info, materialization config
- Version timeline: visual history with clickable versions
- Schema diff viewer: side-by-side comparison between any two versions (added/removed/changed columns)
- Data preview tab: sample rows from feature store parquet
- Validation results display (pass/warn/fail badges per rule)
- **Acceptance Criteria:**
  - [ ] Feature groups load and filter correctly
  - [ ] Version diff renders added (green), removed (red), changed (yellow) columns
  - [ ] Data preview shows first 100 rows with sortable columns

#### 12. Build SQL REPL
- **Depends On:** 5, 8
- **Assigned To:** frontend-builder
- **Agent Type:** frontend-agent
- **Parallel:** true (with tasks 9-11, 13)
- SQL editor using **Monaco Editor** (same as VS Code) with DuckDB syntax highlighting
- Execute button + Ctrl/Cmd+Enter shortcut
- Results pane: data table with sortable/resizable columns
- Query history sidebar (stored locally in Electron)
- Table explorer sidebar: list of registered tables/views with schema on click
- Execution time display
- Export results to CSV
- **Acceptance Criteria:**
  - [ ] SQL execution returns results in tabular format
  - [ ] Syntax highlighting for DuckDB SQL
  - [ ] Query history persists across sessions
  - [ ] Read-only enforcement (write queries show clear error)

#### 13. Build Pipeline Monitor and Environment Manager
- **Depends On:** 3, 5, 8
- **Assigned To:** frontend-builder
- **Agent Type:** frontend-agent
- **Parallel:** true (with tasks 9-12)
- **Pipeline Monitor:**
  - Real-time node status via WebSocket (pending → running → success/failed)
  - Progress bar for overall pipeline completion
  - Node-level logs and timing
  - Run history list with status, duration, timestamp
- **Environment Manager:**
  - List environments with status badges
  - Plan preview: show what changes would be applied
  - Apply button with confirmation dialog
  - Promote workflow with source/target env selection
  - Delete with confirmation
- **Acceptance Criteria:**
  - [ ] WebSocket streams node status changes in real-time
  - [ ] Progress bar updates as nodes complete
  - [ ] Environment operations work end-to-end through the UI

### Phase 4: Packaging & Polish

#### 14. Cross-platform packaging and distribution
- **Depends On:** 6-13
- **Assigned To:** fullstack-builder
- **Agent Type:** general-purpose
- **Parallel:** false
- Configure `electron-builder` for:
  - macOS: .dmg with code signing (optional)
  - Windows: NSIS installer
  - Linux: AppImage + .deb
- Auto-updater configuration (electron-updater)
- Application icon and branding
- First-run onboarding wizard (project directory selection, Python detection)
- **Acceptance Criteria:**
  - [ ] Builds succeed for all 3 platforms
  - [ ] Install and launch works on each platform
  - [ ] First-run wizard guides user through setup

#### 15. Integration testing and documentation
- **Depends On:** 14
- **Assigned To:** fullstack-builder
- **Agent Type:** test-agent
- **Parallel:** false
- FastAPI endpoint tests in `tests/api/`
- React component tests with React Testing Library
- E2E smoke test: launch app → detect Python → start API → navigate all pages
- Update `README.md` with desktop app section
- Add `docs/desktop-app.md` usage guide
- **Acceptance Criteria:**
  - [ ] API tests cover all endpoints
  - [ ] E2E smoke test passes
  - [ ] Documentation covers installation, first run, and all features

## Acceptance Criteria

### Functional Requirements
- [ ] F1: App launches and auto-starts the FastAPI backend server
- [ ] F2: Missing seeknal installation is detected and user is guided to install
- [ ] F3: Pipeline DAG is rendered as interactive graph from parsed project
- [ ] F4: Feature groups are browsable with version history and schema diffs
- [ ] F5: SQL REPL executes queries and displays tabular results
- [ ] F6: Pipeline execution can be triggered and monitored in real-time via WebSocket
- [ ] F7: Environments can be planned, applied, promoted, and deleted through the UI
- [ ] F8: Data preview works for intermediate parquets and feature store outputs
- [ ] F9: Cross-platform builds produce installable packages for macOS, Windows, Linux

### Non-Functional Requirements
- [ ] NF1: API server starts in under 3 seconds
- [ ] NF2: DAG renders pipelines with 50+ nodes without lag
- [ ] NF3: SQL query results return within 1 second for typical queries
- [ ] NF4: App package size under 150MB (excluding Python)
- [ ] NF5: Graceful recovery from Python server crashes

### Quality Gates
- [ ] QG1: All FastAPI endpoints have tests
- [ ] QG2: E2E smoke test passes on at least one platform
- [ ] QG3: No SQL injection vectors in REPL endpoint (read-only allowlist enforced)

## Team Orchestration

As the team lead, you have access to powerful tools for coordinating work across multiple agents. You NEVER write code directly - you orchestrate team members using these tools.

### Task Management Tools

**TaskCreate** - Create tasks in the shared task list:

```typescript
TaskCreate({
  subject: "Scaffold FastAPI module",
  description: "Create src/seeknal/api/ with app factory, deps, schemas...",
  activeForm: "Scaffolding FastAPI module"
})
```

**TaskUpdate** - Update task status, assignment, or dependencies:

```typescript
TaskUpdate({
  taskId: "1",
  status: "in_progress",
  owner: "backend-builder"
})
```

### Orchestration Workflow

1. **Phase 1 (tasks 1-5):** Backend-builder works sequentially on task 1, then tasks 2-5 can run in parallel
2. **Phase 2 (tasks 6-7):** Frontend-builder scaffolds Electron (can start parallel with Phase 1), then implements Python bridge after task 1 completes
3. **Phase 3 (tasks 8-13):** Frontend-builder works on navigation shell first, then all page components in parallel
4. **Phase 4 (tasks 14-15):** Fullstack-builder handles packaging and testing after all views are complete

### Team Members

#### Backend Builder
- **Name:** backend-builder
- **Role:** Python backend developer
- **Agent Type:** backend-agent
- **Responsibilities:** FastAPI server, REST endpoints, WebSocket handler, Pydantic schemas, API tests
- **Resume:** true

#### Frontend Builder
- **Name:** frontend-builder
- **Role:** Electron + React frontend developer
- **Agent Type:** frontend-agent
- **Responsibilities:** Electron shell, React views, React Flow DAG, Monaco REPL, component styling
- **Resume:** true

#### Fullstack Builder
- **Name:** fullstack-builder
- **Role:** Integration, packaging, and testing
- **Agent Type:** general-purpose
- **Responsibilities:** Cross-platform builds, E2E tests, documentation, first-run experience
- **Resume:** true

## Validation Commands

```bash
# Backend tests
pytest tests/api/ -v

# Start API server manually
seeknal api start --port 8765

# Frontend development
cd desktop && npm run dev

# Build for current platform
cd desktop && npm run build

# E2E smoke test
cd desktop && npm run test:e2e
```

## Notes

- The Atlas API pattern (`src/seeknal/cli/atlas.py`) provides prior art for a FastAPI server inside seeknal, though the package is currently unavailable. The new `src/seeknal/api/` module follows the same pattern.
- `REPL.execute_oneshot()` at `src/seeknal/cli/repl.py:375` is the cleanest programmatic interface for the SQL editor — returns structured `(columns, rows)` tuples.
- All data flows through parquet files in `target/` — the data preview feature reads these directly via DuckDB.
- `seeknal parse` already generates `manifest.json` — this is the primary input for the DAG visualization.

---

## Checklist Summary

### Phase 1: FastAPI Backend
- [ ] Scaffold API module with app factory and CLI command
- [ ] Project management endpoints
- [ ] Pipeline endpoints with WebSocket
- [ ] Feature store endpoints with version diff
- [ ] REPL and environment endpoints

### Phase 2: Electron Shell
- [ ] Scaffold Electron + React + TypeScript app
- [ ] Python bridge with auto-detect and lifecycle management

### Phase 3: React Frontend
- [ ] Navigation shell and API client hooks
- [ ] Dashboard page
- [ ] Pipeline DAG Editor (React Flow)
- [ ] Feature Store Browser with version diff
- [ ] SQL REPL (Monaco Editor)
- [ ] Pipeline Monitor + Environment Manager

### Phase 4: Packaging & Polish
- [ ] Cross-platform builds (macOS, Windows, Linux)
- [ ] Integration testing and documentation
