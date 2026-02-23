---
name: qa
description: |
  Run spec-driven QA automation for seeknal medallion E2E pipelines.
  Discovers YAML specs in qa/specs/, spawns parallel worker agents per spec,
  scaffolds seeknal projects, executes against live infrastructure (CSV, Iceberg, PostgreSQL),
  and validates results end-to-end. Inspired by Bowser's composable automation pattern.
allowed-tools: Bash, Read, Write, Glob, Grep, Edit, Task, TaskCreate, TaskUpdate, TaskList, TeamCreate, TeamDelete, SendMessage, AskUserQuestion
---

# QA Automation: Medallion E2E Pipeline Testing

Run automated end-to-end tests for seeknal pipelines across all source types.

## Architecture

```
/qa skill (you are here)
  ↓
Step 0: Parse input — if .md, spawn spec-interpreter to generate .yml
  ↓
Orchestrator: discovers specs, health-checks infra, fans out workers
  ↓
Worker agents (parallel): one per spec file, each scaffolds + executes + validates
  ↓
seeknal CLI + DAGBuilder: actual pipeline execution against live infrastructure
```

**Input modes:**
- `/qa` — run all specs in `qa/specs/`
- `/qa qa/specs/foo.yml` — run a specific YAML spec
- `/qa specs/feature.md` — interpret feature spec, generate YAML, then run it

## Default Infrastructure Credentials

Before running any health checks or spawning workers, export these environment variables in your shell:

```bash
export LAKEKEEPER_URL="http://172.19.0.9:8181"
export LAKEKEEPER_WAREHOUSE_ID="c008ea5c-fb89-11f0-aa64-c32ca2f52144"
export LAKEKEEPER_WAREHOUSE="seeknal-warehouse"
export KEYCLOAK_TOKEN_URL="http://172.19.0.9:8080/realms/atlas/protocol/openid-connect/token"
export KEYCLOAK_CLIENT_ID="duckdb"
export KEYCLOAK_CLIENT_SECRET="duckdb-secret-change-in-production"
export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="CHANGE_THIS_STRONG_PASSWORD"
export AWS_ENDPOINT_URL="http://172.19.0.9:9000"
export AWS_REGION="us-east-1"
export PG_HOST="localhost"
export PG_PORT="5432"
export PG_USER="seeknal"
export PG_PASSWORD="seeknal_pass"
export PG_DATABASE="seeknal_test"
```

These are the canonical credentials for atlas-dev-server (Lakekeeper, Keycloak, MinIO) and local PostgreSQL. Individual specs may override these in their `env:` sections.

## Execution Flow

### Step 0: Parse Input

Check if the `/qa` skill received a file argument.

**Case A — No argument**: Set `target_specs = null`, proceed to Step 1 (discovers all `qa/specs/*.yml`).

**Case B — Argument ends with `.yml`**: Set `target_specs = [argument_path]`, skip to Step 1.

**Case C — Argument ends with `.md`**:

1. **Validate file exists**. If not found, abort with:
   ```
   Error: File not found: {path}
   ```

2. **Warn if not in specs/ directory**:
   ```
   Note: Input file is not from specs/ directory. Proceeding anyway.
   ```

3. **Derive output name** from the `.md` filename:
   - Strip date prefix matching `YYYY-MM-DD-` pattern
   - Strip type prefix matching `feat-`, `fix-`, `refactor-`
   - Use remaining slug as the spec name
   - Output path: `qa/specs/{derived-name}.yml`

   Examples:
   | Input | Output |
   |-------|--------|
   | `specs/named-refs-common-config.md` | `qa/specs/named-refs-common-config.yml` |
   | `specs/2026-02-20-feat-source-defaults-environment-switching.md` | `qa/specs/source-defaults-environment-switching.yml` |
   | `specs/fix-integration-security-issues.md` | `qa/specs/integration-security-issues.yml` |

4. **Spawn spec-interpreter agent** using the Task tool:

   ```
   Task(
     subagent_type="general-purpose",
     name="spec-interpreter",
     prompt=<see interpreter prompt below>
   )
   ```

   **Interpreter prompt template:**

   ```
   You are a QA spec interpreter agent. Your job is to:

   1. Read the feature spec at: {md_file_path}
   2. Read .claude/agents/qa-spec-interpreter.md for detailed instructions
   3. Follow those instructions to generate a QA test spec YAML
   4. Write the output to: {generated_spec_path}

   IMPORTANT: Read .claude/agents/qa-spec-interpreter.md first for detailed instructions.
   Feature spec: {md_file_path}
   Output YAML: {generated_spec_path}
   ```

   Wait for the agent to complete (do NOT use `run_in_background` — this must finish before proceeding).

5. **Validate generated YAML**. Read the output file and confirm it is parseable YAML with required fields (`name`, `source_type`, `pipeline`, `validation`). If validation fails:
   ```
   Error: Generated spec at {path} is not valid YAML or missing required fields.
   ```

6. **Print summary**:
   ```
   Spec generated: {generated_spec_path}
     Source type: {source_type}
     Pipeline nodes: {count}
     Features tested: {count}
   ```

7. Set `target_specs = [generated_spec_path]`, continue to Step 1.

### Step 1: Discover Specs

**If `target_specs` is set** (from Step 0): Use only those spec files. Skip filesystem discovery.

**If `target_specs` is null** (no argument): Read all YAML files from `qa/specs/` directory:

```bash
ls qa/specs/*.yml
```

For each spec file, read it and extract:
- `name`: Test scenario name
- `source_type`: csv, iceberg, or postgresql
- `infrastructure.requires`: List of required services
- `description`: What the test validates

Display discovery summary:
```
Discovered N spec(s):
  - csv-medallion (csv) - requires: none
  - iceberg-medallion (iceberg) - requires: lakekeeper
  - postgresql-medallion (postgresql) - requires: postgresql, lakekeeper
```

### Step 2: Infrastructure Health Check

For each unique infrastructure requirement across all specs:

**lakekeeper**: Check atlas-dev-server Lakekeeper is reachable (accepts 200 or 401 as healthy — auth is handled by seeknal at runtime):
```bash
HTTP_CODE=$(curl -s --connect-timeout 5 -o /dev/null -w '%{http_code}' 'http://172.19.0.9:8181/catalog/v1/config?warehouse=seeknal-warehouse' 2>/dev/null)
if echo "$HTTP_CODE" | grep -qE '^(200|401)'; then echo "OK"; else echo "UNREACHABLE"; fi
```

**postgresql**: Check local PostgreSQL is reachable:
```bash
pg_isready -h localhost -p 5432 -U seeknal -d seeknal_test 2>/dev/null && echo "OK" || echo "UNREACHABLE"
```

If a required service is unreachable, mark that spec as **SKIPPED** (best-effort — other specs still run).

Display health check results:
```
Infrastructure health:
  lakekeeper: OK
  postgresql: OK

Specs to run: N (M skipped due to unavailable infrastructure)
```

### Step 3: Clean Previous Runs

Remove existing run directories for specs that will execute:

```bash
rm -rf qa/runs/csv-medallion qa/runs/iceberg-medallion qa/runs/postgresql-medallion
```

### Step 4: Create Team and Fan Out Workers

Create a team for coordinated execution:

```
TeamCreate: team_name="qa-medallion-run"
```

For each spec that passes health checks:

1. **Create a task** via TaskCreate describing what the worker should do
2. **Spawn a worker agent** using the Task tool:

```
Task(
  subagent_type="general-purpose",
  team_name="qa-medallion-run",
  name="worker-{spec_name}",
  prompt=<see worker prompt below>,
  run_in_background=true
)
```

**Worker prompt template** (adapt per spec):

```
You are a QA worker agent. Your job is to:

1. Read the spec file at: qa/specs/{spec_name}.yml
2. Create the project directory at: qa/runs/{spec_name}/
3. Follow the qa-worker agent instructions in .claude/agents/qa-worker.md
4. Scaffold the seeknal project from the spec
5. Execute the pipeline
6. Validate all outputs
7. Report results back

IMPORTANT: Read .claude/agents/qa-worker.md first for detailed instructions.
Spec file: qa/specs/{spec_name}.yml
Output dir: qa/runs/{spec_name}/
```

Workers run in parallel (one per spec). Use `run_in_background=true` for parallelism.

### Step 5: Collect Results

Wait for all worker agents to complete. Check their output files or messages.

Each worker reports a structured result:
- Spec name
- DAG validation: PASS/FAIL (node count, edge count)
- Execution: PASS/FAIL (exit code)
- Output validation: PASS/FAIL (file existence, row counts)
- Feature coverage: list of features tested
- Errors: any error messages

### Step 6: Display Summary

Format and display the final QA summary table:

```
=== QA Automation Results ===

| Spec                  | Source     | DAG  | Execution | Outputs | Features | Status |
|-----------------------|------------|------|-----------|---------|----------|--------|
| csv-medallion         | csv        | PASS | PASS      | PASS    | 6/6      | PASS   |
| iceberg-medallion     | iceberg    | PASS | PASS      | PASS    | 10/10    | PASS   |
| postgresql-medallion  | postgresql | PASS | PASS      | PASS    | 15/15    | PASS   |

Overall: 3/3 PASSED
```

### Step 7: Cleanup

Shut down all team members and delete the team:

```
SendMessage(type="shutdown_request", recipient="worker-csv-medallion")
SendMessage(type="shutdown_request", recipient="worker-iceberg-medallion")
SendMessage(type="shutdown_request", recipient="worker-postgresql-medallion")
TeamDelete()
```

Report final result to user:
- Overall PASS/FAIL
- Link to `qa/runs/` for manual inspection
- Any failed specs with error details

## Adding New Tests

### Option A: Write a YAML spec manually

Create a YAML file in `qa/specs/`:

```yaml
name: my-new-test
description: What this test validates
source_type: csv|iceberg|postgres
infrastructure:
  requires: []
seed_data:
  my_file.csv: |
    col1,col2
    val1,val2
pipeline:
  bronze: [...]
  silver: [...]
  gold: [...]
validation:
  dag:
    expected_nodes: N
    expected_edges: [...]
  execution:
    success: true
  outputs:
    - node: transform.name
      min_rows: N
features_tested:
  - feature_name
```

No code changes needed. The next `/qa` run will automatically discover and execute it.

### Option B: Generate from a feature spec

Pass a feature implementation spec (`.md` file from `specs/`) directly:

```
/qa specs/my-feature.md
```

The spec-interpreter agent will read the feature spec, generate a QA test spec YAML at `qa/specs/{feature-name}.yml`, and then execute it. The generated spec persists for future reuse — subsequent `/qa` runs (with no args) will include it automatically.
