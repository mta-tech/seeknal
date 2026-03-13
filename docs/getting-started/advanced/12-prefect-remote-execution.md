# Chapter 12: Prefect Remote Execution

> **Duration:** 35 minutes | **Difficulty:** Advanced | **Format:** YAML, Docker & CLI

Learn to run Seeknal pipelines on remote machines via Prefect workers, using either git-clone or Docker deployment.

---

## What You'll Build

A pipeline that runs on a remote worker instead of your laptop:

```
Your Laptop                           Remote Server
┌─────────────────────┐               ┌──────────────────────┐
│ seeknal prefect      │   deploy     │ Prefect Worker        │
│   deploy             │ ──────────→ │   polls for jobs      │
│                      │              │   runs your pipeline  │
│ seeknal prefect      │              │   writes results      │
│   generate --docker  │              │                       │
└─────────────────────┘               └──────────────────────┘
                                              │
                                      ┌───────┴────────┐
                                      │ PostgreSQL      │
                                      │ Iceberg         │
                                      │ (network DBs)   │
                                      └────────────────┘
```

**After this chapter, you'll have:**
- A Prefect deployment that runs your pipeline on a remote worker
- Understanding of `SEEKNAL_PROJECT_PATH` for remote path resolution
- A Docker image for reproducible deployments
- Date range filtering via Prefect UI parameters

---

## Prerequisites

Before starting, ensure you have:

- [ ] [Chapter 9: Database Sources](9-database-sources.md) — Connection profiles and `profiles.yml`
- [ ] A Prefect Server running (self-hosted or Prefect Cloud)
- [ ] Seeknal installed with Prefect: `pip install seeknal[prefect]`
- [ ] Docker installed (for Part 4 only)

---

## Part 1: Local Prefect Pipeline (8 minutes)

### Why Prefect?

Running `seeknal run` on your laptop is great for development. But for production you need:
- **Scheduled execution** — Run every hour, daily, weekly
- **Monitoring** — Track success/failure in a dashboard
- **Date parameters** — Override start/end dates per run from the UI
- **Remote execution** — Run on a powerful server, not your laptop

Seeknal's Prefect integration wraps your pipeline as a Prefect flow with each node as a separate task.

### Serve a Pipeline Locally

Start by running your pipeline as a Prefect flow on your local machine:

```bash
seeknal prefect serve --project-path .
```

This starts a local Prefect worker and registers your pipeline as a flow. Open the Prefect UI at `http://localhost:4200` to see it.

### Add Date Filtering

Your transforms can accept `start_date` and `end_date` parameters. Add them to a transform YAML:

```yaml
# seeknal/transforms/orders_cleaned.yml
kind: transform
name: orders_cleaned
inputs:
  - ref: source.raw_orders
params:
  start_date: "1900-01-01"
  end_date: "2099-12-31"
transform: |
  SELECT *,
    amount * quantity AS revenue
  FROM ref('source.raw_orders')
  WHERE status = 'completed'
    AND order_date >= '{{ start_date }}'
    AND order_date <= '{{ end_date }}'
```

The `params:` section provides defaults. When you run with `--start-date` / `--end-date`, those override the defaults:

```bash
# Run locally with date filter
seeknal run --start-date 2024-06-01 --end-date 2024-06-30

# Or via Prefect — dates appear as editable parameters in the UI
seeknal prefect serve --start-date 2024-06-01 --end-date 2024-06-30
```

**Checkpoint:** Open the Prefect UI. You should see `start_date`, `end_date`, and `full_refresh` as editable parameters on your flow.

---

## Part 2: Deploy to Prefect Server (7 minutes)

### Why Deploy Instead of Serve?

`seeknal prefect serve` runs a one-off local worker. `seeknal prefect deploy` registers your flow with Prefect Server so any worker can pick it up — including workers on remote machines.

### Deploy Your Pipeline

```bash
seeknal prefect deploy \
  --project-path . \
  --start-date 2024-01-01 \
  --end-date 2024-12-31
```

This registers the deployment with your Prefect Server. The `start_date` and `end_date` become default parameter values that can be overridden per-run in the UI.

### Generate prefect.yaml

For repeatable deployments, generate a `prefect.yaml` configuration file:

```bash
seeknal prefect generate --project-path .
```

This creates `prefect.yaml` in your project root:

```yaml
deployments:
- name: seeknal-ecommerce
  entrypoint: seeknal.workflow.prefect_integration:create_pipeline_flow
  parameters:
    project_path: /Users/you/projects/ecommerce
    max_workers: 4
    continue_on_error: false
    full_refresh: false
  work_pool:
    name: default
```

You can then deploy with `prefect deploy --all`.

**Checkpoint:** Run `seeknal prefect generate` and inspect the generated `prefect.yaml`.

---

## Part 3: Remote Execution with Git Clone (10 minutes)

### The Problem

When you deploy locally, `project_path` is frozen as `/Users/you/projects/ecommerce`. A remote worker at `/opt/seeknal/ecommerce` can't find the project — the flow fails with `FileNotFoundError`.

### The Solution: SEEKNAL_PROJECT_PATH

Set the `SEEKNAL_PROJECT_PATH` environment variable on the remote worker. It overrides the frozen `project_path` at runtime:

```
Deploy time (your laptop):     project_path = /Users/you/projects/ecommerce  (frozen)
Runtime (remote server):       SEEKNAL_PROJECT_PATH = /opt/seeknal/ecommerce (overrides)
```

### Set Up the Remote Server

**Step 1: Clone your project on the remote server**

```bash
# On the remote server
git clone git@github.com:your-org/ecommerce-pipeline.git /opt/seeknal/ecommerce
cd /opt/seeknal/ecommerce
```

**Step 2: Install seeknal**

```bash
uv sync
# or: pip install seeknal[prefect]
```

**Step 3: Set environment variables**

```bash
# Project path override
export SEEKNAL_PROJECT_PATH=/opt/seeknal/ecommerce

# Database credentials (used by profiles.yml)
export PG_HOST=db.internal.example.com
export PG_PASSWORD=production_password
export LAKEKEEPER_URL=http://lakekeeper.internal:8181
```

**Step 4: Create a connection profile**

```yaml
# /opt/seeknal/ecommerce/profiles.yml
connections:
  warehouse_pg:
    type: postgresql
    host: ${PG_HOST}
    port: 5432
    user: seeknal
    password: ${PG_PASSWORD}
    database: production
```

**Step 5: Start a Prefect worker**

```bash
prefect worker start --pool default
```

The worker connects to your Prefect Server, picks up the deployment, and runs the pipeline using the local project path.

### How Path Resolution Works

```
Prefect Server sends deployment parameters:
  project_path = /Users/you/projects/ecommerce   ← frozen from deploy time

Worker receives the job:
  1. Checks SEEKNAL_PROJECT_PATH env var          ← /opt/seeknal/ecommerce
  2. Validates path security (rejects /tmp, etc.)
  3. Resolves to /opt/seeknal/ecommerce           ← uses override
  4. Loads YAML definitions from seeknal/
  5. Runs pipeline
```

### Security

`SEEKNAL_PROJECT_PATH` is validated with path security checks. Insecure paths like `/tmp` or `/var/tmp` are rejected:

```
SEEKNAL_PROJECT_PATH=/tmp/malicious  →  ValueError: insecure location
SEEKNAL_PROJECT_PATH=/opt/seeknal    →  OK
```

**Checkpoint:** On a second machine (or a different directory), set `SEEKNAL_PROJECT_PATH`, start a Prefect worker, and trigger a run from the Prefect UI.

---

## Part 4: Docker Deployment (10 minutes)

### Why Docker?

Git-clone works, but you need to install dependencies on every server. Docker bakes everything into an image:

| | Git Clone | Docker |
|---|---|---|
| **Setup** | Clone + install on each server | Pull image |
| **Reproducibility** | Depends on server state | Exact versions pinned |
| **Updates** | `git pull && uv sync` | Pull new image tag |
| **Best for** | Teams with existing servers | CI/CD, Kubernetes, ephemeral workers |

### Use the Official Image

Seeknal publishes an official Docker image with seeknal and Prefect pre-installed:

```bash
docker pull ghcr.io/mta-tech/seeknal:latest
```

### Generate a Project Dockerfile

Generate a Dockerfile that layers your project YAML definitions on top of the official image:

```bash
seeknal prefect generate --docker
```

This creates two files:

**`prefect.yaml`** — Deployment configuration
**`Dockerfile`** — Project image definition:

```dockerfile
FROM ghcr.io/mta-tech/seeknal:2.4.0

# Copy project YAML definitions
COPY seeknal/ ./seeknal/

# profiles.yml is volume-mounted at runtime, not baked in
# Run with: docker run -v /path/to/profiles.yml:/app/profiles.yml ...
```

The generated Dockerfile is minimal because the base image already has seeknal installed and `SEEKNAL_PROJECT_PATH=/app` set.

### Build and Push Your Project Image

```bash
# Build
docker build -t your-registry.com/ecommerce-pipeline:latest .

# Push
docker push your-registry.com/ecommerce-pipeline:latest
```

### Run the Worker

```bash
docker run -d \
  -e PREFECT_API_URL=http://prefect-server:4200/api \
  -e PG_HOST=db.internal.example.com \
  -e PG_PASSWORD=production_password \
  -v /path/to/profiles.yml:/app/profiles.yml \
  your-registry.com/ecommerce-pipeline:latest
```

| Flag | Purpose |
|------|---------|
| `-e PREFECT_API_URL` | Connect to your Prefect Server |
| `-e PG_HOST, PG_PASSWORD` | Database credentials for `profiles.yml` |
| `-v profiles.yml:/app/profiles.yml` | Mount connection profile (not baked in) |

The container starts a Prefect worker that polls for jobs, runs your pipeline, and writes results to `target/`.

### Persist Pipeline State

Without a volume mount for `target/`, pipeline state (cached nodes, run history) is lost between container restarts:

```bash
docker run -d \
  -e PREFECT_API_URL=http://prefect-server:4200/api \
  -v /path/to/profiles.yml:/app/profiles.yml \
  -v /data/seeknal-state:/app/target \
  your-registry.com/ecommerce-pipeline:latest
```

**Checkpoint:** Build your project Docker image, run it locally with `docker run`, and verify it connects to your Prefect Server.

---

## Summary

| What | Command / Config |
|------|-----------------|
| Run locally as Prefect flow | `seeknal prefect serve` |
| Register with Prefect Server | `seeknal prefect deploy` |
| Generate deployment config | `seeknal prefect generate` |
| Generate Dockerfile | `seeknal prefect generate --docker` |
| Filter by date range | `--start-date 2024-01-01 --end-date 2024-12-31` |
| Override path on remote worker | `export SEEKNAL_PROJECT_PATH=/opt/seeknal/project` |
| Official Docker image | `ghcr.io/mta-tech/seeknal:latest` |

### Decision Guide

```
Do you have existing servers with seeknal installed?
  Yes → Git Clone mode (Part 3)
  No  → Docker mode (Part 4)

Do you need exact version pinning?
  Yes → Docker mode
  No  → Either works

Are you deploying to Kubernetes/ECS?
  Yes → Docker mode (build image, deploy as pod/task)
  No  → Either works
```

---

## Key Commands

```bash
# Install Prefect integration
pip install seeknal[prefect]

# Serve locally (development)
seeknal prefect serve --project-path . --start-date 2024-01-01

# Deploy to Prefect Server (production)
seeknal prefect deploy --project-path . --start-date 2024-01-01

# Generate deployment files
seeknal prefect generate                # prefect.yaml only
seeknal prefect generate --docker       # prefect.yaml + Dockerfile

# Start a remote worker (git-clone mode)
export SEEKNAL_PROJECT_PATH=/opt/seeknal/project
prefect worker start --pool default

# Run with Docker
docker pull ghcr.io/mta-tech/seeknal:latest
docker run -v profiles.yml:/app/profiles.yml your-image:latest
```

---

## Next Steps

- **[Pipeline Tags →](11-pipeline-tags.md)** — Run filtered subsets of your pipeline
- **[Database Sources →](9-database-sources.md)** — Connect to PostgreSQL and Iceberg for remote pipelines
- **[Production Environments →](../data-engineer-path/3-production-environments.md)** — Combine with `seeknal env` for dev/staging/prod

---

*Last updated: March 2026 | Seeknal Documentation*
