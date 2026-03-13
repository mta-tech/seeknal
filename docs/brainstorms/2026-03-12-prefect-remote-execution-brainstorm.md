---
title: "Prefect Remote Execution for Seeknal Pipelines"
type: feat
date: 2026-03-12
status: brainstorm
tags: [prefect, remote-execution, docker, deployment]
---

# Prefect Remote Execution for Seeknal Pipelines

## What We're Building

Enable seeknal pipelines to run on remote machines via Prefect workers, supporting two deployment modes:

1. **Git Clone + Worker** — Remote server clones the seeknal project repo, installs seeknal, runs a Prefect worker that polls for jobs
2. **Docker Container + Worker** — Build a Docker image with seeknal + project YAML baked in, run as a Prefect worker on any machine

Both modes connect to an existing self-hosted Prefect Server. Data sources are network databases (PostgreSQL, Iceberg/Lakekeeper) — no local CSV files in production. Credentials are managed via environment variables on the remote worker.

## Why This Approach

### Current Limitation

`SeeknalPrefectFlow` hardcodes `project_path` as an absolute local filesystem path. When `seeknal prefect deploy` registers a flow, the path (e.g., `/Users/fitra/projects/ecommerce`) is frozen into the deployment parameters. A remote worker at a different path (e.g., `/opt/seeknal/ecommerce`) can't find the project.

### Both Modes (C) Because

- **Git Clone** is the simplest path — minimal setup, good for teams that already have server infra
- **Docker** is needed for reproducibility and CI/CD — image pins exact versions of seeknal + project definitions
- The core fix is the same: **make `project_path` resolvable on the worker**, not hardcoded at deploy time

## Key Decisions

1. **`SEEKNAL_PROJECT_PATH` env var override** — If set on the worker machine, it overrides the `project_path` parameter from the deployment. This is the single change that enables both git-clone and Docker modes.

2. **Network databases only for production** — No need to sync local CSV/parquet files to remote workers. All sources connect over the network via profiles.yml or env vars.

3. **Environment variables for credentials** — Standard approach. Set `PG_HOST`, `PG_PASSWORD`, `LAKEKEEPER_URL`, etc. on the worker. No Prefect Secrets complexity needed.

4. **Existing Prefect Server** — Workers connect to a self-hosted Prefect Server (already running). No Prefect Cloud dependency.

5. **Docker as optional packaging** — `seeknal prefect generate --docker` generates a Dockerfile. Not required — git-clone works without Docker.

## Scope

### In Scope

- `SEEKNAL_PROJECT_PATH` env var override in `SeeknalPrefectFlow.__init__()`
- `seeknal prefect generate --docker` to generate a Dockerfile
- Documentation for both deployment modes (git-clone and Docker)
- Work pool configuration guidance

### Out of Scope

- Prefect Cloud integration (can be added later)
- Kubernetes/ECS deployment (Docker image is the building block — k8s orchestration is a separate concern)
- Local file sync to remote workers (production uses network databases)
- Prefect Blocks/Secrets for credentials (env vars are sufficient)

## Resolved Questions

1. **Should `profiles.yml` be baked into the Docker image or mounted as a volume?** — **Volume mount.** More flexible — change credentials without rebuilding the image. Mount via `docker run -v /path/to/profiles.yml:/app/profiles.yml`.

2. **Should the Dockerfile use `uv` or `pip` for installation?** — **uv.** Faster installs, matches local dev setup. Use `ghcr.io/astral-sh/uv` as base image.
