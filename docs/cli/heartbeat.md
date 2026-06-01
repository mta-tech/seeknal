# seeknal heartbeat

Polling daemon + smart inbox layer. `HEARTBEAT.md` at the project root drives
the loop: scan inbox folders → ingest new files → invoke `DAGRunner` → invoke
`ExposureExecutor` → append a run record to `target/heartbeat/runs.jsonl`.

The daemon is deterministic. Best-effort failure handling means it survives
malformed CSVs, DAG crashes, and exposure failures without exiting.

## Commands

| Command | Purpose |
|---------|---------|
| `seeknal heartbeat start` | Run the daemon (loops until SIGTERM/SIGINT). |
| `seeknal heartbeat tick` | Run a single tick and exit (useful for cron / k8s CronJob). |
| `seeknal heartbeat status [--limit N]` | Print the last N rows from `runs.jsonl`. |
| `seeknal heartbeat review <run_id>` | Interactive quarantine review (CLI parity with the Telegram flow). |

All commands accept `--profile <path>` to override the `profiles.yml` location.

## HEARTBEAT.md schema

`HEARTBEAT.md` is the project-level config. The body is either pure YAML, or
exactly one fenced ```` ```yaml ... ``` ```` block (other content is treated as
documentation comments).

```yaml
# Heartbeat (seeknal)
# Polling daemon config — read by `seeknal heartbeat start|tick`.
# Set interval: 0s to disable the daemon (tick still works).

interval: 60s              # Loop interval. 30s, 2m, 1h, 0s.
enabled: true              # Master switch. false → `start` exits cleanly.
inbox_folders:             # Folders scanned each tick.
  - inbox
ingested_target: parquet   # parquet | iceberg. Defaults to profile then "parquet".
quarantine_dir: target/heartbeat/quarantine
# project_name: my_project # Optional override; defaults to seeknal_project.yml.
```

## Inbox layout

```
<project>/
  HEARTBEAT.md
  inbox/              <- drop CSV/TSV/JSON/JSONL/Parquet here
  target/
    heartbeat/
      runs.jsonl                       <- one JSON line per tick
      runs/<run_id>.json               <- pretty sidecar per tick
      inbox_state.json                 <- per-file checksum/status
      quarantine/                      <- bad files
        needs_review/<run_id>/...      <- low-confidence classifier proposals
      .lock                            <- cross-process flock
    ingested/<table>/
      data.parquet
      _meta.json
```

## Idempotence

The daemon stores per-file SHA-256 checksums in `inbox_state.json`. A re-drop
of an unchanged file is a no-op — no DAG re-run, no exposure call, line in
`runs.jsonl` has `ingested: []` and `reason: interval`.

## Single-flight

Two overlapping ticks (within the same process or across processes) are
detected via an asyncio lock + `fcntl.flock` on `target/heartbeat/.lock`.
The losing tick is recorded as `{status: "skipped", reason: "tick-in-flight"}`.

## Best-effort failure handling

| Failure | Behavior |
|---------|----------|
| Malformed CSV | File moved to `target/heartbeat/quarantine/`, status `quarantined`. |
| DAGRunner exception | `dag.nodes_failed`/`errors[]` populated. Daemon continues. |
| ExposureExecutor failure | `exposure[].status = failed`. Daemon continues. |
| Classifier LLM outage (v0.5) | Fallback to deterministic `ingested.<stem>` with a warning. |

## Run record schema (AC4)

```json
{
  "run_id": "abc123",
  "started_at": "2026-05-15T10:00:00.000000Z",
  "finished_at": "2026-05-15T10:00:01.000000Z",
  "duration_ms": 1234,
  "status": "ok",            // ok | partial | failed | skipped
  "reason": null,            // populated when status=skipped
  "ingested": [
    {"file": "...", "table": "sales", "rows": 100, "status": "ok", ...}
  ],
  "dag": {
    "nodes_attempted": 5,
    "nodes_skipped": 1,
    "nodes_failed": 0,
    "fingerprints": {"node_id": "hash"},
    "per_node": [{"node_id": "..", "status": "..", "duration": 0.0, "row_count": 0}]
  },
  "exposure": [{"node": "exp1", "target": "...", "status": "ok"}],
  "errors": [],
  "classifier": [           // v0.5 only
    {"file": "...", "source_used": "catalog", "target_table": "bronze.sales",
     "confidence": 0.92, "decision": "routed"}
  ]
}
```

## Graceful shutdown

`SIGTERM` during a tick does not abort the in-flight tick — the current tick
completes (state writes are atomic via `os.replace` + flock + atomic append),
then the loop exits at the next iteration. For long-pipeline projects, set
Docker `--stop-timeout` ≥ longest expected tick duration (default 10s; bump
to 600s for slow pipelines).

## Telegram → inbox bridge (Step 7)

When `telegram.inbox_drop.enabled: true` is set in `seeknal_agent.yml`, the
existing Telegram document handler also copies the downloaded file into the
inbox folder. Heartbeat code never imports telegram — the boundary is the
file system.

```yaml
# seeknal_agent.yml
telegram:
  inbox_drop:
    enabled: true
    folder: inbox             # optional; default reads HEARTBEAT.md inbox_folders[0]
```

## See also

- `docs/cli/init.md` — `seeknal init` scaffolds `HEARTBEAT.md`.
- `docs/reference/configuration.md` — `source_defaults.ingested.target` profile fallback.
