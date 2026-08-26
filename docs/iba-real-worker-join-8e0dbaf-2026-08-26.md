# Real IBA / Seeknal worker acceptance — 2026-08-26

## Scope

Fresh local acceptance run against a detached IBA checkout pinned to exact
remote-durable `origin/main`
`8e0dbaf0e54122e7678ff008134e2ad1b96e208a`. This is separate from the
earlier `bf43f3e`, `43b7a8b`, and `679169ab` reports.

- Seeknal revision: `4dfc16c0aa63ccb83cdbbfa6fe3a63ce57c810c5`.
- IBA app: real local FastAPI app using an explicit generated trajectory
  secret, registry entry for `join-model`, trusted resolver
  `browser:join-user -> tenant-a`, tenant-`a` WorkerToken, default
  broker-backed EventSource, real bridge, and real user streaming route.
- Worker: shipped `seeknal gateway worker` CLI and its unchanged production
  HTTP client/callback code.
- Observer: transparent local forwarder only. It passed every worker request
  unchanged to the real IBA app and recorded no bearer value, request body,
  secret, or upstream error text.
- Deterministic seam: only `server._run_agent_streaming` in the premises
  worker process was locally set to emit stable success or failure events. It
  did not replace the CLI, worker HTTP client, IBA broker/auth/routes/EventSource,
  bridge, or user route. This validates the transport join rather than an
  external model provider.

## Secret-safe real worker command shape

```sh
JOIN_ARM=<success|failure> PYTHONPATH=<local-runtime-seam>:src \
  <signal-venv>/bin/seeknal gateway worker --transport auto \
  --project /Users/fitrakacamarga/project/mta/signal-iba \
  --gateway-url http://127.0.0.1:18082 \
  --api-token <generated-ephemeral-token> --shutdown-timeout 1
```

The observer on `18082` forwarded unchanged to the actual IBA app on `18081`.
The user POSTs went directly to IBA at `/api/agent/analyst-streaming` with
`X-Identity-Id: browser:join-user`.

## OBSERVED — successful arm (2026-08-26T20:35:54+07:00)

The user POST returned HTTP `200` with `application/x-ndjson`. Its payload also
included a deliberately forged `tenant_id: tenant-b`; the observer saw a
claimed work object carrying `tenant_id: tenant-a`.

| Client-observed operation | Result |
| --- | --- |
| `GET /internal/worker/config` | `200` |
| `GET /internal/worker/work-stream` | `200`, bare object—not a `work` envelope—with top-level `attempt`, `question`, `session_id`, `tenant_id`, `work_id`; tenant `tenant-a` |
| `POST .../event` type `token` | `200` |
| `POST .../event` type `answer` | `200` |
| `POST .../event` type `done` | `200` |
| `POST .../complete` | `200`; `event_count=3`, no error |
| Later polls while the CLI remained live | `204` no-work |
| Worker log / process | `complete events=3 status=ok`; exit `0` |

Decoded user events, in order: `text_delta` on `assistant` with deterministic
marker `acceptance `; `text_delta` on `assistant` with `accepted answer`; then
`completion` with `elapsed_ms: 1`.

## OBSERVED — deterministic runtime-failure arm (2026-08-26T20:36:16+07:00)

The user POST returned HTTP `200` with `application/x-ndjson`. This was a
runtime failure trajectory, not a worker authentication or protocol failure.

| Client-observed operation | Result |
| --- | --- |
| `GET /internal/worker/config` | `200` |
| `GET /internal/worker/work-stream` | `200`, same bare work shape; tenant `tenant-a` |
| `POST .../event` type `error` | `200` |
| `POST .../event` type `done` | `200` |
| `POST .../complete` | `200`; `event_count=2`, error present |
| Later polls while the CLI remained live | `204` no-work |
| Worker log / process | `complete events=2 status=error`; exit `0` |

Decoded user events: exactly one terminal
`{"type":"error","code":"agent_error","message":"The agent run failed."}`.
The detailed local failure text was deliberately not retained.

## REASONED boundary statement

No server log or source inference was used to establish either verdict. The
tenant result is client-observed in the bare work object returned to the shipped
worker: it was `tenant-a` even when the successful caller body supplied
`tenant-b`. That establishes the exercised identity-to-tenant and token queue
axes for this acceptance run; it does not claim a broader production deployment
or external-model proof.

## Verdicts and cleanup

| Arm | Verdict |
| --- | --- |
| Success | **PASS** |
| Deterministic runtime failure | **PASS** |

The IBA app and observer were stopped after both runs. The detached IBA
worktree and temporary evidence directory were removed from the working area
after this report was written. No IBA source or router was modified.
