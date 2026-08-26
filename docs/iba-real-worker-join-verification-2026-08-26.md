# IBA real worker join verification — 2026-08-26

## Scope and verdicts

This is a local, client-observed join test of the shipped Seeknal HTTP worker
against IBA v2's actual premises-worker routes. It is not a deployment or a
test against customer data.

| Arm | Verdict | Why |
| --- | --- | --- |
| Successful run | **PASS** | The real CLI worker claimed bare broker work, all three callbacks and `/complete` received `200`, the user stream received assistant deltas and one completion, and the worker exited `0`. |
| Deterministic agent-run failure | **PARTIAL / route-race defect** | The real CLI worker sent `error`, `done`, then `/complete`; the first two callbacks were accepted and the user received a terminal `agent_error`, but `/complete` received `404` in two independent runs. The worker nevertheless exited `0` and did not report the refused callback. |

The failure arm used a deliberately local, deterministic runtime seam rather
than an external model: `seeknal.ask.gateway.server._run_agent_streaming` was
made to emit `error` then `done`. The shipped `seeknal gateway worker` CLI,
its HTTP client, its event/complete callback code, the IBA app, IBA routes,
WorkerRegistry, broker-backed EventSource, and user HTTP request were all
unchanged and real. Therefore it proves the error-envelope join and exposes
the `/complete` race; it does **not** prove a particular model provider's
failure behavior.

## Revisions and configuration

- IBA checkout: detached worktree at
  `bf43f3e63ab6fe22ffb4e94900352ebc75383c68` (`origin/main` resolved exactly
  to this SHA before execution). This includes route-race fence `2ccc9db`.
- Seeknal checkout: `a88e895150ec4e90a7cc96866233581f06929d55`.
- IBA app: local `create_app(...)`, with a generated ephemeral trajectory
  secret, model registry containing only `join-model`, and a `WorkerRegistry`
  token bound to `browser:join-user`. No token, secret, or request text is
  retained here.
- Event source: omitted from `create_app`, so IBA selected its default
  broker-backed premises EventSource.
- Observer: a local transparent forwarding proxy recorded only HTTP
  method/path/status, work response top-level keys, event type, and completion
  metadata. It forwarded each request unchanged to the local IBA app and did
  not log bearer headers, bodies, secrets, or error detail. This is client-side
  evidence, not an IBA server-log assertion.

## Secret-safe command shape

The app was started with environment-provided generated values and the
detached IBA worktree on `PYTHONPATH`:

```sh
JOIN_WORKER_TOKEN=<generated> JOIN_TRAJECTORY_SECRET=<generated> \
JOIN_DATA_DIR=<temporary-dir> JOIN_PORT=18081 \
PYTHONPATH=<detached-iba>/backend python <temporary>/iba_app.py
```

For each arm, the user-facing request was sent directly to IBA:

```sh
curl -sS -N -X POST http://127.0.0.1:18081/api/agent/analyst-streaming \
  -H 'Content-Type: application/json' \
  -H 'X-Identity-Id: browser:join-user' \
  --data '{"model":"join-model","user_question":"<arm marker>"}'
```

The actual shipped worker entry point was run unchanged, directed through the
transparent observer to the same IBA app:

```sh
JOIN_ARM=<success|failure> PYTHONPATH=<temporary-runtime-seam>:src \
<signal-venv>/bin/seeknal gateway worker --transport auto \
  --project /Users/fitrakacamarga/project/mta/signal-iba \
  --gateway-url http://127.0.0.1:18082 \
  --api-token <generated> --shutdown-timeout 1
```

The observer's upstream was `http://127.0.0.1:18081`; it was not a stub
gateway. `PYTHONPATH` provided the explicit deterministic agent-runtime seam
only inside the worker process.

## Positive control — observed 2026-08-26T14:41:44+07:00

With no `seeknal gateway worker` process running, the same user streaming
request was allowed three seconds. `curl` exited `28` (timeout) after receiving
zero bytes. This proves the measurement distinguishes an absent worker from a
delivered response; no null user stream was counted as success.

Two exploratory no-worker requests remained queued and were later claimed
when a worker was started. They were deliberately excluded from both arm
verdicts. This also means a client timeout alone must not be interpreted as
broker cancellation or completion.

## Successful arm — observed 2026-08-26T14:45:25+07:00

The worker remained live through work claim and subsequent `204` polls, then
was stopped intentionally after the user stream completed. The request process
and worker process both exited `0`.

| Client-observed step | Evidence |
| --- | --- |
| Worker config | `GET /internal/worker/config` -> `200` |
| Raw work claim | `GET /internal/worker/work-stream` -> `200`; top-level keys were `attempt`, `question`, `session_id`, `tenant_id`, `work_id`; `work_wrapper=false` |
| Event callbacks | `POST .../event` for `token`, `answer`, and `done` -> `200` each |
| Completion | `POST .../complete` with `event_count=3`, no error -> `200` |
| Continued liveness | repeated later `GET .../work-stream` -> `204` while the CLI was still running |
| Worker CLI | logged `start`, then `complete events=3 status=ok`; intentional shutdown; exit `0` |
| User NDJSON | `text_delta`/`assistant` with local marker `joined `; `text_delta`/`assistant` with `joined answer`; one `completion` with `elapsed_ms: 1` |

This establishes raw broker work consumption and producer-to-user delivery.
The visible content is deterministic local test text, not an LLM answer.

## Failure arm — observed 2026-08-26T14:44:31+07:00; repeated

The failure was a runtime/agent failure, not a protocol or authentication
failure: config and work claim both returned `200`, and the worker sent its
normal error trajectory. Both the first run and an independent repeat had the
same callback-status sequence:

| Client-observed step | First run | Independent repeat |
| --- | --- | --- |
| `GET /internal/worker/config` | `200` | `200` |
| Raw `GET /work-stream` | `200`, bare work object | `200`, bare work object |
| `POST .../event` type `error` | `200` | `200` |
| `POST .../event` type `done` | `200` | `200` |
| `POST .../complete` (`event_count=2`, error present) | **`404`** | **`404`** |
| User NDJSON | `{"type":"error","code":"agent_error","message":"The agent run failed."}` | same |
| Worker CLI / exit | `complete events=2 status=error`; exit `0` | same |

The exact local failure detail was intentionally not retained. The user-facing
message above is the IBA terminal classification actually received by the
client.

## Finding and limitation

**Observed finding:** on the failure trajectory, IBA accepts the terminal
`error` and `done` callbacks but rejects the worker's subsequent `/complete`
with `404`. The shipped worker does not treat that callback refusal as a
process failure: it logs completion with `status=error` and exits `0`.

This report does not infer why the work was gone; it only records the
reproducible client-side sequence. The failure arm is therefore not a clean
end-to-end completion acknowledgement despite successful user error delivery.
The route owner should decide whether terminal-event cleanup must retain the
work until `/complete`, or whether the worker/route contract deliberately
permits this `404` and needs an explicit acknowledgement rule.
