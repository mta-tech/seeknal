# IBA real worker join verification — 2026-08-26

## Original scope and verdicts — IBA `bf43f3e`

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

## Re-verification — IBA `43b7a8b` (observed 2026-08-26)

The original defect was fixed by IBA commit
`43b7a8b7282080a54b613d6329df63970a87f465`, verified as the exact
`origin/main` revision before this re-run. This section does not revise the
historical `bf43f3e` evidence above: it records the client-observed result
after the fix.

Configuration and boundary were intentionally the same as the original join
test: a fresh detached IBA checkout, generated local trajectory secret,
single allowed `join-model`, tenant-matched `WorkerRegistry` token, default
broker-backed EventSource, the shipped `seeknal gateway worker` CLI, real IBA
worker routes, and a transparent observer that forwarded the worker's requests
to the actual app. The deterministic local `server._run_agent_streaming` seam
was retained solely to produce stable success and failure trajectories; no
Seeknal product code or IBA-owned files were edited.

| Arm | Current verdict | Client-observed result |
| --- | --- | --- |
| Success | **PASS** | Bare work claim; `token`, `answer`, `done`, and `/complete` callbacks all `200`; user received two assistant `text_delta` records and one `completion`; request and worker processes exited `0`. |
| Deterministic failure | **PASS** | Bare work claim; `error`, `done`, and `/complete` callbacks all `200`; user received one terminal `error` (`agent_error`); request and worker processes exited `0`. |

### Success callback sequence — observed 2026-08-26T14:53:40+07:00

1. `GET /internal/worker/config` -> `200`.
2. `GET /internal/worker/work-stream` -> `200`, bare object with top-level
   `attempt`, `question`, `session_id`, `tenant_id`, and `work_id`; no `work`
   wrapper.
3. `POST .../event` for `token` -> `200`.
4. `POST .../event` for `answer` -> `200`.
5. `POST .../event` for `done` -> `200`.
6. `POST .../complete` with `event_count=3`, no error -> `200`.
7. Continued worker polls returned `204` before intentional shutdown.

The real worker logged `complete events=3 status=ok` and exited `0`. The user
stream contained `text_delta` on `assistant` for the deterministic markers
`joined ` and `joined answer`, followed by `completion` with `elapsed_ms: 1`.

### Failure callback sequence — observed 2026-08-26T14:54:00+07:00

1. `GET /internal/worker/config` -> `200`.
2. `GET /internal/worker/work-stream` -> `200`, the same bare-object shape.
3. `POST .../event` for `error` -> `200`.
4. `POST .../event` for `done` -> `200`.
5. `POST .../complete` with `event_count=2`, error present -> **`200`**.
6. Continued worker polls returned `204` before intentional shutdown.

The real worker logged `complete events=2 status=error` and exited `0`. The
user stream contained exactly one terminal event:
`{"type":"error","code":"agent_error","message":"The agent run failed."}`.

### Current conclusion

The prior `bf43f3e` failure was real and remains documented above. At
`43b7a8b`, the same real-worker route ordering now retains the work through
the failure trajectory's `/complete` callback: the previously reproducible
`404` was not observed. Both client-observed arms pass under the stated local
deterministic runtime seam. This remains a local join verification, not a live
external-model or production deployment test.

## Tenant-namespace re-verification — IBA `679169ab` (observed 2026-08-26)

This is a fresh local join run at exact remote-durable IBA `origin/main`
`679169ab9c496241d4df1255500905493bd552de`. It extends, rather than replaces,
the `bf43f3e` failure and `43b7a8b` repair evidence above.

The deliberate authority axes were:

| Axis | Value |
| --- | --- |
| Verified caller identity | `browser:join-user` |
| Configured trusted resolver result | `tenant-a` |
| WorkerToken tenant | `tenant-a` |
| Forged caller payload field, success arm | `tenant_id: tenant-b` |

The IBA app was a fresh detached checkout configured with a generated local
trajectory secret, one `join-model`, the default broker-backed EventSource,
the resolver above, and the tenant-`a` WorkerToken. The real shipped
`seeknal gateway worker` CLI and its unchanged HTTP client/callback code were
used through the same transparent observer as prior runs. A local deterministic
runtime seam emitted the stable success or failure trajectory only; no product
source or IBA router was changed.

### OBSERVED — successful arm at 2026-08-26T16:55:19+07:00

The user request carried the forged `tenant_id: tenant-b` alongside
`X-Identity-Id: browser:join-user`. The observer saw:

1. `GET /internal/worker/config` -> `200`.
2. `GET /internal/worker/work-stream` -> `200`, a **bare** work object with
   top-level `attempt`, `question`, `session_id`, `tenant_id`, and `work_id`.
   Its `tenant_id` was **`tenant-a`**, not the supplied `tenant-b`.
3. `POST .../event` for `token`, `answer`, and `done` -> `200` each.
4. `POST .../complete` with `event_count=3`, no error -> `200`.
5. Continued worker polls returned `204` until intentional shutdown.

The user stream contained assistant `text_delta`, assistant `text_delta`, and
`completion`. The real worker logged `complete events=3 status=ok`; the user
request and worker process each exited `0`.

### OBSERVED — deterministic failure arm at 2026-08-26T16:55:40+07:00

1. `GET /internal/worker/config` -> `200`.
2. `GET /internal/worker/work-stream` -> `200`, the same bare object shape,
   with `tenant_id: tenant-a`.
3. `POST .../event` for `error` -> `200`.
4. `POST .../event` for `done` -> `200`.
5. `POST .../complete` with `event_count=2`, error present -> `200`.
6. Continued worker polls returned `204` until intentional shutdown.

The user stream contained exactly one terminal event:
`{"type":"error","code":"agent_error","message":"The agent run failed."}`.
The real worker logged `complete events=2 status=error`; the user request and
worker process each exited `0`.

### OBSERVED — unconfigured resolver negative path at 2026-08-26T16:56:09+07:00

A separate fresh IBA app was configured identically except that it used the
default unconfigured tenant resolver. The same user request, including forged
`tenant_id: tenant-b`, received an ordinary pre-stream HTTP `503` with the
`CONFIG_UNAVAILABLE` envelope. An authenticated raw
`GET /internal/worker/work-stream?timeout=0` using the same tenant-`a` worker
token then received `204` with zero response bytes. No work was admitted in
this negative app instance.

### REASONED — boundary interpretation

The live observations prove that the claimed work's tenant was `tenant-a` and
that a forged `tenant-b` body field did not move this run's queue. Reading the
pinned source explains the boundary: the user request parser has no tenant
field, the EventSource resolves its tenant from the verified identity through
the injected resolver, and worker authorization derives tenant only from the
Bearer token. That source interpretation is not substituted for the observed
claim/callback evidence above.

### Current tenant-namespace verdict

**PASS.** At `679169ab`, both deterministic real-worker trajectories delivered
through the tenant-`a` queue and acknowledged every callback, including
failure `/complete`, with `200`. The forged caller `tenant-b` value had no
observed effect. The unconfigured resolver failed closed before streaming and
admitted no work in the independently configured negative instance. This is
still a local deterministic-runtime join verification, not an external-model
or production deployment test.
