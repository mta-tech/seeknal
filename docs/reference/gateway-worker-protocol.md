# Gateway worker protocol

Seeknal's HTTP-only worker runs the agent locally while exchanging work and
stream events with the gateway over HTTP(S). This page defines the wire
contract between `seeknal gateway` and `seeknal gateway worker`.

## Authentication and tenant scope

The worker sends `Authorization: Bearer <api-token>` on every request. When the
gateway has an API-token registry, the gateway derives the tenant from that
token and ignores caller-provided tenant values. The worker may process only
work owned by that tenant.

Compatibility-mode gateways without a token registry use the existing tenant
resolver (`X-Tenant-ID`, the `tenant` query parameter, or the default tenant).
This mode exists for local and legacy deployments; token-derived tenant scope
is the secure deployment contract.

## Endpoints

All endpoint paths are relative to the configured gateway URL.

### Bootstrap runtime configuration

`GET /internal/worker/config` returns token-derived `tenant_id`, `task_queue`,
`project_path`, `worker_transport`, and optional callback and Temporal settings.
For HTTP-only execution, `worker_transport` is `http`; the worker then uses the
claim, event, and completion endpoints below. Treat returned callback credentials
as secrets.

### Claim work

```http
GET /internal/worker/work-stream?timeout=30
Authorization: Bearer <api-token>
```

`timeout` is a long-poll duration in seconds. The gateway clamps it to the
range 0 through 60. A claimed item returns `200` with this shape:

```json
{
  "work_id": "opaque-work-id",
  "tenant_id": "tenant-name",
  "session_id": "session-name",
  "question": "User question",
  "project_path": "/path/configured/by/gateway",
  "provider": "optional-provider",
  "model": "optional-model",
  "created_at": 1730000000.0
}
```

The gateway returns `204` when no work becomes available before the poll
expires. The worker applies backoff and polls again after gateway or transport
failures.

### Publish a stream event

```http
POST /internal/worker/work/{work_id}/event
Authorization: Bearer <api-token>
X-Seeknal-Event-Id: {work_id}:{sequence}
Content-Type: application/json

{"type": "token", "data": "..."}
```

The request body is the same event object emitted by the agent stream. The
gateway publishes its serialized JSON to the claimed work item's session SSE
channel and tenant.

`X-Seeknal-Event-Id` is the idempotency key for event delivery. Workers use a
stable, unique value for all attempts to deliver one event. The gateway uses a
reserve, publish, commit sequence:

1. The first request reserves the event ID.
2. The gateway publishes the event to SSE.
3. A successful publish commits the event ID as delivered.

If publishing fails, the gateway releases the reservation, returns an error,
and a retry with the same ID may publish the event. If the publish succeeded
but the HTTP acknowledgement was lost, a retry returns `200` with
`{"status":"duplicate"}` without publishing a second copy. A concurrent
request for an ID that is still reserved returns `503`; the worker must retry
because the first request may still fail. Omitting the header preserves legacy
non-deduplicated delivery.

The ledger is scoped to a work item in one gateway process and is removed on
completion, discard, or reset. Unknown or completed work returns `404`.
Successful publication means acceptance by the configured broadcaster for
fan-out; it does not acknowledge receipt by every SSE subscriber. Broadcaster
implementations must surface publish failures for reservation release to work.
The Redis broadcaster returns `true` for acceptance and `false` on failure.
HTTP worker event and completion replay handlers check this outcome and fail
the request with a retryable `5xx` on `false`, releasing the reservation.
Legacy broadcasters returning no value retain their exception-based acceptance
contract. Other gateway publication paths keep their existing best-effort
behavior and may ignore the Redis outcome.

### Complete work

```http
POST /internal/worker/work/{work_id}/complete
Authorization: Bearer <api-token>
Content-Type: application/json
```

The completion body supports these fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `answer` | string | Final answer text retained as the completion fallback. |
| `event_count` | integer | Number of events produced by the agent stream. |
| `error` | string or null | Agent or worker error, if the turn failed. |
| `dropped_event_count` | integer | Number of stream events the worker could not deliver. Malformed values are treated as `0`. |
| `answer_dropped` | boolean | The final `answer` event was not delivered. |
| `done_dropped` | boolean | The terminal `done` event was not delivered. |
| `undelivered_events` | array of event objects | Critical side-channel events that could not be delivered. |
| `undelivered_evicted_count` | integer | Number of replay events evicted before completion. Defaults to `0`; malformed values become `0`. This is a loss metric, not an event to synthesize. |

The worker includes `ask_user`, `visualization`, and `upload_complete` events in
`undelivered_events`. It retains at most five replay events and bounds each
event to at most 16 KB of serialized JSON bytes. When bounding removes payload
content, the event includes `"payload_truncated": true`; consumers must treat
that marker as an explicit incomplete payload rather than the original event.
The gateway preserves the entire replay event, including its truncation marker
and any placeholder or error data supplied by the worker. Replay eviction is
reported separately in `undelivered_evicted_count`.

Before resolving the work, the gateway reconciles dropped events to the work's
session SSE channel in this order:

1. Publish each `undelivered_events` entry in array order.
2. If `answer_dropped` is true and `answer` is non-empty, publish
   `{"type":"answer","data":"..."}`.
3. If `done_dropped` is true, publish `{"type":"done"}`.

The gateway then stores all completion fields in the broker result and resolves
the waiting turn. A successful completion returns `200` with
`{"status":"completed"}`.

If reconciliation fails partway through, work remains in flight. A retry of
the same completion skips replay entries already published and resumes the
remaining entries. Completion retries must preserve the replay array order
and contents. Once completion succeeds, a later retry returns `404` because
the work has been removed; the worker treats that response as work gone.

## Retry and status-code contract

Workers classify event and completion responses as follows:

| Status | Worker behavior |
| --- | --- |
| Any `2xx` | Accepted. Do not retry that request. |
| `404` or `410` | Work is gone or expired. Stop event delivery; further completion attempts are pointless. |
| `429` or any `5xx` | Retryable. Apply bounded backoff; honor `Retry-After` when present. |
| Other `4xx` | Non-retryable request or authentication failure. Log and do not retry that request. |

Transport failures are retryable. Event delivery uses a bounded attempt count;
completion delivery uses a bounded persistence window because it carries the
final answer and reconciliation fields. An event retry must reuse the original
`X-Seeknal-Event-Id` so the gateway can distinguish a lost acknowledgement
from a new event.
