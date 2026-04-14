---
name: publish-to-seeknal-report
description: "Publish a built Evidence.dev report to a Seeknal Report Server and return a shareable URL"
tags: [publishing, sharing, seeknal-report-server, evidence]
version: "1.0.0"
---

# Publish to Seeknal Report Server

Use this workflow AFTER a successful `generate_report` call, when the user
wants a shareable URL hosted on a Seeknal Report Server instance. Recipients
open the URL in any browser — no account required.

## Tool you will call

- `publish_to_seeknal_report` (approval-gated)

## When to use

Trigger when the user says things like:

- "publish this report"
- "give me a shareable link"
- "host this on the report server"
- After a successful `generate_report` call when the post-report menu is shown
  and the user picks `Publish to Seeknal Report Server`.

## Phase 1 — Confirm the report exists

The tool packages the existing `target/reports/<slug>/build/` tree, so a
successful `generate_report` call must have happened first. The slug is
derived from the report title via `_slugify()`.

If no built report exists for the given name, the tool returns an error
asking you to call `generate_report` first.

## Phase 2 — Approval gate

You MUST present an approval menu via `ask_user` with these EXACT options. The
discriminator is `Publish to Seeknal Report Server` — the gate will not grant
approval unless that exact string is one of the options AND the user picks it.

Options (must be these exact strings):

- `Continue analysis`
- `Publish to Seeknal Report Server`
- `Publish memo to Proof`
- `Done for now`
- `Type your own`

Do NOT print these as plain text, bullets, or numbered lists. They MUST come
through `ask_user`. Only call `publish_to_seeknal_report` after the user
explicitly chooses `Publish to Seeknal Report Server`.

## Phase 3 — Call the tool

Required args:

- `report_name`: the same name (or slug) used in the prior `generate_report` call
- `report_title` (optional): human-readable title sent as the
  `X-Seeknal-Report-Title` header. Defaults to `report_name`.
- `server` (optional): per-call override
- `api_key` (optional): per-call override

## Configuration precedence (first match wins)

1. The `server` / `api_key` arguments passed directly
2. `SEEKNAL_PUBLISH_SERVER` and `SEEKNAL_PUBLISH_TOKEN` env vars
3. The `publish.default.server` / `publish.default.api_key` section in
   the project's `profiles.yml`

Example `profiles.yml`:

```yaml
publish:
  default:
    server: http://172.19.0.9:8787
    api_key: ${SEEKNAL_PUBLISH_TOKEN}
```

## Phase 4 — Final answer

After successful publish, your final answer MUST include:

1. The full share URL
2. The slug
3. The owner secret (with a note that the server will NOT return it again)
4. The revoke command: `seeknal publish revoke {slug} --owner-secret <value>`

## Error paths to handle

- `PublishAuthError`: 401, key rejected → tell user to check `SEEKNAL_PUBLISH_TOKEN`
- `PublishContentTypeError`: server rejected the upload Content-Type
- `PublishTooLargeError`: tarball exceeds server max → ask operator to raise
  `SEEKNAL_REPORT_SERVER_MAX_UPLOAD_BYTES` or slim the report
- `PackageTooLargeError`: build tree too big to package
- `PublishServerError`: 5xx → surface server response
