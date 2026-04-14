---
name: edit-proof-document
description: "Apply a full-document rewrite to an existing Proof Editor document via the rewrite.apply op (CAUTION: disruptive, rejected when live collaborators are present)"
tags: [editing, proof, rewrite, ops]
version: "1.0.0"
---

# Edit Proof Document

Use this workflow when the user wants to REWRITE the full markdown body of an
existing Proof Editor document. The new markdown completely REPLACES the
existing body. Comments and marks are preserved server-side when their anchors
can still be matched.

**CAUTION**: `rewrite.apply` is disruptive. Hosted Proof servers REJECT the
rewrite if live collaborators currently have the document open — you will see
a `409 LIVE_CLIENTS_PRESENT` response in that case. For incremental edits,
prefer reading + diffing + applying the full rewrite only when safe.

## Tools you will call

- `read_proof_document` — fetch current markdown + state (NOT approval-gated)
- `edit_proof_document` — apply the rewrite (approval-gated)

## When to use

Trigger when the user says things like:

- "edit this proof doc"
- "rewrite the proof memo"
- "update the document at this URL"
- "fix the typo in the published memo"

## Typical flow

1. Call `read_proof_document(url)` to fetch the current markdown.
2. Compute the modified markdown (preserving structure where possible).
3. Present the approval gate (Phase 2 below).
4. Call `edit_proof_document(url, new_markdown=...)`.

## Phase 1 — Read current state

Call `read_proof_document(url)` first. The URL should be the full share link
(preferred, includes `?token=...` for write access) or a bare slug. The read
returns the current markdown + the `baseToken` / `revision` you'll need for
the rewrite precondition.

If the read returns a `PROJECTION_STALE` warning, the document is in a
recovery state — `rewrite.apply` will be rejected by the server. Surface the
issue to the user and suggest publishing a fresh memo instead.

## Phase 2 — Approval gate

You MUST present an approval menu via `ask_user` with these EXACT options. The
discriminator is `Apply edit to Proof` — the gate will not grant approval
unless that exact string is one of the options AND the user picks it.

Options (must be these exact strings):

- `Continue analysis`
- `Apply edit to Proof`
- `Done for now`
- `Type your own`

Do NOT print these as plain text, bullets, or numbered lists. They MUST come
through `ask_user`. Only call `edit_proof_document` after the user explicitly
chooses `Apply edit to Proof`.

Summarize the proposed edit (which document, what changes) BEFORE the menu so
the user knows what they are approving.

## Phase 3 — Call the tool

Required args:

- `url`: the same Proof share URL (preferred with `?token=`) or bare slug
- `new_markdown`: the complete new markdown body

The tool handles scheme upgrading (http → https for hosted Proof) and the
`baseRevision` / `baseToken` precondition automatically.

## Phase 4 — Final answer

After successful edit, your final answer MUST include:

1. The share URL of the updated document
2. The slug
3. The new markdown length
4. Any server warnings (`success=false`, etc.)

## Error paths to handle

- `401`/`403`: auth failed → URL must include `?token=...` granting write access
  OR `PROOF_API_KEY` must be set
- `404`: document not found at that slug
- `409 LIVE_CLIENTS_PRESENT` / `CONFLICT`: live collaborators have the doc open
  → tell user to retry when collaborators close it
- `409 CONFLICTING_BASE`: precondition mismatch → re-read state and retry
- `PROJECTION_STALE`: server in recovery → suggest publishing a fresh memo

## Environment

- `PROOF_BASE_URL`: fallback base URL when `url` is a bare slug
- `PROOF_API_KEY`: fallback bearer token when `url` has no `?token=...`
