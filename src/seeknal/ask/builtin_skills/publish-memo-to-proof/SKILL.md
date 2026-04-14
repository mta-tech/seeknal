---
name: publish-memo-to-proof
description: "Publish a markdown memo to a Proof Editor server (memokami.exe.xyz / proofeditor.ai / self-hosted) and return a shareable URL"
tags: [publishing, sharing, proof, memos]
version: "1.0.0"
---

# Publish Memo to Proof

Use this workflow when the user wants to share a markdown summary, memo, or
write-up with other people via a public link. Recipients open the returned
URL in any browser — no account required on their end.

## Tool you will call

- `publish_to_proof` (approval-gated)

## When to use

Trigger when the user says things like:

- "share this as a memo"
- "publish this to proof"
- "give me a link I can share"
- "make this public"
- After a successful `generate_report` call when the post-report menu is shown
  and the user picks `Publish memo to Proof`.

## Phase 1 — Draft the memo content

Before calling `publish_to_proof`, draft the memo body as Markdown:

- Use `##` headings for sections
- Use bullet lists, tables, and fenced code blocks where helpful (Proof renders
  them all natively)
- Cite SPECIFIC numbers from the analysis — avoid generic "performance was strong"
- Keep it scannable — recipients may not read the whole thing

## Phase 2 — Approval gate

You MUST present an approval menu via `ask_user` with these EXACT options. The
discriminator is `Publish memo to Proof` — the gate will not grant approval
unless that exact string is one of the options AND the user picks it.

Options (must be these exact strings):

- `Continue analysis`
- `Publish memo to Proof`
- `Done for now`
- `Type your own`

Do NOT print these as plain text, bullets, or numbered lists. They MUST come
through `ask_user`. Only call `publish_to_proof` after the user explicitly
chooses `Publish memo to Proof`.

## Phase 3 — Call the tool

Required args:

- `title`: memo title shown at the top of the Proof document
- `content`: the Markdown body from Phase 1
- `role` (optional, default `commenter`): access role granted to anyone with the
  link — `viewer`, `commenter`, or `editor`
- `base_url` (optional): per-call override for the Proof server URL

## Environment / configuration

- `PROOF_BASE_URL`: Proof server. Default `https://memokami.exe.xyz`. Override
  for `https://proofeditor.ai` or a self-hosted instance like `http://localhost:4000`.
- `PROOF_API_KEY`: Bearer token. Only required when the target server is
  configured with `PROOF_SHARE_MARKDOWN_AUTH_MODE=api_key`.

## Phase 4 — Final answer

After successful publish, your final answer MUST include:

1. The share URL from the response
2. The slug (for future revoke / lookup)
3. The owner secret preview (server masks all but first/last 4 chars)
4. A note that the role granted is `commenter` (or whatever was passed)

## Error paths to handle

- `401`/`403`: auth failed → tell user to set `PROOF_API_KEY`
- `404`: endpoint not found → server may not expose the publish alias
- `429`: rate-limited → tell user to retry in a moment
- Timeout: 30s default — surface clearly with the base URL
