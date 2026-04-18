---
name: record-entry
description: "Data-entry, assistant, and advisor: record one-off events from text (/record ...) or receipt/transfer images into the right ingest table, asking questions until the row is complete"
tags: [record, data-entry, image, ocr, telegram]
version: "1.0.0"
---

# Record Entry

Use this workflow when the user gives you a **single event to record** —
either as a free-form `/record ...` text message or as a receipt /
fund-transfer image. Common shapes:

- `/record fitra, 1 mie ayam, 1 mineral water` — expense to capture
- A photo of a BCA, Mandiri, GoPay, OVO, DANA, or QRIS transfer
- A screenshot of a food order or shop receipt

Think of yourself as a **data entry clerk + advisor**: parse the input,
look at the tables we already have, propose where this row should go, ask
clarifying questions until every required field is resolved, then write.

**Never silently invent data.** Missing amount, missing counterparty,
ambiguous item name — always `ask_user`.

## Tools you will call

- `parse_record` — deterministic parser for `/record ...` text
- `extract_from_image` — Gemini-vision OCR for receipt/transfer images
- `list_tables` / `describe_table` — enumerate existing `ingest_*` tables
- `propose_record_table` — rank existing tables vs draft, recommend match or new
- `ask_user` — resolve missing fields and confirm schema
- `write_ingested_table` — persist as parquet + DuckDB view
- `save_ingestion_skill` — if we just created a new table, save a re-run skill
- `execute_sql` — verify the row landed, answer analytical follow-ups

## Phase 1 — Parse the input and decide the kind

- **Text:** call `parse_record(<full user message>)`. The tool returns
  *hints only* — detected amounts, bank/e-wallet tokens, item lists,
  a best-guess `kind_hint`. It does NOT dictate which fields are
  required. **You** classify the record:

  | Looks like | kind |
  |---|---|
  | Rp / bank / transfer / QRIS | `transfer` |
  | shop / receipt / merchant | `receipt` |
  | food order with items + amount | `order` |
  | workout / run / pushups / gym / yoga | `activity` |
  | meeting / rapat / standup / call / discussed | `meeting` |
  | weight / BP / sleep / pulse / glucose | `health` |
  | money in/out, no counterparty | `expense` |
  | anything else | `note` |

  Add a `kind` field to the draft before calling
  `propose_record_table`. If the message spans two kinds (e.g. a
  transfer receipt with attendees), pick the dominant one and keep the
  rest in `note`.

- **Image:** call `extract_from_image(<absolute image path>, hint='...')`.
  The `hint` is optional — if the user captioned the image ("BCA
  transfer to mom"), pass that verbatim so Gemini can focus.

Both tools store the source path on `ToolContext.last_read_staging_path`
so downstream tools can reference it without the user retyping.

**Custom kinds.** If none of the templates fit (e.g. inventory check,
supplier order, study session), set `kind` to a short lowercase slug and
pass an explicit `_columns` list in the draft — see
`propose_record_table` for the format. Templates are starting points,
not walls.

## Phase 2 — Propose the target table

1. Call `list_tables` to see what exists.
2. Take the JSON draft from Phase 1 and call
   `propose_record_table(draft_json=<json string>, hint_table_name=<what
   the user named it, if anything>)`.
3. Present the proposal via `ask_user` with these options:
   - `Use this table` — accept the proposed match / new table
   - `Pick a different existing table` — user names another
   - `Rename / redesign` — user provides a new slug or column tweak
   - `Type your own`

Never skip this. Even if there's only one existing table, the user might
want a new one for this record.

## Phase 3 — Resolve ambiguous fields (strict)

Look at the draft's null / missing fields. For each one that is required
for the chosen table (see the proposal's business key and schema), call
`ask_user` with concrete options drawn from the draft. Examples:

- **Amount missing from /record:** "I couldn't find an amount. How much
  was spent total?" — free-text.
- **Item price missing:** if the target table has a `price` column and
  the item isn't in the menu, `ask_user` for the unit price.
- **Recipient missing on a transfer:** show the OCR raw text, ask for
  the recipient name.

Do NOT write until every required field is resolved. The user chose
strict clarification — zero dirty rows is the bar.

## Phase 4 — Stage and write

1. Build a one-row parquet for the confirmed draft. The simplest path:
   - Write the row to `target/ask_ingest/_staging/<uuid>/row.csv`.
   - Call `write_ingested_table(source_path=<that csv>, table_name=...,
     business_key=..., mode='create'|'append')`.
2. For `mode='append'`, always call first with `user_confirmed=False`,
   inspect the drift report, then re-call with `user_confirmed=True` and
   the user's chosen `dedup_strategy`.
3. Verify with `execute_sql("SELECT * FROM ingest_<table> ORDER BY
   entered_at DESC LIMIT 3")`.

## Phase 5 — Save a skill (new tables only)

If Phase 2 ended with a new table, call
`save_ingestion_skill(skill_name='record-<table>', table_name=<table>,
business_key=<chosen>, columns_json=<schema JSON>, description='...')`.
Next time the user sends a record that fits this shape,
`propose_record_table` will match it automatically.

## Phase 6 — Offer analysis

After writing, suggest 1-2 short analytical follow-ups using the data
just recorded:

- "Top 3 items this week by quantity?"
- "How much have I transferred to this recipient this month?"

Use `execute_sql` to answer when the user picks one.

## Critical-analyst rules

- Never write a row with required fields still null.
- Never pick an existing table without showing the match score to the user.
- Never OCR an image without showing the draft to the user — OCR mistakes
  happen, especially on blurry phone screenshots.
- On a transfer proof, double-check the `amount`, `recipient_name`, and
  `transaction_date` with the user before writing.
- If a draft field directly contradicts a value the user typed in their
  message, trust the user's text over the OCR.

## Housekeeping

Staged CSVs and images live under `target/ask_ingest/_staging/`. Safe to
delete once the row is persisted; `target/ask_ingest/<table>.parquet`
and the provenance JSON are the source of truth.
