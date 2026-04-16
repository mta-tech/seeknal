---
name: save-report-exposure
description: "Codify a completed analysis as a repeatable seeknal/exposures/{name}.yml spec re-runnable via `seeknal ask report --exposure {name}`"
tags: [reporting, codification, scheduling]
version: "1.0.0"
---

# Save Report Exposure

Use this workflow AFTER an analysis is complete and the user wants to save
it as a repeatable, re-runnable report spec — typically for scheduling or
sharing with teammates.

## Tool you will call

- `save_report_exposure` (gated on the same `Generate report now` approval as
  `generate_report` — if the user already approved generation in the current
  turn, the gate carries through)

## When to use

Trigger this skill when the user says things like:

- "save this as a report"
- "make this re-runnable"
- "schedule this report weekly"
- "I want to run this every Monday"

## Phase 1 — Distill the prompt

Before calling `save_report_exposure`, you MUST distill the analysis into a
clear, self-contained prompt that captures:

- The QUESTION being answered (not just the SQL)
- The TABLES the analysis depends on
- Any FILTERS or DATE RANGES that should be parameterized
- The EXPECTED FORMAT (markdown, html, both)

A good prompt is one another agent could read in isolation and reproduce the
same analytical output.

## Phase 2 — Approval gate

`save_report_exposure` shares the same gate as `generate_report` —
`_REPORT_APPROVAL_DISCRIMINATOR = "generate report now"`. If the user has
already approved generation in the current turn, the gate is satisfied.
Otherwise, present the approval menu (see the `report-generation` skill for
the exact options).

## Phase 3 — Call the tool

Required args:

- `name`: snake_case identifier (e.g. `monthly_revenue_report`). Validated against
  `^[a-z0-9_]+$` — no hyphens, no spaces.
- `prompt`: the distilled re-runnable prompt from Phase 1.
- `inputs`: JSON array of input refs as a STRING — e.g.
  `'["transform.monthly_revenue", "source.orders"]'`.
- `output_format`: one of `markdown`, `html`, `both`.
- `schedule` (optional): 5-field cron string (`minute hour day month weekday`),
  e.g. `0 8 * * MON` for every Monday at 8am. When set, the exposure can be
  deployed to Prefect with
  `seeknal prefect deploy --exposure {name} --work-pool <pool>`.

## Phase 4 — Confirm + tell the user how to re-run

After successful save, your final answer MUST include:

1. The path the exposure was saved to (`seeknal/exposures/{name}.yml`)
2. The re-run command: `seeknal ask report --exposure {name}`
3. If a schedule was set: the deploy command for Prefect.

## Output location

Exposures live at `seeknal/exposures/{name}.yml`. The directory is auto-created
if missing. Existing files are overwritten with a note.
