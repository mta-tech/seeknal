---
name: report-generation
description: "End-to-end Evidence.dev report workflow — exploration, drafting, approval-gated build, codification, and post-report next-steps menu"
tags: [reporting, evidence, visualization]
version: "2.0.0"
---

# Report Generation

Use this workflow when the user asks for a report, dashboard, or visualization.
You will produce a **professional, insight-driven** Evidence.dev report that
goes through an explicit approval gate before building.

## Tools you will call

- `execute_sql` — explore tables, validate the analytical query
- `list_tables` / `describe_table` — discover schemas
- `ask_user` — present the approval menu (with the EXACT discriminator below)
- `generate_report` — build the Evidence.dev project (gated on approval)
- `save_report_exposure` — codify as a re-runnable spec (optional, after approval)
- `open_in_browser` — show the built report (optional, post-build menu)

## Phase 1 — Analysis Process (no approval needed)

BEFORE you can call `generate_report`, you MUST:

1. Run `execute_sql` to explore ALL relevant tables (not just one).
2. Run at least 3-5 queries covering: aggregates, distributions, cross-table JOINs, rankings, trends.
3. Calculate derived metrics in your queries: percentages of total, ratios between segments, rankings.
4. Identify the 3 most interesting findings — these become the report's narrative spine.

DO NOT generate a report after looking at only one table.
DO NOT write generic insights. Every recommendation must cite a specific number from the data.

## Phase 2 — Approval gate (MANDATORY)

After exploration, you MUST present an approval menu using `ask_user` with EXACTLY
these four options. The discriminator label is `Generate report now` — the menu
will not grant approval unless that exact string is one of the options AND the
user picks it.

Options (must be these exact strings):
- `Continue analysis`
- `Generate report now`
- `Done for now`
- `Type your own`

Do NOT print these options as plain text, bullets, or a numbered list — they
must come through `ask_user`. Only call `generate_report` after the user
explicitly chooses `Generate report now`. The same gate applies to
`save_report_exposure` (it shares `_REPORT_APPROVAL_DISCRIMINATOR`).

## Phase 3 — Build

Pass to `generate_report`:
- `title`: a clear, descriptive report title
- `page_content`: Evidence-compatible markdown content (see syntax below)

### Evidence Markdown Syntax

SQL queries in fenced blocks:

```sql query_name
SELECT ... FROM table_name
```

Components (SINGLE curly braces only — never double braces):

- `<BigValue data={query_name} value=column_name />`
- `<BarChart data={query_name} x=column y=column />`
- `<LineChart data={query_name} x=date_col y=value_col />`
- `<AreaChart data={query_name} x=date_col y=value_col />`
- `<DataTable data={query_name} />`
- `<ScatterPlot data={query_name} x=col1 y=col2 />`
- `<Histogram data={query_name} x=column bins=20 />`
- `<FunnelChart data={query_name} name=stage value=count />`

### Common Svelte parse errors

Evidence reports compile through Svelte after `generate_report` returns the
scaffold path, so parse errors surface during the build phase — not when
the tool first returns. The following patterns cause cryptic `Parser.error`
traces and `Build failed` exits:

- **Unescaped `<` and `>` in narrative text.** Write `&lt;` / `&gt;` for
  literal angle brackets in markdown prose. Svelte sees `<` as the start of
  a component tag and chokes if no valid tag name follows.

- **Double curly braces in component props.** `<BarChart data={{query}} />`
  is wrong — Evidence wants single braces `<BarChart data={query} />`. The
  `_fix_evidence_syntax` helper auto-fixes the common `data={{...}}` case,
  but it does NOT cover other props (`x={{col}}`, `y={{col}}`).

- **Template literals or JSX expressions inside components.** Don't write
  `` <BigValue value={`prefix-${var}`} /> ``. Evidence components only
  accept identifier references, not arbitrary expressions.

- **Unclosed component tags.** Every `<DataTable data={x}>` needs a closing
  `</DataTable>` OR self-close as `<DataTable data={x} />`. Svelte does NOT
  forgive missing closers.

- **HTML tags inside SQL fenced blocks.** Don't put `<br>`, `<i>`, or
  similar inside a ```` ```sql ... ``` ```` block. The Svelte parser still
  scans for tags inside fenced code and breaks on partial matches.

If `generate_report` returns a "Build failed" / "Parser.error" message,
inspect the rendered `page_content` for one of the patterns above, fix it,
and call `generate_report` again with the corrected content.

### Report Quality Bar

A professional report MUST have:

1. **Executive Summary** — 3-4 BigValue KPIs answering the core question up front.
2. **Multi-angle Visual Analysis** — Each section explores a DIFFERENT dimension with a DIFFERENT chart type.
3. **Data-backed Narrative** — Between every chart, write 1-2 sentences interpreting the data with SPECIFIC numbers ("Premium customers spend 2.3x more per order than Basic" — NOT "Premium customers tend to spend more").
4. **Actionable Recommendations** — Tied to specific data points ("Target Bandung for Premium acquisition — 0 Premium customers despite $1,786 avg spend" — NOT "consider expanding to new markets").

### Report Content Pattern

- **SECTION 1 — Executive KPIs**: SQL query → BigValue components (3-4 headline numbers)
- **SECTION 2 — Primary breakdown**: SQL with % of total → BarChart + insight + DataTable
- **SECTION 3 — Secondary dimension**: different grouping → different chart type + insight
- **SECTION 4 — Cross-analysis**: JOIN 2+ tables → stacked/grouped chart + insight
- **SECTION 5 — Recommendations**: markdown text with specific, data-cited action items

### Report Writing Rules

- Name queries descriptively (`revenue_by_month`, `top_customers`)
- Use markdown headers (`##`) to create clear sections
- Do NOT include semicolons in SQL queries
- Use the same table names from `list_tables` output
- Each chart should have a descriptive title prop
- Vary chart types — don't use 5 BarCharts in a row

## Phase 4 — Post-build menu

After `generate_report` succeeds, you MUST present the post-report menu via
`ask_user` with these exact options:

- `Open in browser` — calls `open_in_browser` on the built path
- `Publish to Seeknal Report Server` — triggers the `publish-to-seeknal-report` skill
- `Publish memo to Proof` — triggers the `publish-memo-to-proof` skill
- `Continue` — go back to analysis without leaving
- `Type your own` — free-form

Do NOT print these options in your text — they MUST come through `ask_user`.

## Phase 5 — Final answer

After generating a report, your final answer MUST include:

1. A brief summary of the key findings with SPECIFIC numbers
2. The path to the generated HTML report
3. 2-3 actionable recommendations grounded in the data

Do NOT just say "I created a report" — the answer itself should be valuable standalone content.

## Report Codification

After generating, if the user wants to save the analysis as a repeatable spec:
use the `save-report-exposure` skill (separate manual) to call
`save_report_exposure`. The same approval gate carries through within the
same turn.
