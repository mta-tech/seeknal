---
name: business-question-answering
description: "Translate business questions into metrics, SQL evidence, and actionable recommendations"
tags: [business, metrics, recommendation, analysis]
version: "1.0.0"
---

# Business Question Answering

Use this workflow for executive/business questions like "where is revenue
strongest?", "what are the opportunities?", "which segment should we
prioritize?", "what changed?", "apa yang perlu diperhatikan?", or "what should
I watch?".

The user is a business/executive reader, not a developer. Do not explain
schemas, SQL-pair inventory, project setup, or table mechanics unless the user
explicitly asks. Use those artifacts privately to produce insight.

## Workflow

1. **Clarify silently when possible**
   - Infer likely metric, dimension, grain, and time window from available schema and context.
   - Ask the user only when multiple materially different business definitions would change the answer.

2. **Get evidence**
   - If the data lives in a connected database, load `database-analyst` and use `list_tables`/`describe_table`/`execute_sql`.
   - Do not answer quantitative questions without at least one SQL query when SQL tools are available.
   - For broad "what should I pay attention to?" prompts, run 3-5 small
     evidence queries across the most relevant business lenses: headline
     volume, trend, top segment/region/category, risk/status mix, and any
     obvious outlier/missing-data caveat.

3. **Validate**
   - Run a total/check query when recommendations depend on shares or rankings.
   - Compare at least two cuts when the user asks for opportunities, e.g. segment and region, or current and prior period.

4. **Recommend**
   - Separate facts from interpretation.
   - Tie every recommendation to a number from the query result.
   - Include caveats about sample size, filters, and grain.
   - Prefer "so what" language: risk, opportunity, priority, anomaly, next
     decision. Avoid catalog/setup language.

## Output shape

Use this compact structure unless the user asks otherwise:

1. **Executive takeaways** — 3-5 bullets, each with a number and "why it matters".
2. **Evidence** — small table or compact bullets with actual numbers.
3. **Recommended focus** — 1-3 actions or next checks.
4. **Caveat** — one line about source/grain/data quality.

Never make the final answer a list of available SQL pairs, table names, or
developer instructions. Those belong in tool use, not the user-facing answer.
