---
name: report
description: "Deliver a completed analysis as a full Markdown report"
actions: [write_report]
---

# Report delivery

Use this skill only after completing the analysis the report should explain.

Call `write_report` with the complete Markdown document in its `report`
argument. The report is delivered to the IBA client by the premises-worker
bridge. This tool is not terminal: after it returns, decide whether further
work is needed or finish with a normal response.
