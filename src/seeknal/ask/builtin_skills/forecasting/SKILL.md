---
name: forecasting
description: "On-demand time-series forecasting. CAPTURE params from project context, call run_forecast, present deterministic engine results."
tags: [forecasting, time-series, prediction]
version: "1.0.0"
---

# Forecasting

## Workflow: CAPTURE → RUN → PRESENT

### CAPTURE
Read project context (``context/forecast_guide.md``) to determine:
- the source table, date column, and time grain
- the value aggregate and any series filter
- baseline start (regime boundary) and exclusions

Then **construct a 2-column SQL** (X = time grain, Y = value):

```sql
SELECT date_trunc('month', <date_col>) AS x, COUNT(DISTINCT <id_col>) AS y
FROM <schema>.<table>
WHERE <date_col> >= '<baseline>'
GROUP BY 1
ORDER BY 1
```

Lock: {sql, periods}. If the source/series is ambiguous → call ``request_clarification``.

### RUN
Call ``run_forecast(sql, periods)`` with the SQL you constructed and the
horizon in periods. The tool executes the SQL, validates it is exactly
2 columns with enough rows, infers the frequency, and triggers the engine.
**Do NOT do forecast arithmetic.**

If the tool returns ``## Ditolak`` → present the reason to the user.
Do not retry with different params unless the user changes the request.

### PRESENT
Format the tool's structured output for the user:
- lead with a short headline summary
- show quality label (BAIK / CUKUP / LEMAH)
- show projected periods with realistic ranges
- hide σ, p10/p90, raw residuals unless the user asks for methodology

## Notes
- This skill is domain-neutral. All domain specifics (tables, filters, regime)
  come from project context — never hardcode them here.
- Bounds widen with horizon (σ·√h principle).
- The tool is only registered in non-interactive environments when
  ``agent.forecast.enabled: true`` is set in ``seeknal_agent.yml``.
