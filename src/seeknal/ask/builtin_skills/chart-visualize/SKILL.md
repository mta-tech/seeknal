---
name: chart-visualize
description: "On-demand chat charts. DECIDE whether a chart helps, pick the type from the data shape, call visualize_chart once, present it alongside the text."
tags: [visualization, chart, chat]
version: "1.0.0"
---

# Chart Visualization

One tool builds every chart: ``visualize_chart``. No other tool attaches a
chart of its own, so deciding *whether* to chart and *which type* fits is your
judgement, made once per answer, on the data you actually have in front of you.

## Workflow: DECIDE → BUILD → PRESENT

### DECIDE

Ask two questions, in this order.

**1. Does a chart add anything the text does not?**
A chart earns its place when it reveals shape — a trend, a ranking, a share, a
spread, an outlier. It adds nothing when the answer is one or two numbers the
sentence already states, or when the user asked for a list they will read
value by value.

Chart when:
- the user explicitly asks for a chart, grafik, plot, visual, or dashboard —
  then always chart, unless the data genuinely cannot carry one (see refusals)
- the answer is about movement over time, a comparison across categories, a
  composition, a distribution, or a relationship between two metrics — and the
  project has enabled automatic charts

Do not chart when:
- the project has not enabled automatic charts and the user did not ask
- the answer is a single figure with nothing to compare it against
- the result is a lookup or a record listing rather than an analysis

**2. Which type fits the shape of the data?**

Read the question for the intent, then the data for the shape. Both have to
agree: "how has X moved" wants time on X even if the data would also fit a bar
chart; "which category is biggest" wants a ranking even if the data has dates.

| What the answer is about | Type |
|---|---|
| one headline figure | ``big_number`` |
| a trend over time | ``line_chart`` (``area_chart`` when cumulative) |
| several trends over time | ``grouped_line_chart`` |
| comparing categories | ``bar_chart``, or ``horizontal_bar_chart`` when labels are long |
| the same categories split by a second dimension | ``grouped_bar_chart`` |
| share of a total, few slices | ``pie_chart`` |
| two metrics correlated | ``scatter_plot`` |
| a value across two dimensions at once | ``heatmap`` |
| spread within categories | ``box_plot`` |
| nested part-of-whole | ``treemap`` |

Pick the type the data supports, not the type that looks impressive. If no
type fits honestly, answer with a table — that is a valid outcome, not a
failure.

**Two shape rules the renderer enforces, worth knowing before you choose:**

- **A chart reads exactly three columns**: X, Y, and an optional series. It has
  no way to show a fourth. Passing more is refused.
- **Each type fixes its X axis.** ``line_chart``/``area_chart``/
  ``grouped_line_chart`` plot X as a *date*, so periods must be ISO
  (``2024-01-01``, or ``2024-01`` for months) — a label like "Januari" or "Q1"
  belongs on a ``bar_chart``, whose X takes any text. ``scatter_plot`` needs a
  *number* on X. Everything else takes labels.

**Lock: {type, title, source of rows}.**

**3. Is the answer about more than one thing?**

If it is — several codes, several statuses, several segments — the chart should
show all of them, not just the total. One metric charted out of five throws
away most of what the answer says.

The third column is what makes that possible: it splits the chart into one
series per distinct value, each drawn in its own colour with a legend naming
it. A ``line_chart`` becomes several lines; a ``grouped_bar_chart`` becomes
bars side by side per category.

The data has to be in **long** form for that. Answers are usually written wide,
one column per metric:

| period | metric A | metric B | metric C | total |
|---|---|---|---|---|

That shape cannot be charted — only three columns are ever read, so everything
past the third would vanish. Reshape so each metric becomes rows tagged in a
single series column:

```sql
SELECT period, 'metric A' AS series, metric_a AS value FROM t
UNION ALL
SELECT period, 'metric B' AS series, metric_b AS value FROM t
UNION ALL
SELECT period, 'metric C' AS series, metric_c AS value FROM t
ORDER BY 1, 2
```

Then chart ``[period, value, series]`` as ``grouped_line_chart`` over time, or
``grouped_bar_chart`` across categories. Often the long form is the *natural*
query anyway — a ``GROUP BY period, code`` — and the wide table is just how it
was written up for reading.

Keep the number of series small enough to tell apart; beyond roughly eight the
tool keeps the largest and says so. Chart the total *or* the breakdown, not
both in one chart — a total plotted beside its own parts dwarfs them.

### BUILD

Call ``visualize_chart`` **once**. Column order is the axis contract for every
type: X first, Y second, an optional series dimension third.

Three ways to supply the rows. Pick by what the data *is*, not by what ran
first.

**Default — SQL you already ran.** Whenever the chart's numbers can be queried,
this is the source. Pass the same SQL you gave ``execute_sql`` this turn; the
rows are reused from cache, so the chart shows exactly the numbers your text
quotes.

```
visualize_chart(
    widget_type="line_chart",
    widget_title="Registrations per month",
    sql="SELECT month, total FROM ... ORDER BY 1",
)
```

**The exported CSV — for values a query cannot return.** Some numbers are
computed in-process and no SQL brings them back: a forecast's projection, for
instance. Charting the source SQL there would draw the history alone while your
text talks about the projection. If those values were exported to CSV this
turn, chart the CSV: pass no ``sql=`` and no ``data=``, and name the columns to
plot in axis order.

```
visualize_chart(
    widget_type="grouped_line_chart",
    widget_title="History and projection",
    columns=["period", "value", "kind"],
)
```

Nothing is retyped, so the chart, the download and the text cannot disagree.

**Mode 2 — values neither exported nor queryable.** Last resort. Copy the
numbers from the producing tool's output verbatim; never re-derive or round
them. Transcription is the one path where the chart can quietly drift from the
text.

> **An export failure never cancels the chart.** Exporting a CSV and drawing a
> chart are separate outcomes with separate failure modes. If the export fails
> — storage unreachable, upload rejected — say so about the *download*, then
> chart from SQL as usual. Reporting "no chart" because a file could not be
> written is wrong: the reader loses a picture the data fully supported.

Use a third column to distinguish segments whenever a chart mixes measured and
computed values — a projection drawn as if it were history misleads the reader.

**One chart per answer.** A tool such as ``run_forecast`` may run several times
in one question; that does not mean several charts. Choose the single result
worth charting. A second call is refused, so the choice is yours to make
deliberately rather than by call order.

### PRESENT

- write the answer as you normally would: the finding first, in words
- do **not** paste the chart's underlying table as markdown — the chart already
  shows it, and repeating it makes the reply noisy
- a small table alongside the chart is fine when the exact figures matter; the
  chart carries the shape, the table carries the precision
- when the chart draws several series, the tool lists them back to you; describe
  what the reader will actually see — name the series rather than writing as if
  there were one line
- mention any limit the tool reported (top-N bucketing, downsampling) if it
  changes how the chart should be read

## Refusals

Decide these before calling the tool — the tool validates the type name but
never swaps your type for a different one.

- zero rows → no chart
- a single row with nothing to compare → prefer text, or ``big_number``
- many categories → still chart; the tool keeps the top ones and buckets the
  rest into an explicit "Others" row rather than hiding them
- many time points → still chart; the tool downsamples evenly, keeps the final
  point, and strides each series separately so the lines stay comparable
- the shape does not match any type honestly → answer with a table and say why

If the tool refuses a shape, it names the mismatch and the fix — more columns
than a chart reads, a series the chosen type cannot draw, or an X axis the type
cannot parse. Act on the reason rather than retrying the same call: the refusal
means the chart would have rendered *wrong*, not that it failed.

## Notes

- This skill is domain-neutral. Which questions deserve a chart in a given
  project, and what the charts should be titled, come from project context —
  never hardcode them here.
- The tool is registered only in non-interactive environments when
  ``agent.visualize_chart.enabled: true`` is set in ``seeknal_agent.yml``.
  Automatic charting additionally requires ``agent.visualize_chart.auto_emit``;
  without it, chart only when the user asks.
- Charts render in the chat and persist with the conversation. Pinning a chart
  to a dashboard is not part of this tool.
- ``execute_python`` plotting stays for report and artifact output. It produces
  image files that never reach the chat, so it is not an alternative to this
  tool for answering a user.
