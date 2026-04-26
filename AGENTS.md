# Seeknal Agent Guide

This file gives coding/assistant agents project-local guidance for the Seeknal
repository. It complements `CLAUDE.md`; direct user/developer instructions still
win over this file.

## Project overview

Seeknal is an all-in-one data and AI/ML engineering platform. It includes:

- YAML/Python pipeline definitions under `seeknal/`
- feature store and entity consolidation workflows
- a Typer CLI in `src/seeknal/cli/main.py`
- Seeknal Ask: an agent harness for data questions, reports, read-only sources,
  SQL examples, and Ask QA tests

## Ask harness conventions

- Keep tools thin and deterministic. Put workflow and domain behavior in skills,
  generated source context, SQL pairs, tests, and documentation.
- `seeknal_agent.yml` configures Ask mode and read-only connected sources.
- `seeknal source connect/status/inspect/sync/test` manages existing databases
  that Ask can query without a pipeline.
- `seeknal/sql_pairs/*.yml` are prompt-to-SQL examples for agent context.
- `seeknal/tests/*.yml` are executable Ask SQL QA cases for
  `seeknal ask test`.
- In tap-in analysis mode, the connected database remains read-only, but users
  can explicitly teach the agent by saying “remember”, “write this down”, or
  “save this query”; persist non-secret rules to `preferences.yml` and longer
  notes/SQL pairs under `context/`.
- Generated source context under `.seeknal/context/sources/` is derived state;
  refresh it with `seeknal source sync` rather than hand-editing.
- Never hardcode customer/domain SQL in the agent harness.

## Safety

- Never commit database DSNs, API keys, or credentials.
- Use `.env` plus `dsn_env` in `seeknal_agent.yml` for connected sources.
- Prefer read-only SQL for exploration and validation.
- Preserve existing CLI and gateway behavior when changing Ask internals.

## Verification

For Ask harness changes, run focused tests such as:

```bash
PYTHONPATH=src pytest -q tests/ask/test_ask_sql_tests.py tests/ask/test_context_tools.py tests/ask/test_toolset.py tests/ask/test_prompt_builder.py tests/cli/test_source.py
```

For init/template changes, include:

```bash
PYTHONPATH=src pytest -q tests/test_cli.py::TestInitCommand tests/ask/test_skills.py
```
