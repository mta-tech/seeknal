---
title: "feat: TUI Visual Refresh — Claude Code-Inspired Design with Seeknal Fox Mascot"
type: feat
status: completed
date: 2026-04-01
origin: docs/brainstorms/2026-04-01-tui-visual-refresh-claude-code-style-requirements.md
---

# feat: TUI Visual Refresh — Claude Code-Inspired Design with Seeknal Fox Mascot

## Overview

Comprehensive visual overhaul of seeknal's terminal output to match the polish and brand identity of Claude Code. Introduces a fox mascot in Unicode block characters, a teal+orange brand palette, a multi-theme system (dark/light/ansi), custom spinners, Rich-based progress displays, and branded output across all CLI modes (commands, REPL, Ask).

## Problem Frame

Seeknal's CLI has no visual identity. Output uses bare `typer.echo`/`typer.style` with 4 hardcoded ANSI colors, duplicated `_echo_*` helpers across 3 files (imported by 8+ workflow modules), inconsistent table formatting (3 different `tablefmt` values), and zero branding. The REPL uses raw `print()` with no styling at all. Rich exists as an optional dependency only for the Ask module.

A polished CLI experience — as demonstrated by Claude Code — builds user affinity and signals product quality. This is a visual layer change; no CLI command structure, semantics, or behavior changes.

(see origin: docs/brainstorms/2026-04-01-tui-visual-refresh-claude-code-style-requirements.md)

## Requirements Trace

- R1. Fox mascot in Unicode block characters (multiple poses, 6-8 lines, ~20 chars wide)
- R2. Branded color palette: Teal primary `rgb(0,188,180)`, Fox orange accent `rgb(255,165,60)`
- R3. Animated welcome asterisk with teal hue sweep
- R4. Welcome banner on first interaction per session
- R5. REPL branded header with Rich panels
- R6. Ask mode branded header
- R7. Multi-theme support (dark, light, dark-ansi)
- R8. Theme configuration via config.toml / `SEEKNAL_THEME` env var
- R9. Custom platform-aware spinner system
- R10. Stalled state spinner color transition
- R11. Rich progress bars for DAG execution
- R12. Rich panels for all output modes
- R13. Branded status symbols
- R14. Syntax highlighting with branded theme
- R15. Table styling with Rich Tables
- R16. Reduced motion mode
- R17. Color-blind safe variants (future: daltonized themes)
- R18. Terminal capability detection and graceful degradation
- R19. Platform-specific glyphs with fallbacks

## Scope Boundaries

- No custom layout engine — use Rich, not a React Ink equivalent
- No Textual — remains a CLI tool, not a full-screen TUI app
- No CLI command structure changes
- No mouse support or interactive widgets beyond Rich builtins
- Animations are terminal-safe (Rich Live/Status only)
- R17 (daltonized themes) is future work — the theme architecture supports it, but no daltonized palette ships in this plan

## Context & Research

### Relevant Code and Patterns

**Current output system (3 disconnected islands):**
1. **CLI commands** (Typer): `_echo_*` functions in `cli/main.py:560-577`, duplicated in `cli/docs.py` and `cli/materialization_cli.py`. Imported by 8 workflow modules (`runner.py`, `parallel.py`, `executor.py`, `apply.py`, `draft.py`, `dry_run.py`, `validators.py`, `transform_executor.py`)
2. **REPL** (raw Python): `cli/repl.py` — all `print()` + `tabulate("simple")`, zero Rich
3. **Ask mode** (Rich): `cli/ask.py` + `ask/streaming.py` — `Console`, `Panel`, `Table`, `Syntax`, `Markdown`. Each function creates its own `Console()` (no shared instance)

**Unused infrastructure:**
- `utils/formatters.py` — 18 formatting functions, never imported by anything
- `constants.py:98-102` — `CLI_SUCCESS_SYMBOL`, `CLI_ERROR_SYMBOL`, etc. — defined but not used (symbols hardcoded inline)

**Table format inconsistency:** `"github"` (data models), `"simple"` (CLI/REPL), `"psql"` (workflow/inspect)

**Rich is optional:** Listed only in `[project.optional-dependencies] ask = [... "rich>=13.0.0" ...]`. Must become a core dependency.

**Configuration system:** `config.toml` via `configuration.py` `DotDict` class. Currently only `[context.secrets]` and `[context.database]` sections. No UI config exists.

### External References

- **Rich Theme API**: `Theme(styles={"name": Style})` with `Console(theme=...)`. Separate instances for dark/light/ansi. `Console.color_system` returns `"standard"` / `"256"` / `"truecolor"` / `None`. Auto-downgrades RGB hex to nearest match.
- **Rich Spinner**: Not subclassable. Override `.frames`/`.interval` directly, or register into `SPINNERS` dict. Use `Status` for spinner+text. Always pass shared Console.
- **Mascot best practices**: Show sparingly (first-run, help, explicit flag). 5-12 lines max. Half blocks (`▀▄`) + full block (`█`) + shades (`░▒▓`) are safest cross-terminal. Avoid eighth blocks.
- **NO_COLOR / FORCE_COLOR**: Industry standards to respect. Rich handles `NO_COLOR` natively.
- **Reduced motion**: No universal standard. Detect via `--no-animation` flag, `NO_COLOR`, `CI` env var, `TERM=dumb`, `!isatty(stdout)`.

## Key Technical Decisions

- **Rich as core dependency**: Move `rich>=13.0.0` from optional `[ask]` to core `[dependencies]`. Rich is lightweight (~3MB) and already transitively pulled by Typer in many installs. The visual refresh touches every output path.
- **Console singleton pattern**: Create `seeknal.ui.console` module with a shared `Console` instance. All modules import from here instead of creating their own. Theme applied once at creation.
- **Theme via Rich `Theme` objects**: Three `Theme` instances (dark, light, dark-ansi) with named semantic styles (`brand.primary`, `status.success`, `status.error`, etc.). Selected at Console creation based on config/env/auto-detect.
- **Fox mascot in half-block technique**: Use `▀▄█░▒▓` with foreground+background colors for 2x vertical resolution. Quadrant chars (`▛▜▙▟`) used selectively with ANSI fallback. Mascot shown on welcome, `--help`, REPL startup — not on every command.
- **Gradual migration**: Replace `_echo_*` → `ui.output` functions first, then tables, then panels. Existing CLI tests check string content (not ANSI codes), so they should largely pass through the migration.
- **`utils/formatters.py` retirement**: This module is unused. Rather than rehabilitating it, the new `ui/` package supersedes it. Remove `formatters.py` to avoid confusion.

## Open Questions

### Resolved During Planning

- **Should themes use Rich `Theme` or custom dataclasses?** → Rich `Theme` objects. They integrate natively with Console, support named styles, inherit defaults, and auto-downgrade colors. No need for a custom abstraction.
- **How to implement bi-directional spinner?** → Register custom frames into `rich._spinners.SPINNERS` dict before construction. Forward+reverse frame sequence in the list itself. Use `Status` wrapper.
- **How to integrate Rich progress with DAGRunner?** → `DAGRunner` and `ParallelExecutor` accept an optional `progress_callback` parameter. The UI layer provides a callback that updates a `Rich.Progress` instance. The workflow layer has zero Rich imports.
- **What does Rich provide for terminal detection?** → `Console.color_system` covers truecolor/256/standard/None. `Console.is_terminal` for TTY. `NO_COLOR` respected natively. Sufficient — no gaps to fill.

### Deferred to Implementation

- Exact Unicode characters for the fox mascot — requires visual prototyping in a real terminal
- Precise teal hue sweep animation timing for the welcome asterisk — tune during implementation
- Whether `tabulate` dependency can be fully removed or needs to stay for edge cases (REPL raw mode, non-TTY piped output)

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification. The implementing agent should treat it as context, not code to reproduce.*

```
Module dependency flow:

  pyproject.toml
       │  (rich moves to core dependencies)
       ▼
  seeknal/ui/
  ├── theme.py        ← Color constants + Theme definitions (dark/light/ansi)
  ├── console.py      ← Console singleton (imports theme.py)
  ├── figures.py      ← Platform-aware Unicode symbols
  ├── fox.py          ← Fox mascot art (imports theme for colors)
  ├── output.py       ← echo_success/error/warning/info (imports console.py)
  ├── tables.py       ← Rich Table factory (imports console.py)
  ├── panels.py       ← Rich Panel factory (imports console.py)
  ├── spinner.py      ← Custom spinner + stalled detection (imports console.py)
  ├── progress.py     ← DAG execution progress (imports console.py, spinner.py)
  └── welcome.py      ← Welcome banner (imports fox.py, console.py)
       │
       ▼
  CLI layer (cli/main.py, cli/repl.py, cli/ask.py, etc.)
       │  imports seeknal.ui.output, seeknal.ui.welcome, etc.
       ▼
  Workflow layer (workflow/runner.py, workflow/parallel.py, etc.)
       │  imports seeknal.ui.output (replaces cli/main.py import)
       ▼
  No more circular coupling: workflow → ui (not workflow → cli)
```

```
Theme selection flow:

  1. --theme CLI flag (highest priority)
  2. SEEKNAL_THEME env var
  3. config.toml [ui] theme = "dark"
  4. Auto-detect: Console.color_system
     - "truecolor" or "256" → dark theme (RGB colors)
     - "standard" → dark-ansi theme (named ANSI colors only)
     - None → no styling
```

## Implementation Units

### Phase 1: Foundation

- [ ] **Unit 1: UI Package Skeleton + Theme System**

  **Goal:** Create the `seeknal/ui/` package with theme definitions, color palette, and Console singleton. Make Rich a core dependency.

  **Requirements:** R2, R7, R8, R18

  **Dependencies:** None

  **Files:**
  - Modify: `pyproject.toml` (move `rich>=13.0.0` to core dependencies)
  - Create: `src/seeknal/ui/__init__.py`
  - Create: `src/seeknal/ui/theme.py`
  - Create: `src/seeknal/ui/console.py`
  - Create: `src/seeknal/ui/figures.py`
  - Test: `tests/ui/test_theme.py`
  - Test: `tests/ui/test_console.py`

  **Approach:**
  - `theme.py`: Define brand color constants (teal, orange, semantic colors) and three `Theme` instances (`DARK_THEME`, `LIGHT_THEME`, `ANSI_THEME`) using Rich `Theme` with named styles like `brand.primary`, `brand.accent`, `status.success`, `status.error`, `status.warning`, `status.info`, `text.muted`, `table.header`, `panel.border`
  - `console.py`: Module-level `get_console()` function that lazily creates a themed `Console` singleton. Theme selection follows the priority chain (flag → env → config → auto-detect). Export `console` for simple import. Provide `err_console` for stderr.
  - `figures.py`: Platform-aware Unicode symbols dict. Detect platform (`sys.platform`, `TERM` env var) and provide appropriate glyphs. Include fallback ASCII set for `TERM=dumb`.
  - Move `rich>=13.0.0` from `[project.optional-dependencies] ask` to `[project] dependencies`

  **Patterns to follow:**
  - Claude Code's `src/utils/theme.ts` for theme structure
  - Claude Code's `src/constants/figures.ts` for platform-aware symbols
  - Rich's documented singleton pattern (own module > `rich.get_console()`)

  **Test scenarios:**
  - Theme objects contain all expected named styles
  - Console auto-selects dark-ansi theme when color_system is "standard"
  - Console respects `SEEKNAL_THEME` env var
  - Console respects `NO_COLOR` (Rich handles this, but verify)
  - `figures` returns ASCII fallbacks when `TERM=dumb`
  - Dark and light themes define identical style name sets

  **Verification:**
  - Import `from seeknal.ui.console import get_console` succeeds
  - Console instance has correct theme applied
  - All three themes are valid Rich Theme objects

---

- [ ] **Unit 2: Fox Mascot Art**

  **Goal:** Create the seeknal fox mascot in Unicode block characters with multiple poses.

  **Requirements:** R1

  **Dependencies:** Unit 1 (uses theme colors)

  **Files:**
  - Create: `src/seeknal/ui/fox.py`
  - Test: `tests/ui/test_fox.py`

  **Approach:**
  - Design fox using half-block doubling technique (`▀▄` with fg+bg colors for 2x vertical resolution)
  - Three poses: `default`, `look_left`, `look_right` — stored as multi-line strings with Rich markup for colors
  - Fox body in teal (`brand.primary`), ears/tail accents in orange (`brand.accent`), eyes as bright white dots
  - Target size: 6-8 lines tall, ~18-22 chars wide (compact enough for side-by-side with text)
  - Provide `render_fox(pose, theme)` function returning a `Rich.Text` or renderable
  - Include ANSI fallback (same art, ANSI named colors instead of RGB)
  - Include ASCII-only fallback for `TERM=dumb`

  **Patterns to follow:**
  - Claude Code's `src/components/LogoV2/` for pose-based mascot design
  - Claude Code's body segments approach: eyes, ears, body as composable pieces

  **Test scenarios:**
  - Each pose renders without errors
  - Fox art fits within 22-char width and 8-line height
  - ANSI fallback produces valid output
  - ASCII fallback uses only printable ASCII

  **Verification:**
  - `render_fox("default")` returns a renderable that displays a recognizable fox shape in the terminal

---

### Phase 2: Output Infrastructure

- [ ] **Unit 3: Output Functions (Replace `_echo_*`)**

  **Goal:** Create Rich-based replacements for `_echo_success/error/warning/info` that use the shared Console and theme. Create panel and table factory helpers.

  **Requirements:** R12, R13, R14, R15

  **Dependencies:** Unit 1

  **Files:**
  - Create: `src/seeknal/ui/output.py`
  - Create: `src/seeknal/ui/tables.py`
  - Create: `src/seeknal/ui/panels.py`
  - Test: `tests/ui/test_output.py`

  **Approach:**
  - `output.py`: Define `echo_success()`, `echo_error()`, `echo_warning()`, `echo_info()` using `console.print()` with theme style names. Use symbols from `figures.py`. Drop-in signature match for easy migration.
  - `tables.py`: `make_table(headers, rows, ...)` factory that returns a `Rich.Table` with branded header style, consistent box style (`ROUNDED`), and optional title. Replaces scattered `tabulate()` calls.
  - `panels.py`: `info_panel()`, `success_panel()`, `error_panel()`, `summary_panel()` factories returning `Rich.Panel` with themed border colors. `code_panel(code, language)` for syntax highlighting with monokai theme.
  - All functions use the shared Console from `console.py`

  **Patterns to follow:**
  - Existing `_echo_*` signatures (one `message: str` param) for backward compat
  - Rich's `Panel.fit()` for content-width panels
  - Claude Code's panel border colors: green for success, red for error, dim for info

  **Test scenarios:**
  - `echo_success("done")` produces output containing the success symbol and message
  - `make_table(["Name"], [["Alice"]])` returns a Rich Table with branded header style
  - `code_panel("SELECT 1", "sql")` returns a Panel with Syntax-highlighted content
  - Functions work with `NO_COLOR` set (plain text fallback)
  - Output is compatible with CliRunner capture (no raw ANSI in assertion-breaking places)

  **Verification:**
  - All output functions produce themed Rich output when imported and called
  - Table output is visually consistent (single box style across all tables)

---

- [ ] **Unit 4: Custom Spinner System**

  **Goal:** Create branded spinner with platform-aware frames, bi-directional animation, and stalled-state color transition.

  **Requirements:** R9, R10

  **Dependencies:** Unit 1

  **Files:**
  - Create: `src/seeknal/ui/spinner.py`
  - Test: `tests/ui/test_spinner.py`

  **Approach:**
  - Define custom spinner frames inspired by Claude Code's `['·', '✢', '✳', '✶', '✻', '✽']` but with seeknal's brand character set. Include reverse frames for bi-directional animation.
  - Platform detection: macOS gets full Unicode frames, Linux/Windows gets safe subset, `TERM=dumb` gets `['-', '\\', '|', '/']`
  - Register custom frames into `rich._spinners.SPINNERS["seeknal"]` at module import time
  - `seeknal_status(message)` context manager wrapping `Console.status()` with the custom spinner and brand color
  - Stalled detection: Track elapsed time. After threshold (e.g., 30s), spinner style transitions from `brand.primary` toward `status.error` by updating `Status.spinner_style` on a timer. Use Rich `Live` with `get_renderable` callback for smooth transition.

  **Patterns to follow:**
  - Claude Code's `src/components/Spinner/` for frame selection and stalled logic
  - Rich's `Status` API for spinner+text combos

  **Test scenarios:**
  - Correct spinner frames selected per platform
  - Stalled color transition triggers after threshold
  - Reduced motion mode returns static indicator (no animation)
  - Spinner works with shared Console instance

  **Verification:**
  - `with seeknal_status("Loading..."):` shows animated branded spinner
  - After 30s, spinner color shifts toward red

---

- [ ] **Unit 5: Welcome Banner**

  **Goal:** Create the branded welcome screen shown on first interaction per session.

  **Requirements:** R3, R4

  **Dependencies:** Units 1, 2

  **Files:**
  - Create: `src/seeknal/ui/welcome.py`
  - Test: `tests/ui/test_welcome.py`

  **Approach:**
  - `show_welcome()` function that renders: fox mascot (default pose) + "Welcome to Seeknal" in `brand.primary` + dimmed version number + Unicode horizontal separator
  - Layout: Fox on the left, text on the right, using `Rich.Columns` or `Table.grid()`
  - Animated asterisk (`✻`) with teal color on startup — use Rich `Live` with short animation loop (~1-2 seconds), then settle to static
  - Session tracking: Check env var or temp marker to avoid showing on every command. Use `SEEKNAL_WELCOME_SHOWN` env var set after first display, or a timestamp file in `~/.seeknal/`
  - Respect `--no-animation` and `NO_COLOR` — show static banner without animation

  **Patterns to follow:**
  - Claude Code's `src/components/LogoV2/WelcomeV2.tsx` for layout
  - Claude Code's `AnimatedAsterisk.tsx` for the hue sweep concept

  **Test scenarios:**
  - Welcome banner renders fox + title + version
  - Banner not shown when `SEEKNAL_WELCOME_SHOWN` is set
  - Banner works in non-TTY mode (static, no animation)
  - Reduced motion mode shows static banner

  **Verification:**
  - `show_welcome()` displays a visually branded welcome screen in the terminal
  - Subsequent calls in the same session are no-ops

---

- [ ] **Unit 6: DAG Execution Progress Display**

  **Goal:** Replace plain-text execution summaries with Rich progress panels and a branded summary.

  **Requirements:** R11, R12

  **Dependencies:** Units 1, 3, 4

  **Files:**
  - Create: `src/seeknal/ui/progress.py`
  - Test: `tests/ui/test_progress.py`

  **Approach:**
  - `ExecutionProgress` class wrapping `Rich.Progress` with custom columns: `SpinnerColumn` (seeknal spinner), `TextColumn` (node name), `BarColumn` (branded colors), `TimeElapsedColumn`
  - Provides `start_node(name)`, `complete_node(name, rows, duration)`, `fail_node(name, error)` methods that update progress tasks
  - `render_summary(stats)` function returning a `Rich.Panel` with the execution summary (total nodes, executed, cached, failed, duration) — replaces both `print_summary()` and `print_parallel_summary()`
  - The workflow layer calls these through an optional callback/interface, keeping Rich imports out of the workflow layer
  - Non-TTY mode: Falls back to sequential `echo_info` lines (current behavior)

  **Patterns to follow:**
  - Current `print_summary()` in `workflow/runner.py:1123-1246` for data fields
  - Current `print_parallel_summary()` in `workflow/parallel.py:232-269` for parallel stats
  - Rich Progress custom columns pattern

  **Test scenarios:**
  - Progress display tracks node start/complete/fail correctly
  - Summary panel includes all stats fields (total, executed, cached, failed, duration)
  - Non-TTY mode produces plain text output
  - Progress works with parallel execution (multiple concurrent tasks)

  **Verification:**
  - DAG execution shows a live progress display with spinner and node names
  - Completion shows a branded summary panel

---

### Phase 3: Migration

- [ ] **Unit 7: CLI Core Migration**

  **Goal:** Replace all `_echo_*` usage in `cli/main.py`, remove duplicates from `cli/docs.py` and `cli/materialization_cli.py`, and migrate tables to Rich.

  **Requirements:** R12, R13, R15

  **Dependencies:** Unit 3

  **Files:**
  - Modify: `src/seeknal/cli/main.py`
  - Modify: `src/seeknal/cli/docs.py`
  - Modify: `src/seeknal/cli/materialization_cli.py`
  - Modify: `src/seeknal/cli/source.py`
  - Modify: `src/seeknal/cli/atlas.py`
  - Delete: `src/seeknal/utils/formatters.py`
  - Test: `tests/cli/` (existing tests should pass)

  **Approach:**
  - Replace `from seeknal.cli.main import _echo_success, ...` with `from seeknal.ui.output import echo_success, ...` across all files
  - Remove the three copies of `_echo_*` function definitions
  - Replace `tabulate(data, headers, tablefmt=...)` calls with `ui.tables.make_table()` — use `console.print()` to render
  - Keep backward-compatible function signatures so existing call sites need minimal changes (mostly import path)
  - Delete `utils/formatters.py` (completely unused, superseded by `ui/`)
  - Update `constants.py` CLI symbols to reference `ui.figures` (or remove them if no longer needed)

  **Patterns to follow:**
  - Surgical replacement: change imports and function calls, don't restructure commands

  **Test scenarios:**
  - All existing CLI tests in `tests/cli/` pass (they check string content, not ANSI codes)
  - `seeknal list feature-groups` produces a Rich-styled table
  - `seeknal run --show-plan` shows branded panels
  - `--output json` / `--output yaml` modes still produce machine-readable output (no Rich markup)

  **Verification:**
  - `pytest tests/cli/` passes
  - No remaining `_echo_*` function definitions in the codebase (only imports from `ui.output`)
  - No remaining `from seeknal.cli.main import _echo_*` in workflow modules

---

- [ ] **Unit 8: Workflow Layer Migration**

  **Goal:** Replace workflow module output with `ui.output` and integrate the progress display with DAGRunner and ParallelExecutor.

  **Requirements:** R11, R12

  **Dependencies:** Units 3, 6

  **Files:**
  - Modify: `src/seeknal/workflow/runner.py`
  - Modify: `src/seeknal/workflow/parallel.py`
  - Modify: `src/seeknal/workflow/executor.py`
  - Modify: `src/seeknal/workflow/apply.py`
  - Modify: `src/seeknal/workflow/draft.py`
  - Modify: `src/seeknal/workflow/dry_run.py`
  - Modify: `src/seeknal/workflow/validators.py`
  - Modify: `src/seeknal/workflow/executors/transform_executor.py`
  - Modify: `src/seeknal/workflow/executors/model_executor.py`
  - Test: `tests/` (existing workflow tests)

  **Approach:**
  - Replace `from seeknal.cli.main import _echo_*` → `from seeknal.ui.output import echo_*` in all 8+ workflow modules. This also eliminates the workflow→CLI circular coupling.
  - Replace `print_summary()` in `runner.py` with `ui.progress.render_summary()` call
  - Replace `print_parallel_summary()` in `parallel.py` with same
  - Add optional `progress_display` parameter to `DAGRunner` and `ParallelExecutor`. When provided, nodes call `progress.start_node()` / `progress.complete_node()` instead of individual echo calls. When `None`, fall back to current echo-based output.
  - Replace `tabulate` calls in `executor.py` (inspect/dry-run) with `ui.tables.make_table()`

  **Patterns to follow:**
  - Existing `ExecutionContext` optional parameter pattern in `DAGRunner` for backward compatibility
  - The callback/interface pattern from Unit 6

  **Test scenarios:**
  - All existing workflow tests pass
  - DAGRunner with progress_display shows Rich progress
  - DAGRunner without progress_display shows plain echo output (backward compat)
  - Parallel summary renders as a branded panel

  **Verification:**
  - `pytest tests/` passes
  - `seeknal run` shows branded progress and summary
  - No remaining imports of `_echo_*` from `cli/main.py` in workflow modules

---

- [ ] **Unit 9: REPL Branded Header**

  **Goal:** Replace the plain-text REPL banner with a Rich-styled branded header.

  **Requirements:** R5

  **Dependencies:** Units 1, 2, 3

  **Files:**
  - Modify: `src/seeknal/cli/repl.py`
  - Test: `tests/cli/test_repl.py` (existing + new)

  **Approach:**
  - Replace the `print()` banner block in `REPL.run()` with a Rich Panel containing: small fox accent (single-line variant or just the brand symbol), "Seeknal SQL REPL" in `brand.primary`, project info, environment name, registered sources count
  - Use `console.print()` for the banner only. Keep `readline`-based input and `tabulate` for query results (REPL query output format should stay stable for now — users may depend on it for piping)
  - Optionally: style the prompt string with ANSI codes (Rich can generate styled prompt strings)
  - Keep REPL internals unchanged — this is banner + header only

  **Patterns to follow:**
  - Current REPL banner structure in `repl.py:439-472` for content
  - Rich `Panel` with `box.ROUNDED` and `border_style="brand.primary"`

  **Test scenarios:**
  - REPL banner contains project name and environment
  - REPL still works in non-TTY mode
  - Existing REPL tests pass

  **Verification:**
  - `seeknal repl` shows a branded header panel
  - REPL query execution and results are unaffected

---

- [ ] **Unit 10: Ask Mode Branded Header**

  **Goal:** Consolidate Ask mode's Rich usage with the shared Console and add branded entry screens.

  **Requirements:** R6, R14

  **Dependencies:** Units 1, 3

  **Files:**
  - Modify: `src/seeknal/cli/ask.py`
  - Modify: `src/seeknal/ask/streaming.py`
  - Test: `tests/cli/test_ask.py` (existing)

  **Approach:**
  - Replace all `Console()` instantiations in `ask.py` and `streaming.py` with `from seeknal.ui.console import get_console`
  - Add branded header for `seeknal ask chat` mode: "Seeknal Ask — Interactive Data Analysis" in `brand.primary` with fox accent
  - Apply theme styles to existing panels: reasoning (`border_style="text.muted"`), answer (`border_style="status.success"`), error (`border_style="status.error"`)
  - Syntax highlighting: keep monokai theme but apply consistent padding and panel wrapping
  - Remove `try: from rich...` fallback pattern since Rich is now a core dependency

  **Patterns to follow:**
  - Current Rich usage in `streaming.py:34-200` for panel/syntax patterns
  - Upgrade inline styles (`"[bold green]"`) to theme style names where possible

  **Test scenarios:**
  - Ask oneshot mode works with shared Console
  - Chat mode shows branded header
  - Existing ask tests pass
  - Streaming output renders correctly with theme

  **Verification:**
  - `seeknal ask chat` shows branded header
  - `seeknal ask "query"` renders themed panels for reasoning/answer

---

### Phase 4: Configuration & Polish

- [ ] **Unit 11: Theme Configuration**

  **Goal:** Add theme selection to config.toml and support `SEEKNAL_THEME` env var and `--no-animation` flag.

  **Requirements:** R8, R16

  **Dependencies:** Unit 1

  **Files:**
  - Modify: `src/seeknal/ui/console.py` (read config)
  - Modify: `src/seeknal/configuration.py` (add UI section)
  - Modify: `config.toml.example` (add `[ui]` section)
  - Modify: `src/seeknal/cli/main.py` (add `--no-animation` callback)
  - Test: `tests/ui/test_console.py` (expand)

  **Approach:**
  - Add `[ui]` section to `config.toml.example`: `theme = "dark"` (options: dark, light, dark-ansi, light-ansi, auto)
  - `console.py` reads config via `configuration.py` DotDict. Priority: `--theme` flag > `SEEKNAL_THEME` env > config.toml > auto-detect
  - `--no-animation` as a Typer callback on the main app that sets a module-level flag in `ui/console.py`
  - Reduced motion detection chain: `--no-animation` > `NO_COLOR` > `CI` env > `TERM=dumb` > `!isatty`
  - Expose `is_animation_enabled()` function for spinner/welcome/progress to check

  **Patterns to follow:**
  - Existing config.toml pattern with DotDict access
  - Starship's `[theme]` TOML section for inspiration

  **Test scenarios:**
  - `SEEKNAL_THEME=light` selects light theme
  - `--no-animation` disables all animations
  - `CI=true` disables animations
  - Config file theme setting is respected when no env var
  - Invalid theme name falls back to dark

  **Verification:**
  - Theme can be switched via env var or config
  - Animations respect reduced motion settings

---

- [ ] **Unit 12: Platform Awareness & Accessibility**

  **Goal:** Ensure graceful degradation across terminal capabilities and platforms.

  **Requirements:** R16, R18, R19

  **Dependencies:** Units 1, 2, 4

  **Files:**
  - Modify: `src/seeknal/ui/figures.py` (expand platform detection)
  - Modify: `src/seeknal/ui/fox.py` (ANSI/ASCII fallback rendering)
  - Modify: `src/seeknal/ui/spinner.py` (platform frames + reduced motion)
  - Modify: `src/seeknal/ui/welcome.py` (static mode)
  - Test: `tests/ui/test_platform.py`

  **Approach:**
  - `figures.py`: Detect `sys.platform`, `os.environ.get("TERM")`, `os.environ.get("WT_SESSION")` for Windows Terminal. Three glyph tiers: unicode-full (macOS/modern), unicode-safe (cross-platform subset), ascii-only.
  - Fox fallback: ANSI theme uses named colors. ASCII fallback uses `#`, `@`, `.`, `\/` chars.
  - Spinner fallback: `TERM=dumb` gets no animation (static text). Reduced motion gets slow-pulse (2s cycle).
  - Terminal.app detection: When `TERM_PROGRAM=Apple_Terminal`, skip truecolor (only 256-color) and avoid quadrant characters.
  - Pipe detection: When `!isatty(stdout)`, all output functions produce plain text (no ANSI codes). Rich handles this via `Console(force_terminal=False)`.

  **Patterns to follow:**
  - Claude Code's platform-aware glyph selection in `src/constants/figures.ts`
  - `cross-platform-terminal-characters` project for safe character sets

  **Test scenarios:**
  - `TERM=dumb` produces ASCII-only output
  - `NO_COLOR=1` produces uncolored output
  - Piped output (non-TTY) has no ANSI escape codes
  - macOS Terminal.app gets 256-color theme, not truecolor
  - Windows Terminal detected via `WT_SESSION`

  **Verification:**
  - `seeknal list feature-groups | cat` produces clean, parseable output
  - `TERM=dumb seeknal --help` shows ASCII fox and plain text

## System-Wide Impact

- **Interaction graph:** Every CLI command, REPL session, and Ask interaction is affected. The `_echo_*` functions are called from 13+ files across CLI and workflow layers. All paths through output rendering change.
- **Error propagation:** No change to error semantics. Output functions are display-only wrappers. If Rich fails to render (unlikely), errors should be caught and fall back to plain `print()`.
- **State lifecycle risks:** The Console singleton must be safe for concurrent use (Rich Console is thread-safe). The welcome session marker must not block parallel `seeknal` invocations.
- **API surface parity:** `--output json` and `--output yaml` modes must continue producing machine-readable output with no Rich markup. Non-TTY piped output must be clean.
- **Integration coverage:** Full CLI test suite (`tests/cli/`) exercises the output paths. Tests check string content presence, not ANSI formatting, so they should survive the migration.

## Risks & Dependencies

- **Test breakage from Rich markup in output:** CLI tests assert `"text" in result.stdout`. Rich may wrap text with ANSI codes that don't break substring checks, but could break exact-match assertions. Mitigation: Use `CliRunner` which captures output as rendered text. Run full test suite after each migration unit.
- **`tabulate` removal risk:** Some output paths (REPL query results, piped/non-TTY output) may need `tabulate` to stay. Don't force-remove it from dependencies — deprecate gradually.
- **Rich version compatibility:** Moving Rich to core dependencies pins `>=13.0.0`. Verify no conflicts with other dependencies (pydantic-ai-slim already pulls Rich transitively via `[ask]`).
- **Performance:** Rich rendering is marginally slower than raw `print()`. For high-frequency output (e.g., logging every row), ensure the progress display uses update batching, not per-row rendering.

## Documentation / Operational Notes

- Update `README.md` quick start to mention the visual experience
- Add `[ui]` section documentation to any config reference docs
- Document `SEEKNAL_THEME`, `--no-animation`, `NO_COLOR` in CLI help text
- Consider a "What's New" section or changelog entry highlighting the visual refresh

## Sources & References

- **Origin document:** [docs/brainstorms/2026-04-01-tui-visual-refresh-claude-code-style-requirements.md](docs/brainstorms/2026-04-01-tui-visual-refresh-claude-code-style-requirements.md)
- Related code: `src/seeknal/cli/main.py:560-577` (`_echo_*` functions), `src/seeknal/ask/streaming.py` (Rich usage), `src/seeknal/cli/repl.py` (REPL banner)
- Claude Code reference: `/Volumes/Hyperdisk/project/self/claude-code/src/utils/theme.ts`, `src/components/LogoV2/`, `src/components/Spinner/`, `src/constants/figures.ts`
- Rich documentation: Theme API, Spinner, Progress, Console, Live
- External: [GitHub Copilot CLI ASCII Banner Engineering](https://github.blog/engineering/from-pixels-to-characters-the-engineering-behind-github-copilot-clis-animated-ascii-banner/), [CLI UX Best Practices (Evil Martians)](https://evilmartians.com/chronicles/cli-ux-best-practices-3-patterns-for-improving-progress-displays), [NO_COLOR standard](https://no-color.org), [FORCE_COLOR standard](https://force-color.org)
