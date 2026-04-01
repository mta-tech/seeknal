---
date: 2026-04-01
topic: tui-visual-refresh-claude-code-style
---

# TUI Visual Refresh: Claude Code-Inspired Design with Seeknal Fox Mascot

## Problem Frame

Seeknal's CLI currently has a purely functional visual identity — basic Typer echo functions with simple `✓ ✗ ⚠ ℹ` symbols, 60-char separators, and no branding. In a crowded data/ML tooling space, this means every terminal interaction is a missed opportunity to build user affinity and signal product quality. Claude Code has demonstrated that a polished CLI experience creates strong user loyalty.

## Requirements

### Brand Identity

- R1. **Fox mascot in Unicode block characters** — A seeknal fox rendered using Unicode block elements (▛▜▙▟░▒▓), similar to Claude Code's "Clawd" mascot. The fox should have multiple poses (default, looking left/right) and be compact enough for terminal headers (roughly 6-8 lines tall, ~20 chars wide).
- R2. **Branded color palette** — Primary: Teal `rgb(0, 188, 180)`. Accent: Fox orange `rgb(255, 165, 60)`. Plus semantic colors for success (green), error (red), warning (amber), and dim (gray). All colors defined as constants in a theme module.
- R3. **Animated welcome asterisk** — A seeknal-branded spinner character (e.g., `✻` or `◈`) with a teal hue sweep animation on startup, matching Claude Code's rainbow asterisk pattern.

### Welcome & Startup Screen

- R4. **Welcome banner** — Display on first interaction per session: fox mascot + "Welcome to Seeknal" in brand teal + dimmed version number. Styled horizontal separator using Unicode characters.
- R5. **REPL branded header** — Replace the current plain-text REPL header with a Rich-styled panel showing the fox, project info, environment, and registered data sources in a compact branded layout.
- R6. **Ask mode branded header** — Branded entry screen for `seeknal ask` and `seeknal ask chat` modes with fox accent and teal styling.

### Theme System

- R7. **Multi-theme support** — At minimum: `dark`, `light`, `dark-ansi` (for limited terminals). Themes define all UI colors (brand, semantic, text, borders, dim levels). Auto-detect terminal capabilities and fall back gracefully.
- R8. **Theme configuration** — Configurable via `config.toml` or environment variable (`SEEKNAL_THEME`). Default to `dark`.

### Spinners & Progress

- R9. **Custom spinner system** — Platform-aware spinner frames (macOS vs Linux/Windows) using Unicode characters that match the brand aesthetic. Bi-directional animation (forward then reverse) like Claude Code's spinner.
- R10. **Stalled state indication** — When operations exceed a threshold, smoothly transition spinner color toward error red, signaling to the user that something may be stuck.
- R11. **Rich progress bars** — Replace plain-text execution summaries with Rich-styled progress panels showing node execution status, timing, and results with proper color coding.

### Styled Output

- R12. **Rich panels for all output modes** — Execution plans, summaries, and results rendered in Rich panels with branded borders (teal for info, green for success, red for errors).
- R13. **Branded status symbols** — Upgrade from plain `✓ ✗ ⚠ ℹ` to styled symbols with brand-consistent coloring and optional Rich markup.
- R14. **Syntax highlighting** — SQL and Python code blocks in output use Rich Syntax with a branded theme (monokai base with teal accents).
- R15. **Table styling** — All tabular output (feature groups, versions, entities) rendered with Rich Tables using branded header colors and consistent styling.

### Accessibility

- R16. **Reduced motion mode** — Detect `REDUCE_MOTION` env var or `--no-animation` flag. Replace animations with static indicators.
- R17. **Color-blind safe variants** — The daltonized theme variants (future: `dark-daltonized`, `light-daltonized`) should use patterns/symbols in addition to color to convey status.

### Platform Awareness

- R18. **Terminal capability detection** — Detect truecolor, ANSI256, and basic ANSI support. Degrade gracefully: truecolor gets full palette, ANSI256 gets approximated colors, basic ANSI gets the `dark-ansi`/`light-ansi` theme.
- R19. **Platform-specific glyphs** — Use appropriate Unicode characters per platform (macOS supports broader Unicode than some Linux terminals). Provide fallback character sets.

## Success Criteria

- Running `seeknal` commands produces visually branded output that is immediately recognizable as seeknal
- The fox mascot appears on welcome/startup and is charming and distinctive
- A user familiar with Claude Code's TUI would recognize a comparable level of visual polish
- All output remains functional and readable on terminals with limited color support
- Existing CLI behavior and output semantics are preserved — this is a visual layer, not a behavior change

## Scope Boundaries

- **Not building a custom layout engine** — We use Rich (already a dependency), not a custom React Ink-like renderer. The goal is visual output parity, not architectural parity.
- **Not adding Textual** — No full-screen interactive TUI app. Seeknal remains a CLI tool with Rich-enhanced output.
- **Not changing CLI command structure** — All existing commands, flags, and output semantics stay the same.
- **Not adding mouse support or interactive widgets** — Beyond what Rich provides out of the box.
- **Animation scope is terminal-safe** — No ANSI escape abuse. Use Rich's built-in Live display and Status for animations.

## Key Decisions

- **Mascot: Fox** — Represents seeknal's "clever, agile data seeker" identity. Rendered in Unicode block characters for terminal compatibility.
- **Primary color: Teal `rgb(0, 188, 180)`** — Distinctive from Claude's orange, evokes data/signals/digital. Fox orange as accent color creates a dual-identity palette.
- **Implementation via Rich** — Python's Rich library provides panels, tables, syntax highlighting, spinners, progress bars, and Live display. This covers 90%+ of Claude Code's visual features without a custom renderer.
- **Visual parity, not architectural parity** — Match the output quality and brand polish of Claude Code, but use idiomatic Python tooling rather than replicating React Ink.

## Dependencies / Assumptions

- Rich is already a dependency (used in `ask/streaming.py`)
- Terminal color detection can rely on Rich's built-in `Console.color_system`
- The fox mascot will be manually crafted in Unicode — no image-to-ASCII conversion needed

## Outstanding Questions

### Deferred to Planning

- [Affects R1][Needs research] What specific Unicode block characters produce the best fox rendering at 6-8 lines? Need to prototype multiple designs and test across terminals (iTerm2, Terminal.app, VS Code integrated terminal, Ghostty).
- [Affects R7][Technical] Should themes be implemented as Rich `Theme` objects or custom dataclasses? Rich has a built-in theme system that may or may not be flexible enough.
- [Affects R9][Technical] How to implement bi-directional spinner animation in Rich? May need a custom `Spinner` subclass or use `Live` display with manual frame control.
- [Affects R11][Technical] How to integrate Rich progress panels with the existing DAGRunner execution flow without changing execution semantics?
- [Affects R18][Needs research] What terminal capability detection does Rich provide out of the box, and what gaps need to be filled?

## Next Steps

→ `/ce:plan` for structured implementation planning
