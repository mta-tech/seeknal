# CLI-Accessible Documentation for AI Agents

**Date:** 2026-02-21
**Status:** Brainstorm Complete
**Inspiration:** OpenClaw's `openclaw docs [query]` pattern

---

## What We're Building

Add a `seeknal docs` command that provides CLI-accessible documentation optimized for AI agents. This enables:

1. **AI agents** to query documentation via CLI without browsing filesystem
2. **Developers** to quickly find relevant docs from terminal
3. **Structured output** for machine consumption (JSON, concise snippets)

**Command examples:**
```bash
seeknal docs init              # Show init command docs
seeknal docs "feature group"   # Search for "feature group"
seeknal docs --list            # List all available topics
seeknal docs --json "version"  # JSON output for AI consumption
```

---

## Why This Approach

### AI-First Design

- **Structured output:** JSON with file paths, line numbers, snippets (token-efficient for agents)
- **Concise snippets:** Truncate to ~200 chars like OpenClaw (reduces token usage)
- **Metadata frontmatter:** Add `summary` and `read_when` to key docs for context hints

### Ripgrep-Based Local Search

- **Fast:** Rust-based ripgrep is 10-100x faster than Python alternatives
- **Simple:** Subprocess call to `rg` with structured output parsing
- **Powerful:** Regex support, respects .gitignore, smart case handling
- **Fallback:** Check for `rg` availability, provide install instructions if missing

### CLI-Focused Documentation Structure

Following OpenClaw's proven pattern:
```
docs/cli/
├── index.md              # CLI overview (comprehensive reference)
├── init.md               # seeknal init
├── list.md               # seeknal list
├── show.md               # seeknal show
├── delete.md             # seeknal delete
├── validate.md           # seeknal validate
├── version.md            # seeknal version
├── atlas.md              # seeknal atlas
└── ... (all 20+ commands)
```

Each file includes:
- **Frontmatter:** `summary`, `read_when`, `related`
- **Synopsis:** Command signature and options
- **Description:** What it does
- **Examples:** Common usage patterns
- **See also:** Links to related docs

---

## Key Decisions

| Decision | Rationale |
|----------|-----------|
| **AI agents first** | Optimizes for machine consumption while remaining human-readable |
| **Local file search** | Works offline, no hosting dependency, simpler than external MCP API |
| **CLI-focused docs/**** | Cleaner search results than full docs/ directory, follows OpenClaw pattern |
| **Full coverage immediately** | Complete from day one (~20 command docs based on current CLI) |
| **Ripgrep backend** | Fastest option, commonly available, excellent search quality |
| **JSON output option** | Enables programmatic use by AI agents and scripts |

---

## Open Questions

### 1. Ripgrep Fallback Strategy

**Question:** What happens if `rg` isn't installed?

**Options:**
- A) Error with install instructions (simplest)
- B) Fall back to Python-based search (more work, slower)
- C) Bundle ripgrep binary (complex, platform-specific)

**Recommendation:** Start with A (error + instructions), consider B if feedback demands it.

---

### 2. Doc File Generation

**Question:** Should CLI docs be hand-written or auto-generated from code?

**Options:**
- A) Hand-written markdown (more context, examples, explanation)
- B) Auto-generated from docstrings (always in sync, but limited context)
- C) Hybrid: auto-generate base, enhance with manual additions

**Recommendation:** Start with A (hand-written) for quality, consider C tooling later.

---

### 3. Search Result Ranking

**Question:** How should we rank search results when multiple files match?

**Options:**
- A) Ripgrep's default (line number order)
- B) Boost exact title matches
- C) Use file recency or usage metrics

**Recommendation:** Start with A (simplest), enhance based on usage patterns.

---

### 4. Related Documentation

**Question:** Should non-CLI docs be searchable too?

**Options:**
- A) Strict `docs/cli/` only (cleanest results)
- B) Include `docs/api/` and `docs/concepts/` (broader but noisier)
- C) Add `--scope` flag to choose (flexible but more complex)

**Recommendation:** Start with A, add C if needed based on feedback.

---

## Next Steps

1. **Proceed to planning:** Run `/workflows:plan` to design implementation details
2. **Validate approach:** Get feedback on open questions before implementation
3. **Review OpenClaw implementation:** Deep-dive into `openclaw docs` code if needed

---

## References

- **OpenClaw implementation:** `~/project/self/bmad-new/openclaw/src/cli/docs-cli.ts`
- **Seeknal CLI:** `src/seeknal/cli/main.py` (current command structure)
- **Existing docs:** `docs/` directory structure
- **Brainstorm skill:** For question techniques and YAGNI principles
