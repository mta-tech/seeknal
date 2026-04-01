"""
Seeknal theme definitions.

Defines the brand color palette and Rich Theme instances for dark, light,
and ANSI-only terminal modes.
"""

from rich.theme import Theme

# =============================================================================
# Brand Color Palette
# =============================================================================

# Primary brand color — teal
BRAND_TEAL = "#00BCB4"
BRAND_TEAL_DIM = "#008C86"
BRAND_TEAL_BRIGHT = "#33D6CF"

# Accent — fox orange
BRAND_ORANGE = "#FFA53C"
BRAND_ORANGE_DIM = "#CC842F"
BRAND_ORANGE_BRIGHT = "#FFB960"

# Semantic colors
COLOR_SUCCESS = "#2C7A39"
COLOR_SUCCESS_BRIGHT = "#6DDB7C"
COLOR_ERROR = "#AB2B3F"
COLOR_ERROR_BRIGHT = "#FFA8B4"
COLOR_WARNING = "#966C1E"
COLOR_WARNING_BRIGHT = "#FFCF40"
COLOR_INFO = "#5769F7"
COLOR_INFO_BRIGHT = "#8B9BFF"

# Neutral
COLOR_MUTED = "#808080"
COLOR_DIM = "#555555"

# =============================================================================
# Style name registry — all themes MUST define these keys
# =============================================================================

STYLE_NAMES = [
    "brand.primary",
    "brand.primary.bold",
    "brand.accent",
    "brand.accent.bold",
    "status.success",
    "status.error",
    "status.warning",
    "status.info",
    "text.muted",
    "text.dim",
    "text.highlight",
    "table.header",
    "table.border",
    "table.row.alt",
    "panel.border",
    "panel.title",
    "spinner.active",
    "spinner.stalled",
    "code.keyword",
    "code.string",
    "code.number",
    "code.comment",
    "progress.bar.complete",
    "progress.bar.remaining",
    "welcome.title",
    "welcome.version",
    "welcome.separator",
]

# =============================================================================
# Dark Theme (default — truecolor / 256-color terminals)
# =============================================================================

DARK_THEME = Theme(
    {
        "brand.primary": f"bold {BRAND_TEAL}",
        "brand.primary.bold": f"bold {BRAND_TEAL_BRIGHT}",
        "brand.accent": f"{BRAND_ORANGE}",
        "brand.accent.bold": f"bold {BRAND_ORANGE_BRIGHT}",
        "status.success": f"bold {COLOR_SUCCESS_BRIGHT}",
        "status.error": f"bold {COLOR_ERROR_BRIGHT}",
        "status.warning": f"bold {COLOR_WARNING_BRIGHT}",
        "status.info": f"bold {COLOR_INFO_BRIGHT}",
        "text.muted": f"{COLOR_MUTED}",
        "text.dim": f"dim",
        "text.highlight": f"bold white",
        "table.header": f"bold {BRAND_TEAL}",
        "table.border": f"{COLOR_DIM}",
        "table.row.alt": "dim",
        "panel.border": f"{BRAND_TEAL_DIM}",
        "panel.title": f"bold {BRAND_TEAL}",
        "spinner.active": f"bold {BRAND_TEAL}",
        "spinner.stalled": f"bold {COLOR_ERROR}",
        "code.keyword": "bold bright_magenta",
        "code.string": "bright_green",
        "code.number": "bright_yellow",
        "code.comment": "dim italic",
        "progress.bar.complete": f"{BRAND_TEAL}",
        "progress.bar.remaining": f"{COLOR_DIM}",
        "welcome.title": f"bold {BRAND_TEAL_BRIGHT}",
        "welcome.version": "dim",
        "welcome.separator": f"{COLOR_DIM}",
    }
)

# =============================================================================
# Light Theme (for light terminal backgrounds)
# =============================================================================

LIGHT_THEME = Theme(
    {
        "brand.primary": f"bold {BRAND_TEAL_DIM}",
        "brand.primary.bold": f"bold {BRAND_TEAL_DIM}",
        "brand.accent": f"{BRAND_ORANGE_DIM}",
        "brand.accent.bold": f"bold {BRAND_ORANGE_DIM}",
        "status.success": f"bold {COLOR_SUCCESS}",
        "status.error": f"bold {COLOR_ERROR}",
        "status.warning": f"bold {COLOR_WARNING}",
        "status.info": f"bold {COLOR_INFO}",
        "text.muted": "bright_black",
        "text.dim": "dim",
        "text.highlight": "bold black",
        "table.header": f"bold {BRAND_TEAL_DIM}",
        "table.border": "bright_black",
        "table.row.alt": "dim",
        "panel.border": f"{BRAND_TEAL_DIM}",
        "panel.title": f"bold {BRAND_TEAL_DIM}",
        "spinner.active": f"bold {BRAND_TEAL_DIM}",
        "spinner.stalled": f"bold {COLOR_ERROR}",
        "code.keyword": "bold magenta",
        "code.string": "green",
        "code.number": "yellow",
        "code.comment": "dim italic",
        "progress.bar.complete": f"{BRAND_TEAL_DIM}",
        "progress.bar.remaining": "bright_black",
        "welcome.title": f"bold {BRAND_TEAL_DIM}",
        "welcome.version": "dim",
        "welcome.separator": "bright_black",
    }
)

# =============================================================================
# ANSI Theme (safe for 16-color terminals — named colors only)
# =============================================================================

ANSI_THEME = Theme(
    {
        "brand.primary": "bold cyan",
        "brand.primary.bold": "bold bright_cyan",
        "brand.accent": "yellow",
        "brand.accent.bold": "bold bright_yellow",
        "status.success": "bold green",
        "status.error": "bold red",
        "status.warning": "bold yellow",
        "status.info": "bold blue",
        "text.muted": "bright_black",
        "text.dim": "dim",
        "text.highlight": "bold white",
        "table.header": "bold cyan",
        "table.border": "bright_black",
        "table.row.alt": "dim",
        "panel.border": "cyan",
        "panel.title": "bold cyan",
        "spinner.active": "bold cyan",
        "spinner.stalled": "bold red",
        "code.keyword": "bold magenta",
        "code.string": "green",
        "code.number": "yellow",
        "code.comment": "dim",
        "progress.bar.complete": "cyan",
        "progress.bar.remaining": "bright_black",
        "welcome.title": "bold bright_cyan",
        "welcome.version": "dim",
        "welcome.separator": "bright_black",
    }
)

# =============================================================================
# Theme registry
# =============================================================================

THEMES = {
    "dark": DARK_THEME,
    "light": LIGHT_THEME,
    "dark-ansi": ANSI_THEME,
    "light-ansi": ANSI_THEME,  # same palette works for both
}

DEFAULT_THEME_NAME = "dark"


def get_theme(name: str) -> Theme:
    """Return the named theme, falling back to dark if not found."""
    return THEMES.get(name, DARK_THEME)
