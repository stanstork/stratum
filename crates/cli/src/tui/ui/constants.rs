use ratatui::style::{Color, Modifier, Style};

// Layout constants
pub const HEADER_HEIGHT: u16 = 2;
pub const FOOTER_HEIGHT: u16 = 1;
pub const STATS_HEIGHT: u16 = 5;
pub const SPACER_HEIGHT: u16 = 1;

// Progress bar widths
pub const PROGRESS_BAR_WIDTH_SMALL: usize = 20;
pub const PROGRESS_BAR_WIDTH_LARGE: usize = 40;

// Table constraints
pub const STATUS_COLUMN_WIDTH: u16 = 12;
pub const PROGRESS_COLUMN_WIDTH: u16 = 30;
pub const ROWS_COLUMN_WIDTH: u16 = 12;
pub const RATE_COLUMN_WIDTH: u16 = 10;
pub const ETA_COLUMN_WIDTH: u16 = 8;

// Detail view
pub const DETAIL_HEADER_HEIGHT: u16 = 4;
pub const DETAIL_SOURCE_DEST_HEIGHT: u16 = 6;
pub const DETAIL_STATS_HEIGHT: u16 = 5;
pub const MAX_MAPPINGS_DISPLAY: usize = 10;

// Sparkline
pub const SPARKLINE_CHARS: [char; 8] = [' ', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

// Colors
pub mod colors {
    use super::*;

    pub const BACKGROUND: Color = Color::Rgb(30, 30, 30);
    pub const BORDER: Color = Color::DarkGray;
    pub const TEXT_PRIMARY: Color = Color::White;
    pub const TEXT_SECONDARY: Color = Color::Gray;
    pub const TEXT_MUTED: Color = Color::DarkGray;

    pub const STATUS_PENDING: Color = Color::DarkGray;
    pub const STATUS_QUEUED: Color = Color::Cyan;
    pub const STATUS_RUNNING: Color = Color::Green;
    pub const STATUS_PAUSED: Color = Color::Yellow;
    pub const STATUS_COMPLETED: Color = Color::Blue;
    pub const STATUS_FAILED: Color = Color::Red;

    pub const PROGRESS_BAR: Color = Color::Cyan;
    pub const THROUGHPUT: Color = Color::Magenta;
    pub const DATA_VOLUME: Color = Color::Green;
    pub const TIMING_ETA: Color = Color::Yellow;

    pub const SELECTION_BG: Color = Color::Rgb(40, 40, 40);
}

// Styles
pub mod styles {
    use super::*;

    pub fn border() -> Style {
        Style::default().fg(colors::BORDER)
    }

    pub fn header_title() -> Style {
        Style::default()
            .fg(colors::TEXT_PRIMARY)
            .add_modifier(Modifier::BOLD)
    }

    pub fn selected_row() -> Style {
        Style::default().bg(colors::SELECTION_BG)
    }

    pub fn table_header() -> Style {
        Style::default()
            .fg(colors::TEXT_SECONDARY)
            .add_modifier(Modifier::BOLD)
    }

    pub fn label() -> Style {
        Style::default().fg(colors::TEXT_SECONDARY)
    }

    pub fn value_bold() -> Style {
        Style::default()
            .fg(colors::TEXT_PRIMARY)
            .add_modifier(Modifier::BOLD)
    }

    pub fn muted() -> Style {
        Style::default().fg(colors::TEXT_MUTED)
    }

    pub fn success() -> Style {
        Style::default().fg(colors::STATUS_RUNNING)
    }

    pub fn warning() -> Style {
        Style::default().fg(colors::STATUS_PAUSED)
    }

    pub fn error() -> Style {
        Style::default().fg(colors::STATUS_FAILED)
    }
}

// Status symbols and display
pub struct StatusDisplay {
    pub symbol: &'static str,
    pub text: &'static str,
    pub color: Color,
}

impl StatusDisplay {
    pub const PENDING: Self = Self {
        symbol: "○",
        text: "Pending",
        color: colors::STATUS_PENDING,
    };

    pub const QUEUED: Self = Self {
        symbol: "◎",
        text: "Queued",
        color: colors::STATUS_QUEUED,
    };

    pub const RUNNING: Self = Self {
        symbol: "▶",
        text: "Running",
        color: colors::STATUS_RUNNING,
    };

    pub const PAUSED: Self = Self {
        symbol: "Ⅱ",
        text: "Paused",
        color: colors::STATUS_PAUSED,
    };

    pub const COMPLETED: Self = Self {
        symbol: "✔",
        text: "Done",
        color: colors::STATUS_COMPLETED,
    };

    pub const FAILED: Self = Self {
        symbol: "✖",
        text: "Failed",
        color: colors::STATUS_FAILED,
    };

    pub const SKIPPED: Self = Self {
        symbol: "◌",
        text: "Skipped",
        color: colors::STATUS_PENDING,
    };
}

// Markers
pub const ACTIVE_MARKER: &str = " ●";
pub const COMPLETED_MARKER: &str = " ✓";
pub const INDEXED_MARKER: &str = " ✓";
pub const SELECTION_INDICATOR: &str = "> ";
