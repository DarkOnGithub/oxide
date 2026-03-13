use std::io::{self, IsTerminal};
use std::time::Duration;

use nu_ansi_term::{Color, Style};
use terminal_size::{Width, terminal_size};

#[derive(Clone, Copy)]
pub enum StreamTarget {
    Stdout,
    Stderr,
}

#[derive(Clone, Copy)]
pub enum Tone {
    Accent,
    Info,
    Success,
    Warning,
    Muted,
}

pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

pub fn format_bytes_f64(bytes: f64) -> String {
    if !bytes.is_finite() || bytes <= 0.0 {
        return "0 B".to_string();
    }

    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }

    if unit == 0 {
        format!("{value:.0} {}", UNITS[unit])
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

pub fn format_rate(bytes_per_second: f64) -> String {
    if !bytes_per_second.is_finite() || bytes_per_second <= 0.0 {
        return "0 B".to_string();
    }

    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes_per_second;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{value:.0} {}", UNITS[unit])
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

pub fn format_live_rate(bytes_per_second: f64) -> String {
    if !bytes_per_second.is_finite() || bytes_per_second <= 0.0 {
        return "0B/s".to_string();
    }

    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes_per_second;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }

    if unit == 0 {
        format!("{value:.0}B/s")
    } else if value >= 100.0 {
        format!("{value:.0}{}/s", UNITS[unit])
    } else if value >= 10.0 {
        format!("{value:.1}{}/s", UNITS[unit])
    } else {
        format!("{value:.2}{}/s", UNITS[unit])
    }
}

pub fn format_duration(duration: Duration) -> String {
    let total_seconds = duration.as_secs();
    let millis = duration.subsec_millis();
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;

    if hours > 0 {
        format!("{hours:02}:{minutes:02}:{seconds:02}")
    } else if minutes > 0 {
        format!("{minutes:02}:{seconds:02}")
    } else {
        format!("{seconds}.{millis:03}s")
    }
}

pub fn format_duration_compact(duration: Duration) -> String {
    let secs = duration.as_secs_f64();
    if secs >= 60.0 {
        format_duration(duration)
    } else if secs >= 1.0 {
        format!("{secs:.2}s")
    } else if secs >= 0.001 {
        format!("{:.2}ms", secs * 1_000.0)
    } else {
        format!("{:.0}us", secs * 1_000_000.0)
    }
}

pub fn tagged_message(target: StreamTarget, tone: Tone, tag: &str, message: &str) -> String {
    paint(target, tone, &format!("[{tag}] {message}"))
}

pub fn progress_bar(target: StreamTarget, tone: Tone, percent: f64, width: usize) -> String {
    let clamped = percent.clamp(0.0, 100.0);
    let filled = ((clamped / 100.0) * width as f64).round() as usize;
    let filled = filled.min(width);
    let empty = width.saturating_sub(filled);
    let body = format!("{}{}", "█".repeat(filled), "░".repeat(empty));
    paint(target, tone, &body)
}

pub fn terminal_width(target: StreamTarget) -> usize {
    if !stream_is_terminal(target) {
        return 80;
    }

    terminal_size()
        .map(|(Width(width), _)| usize::from(width))
        .or_else(|| {
            std::env::var("COLUMNS")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
        })
        .filter(|width| *width >= 40)
        .unwrap_or(100)
}

pub fn print_startup_banner() {
    let banner = [
        "██╗      ██████╗ ██╗  ██╗██╗██████╗ ███████╗",
        "╚██╗    ██╔═══██╗╚██╗██╔╝██║██╔══██╗██╔════╝",
        " ╚██╗   ██║   ██║ ╚███╔╝ ██║██║  ██║█████╗  ",
        " ██╔╝   ██║   ██║ ██╔██╗ ██║██║  ██║██╔══╝  ",
        "██╔╝    ╚██████╔╝██╔╝ ██╗██║██████╔╝███████╗",
        "╚═╝      ╚═════╝ ╚═╝  ╚═╝╚═╝╚═════╝ ╚══════╝",
    ];

    for line in banner {
        println!("{}", paint(StreamTarget::Stdout, Tone::Warning, line));
    }
    println!();
}

pub fn paint(target: StreamTarget, tone: Tone, text: &str) -> String {
    if stream_is_terminal(target) {
        tone.style().paint(text).to_string()
    } else {
        text.to_string()
    }
}

impl Tone {
    fn style(self) -> Style {
        match self {
            Self::Accent => Color::Cyan.bold(),
            Self::Info => Color::Blue.bold(),
            Self::Success => Color::Green.bold(),
            Self::Warning => Color::Yellow.bold(),
            Self::Muted => Style::new().dimmed(),
        }
    }
}

fn stream_is_terminal(target: StreamTarget) -> bool {
    match target {
        StreamTarget::Stdout => io::stdout().is_terminal(),
        StreamTarget::Stderr => io::stderr().is_terminal(),
    }
}
