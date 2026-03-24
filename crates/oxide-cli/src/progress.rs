use std::io::{self, IsTerminal, Write};
use std::time::{Duration, Instant};

use oxide_core::{ArchiveProgressEvent, TelemetryEvent, TelemetrySink};

use crate::ui::{
    format_bytes, format_duration, format_live_rate, tagged_message, terminal_width, StreamTarget,
    Tone,
};

pub struct ArchiveCliSink<'a> {
    live_rates: &'a mut LiveRateStats,
    discovery_started: Instant,
    discovery_reported: bool,
    interactive: bool,
    rendered_lines: usize,
}

impl<'a> ArchiveCliSink<'a> {
    pub fn new(
        live_rates: &'a mut LiveRateStats,
        discovery_started: Instant,
        interactive: bool,
    ) -> Self {
        Self {
            live_rates,
            discovery_started,
            discovery_reported: false,
            interactive,
            rendered_lines: 0,
        }
    }

    pub fn finish_discovery_notice(&mut self) {
        if self.interactive && !self.discovery_reported {
            self.report_discovery_ready(self.discovery_started.elapsed());
        }
    }

    fn report_discovery_ready(&mut self, elapsed: Duration) {
        if self.interactive && !self.discovery_reported {
            self.discovery_reported = true;
            eprintln!("{}", ready_message(elapsed));
        }
    }
}

impl TelemetrySink for ArchiveCliSink<'_> {
    fn on_event(&mut self, event: TelemetryEvent) {
        if !self.interactive {
            return;
        }

        let snapshot = match event {
            TelemetryEvent::ArchivePlanningComplete(ready) => {
                self.report_discovery_ready(ready.elapsed);
                return;
            }
            TelemetryEvent::ArchiveProgress(snapshot) => {
                if !self.discovery_reported {
                    self.report_discovery_ready(self.discovery_started.elapsed());
                }
                snapshot
            }
            _ => return,
        };

        let total = snapshot.input_bytes_total;
        let done = snapshot.input_bytes_completed.min(total);
        let (read_avg_bps, read_instant_bps, _write_avg_bps, write_instant_bps) =
            self.live_rates.update(&snapshot);
        let percent = if total > 0 {
            (done as f64 / total as f64) * 100.0
        } else {
            100.0
        };
        let remaining = total.saturating_sub(done);
        let eta = if read_avg_bps > 0.0 {
            Duration::from_secs_f64(remaining as f64 / read_avg_bps)
        } else {
            Duration::ZERO
        };
        let active_workers = snapshot
            .runtime
            .workers
            .iter()
            .filter(|worker| worker.tasks_completed > 0 || worker.busy > Duration::ZERO)
            .count();
        let panel_width = compact_panel_width();
        let title = format!("{} {}", "[archive]", format!("{percent:5.1}%"));
        let bar = plain_progress_bar(percent, panel_width.saturating_sub(4));
        let lines = progress_panel_lines(
            Tone::Success,
            panel_width,
            &title,
            &bar,
            &[
                progress_row(
                    "Processed",
                    &format!("{} / {}", format_bytes(done), format_bytes(total)),
                    panel_width,
                ),
                progress_row(
                    "Blocks",
                    &format!("{}/{}", snapshot.blocks_completed, snapshot.blocks_total),
                    panel_width,
                ),
                progress_row("ETA", &format_duration(eta), panel_width),
                progress_row("Read", &format_live_rate(read_instant_bps), panel_width),
                progress_row("Write", &format_live_rate(write_instant_bps), panel_width),
                progress_row(
                    "Compress wall",
                    &format_live_rate(snapshot.compression_wall_avg_bps),
                    panel_width,
                ),
                progress_row("Queue", &snapshot.blocks_pending.to_string(), panel_width),
                progress_row(
                    "Workers",
                    &format!("{active_workers}/{}", snapshot.runtime.workers.len()),
                    panel_width,
                ),
            ],
        );
        render_progress_lines(&lines, &mut self.rendered_lines);
    }
}

pub struct ExtractCliSink {
    interactive: bool,
    last_elapsed: Duration,
    last_archive_bytes: u64,
    rendered_line: bool,
    rendered_lines: usize,
}

impl ExtractCliSink {
    pub fn new() -> Self {
        Self {
            interactive: io::stderr().is_terminal(),
            last_elapsed: Duration::ZERO,
            last_archive_bytes: 0,
            rendered_line: false,
            rendered_lines: 0,
        }
    }

    pub fn rendered_line(&self) -> bool {
        self.rendered_line
    }
}

impl TelemetrySink for ExtractCliSink {
    fn on_event(&mut self, event: TelemetryEvent) {
        let TelemetryEvent::ExtractProgress(progress) = event else {
            return;
        };
        if !self.interactive {
            return;
        }

        let elapsed_secs = progress.elapsed.as_secs_f64().max(1e-6);
        let read_avg_bps = progress.archive_bytes_completed as f64 / elapsed_secs;
        let delta_bytes = progress
            .archive_bytes_completed
            .saturating_sub(self.last_archive_bytes);
        let delta_elapsed = progress.elapsed.saturating_sub(self.last_elapsed);
        let delta_secs = delta_elapsed.as_secs_f64();
        let read_instant_bps = if delta_secs > 0.0 {
            delta_bytes as f64 / delta_secs
        } else {
            read_avg_bps
        };

        self.last_elapsed = progress.elapsed;
        self.last_archive_bytes = progress.archive_bytes_completed;

        let percent = if progress.blocks_total > 0 {
            (progress.blocks_completed as f64 / progress.blocks_total as f64) * 100.0
        } else {
            100.0
        };
        let active_workers = progress
            .runtime
            .workers
            .iter()
            .filter(|worker| worker.tasks_completed > 0 || worker.busy > Duration::ZERO)
            .count();
        let panel_width = compact_panel_width();
        let title = format!("{} {}", "[extract]", format!("{percent:5.1}%"));
        let bar = plain_progress_bar(percent, panel_width.saturating_sub(4));
        let lines = progress_panel_lines(
            Tone::Info,
            panel_width,
            &title,
            &bar,
            &[
                progress_row(
                    "Archive read",
                    &format_bytes(progress.archive_bytes_completed),
                    panel_width,
                ),
                progress_row(
                    "Decoded",
                    &format_bytes(progress.decoded_bytes_completed),
                    panel_width,
                ),
                progress_row(
                    "Blocks",
                    &format!("{}/{}", progress.blocks_completed, progress.blocks_total),
                    panel_width,
                ),
                progress_row("Average read", &format_live_rate(read_avg_bps), panel_width),
                progress_row(
                    "Live read",
                    &format_live_rate(read_instant_bps),
                    panel_width,
                ),
                progress_row(
                    "Decode",
                    &format_live_rate(progress.decode_avg_bps),
                    panel_width,
                ),
                progress_row(
                    "Workers",
                    &format!("{active_workers}/{}", progress.runtime.workers.len()),
                    panel_width,
                ),
            ],
        );
        render_progress_lines(&lines, &mut self.rendered_lines);
        self.rendered_line = true;
    }
}

fn progress_panel_lines(
    tone: Tone,
    width: usize,
    title: &str,
    bar: &str,
    rows: &[String],
) -> Vec<String> {
    let inner_width = width.saturating_sub(2);
    let mut lines = Vec::with_capacity(rows.len() + 4);
    lines.push(paint_frame(tone, &format!("╭{}╮", "─".repeat(inner_width))));
    lines.push(paint_frame(
        tone,
        &frame_line_plain(&center_text(title, width.saturating_sub(4)), width),
    ));
    lines.push(paint_frame(
        tone,
        &frame_line_plain(&center_text(bar, width.saturating_sub(4)), width),
    ));
    for row in rows {
        lines.push(frame_line_plain(row, width));
    }
    lines.push(paint_frame(tone, &format!("╰{}╯", "─".repeat(inner_width))));
    lines
}

fn progress_row(label: &str, value: &str, width: usize) -> String {
    let inner_width = width.saturating_sub(4);
    let left = format!("{label}:");
    let spacer = inner_width.saturating_sub(left.len() + value.len()).max(1);
    format!("{left}{}{}", " ".repeat(spacer), value)
}

fn frame_line_plain(content: &str, width: usize) -> String {
    let inner_width = width.saturating_sub(4);
    let content = truncate_for_width(content, inner_width);
    format!(
        "│ {}{} │",
        content,
        " ".repeat(inner_width.saturating_sub(content.len()))
    )
}

fn truncate_for_width(content: &str, width: usize) -> String {
    content.chars().take(width).collect()
}

fn center_text(content: &str, width: usize) -> String {
    let visible = content.chars().count().min(width);
    let left = (width.saturating_sub(visible)) / 2;
    let right = width.saturating_sub(visible + left);
    format!("{}{}{}", " ".repeat(left), content, " ".repeat(right))
}

fn compact_panel_width() -> usize {
    let width = terminal_width(StreamTarget::Stderr);
    ((width * 68) / 100).clamp(58, 92)
}

fn paint_frame(tone: Tone, text: &str) -> String {
    crate::ui::paint(StreamTarget::Stderr, tone, text)
}

fn plain_progress_bar(percent: f64, width: usize) -> String {
    let clamped = percent.clamp(0.0, 100.0);
    let filled = ((clamped / 100.0) * width as f64).round() as usize;
    let filled = filled.min(width);
    let empty = width.saturating_sub(filled);
    format!("{}{}", "█".repeat(filled), "░".repeat(empty))
}

fn render_progress_lines(lines: &[String], rendered_lines: &mut usize) {
    if *rendered_lines > 0 {
        eprint!("\x1b[{}A", *rendered_lines);
    }
    for line in lines {
        eprint!("\r\x1b[2K{line}\n");
    }
    let _ = io::stderr().flush();
    *rendered_lines = lines.len();
}

#[derive(Debug, Default)]
pub struct LiveRateStats {
    last_input_bytes: u64,
    last_output_bytes: u64,
    last_elapsed: Duration,
    pub peak_read_bps: f64,
    pub peak_write_bps: f64,
}

impl LiveRateStats {
    pub fn update(&mut self, snapshot: &ArchiveProgressEvent) -> (f64, f64, f64, f64) {
        let elapsed = snapshot.elapsed;
        let done = snapshot
            .input_bytes_completed
            .min(snapshot.input_bytes_total);
        let written = snapshot.output_bytes_completed;
        let elapsed_secs = elapsed.as_secs_f64().max(1e-6);
        let read_avg_bps = done as f64 / elapsed_secs;
        let write_avg_bps = written as f64 / elapsed_secs;
        let delta_read_bytes = done.saturating_sub(self.last_input_bytes);
        let delta_write_bytes = written.saturating_sub(self.last_output_bytes);
        let delta_elapsed = elapsed.saturating_sub(self.last_elapsed);
        let delta_secs = delta_elapsed.as_secs_f64();
        let read_instant_bps = if delta_secs > 0.0 {
            delta_read_bytes as f64 / delta_secs
        } else {
            read_avg_bps
        };
        let write_instant_bps = if delta_secs > 0.0 {
            delta_write_bytes as f64 / delta_secs
        } else {
            write_avg_bps
        };

        self.last_input_bytes = done;
        self.last_output_bytes = written;
        self.last_elapsed = elapsed;
        self.peak_read_bps = self.peak_read_bps.max(read_instant_bps);
        self.peak_write_bps = self.peak_write_bps.max(write_instant_bps);

        (
            read_avg_bps,
            read_instant_bps,
            write_avg_bps,
            write_instant_bps,
        )
    }
}

fn ready_message(elapsed: Duration) -> String {
    tagged_message(
        StreamTarget::Stderr,
        Tone::Info,
        "ready",
        &format!(
            "discovery complete in {}, starting workers...",
            format_duration(elapsed)
        ),
    )
}
