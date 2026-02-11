use std::cmp::Reverse;
use std::time::Duration;

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Borders, Cell, Clear, Gauge, List, ListItem, Paragraph, Row, Sparkline, Table, Wrap,
};

use crate::app::{ActiveField, App};
use crate::formatters::{format_bytes, format_duration, format_rate};
use crate::job::JobMode;

pub fn draw(frame: &mut Frame<'_>, app: &App) {
    let areas = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(10),
            Constraint::Length(13),
            Constraint::Min(14),
        ])
        .split(frame.area());

    render_config(frame, areas[0], app);
    render_live_runtime(frame, areas[1], app);

    let lower = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(62), Constraint::Percentage(38)])
        .split(areas[2]);

    render_profile(frame, lower[0], app);
    render_logs(frame, lower[1], app);

    if app.browser.is_some() {
        render_browser_modal(frame, app);
    }
}

fn render_config(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let mode = match app.mode {
        JobMode::Archive => "archive",
        JobMode::Extract => "extract",
    };

    let compression = match app.compression {
        oxide_core::CompressionAlgo::Lz4 => "lz4",
        oxide_core::CompressionAlgo::Lzma => "lzma",
        oxide_core::CompressionAlgo::Deflate => "deflate",
    };

    let lines = vec![
        styled_kv_line("mode", mode, app, ActiveField::Mode),
        styled_kv_line("input", &app.input_path, app, ActiveField::Input),
        styled_kv_line("output", &app.output_path, app, ActiveField::Output),
        styled_kv_line("compression", compression, app, ActiveField::Compression),
        styled_kv_line("workers", &app.workers, app, ActiveField::Workers),
        styled_kv_line("block size", &app.block_size, app, ActiveField::BlockSize),
        styled_kv_line(
            "stats interval",
            &format!("{} ms", app.stats_interval_ms),
            app,
            ActiveField::StatsInterval,
        ),
        Line::from(vec![
            Span::styled("status: ", Style::default().fg(Color::Yellow)),
            Span::raw(&app.status_line),
            Span::raw(if app.running {
                "  [RUNNING]"
            } else {
                "  [IDLE]"
            }),
        ]),
        Line::from(
            "keys: Tab shift field | Enter edit/toggle | Left/Right toggle enum | b browser | r run | d default output | c clear | q quit",
        ),
    ];

    let title = if app.editing {
        "Oxide TUI (editing)"
    } else {
        "Oxide TUI"
    };

    let paragraph = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title(title))
        .wrap(Wrap { trim: true });
    frame.render_widget(paragraph, area);
}

fn render_live_runtime(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(5),
            Constraint::Min(4),
        ])
        .split(area);

    let (progress_ratio, progress_label) = if let Some(progress) = &app.archive_progress {
        let total = progress.input_bytes_total.max(1);
        let done = progress.input_bytes_completed.min(total);
        let ratio = done as f64 / total as f64;
        (
            ratio,
            format!(
                "archive {:.2}% | blocks {}/{}",
                ratio * 100.0,
                progress.blocks_completed,
                progress.blocks_total
            ),
        )
    } else if let Some(progress) = &app.extract_progress {
        let total = progress.blocks_total.max(1);
        let done = progress.blocks_completed.min(total);
        let ratio = done as f64 / total as f64;
        (
            ratio,
            format!(
                "extract {:.2}% | blocks {}/{}",
                ratio * 100.0,
                progress.blocks_completed,
                progress.blocks_total
            ),
        )
    } else {
        (0.0, "awaiting telemetry".to_string())
    };

    let gauge = Gauge::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Live Progress"),
        )
        .gauge_style(Style::default().fg(Color::Green).bg(Color::Black))
        .ratio(progress_ratio.clamp(0.0, 1.0))
        .label(progress_label);
    frame.render_widget(gauge, sections[0]);

    let summary_lines = build_summary_lines(app);
    let summary = Paragraph::new(summary_lines)
        .block(Block::default().borders(Borders::ALL).title("Live Metrics"))
        .wrap(Wrap { trim: true });
    frame.render_widget(summary, sections[1]);

    let history_sections = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(sections[2]);

    let left_title = if app.extract_progress.is_some() {
        "Read Rate (instant B/s)"
    } else {
        "Read Rate (instant B/s)"
    };
    let left_history = Sparkline::default()
        .block(Block::default().borders(Borders::ALL).title(left_title))
        .style(Style::default().fg(Color::Cyan))
        .data(&app.read_rate_history);
    frame.render_widget(left_history, history_sections[0]);

    let (right_title, right_data, right_color) = if app.extract_progress.is_some() {
        (
            "Decode Rate (instant B/s)",
            &app.decode_rate_history,
            Color::LightMagenta,
        )
    } else {
        (
            "Write Rate (instant B/s)",
            &app.write_rate_history,
            Color::LightBlue,
        )
    };
    let right_history = Sparkline::default()
        .block(Block::default().borders(Borders::ALL).title(right_title))
        .style(Style::default().fg(right_color))
        .data(right_data);
    frame.render_widget(right_history, history_sections[1]);
}

fn build_summary_lines(app: &App) -> Vec<Line<'static>> {
    let mut lines = Vec::new();

    if let Some(progress) = &app.archive_progress {
        let total = progress.input_bytes_total.max(1);
        let done = progress.input_bytes_completed.min(total);
        let remaining = total.saturating_sub(done);
        let eta = if app.archive_live.read_avg_bps > 0.0 {
            Duration::from_secs_f64(remaining as f64 / app.archive_live.read_avg_bps)
        } else {
            Duration::ZERO
        };
        let active_workers = progress
            .runtime
            .workers
            .iter()
            .filter(|worker| worker.tasks_completed > 0 || worker.busy > Duration::ZERO)
            .count();

        lines.push(Line::from(format!(
            "source: {:?} | elapsed {} | pending blocks {}",
            progress.source_kind,
            format_duration(progress.elapsed),
            progress.blocks_pending,
        )));
        lines.push(Line::from(format!(
            "workers active {}/{}",
            active_workers,
            progress.runtime.workers.len()
        )));
        lines.push(Line::from(format!(
            "input {} / {} | output {}",
            format_bytes(done),
            format_bytes(total),
            format_bytes(progress.output_bytes_completed),
        )));
        lines.push(Line::from(format!(
            "read avg {}/s inst {}/s peak {}/s",
            format_rate(app.archive_live.read_avg_bps),
            format_rate(app.archive_live.read_inst_bps),
            format_rate(app.archive_live.peak_read_bps),
        )));
        lines.push(Line::from(format!(
            "write avg {}/s inst {}/s peak {}/s | ETA {}",
            format_rate(app.archive_live.write_avg_bps),
            format_rate(app.archive_live.write_inst_bps),
            format_rate(app.archive_live.peak_write_bps),
            format_duration(eta),
        )));
    } else if let Some(progress) = &app.extract_progress {
        lines.push(Line::from(format!(
            "source: {:?} | elapsed {} | blocks {}/{}",
            progress.source_kind,
            format_duration(progress.elapsed),
            progress.blocks_completed,
            progress.blocks_total,
        )));
        lines.push(Line::from(format!(
            "archive read {} | decoded {}",
            format_bytes(progress.archive_bytes_completed),
            format_bytes(progress.decoded_bytes_completed),
        )));
        lines.push(Line::from(format!(
            "read avg {}/s inst {}/s peak {}/s",
            format_rate(app.extract_live.read_avg_bps),
            format_rate(app.extract_live.read_inst_bps),
            format_rate(app.extract_live.peak_read_bps),
        )));
        lines.push(Line::from(format!(
            "decode avg {}/s inst {}/s peak {}/s",
            format_rate(app.extract_live.decode_avg_bps),
            format_rate(app.extract_live.decode_inst_bps),
            format_rate(app.extract_live.peak_decode_bps),
        )));
    } else {
        lines.push(Line::from("No active run telemetry"));
    }

    if let Some(report) = &app.archive_report {
        lines.push(Line::from(format!(
            "final archive: elapsed {} | in {} out {} | ratio {:.3}x | workers {}",
            format_duration(report.elapsed),
            format_bytes(report.input_bytes_total),
            format_bytes(report.output_bytes_total),
            report.output_input_ratio,
            report.workers.len(),
        )));
    }
    if let Some(report) = &app.extract_report {
        lines.push(Line::from(format!(
            "final extract: elapsed {} | archive {} output {} | ratio {:.3}x | workers {}",
            format_duration(report.elapsed),
            format_bytes(report.archive_bytes_total),
            format_bytes(report.output_bytes_total),
            report.output_archive_ratio,
            report.workers.len(),
        )));
    }

    lines
}

fn render_profile(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(9), Constraint::Min(5)])
        .split(area);

    let mut profile_rows = app
        .profile_aggregates
        .iter()
        .map(|(key, aggregate)| (key.clone(), aggregate.clone()))
        .collect::<Vec<_>>();
    profile_rows.sort_by_key(|(_, aggregate)| Reverse(aggregate.total_us));

    let rows = profile_rows
        .iter()
        .take(7)
        .map(|(key, aggregate)| {
            Row::new(vec![
                Cell::from(key.clone()),
                Cell::from(aggregate.count.to_string()),
                Cell::from(format!("{} us", aggregate.total_us)),
                Cell::from(format!("{} us", aggregate.max_us)),
                Cell::from(aggregate.last_result.clone()),
            ])
        })
        .collect::<Vec<_>>();

    let table = Table::new(
        rows,
        [
            Constraint::Percentage(43),
            Constraint::Length(7),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Length(10),
        ],
    )
    .header(
        Row::new(vec!["target::op", "count", "total", "max", "result"]).style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title("Profiling Aggregates"),
    );

    frame.render_widget(table, sections[0]);

    let recent_items = app
        .profile_events
        .iter()
        .rev()
        .take(10)
        .map(|event| {
            ListItem::new(format!(
                "{} {} {} {}us tags={}",
                event.target,
                event.op,
                event.result,
                event.elapsed_us,
                event.tags.join("|")
            ))
        })
        .collect::<Vec<_>>();

    let list = List::new(recent_items).block(
        Block::default()
            .borders(Borders::ALL)
            .title("Recent Profile Events"),
    );
    frame.render_widget(list, sections[1]);
}

fn render_logs(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let mut items = app
        .log_lines
        .iter()
        .rev()
        .take(26)
        .map(|line| ListItem::new(line.clone()))
        .collect::<Vec<_>>();

    if items.is_empty() {
        items.push(ListItem::new("No logs yet"));
    }

    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title("Logs / Run Messages"),
    );
    frame.render_widget(list, area);
}

fn render_browser_modal(frame: &mut Frame<'_>, app: &App) {
    let Some(browser) = &app.browser else {
        return;
    };

    let area = centered_rect(88, 80, frame.area());
    frame.render_widget(Clear, area);

    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(5), Constraint::Length(2)])
        .split(area);

    let mut items = Vec::new();
    for (idx, entry) in browser.entries().iter().enumerate() {
        let prefix = if entry.is_parent {
            "[..] "
        } else if entry.is_dir {
            "[D] "
        } else {
            "[F] "
        };

        let label = if entry.is_dir && !entry.is_parent {
            format!("{}{}/", prefix, entry.name)
        } else {
            format!("{}{}", prefix, entry.name)
        };

        let mut style = Style::default();
        if idx == browser.selected() {
            style = style.fg(Color::Black).bg(Color::Cyan);
        } else if entry.is_dir {
            style = style.fg(Color::LightBlue);
        }

        items.push(ListItem::new(label).style(style));
    }

    if items.is_empty() {
        items.push(ListItem::new("(empty directory)"));
    }

    let target = match browser.target {
        crate::browser::BrowseTarget::Input => "input",
        crate::browser::BrowseTarget::Output => "output",
    };

    let title = format!(
        "Browse {}: {} (hidden: {})",
        target,
        browser.current_dir().display(),
        browser.show_hidden()
    );
    let list = List::new(items).block(Block::default().borders(Borders::ALL).title(title));
    frame.render_widget(list, sections[0]);

    let hint = Paragraph::new(
        "Up/Down select | Enter open/select file | Left/Backspace parent | s select | d select dir | h toggle hidden | q/Esc close",
    )
    .block(Block::default().borders(Borders::ALL));
    frame.render_widget(hint, sections[1]);
}

fn styled_kv_line(label: &str, value: &str, app: &App, field: ActiveField) -> Line<'static> {
    let selected = app.active_field == field;
    let marker = if selected { "▶" } else { " " };
    let mut style = Style::default();
    if selected {
        style = style.fg(Color::Yellow).add_modifier(Modifier::BOLD);
    }

    let editing_suffix = if selected && app.editing {
        " (editing)"
    } else {
        ""
    };
    let value = if value.trim().is_empty() {
        "<empty>"
    } else {
        value
    };

    Line::from(vec![
        Span::styled(format!("{marker} {label}: "), style),
        Span::raw(value.to_string()),
        Span::raw(editing_suffix.to_string()),
    ])
}

fn centered_rect(percent_x: u16, percent_y: u16, rect: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(rect);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
