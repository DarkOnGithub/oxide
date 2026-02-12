use std::borrow::Cow;
use std::time::Duration;

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Borders, Cell, Clear, Gauge, List, ListItem, Paragraph, Row, Sparkline, Table, Wrap,
};

use crate::app::{ActiveField, App, BrowserAction, ProfileAggregate, RunAction, UiTab};
use crate::formatters::{format_bytes, format_duration, format_rate};
use crate::job::JobMode;

const COLOR_TEXT: Color = Color::White;
const COLOR_MUTED: Color = Color::DarkGray;
const COLOR_ACCENT: Color = Color::Cyan;
const COLOR_ACCENT_ALT: Color = Color::LightBlue;
const COLOR_INFO: Color = Color::LightCyan;
const COLOR_OK: Color = Color::Green;
const COLOR_WARN: Color = Color::Yellow;
const COLOR_DANGER: Color = Color::LightRed;

fn compression_label(algo: oxide_core::CompressionAlgo) -> &'static str {
    match algo {
        oxide_core::CompressionAlgo::Lz4 => "lz4",
        oxide_core::CompressionAlgo::Lzma => "lzma",
        oxide_core::CompressionAlgo::Deflate => "deflate",
    }
}

fn panel_block<'a, T>(title: T, border: Color) -> Block<'a>
where
    T: Into<Cow<'a, str>>,
{
    Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(border))
        .title(Span::styled(
            title,
            Style::default().fg(border).add_modifier(Modifier::BOLD),
        ))
}

fn utilization_color(utilization: f64) -> Color {
    if utilization >= 0.85 {
        COLOR_OK
    } else if utilization >= 0.5 {
        COLOR_WARN
    } else {
        COLOR_INFO
    }
}

fn latency_color(p95_us: u64) -> Color {
    if p95_us >= 1_000_000 {
        COLOR_DANGER
    } else if p95_us >= 250_000 {
        COLOR_WARN
    } else if p95_us > 0 {
        COLOR_OK
    } else {
        COLOR_MUTED
    }
}

fn format_latency_us(us: u64) -> String {
    format_latency_f64(us as f64)
}

fn format_latency_f64(us: f64) -> String {
    if !us.is_finite() || us <= 0.0 {
        return "0us".to_string();
    }

    if us < 1_000.0 {
        format!("{us:.0}us")
    } else if us < 1_000_000.0 {
        format!("{:.2}ms", us / 1_000.0)
    } else if us < 60_000_000.0 {
        format!("{:.2}s", us / 1_000_000.0)
    } else if us < 3_600_000_000.0 {
        format!("{:.2}m", us / 60_000_000.0)
    } else {
        format!("{:.2}h", us / 3_600_000_000.0)
    }
}

fn runtime_utilization_percent(app: &App) -> f64 {
    let Some(runtime) = app.current_runtime() else {
        return 0.0;
    };

    let total_uptime_us = runtime
        .workers
        .iter()
        .map(|worker| worker.uptime.as_micros())
        .sum::<u128>();
    if total_uptime_us == 0 {
        return 0.0;
    }

    let total_busy_us = runtime
        .workers
        .iter()
        .map(|worker| worker.busy.as_micros())
        .sum::<u128>();
    (total_busy_us as f64 / total_uptime_us as f64) * 100.0
}

fn top_stage_lines(app: &App, limit: usize) -> Vec<Line<'static>> {
    let mut stages = if let Some(report) = &app.archive_report {
        report
            .main_thread
            .stage_us
            .iter()
            .map(|(name, us)| (name.clone(), *us))
            .collect::<Vec<_>>()
    } else if let Some(report) = &app.extract_report {
        report
            .main_thread
            .stage_us
            .iter()
            .map(|(name, us)| (name.clone(), *us))
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    if stages.is_empty() {
        return vec![Line::from("stage timings appear after first completed run")];
    }

    let total = stages.iter().map(|(_, us)| *us).sum::<u64>().max(1);
    stages.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    stages
        .into_iter()
        .take(limit.max(1))
        .map(|(name, us)| {
            let pct = (us as f64 / total as f64) * 100.0;
            Line::from(format!(
                "{name:<20} {:>8} ({pct:5.1}%)",
                format_duration(Duration::from_micros(us))
            ))
        })
        .collect()
}

#[derive(Debug, Clone, Default)]
pub struct LayoutInfo {
    pub run_field_hits: Vec<(ActiveField, Rect)>,
    pub run_action_hits: Vec<(RunAction, Rect)>,
    pub logs_rect: Option<Rect>,
    pub profile_rect: Option<Rect>,
    pub browser: Option<BrowserLayout>,
}

impl LayoutInfo {
    pub fn run_field_at(&self, x: u16, y: u16) -> Option<ActiveField> {
        self.run_field_hits
            .iter()
            .find_map(|(field, rect)| rect_contains(*rect, x, y).then_some(*field))
    }

    pub fn run_action_at(&self, x: u16, y: u16) -> Option<RunAction> {
        self.run_action_hits
            .iter()
            .find_map(|(action, rect)| rect_contains(*rect, x, y).then_some(*action))
    }
}

#[derive(Debug, Clone, Default)]
pub struct BrowserLayout {
    pub list_rect: Rect,
    pub list_start_index: usize,
    pub action_hits: Vec<(BrowserAction, Rect)>,
}

impl BrowserLayout {
    pub fn action_at(&self, x: u16, y: u16) -> Option<BrowserAction> {
        self.action_hits
            .iter()
            .find_map(|(action, rect)| rect_contains(*rect, x, y).then_some(*action))
    }
}

pub fn draw(frame: &mut Frame<'_>, app: &App) -> LayoutInfo {
    let mut layout_info = LayoutInfo::default();

    let areas = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(6),
            Constraint::Length(15),
            Constraint::Min(8),
        ])
        .split(frame.area());

    render_header(frame, areas[0], app, &mut layout_info);
    render_run_tab(frame, areas[1], app, &mut layout_info);
    render_detail_pane(frame, areas[2], app, &mut layout_info);

    if app.browser.is_some() {
        render_browser_modal(frame, app, &mut layout_info);
    }

    layout_info
}

fn render_header(frame: &mut Frame<'_>, area: Rect, app: &App, _layout: &mut LayoutInfo) {
    let block = panel_block("Oxide TUI", COLOR_ACCENT_ALT);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let detail = Span::styled(
        format!("detail={}", app.active_tab.label()),
        Style::default()
            .fg(Color::Black)
            .bg(COLOR_ACCENT)
            .add_modifier(Modifier::BOLD),
    );
    let tabs = Paragraph::new(Line::from(vec![
        Span::styled("single dashboard ", Style::default().fg(COLOR_TEXT)),
        detail,
        Span::styled("  (m to cycle)", Style::default().fg(COLOR_MUTED)),
    ]));
    frame.render_widget(tabs, Rect { height: 1, ..inner });

    let status_line = Line::from(vec![
        Span::styled("status: ", Style::default().fg(COLOR_WARN)),
        Span::styled(&app.status_line, Style::default().fg(COLOR_TEXT)),
        Span::styled(
            if app.running {
                "  [RUNNING]"
            } else {
                "  [IDLE]"
            },
            Style::default()
                .fg(if app.running { COLOR_OK } else { COLOR_INFO })
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            if app.mouse_enabled {
                "  [MOUSE ON]"
            } else {
                "  [MOUSE OFF]"
            },
            Style::default().fg(COLOR_INFO),
        ),
    ]);
    let mode_label = match app.mode {
        JobMode::Archive => "archive",
        JobMode::Extract => "extract",
    };
    let context_line = Line::from(vec![
        Span::styled("mode=", Style::default().fg(COLOR_MUTED)),
        Span::styled(mode_label, Style::default().fg(COLOR_INFO)),
        Span::styled(" algo=", Style::default().fg(COLOR_MUTED)),
        Span::styled(
            compression_label(app.compression),
            Style::default().fg(COLOR_INFO),
        ),
        Span::styled(" workers=", Style::default().fg(COLOR_MUTED)),
        Span::styled(app.workers.trim(), Style::default().fg(COLOR_TEXT)),
        Span::styled(" block=", Style::default().fg(COLOR_MUTED)),
        Span::styled(app.block_size.trim(), Style::default().fg(COLOR_TEXT)),
        Span::styled(" prod=", Style::default().fg(COLOR_MUTED)),
        Span::styled(app.producer_threads.trim(), Style::default().fg(COLOR_TEXT)),
        Span::styled(" inflight=", Style::default().fg(COLOR_MUTED)),
        Span::styled(app.inflight_bytes.trim(), Style::default().fg(COLOR_TEXT)),
        Span::styled(" stats=", Style::default().fg(COLOR_MUTED)),
        Span::styled(
            format!("{}ms", app.stats_interval_ms.trim()),
            Style::default().fg(COLOR_TEXT),
        ),
        Span::styled(" | profile ", Style::default().fg(COLOR_MUTED)),
        Span::styled(
            &app.view.profile_target_filter,
            Style::default().fg(COLOR_INFO),
        ),
        Span::styled(" / ", Style::default().fg(COLOR_MUTED)),
        Span::styled(&app.view.profile_op_filter, Style::default().fg(COLOR_INFO)),
        Span::styled(" / ", Style::default().fg(COLOR_MUTED)),
        Span::styled(
            &app.view.profile_tag_filter,
            Style::default().fg(COLOR_INFO),
        ),
    ]);
    let hints = Line::from(vec![Span::styled(
        "keys: r run | b browse | e ephemeral | n null | m detail | Tab fields | Enter edit | p/t/o/g profile | w worker sort | +/- rows | ,/. window | q quit",
        Style::default().fg(COLOR_MUTED),
    )]);

    let status_area = Rect {
        x: inner.x,
        y: inner.y.saturating_add(1),
        width: inner.width,
        height: inner.height.saturating_sub(1),
    };
    let status = Paragraph::new(vec![status_line, context_line, hints]).wrap(Wrap { trim: true });
    frame.render_widget(status, status_area);
}

fn render_detail_pane(frame: &mut Frame<'_>, area: Rect, app: &App, layout: &mut LayoutInfo) {
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(6)])
        .split(area);

    let detail_header = Paragraph::new(Line::from(format!(
        "detail pane: {} (m/M cycle) | scroll: mouse wheel or Up/Down/PageUp/PageDown",
        app.active_tab.label()
    )))
    .block(panel_block("Detail", COLOR_ACCENT));
    frame.render_widget(detail_header, sections[0]);

    match app.active_tab {
        UiTab::Live => render_live_tab(frame, sections[1], app),
        UiTab::Profile => render_profile_tab(frame, sections[1], app, layout),
        UiTab::Logs => render_logs_tab(frame, sections[1], app, layout),
    }
}

fn render_run_tab(frame: &mut Frame<'_>, area: Rect, app: &App, layout: &mut LayoutInfo) {
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(16),
            Constraint::Length(3),
            Constraint::Min(3),
        ])
        .split(area);

    let config_and_snapshot = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(67), Constraint::Percentage(33)])
        .split(sections[0]);

    let block = panel_block("Run Config", COLOR_ACCENT_ALT);
    let inner = block.inner(config_and_snapshot[0]);
    frame.render_widget(block, config_and_snapshot[0]);

    let fields = [
        (
            ActiveField::Mode,
            match app.mode {
                JobMode::Archive => "archive",
                JobMode::Extract => "extract",
            }
            .to_string(),
        ),
        (ActiveField::Input, app.input_path.clone()),
        (ActiveField::Output, app.output_path.clone()),
        (
            ActiveField::Compression,
            compression_label(app.compression).to_string(),
        ),
        (ActiveField::Workers, app.workers.clone()),
        (ActiveField::BlockSize, app.block_size.clone()),
        (ActiveField::InflightBytes, app.inflight_bytes.clone()),
        (
            ActiveField::StreamReadBuffer,
            app.stream_read_buffer.clone(),
        ),
        (ActiveField::ProducerThreads, app.producer_threads.clone()),
        (
            ActiveField::DirectoryMmapThreshold,
            app.directory_mmap_threshold.clone(),
        ),
        (
            ActiveField::WriterQueueBlocks,
            app.writer_queue_blocks.clone(),
        ),
        (
            ActiveField::ResultWaitMs,
            format!("{} ms", app.result_wait_ms),
        ),
        (
            ActiveField::StatsInterval,
            format!("{} ms", app.stats_interval_ms),
        ),
    ];

    let mut lines = Vec::new();
    for (idx, (field, value)) in fields.iter().enumerate() {
        let y = inner.y.saturating_add(idx as u16);
        let rect = Rect {
            x: inner.x,
            y,
            width: inner.width,
            height: 1,
        };
        layout.run_field_hits.push((*field, rect));

        let selected = app.active_field == *field;
        let prefix = if selected { ">" } else { " " };
        let style = if selected {
            Style::default().fg(COLOR_WARN).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(COLOR_TEXT)
        };
        let shown = if value.trim().is_empty() {
            "<empty>"
        } else {
            value
        };
        lines.push(Line::from(vec![
            Span::styled(format!("{} {}: ", prefix, field.label()), style),
            Span::styled(shown, Style::default().fg(COLOR_INFO)),
            Span::styled(
                if selected && app.editing {
                    " (editing)"
                } else {
                    ""
                },
                Style::default().fg(COLOR_OK),
            ),
        ]));
    }

    frame.render_widget(
        Paragraph::new(lines).wrap(Wrap { trim: true }),
        Rect {
            x: inner.x,
            y: inner.y,
            width: inner.width,
            height: inner.height,
        },
    );

    let runtime_snapshot = Paragraph::new(build_run_snapshot_lines(app))
        .block(panel_block("Runtime Snapshot", COLOR_ACCENT))
        .wrap(Wrap { trim: true });
    frame.render_widget(runtime_snapshot, config_and_snapshot[1]);

    let action_block = panel_block("Actions", COLOR_ACCENT_ALT);
    let action_inner = action_block.inner(sections[1]);
    frame.render_widget(action_block, sections[1]);

    let actions = [
        RunAction::StartRun,
        RunAction::BrowseInput,
        RunAction::BrowseOutput,
        RunAction::EphemeralOutput,
        RunAction::NullOutput,
        RunAction::DefaultOutput,
        RunAction::Clear,
    ];
    let mut spans = Vec::new();
    let mut x = action_inner.x;
    for action in actions {
        let label = format!("[{}]", action.label());
        let width = label.chars().count() as u16;
        if x.saturating_add(width) > action_inner.x.saturating_add(action_inner.width) {
            break;
        }
        let rect = Rect {
            x,
            y: action_inner.y,
            width,
            height: 1,
        };
        layout.run_action_hits.push((action, rect));
        let chip_style = match action {
            RunAction::StartRun => Style::default().fg(Color::Black).bg(COLOR_OK),
            RunAction::BrowseInput | RunAction::BrowseOutput => {
                Style::default().fg(Color::Black).bg(COLOR_INFO)
            }
            RunAction::EphemeralOutput => Style::default().fg(Color::Black).bg(Color::Magenta),
            RunAction::NullOutput => Style::default().fg(Color::Black).bg(Color::Red),
            RunAction::DefaultOutput => Style::default().fg(Color::Black).bg(COLOR_ACCENT_ALT),
            RunAction::Clear => Style::default().fg(Color::Black).bg(COLOR_WARN),
        };
        spans.push(Span::styled(label, chip_style));
        spans.push(Span::raw(" "));
        x = x.saturating_add(width + 1);
    }

    frame.render_widget(Paragraph::new(Line::from(spans)), action_inner);

    let help = Paragraph::new(vec![
        Line::from("Dashboard controls: Tab/Shift+Tab field select, Enter edit/toggle, Left/Right enum cycle."),
        Line::from("Detail pane controls: m/M cycle Live/Profile/Logs, +/- rows, Up/Down scroll, p/t/o/g filters."),
        Line::from("Output shortcuts: e for ephemeral temp path, n for null sink (discard output)."),
        Line::from(format!(
            "profile sort={} window={}s | worker sort={} rows={}",
            app.view.profile_sort_mode.label(),
            app.view.profile_window_seconds,
            app.view.worker_sort_mode.label(),
            app.view.worker_rows_limit,
        )),
    ])
    .block(panel_block("Hints", COLOR_ACCENT))
    .wrap(Wrap { trim: true });
    frame.render_widget(help, sections[2]);
}

fn render_live_tab(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(5),
            Constraint::Length(5),
            Constraint::Min(8),
        ])
        .split(area);

    let (ratio, label) = if let Some(progress) = &app.archive_progress {
        let total = progress.input_bytes_total.max(1);
        let done = progress.input_bytes_completed.min(total);
        let fraction = done as f64 / total as f64;
        (
            fraction,
            format!(
                "archive {:6.2}% | blocks {}/{}",
                fraction * 100.0,
                progress.blocks_completed,
                progress.blocks_total
            ),
        )
    } else if let Some(progress) = &app.extract_progress {
        let total = progress.blocks_total.max(1);
        let done = progress.blocks_completed.min(total);
        let fraction = done as f64 / total as f64;
        (
            fraction,
            format!(
                "extract {:6.2}% | blocks {}/{}",
                fraction * 100.0,
                progress.blocks_completed,
                progress.blocks_total
            ),
        )
    } else {
        (0.0, "awaiting telemetry".to_string())
    };

    frame.render_widget(
        Gauge::default()
            .block(panel_block("Live Progress", COLOR_OK))
            .gauge_style(Style::default().fg(COLOR_OK))
            .ratio(ratio.clamp(0.0, 1.0))
            .label(label),
        sections[0],
    );

    let metric_cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(34),
            Constraint::Percentage(33),
            Constraint::Percentage(33),
        ])
        .split(sections[1]);
    let metric_cards = build_live_metric_cards(app);
    frame.render_widget(
        Paragraph::new(metric_cards.0)
            .block(panel_block("Throughput", COLOR_INFO))
            .wrap(Wrap { trim: true }),
        metric_cols[0],
    );
    frame.render_widget(
        Paragraph::new(metric_cards.1)
            .block(panel_block("Volume + Ratios", COLOR_ACCENT))
            .wrap(Wrap { trim: true }),
        metric_cols[1],
    );
    frame.render_widget(
        Paragraph::new(metric_cards.2)
            .block(panel_block("Workers + Runtime", COLOR_WARN))
            .wrap(Wrap { trim: true }),
        metric_cols[2],
    );

    let history = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(sections[2]);

    frame.render_widget(
        Sparkline::default()
            .block(panel_block("Read Rate (inst B/s)", COLOR_INFO))
            .style(Style::default().fg(COLOR_INFO))
            .data(&app.read_rate_history),
        history[0],
    );

    let (right_title, right_data, right_color) = if app.extract_progress.is_some() {
        (
            "Decode Rate (inst B/s)",
            &app.decode_rate_history,
            Color::Magenta,
        )
    } else {
        (
            "Write Rate (inst B/s)",
            &app.write_rate_history,
            COLOR_ACCENT_ALT,
        )
    };

    frame.render_widget(
        Sparkline::default()
            .block(panel_block(right_title, right_color))
            .style(Style::default().fg(right_color))
            .data(right_data),
        history[1],
    );

    let bottom = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(62), Constraint::Percentage(38)])
        .split(sections[3]);
    render_worker_table(frame, bottom[0], app);
    render_stage_panel(frame, bottom[1], app);
}

fn render_stage_panel(frame: &mut Frame<'_>, area: Rect, app: &App) {
    frame.render_widget(
        Paragraph::new(top_stage_lines(app, 8))
            .block(panel_block("Top Stage Bottlenecks", COLOR_DANGER))
            .wrap(Wrap { trim: true }),
        area,
    );
}

fn render_worker_table(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let rows = app.sorted_worker_rows();
    let total_tasks = rows
        .iter()
        .map(|worker| worker.tasks_completed)
        .sum::<usize>();
    let avg_util = if rows.is_empty() {
        0.0
    } else {
        (rows.iter().map(|worker| worker.utilization).sum::<f64>() / rows.len() as f64) * 100.0
    };
    let shown = rows
        .iter()
        .take(app.view.worker_rows_limit)
        .cloned()
        .collect::<Vec<_>>();

    let body = shown
        .iter()
        .map(|worker| {
            let util_color = utilization_color(worker.utilization);
            Row::new(vec![
                Cell::from(format!("{}", worker.worker_id)),
                Cell::from(format!("{}", worker.tasks_completed)),
                Cell::from(format_duration(worker.busy)),
                Cell::from(format_duration(worker.idle)),
                Cell::from(format!("{:.2}%", worker.utilization * 100.0))
                    .style(Style::default().fg(util_color).add_modifier(Modifier::BOLD)),
            ])
            .style(Style::default().fg(COLOR_TEXT))
        })
        .collect::<Vec<_>>();

    let title = format!(
        "Workers (sort={} rows={} avg util={:.1}% tasks={})",
        app.view.worker_sort_mode.label(),
        app.view.worker_rows_limit,
        avg_util,
        total_tasks
    );

    frame.render_widget(
        Table::new(
            body,
            [
                Constraint::Length(4),
                Constraint::Length(8),
                Constraint::Length(10),
                Constraint::Length(10),
                Constraint::Length(9),
            ],
        )
        .header(
            Row::new(vec!["id", "tasks", "busy", "idle", "util"])
                .style(Style::default().fg(COLOR_WARN).add_modifier(Modifier::BOLD)),
        )
        .block(panel_block(&title, COLOR_WARN)),
        area,
    );
}

fn build_run_snapshot_lines(app: &App) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    if let Some(progress) = &app.archive_progress {
        let done = progress
            .input_bytes_completed
            .min(progress.input_bytes_total);
        lines.push(Line::from(format!(
            "active: archive {} / {}",
            format_bytes(done),
            format_bytes(progress.input_bytes_total)
        )));
        lines.push(Line::from(format!(
            "blocks: {}/{} (pending {})",
            progress.blocks_completed, progress.blocks_total, progress.blocks_pending
        )));
        lines.push(Line::from(format!(
            "elapsed: {}",
            format_duration(progress.elapsed)
        )));
    } else if let Some(progress) = &app.extract_progress {
        lines.push(Line::from(format!(
            "active: extract {} decoded",
            format_bytes(progress.decoded_bytes_completed)
        )));
        lines.push(Line::from(format!(
            "blocks: {}/{} (pending {})",
            progress.blocks_completed, progress.blocks_total, progress.runtime.pending
        )));
        lines.push(Line::from(format!(
            "elapsed: {}",
            format_duration(progress.elapsed)
        )));
    } else {
        lines.push(Line::from("active: none"));
    }

    if let Some(runtime) = app.current_runtime() {
        lines.push(Line::from(format!(
            "pool: submitted {} completed {} pending {}",
            runtime.submitted, runtime.completed, runtime.pending
        )));
        lines.push(Line::from(format!(
            "workers: {} util {:.1}%",
            runtime.workers.len(),
            runtime_utilization_percent(app)
        )));
    }

    if let Some(report) = &app.archive_report {
        lines.push(Line::from(format!(
            "last archive: {} out/in {:.3}x",
            format_duration(report.elapsed),
            report.output_input_ratio
        )));
    } else if let Some(report) = &app.extract_report {
        lines.push(Line::from(format!(
            "last extract: {} out/archive {:.3}x",
            format_duration(report.elapsed),
            report.output_archive_ratio
        )));
    }

    lines
}

fn build_live_metric_cards(
    app: &App,
) -> (Vec<Line<'static>>, Vec<Line<'static>>, Vec<Line<'static>>) {
    if let Some(progress) = &app.archive_progress {
        let done = progress
            .input_bytes_completed
            .min(progress.input_bytes_total);
        let remaining = progress.input_bytes_total.saturating_sub(done);
        let eta = if app.archive_live.read_avg_bps > 0.0 {
            Duration::from_secs_f64(remaining as f64 / app.archive_live.read_avg_bps)
        } else {
            Duration::ZERO
        };

        let throughput = vec![
            Line::from(format!(
                "read avg  {}/s",
                format_rate(app.archive_live.read_avg_bps)
            )),
            Line::from(format!(
                "read inst {}/s",
                format_rate(app.archive_live.read_inst_bps)
            )),
            Line::from(format!(
                "write avg {}/s",
                format_rate(app.archive_live.write_avg_bps)
            )),
            Line::from(format!(
                "write inst {}/s",
                format_rate(app.archive_live.write_inst_bps)
            )),
            Line::from(format!(
                "prep avg  {}/s",
                format_rate(app.archive_live.preprocessing_avg_bps)
            )),
            Line::from(format!(
                "comp avg  {}/s",
                format_rate(app.archive_live.compression_avg_bps)
            )),
            Line::from(format!(
                "prep+comp {}/s",
                format_rate(app.archive_live.preprocessing_compression_avg_bps)
            )),
        ];
        let volume = vec![
            Line::from(format!(
                "input  {} / {}",
                format_bytes(done),
                format_bytes(progress.input_bytes_total)
            )),
            Line::from(format!(
                "output {}",
                format_bytes(progress.output_bytes_completed)
            )),
            Line::from(format!(
                "out/in {:.3}x  comp {:.3}x",
                app.archive_live.output_input_ratio, app.archive_live.compression_ratio
            )),
            Line::from(format!("ETA {}", format_duration(eta))),
        ];
        let workers = vec![
            Line::from(format!(
                "submitted {} completed {}",
                progress.runtime.submitted, progress.runtime.completed
            )),
            Line::from(format!("pending {}", progress.runtime.pending)),
            Line::from(format!(
                "worker util {:.1}%",
                runtime_utilization_percent(app)
            )),
            Line::from(format!("workers {}", progress.runtime.workers.len())),
        ];
        return (throughput, volume, workers);
    }

    if let Some(progress) = &app.extract_progress {
        let throughput = vec![
            Line::from(format!(
                "read avg   {}/s",
                format_rate(app.extract_live.read_avg_bps)
            )),
            Line::from(format!(
                "read inst  {}/s",
                format_rate(app.extract_live.read_inst_bps)
            )),
            Line::from(format!(
                "decode avg {}/s",
                format_rate(app.extract_live.decode_avg_bps)
            )),
            Line::from(format!(
                "decode inst {}/s",
                format_rate(app.extract_live.decode_inst_bps)
            )),
        ];
        let volume = vec![
            Line::from(format!(
                "archive {}",
                format_bytes(progress.archive_bytes_completed)
            )),
            Line::from(format!(
                "decoded {}",
                format_bytes(progress.decoded_bytes_completed)
            )),
            Line::from(format!(
                "decode/archive {:.3}x",
                app.extract_live.decode_archive_ratio
            )),
            Line::from(format!("elapsed {}", format_duration(progress.elapsed))),
        ];
        let workers = vec![
            Line::from(format!(
                "submitted {} completed {}",
                progress.runtime.submitted, progress.runtime.completed
            )),
            Line::from(format!("pending {}", progress.runtime.pending)),
            Line::from(format!(
                "worker util {:.1}%",
                runtime_utilization_percent(app)
            )),
            Line::from(format!("workers {}", progress.runtime.workers.len())),
        ];
        return (throughput, volume, workers);
    }

    let idle = vec![
        Line::from("no active telemetry stream"),
        Line::from("press r to start a run"),
    ];
    let mut summary = vec![Line::from("latest report: none")];
    if let Some(report) = &app.archive_report {
        summary = vec![
            Line::from(format!(
                "latest archive {}",
                format_duration(report.elapsed)
            )),
            Line::from(format!("out/in {:.3}x", report.output_input_ratio)),
        ];
    } else if let Some(report) = &app.extract_report {
        summary = vec![
            Line::from(format!(
                "latest extract {}",
                format_duration(report.elapsed)
            )),
            Line::from(format!("out/archive {:.3}x", report.output_archive_ratio)),
        ];
    }
    (idle.clone(), summary, idle)
}

fn render_profile_tab(frame: &mut Frame<'_>, area: Rect, app: &App, layout: &mut LayoutInfo) {
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(3),
            Constraint::Min(8),
        ])
        .split(area);

    let rows = app.filtered_sorted_profile_rows();
    let total_events = rows.iter().map(|row| row.count).sum::<u64>();
    let total_us = rows.iter().map(|row| row.total_us).sum::<u64>();
    let hottest = rows.first().map(|row| row.key.as_str()).unwrap_or("n/a");
    let summary = Paragraph::new(Line::from(format!(
        "rows={} events={} total={} hottest={}",
        rows.len(),
        total_events,
        format_duration(Duration::from_micros(total_us)),
        hottest
    )))
    .block(panel_block("Profile Summary", COLOR_ACCENT_ALT));
    frame.render_widget(summary, sections[0]);

    let controls = Paragraph::new(vec![
        Line::from(format!(
            "sort={} target={} op={} tag={} window={}s rows={} (p/t/o/g to adjust)",
            app.view.profile_sort_mode.label(),
            app.view.profile_target_filter,
            app.view.profile_op_filter,
            app.view.profile_tag_filter,
            app.view.profile_window_seconds,
            app.view.profile_rows_limit,
        )),
        Line::from("scroll: mouse wheel or Up/Down/PageUp/PageDown"),
    ])
    .block(panel_block("Profile Controls", COLOR_INFO))
    .wrap(Wrap { trim: true });
    frame.render_widget(controls, sections[1]);

    let content = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(64), Constraint::Percentage(36)])
        .split(sections[2]);

    layout.profile_rect = Some(content[0]);

    render_profile_table(frame, content[0], app);
    render_profile_events(frame, content[1], app);
}

fn render_profile_table(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let rows = app.filtered_sorted_profile_rows();
    let start = app.profile_scroll_offset.min(rows.len());
    let end = (start + app.view.profile_rows_limit).min(rows.len());
    let shown = &rows[start..end];

    let body = shown.iter().map(profile_row).collect::<Vec<_>>();

    let title = format!("Aggregates (showing {}..{} / {})", start, end, rows.len());

    frame.render_widget(
        Table::new(
            body,
            [
                Constraint::Percentage(28),
                Constraint::Length(7),
                Constraint::Length(10),
                Constraint::Length(10),
                Constraint::Length(10),
                Constraint::Length(10),
                Constraint::Length(10),
                Constraint::Length(9),
            ],
        )
        .header(
            Row::new(vec![
                "target::op",
                "count",
                "total",
                "avg",
                "min",
                "max",
                "p95",
                "ev/s",
            ])
            .style(Style::default().fg(COLOR_WARN).add_modifier(Modifier::BOLD)),
        )
        .block(panel_block(&title, COLOR_ACCENT_ALT)),
        area,
    );
}

fn profile_row(aggregate: &ProfileAggregate) -> Row<'static> {
    let row_color = latency_color(aggregate.p95_us);
    Row::new(vec![
        Cell::from(aggregate.key.clone()).style(Style::default().fg(COLOR_TEXT)),
        Cell::from(aggregate.count.to_string()).style(Style::default().fg(COLOR_INFO)),
        Cell::from(format_latency_us(aggregate.total_us)).style(Style::default().fg(COLOR_INFO)),
        Cell::from(format_latency_f64(aggregate.avg_us)).style(Style::default().fg(row_color)),
        Cell::from(format_latency_us(aggregate.min_display()))
            .style(Style::default().fg(row_color)),
        Cell::from(format_latency_us(aggregate.max_us)).style(Style::default().fg(row_color)),
        Cell::from(format_latency_us(aggregate.p95_us))
            .style(Style::default().fg(row_color).add_modifier(Modifier::BOLD)),
        Cell::from(format!("{:.2}", aggregate.events_per_sec))
            .style(Style::default().fg(COLOR_ACCENT)),
    ])
}

fn render_profile_events(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let items = app
        .profile_events
        .iter()
        .rev()
        .take(14)
        .map(|event| {
            ListItem::new(format!(
                "{} {} {} {}us tags={} msg={}",
                event.target,
                event.op,
                event.result,
                event.elapsed_us,
                event.tags.join("|"),
                event.message,
            ))
        })
        .collect::<Vec<_>>();

    frame.render_widget(
        List::new(items).block(panel_block("Recent Events", COLOR_WARN)),
        area,
    );
}

fn render_logs_tab(frame: &mut Frame<'_>, area: Rect, app: &App, layout: &mut LayoutInfo) {
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(4)])
        .split(area);
    layout.logs_rect = Some(sections[1]);

    let log_header = Paragraph::new(Line::from(format!(
        "entries={} offset={} mode={} running={}",
        app.log_lines.len(),
        app.logs_scroll_offset,
        match app.mode {
            JobMode::Archive => "archive",
            JobMode::Extract => "extract",
        },
        app.running
    )))
    .block(panel_block("Log Context", COLOR_ACCENT_ALT));
    frame.render_widget(log_header, sections[0]);

    let visible_rows = sections[1].height.saturating_sub(2) as usize;
    let items = app
        .log_lines
        .iter()
        .rev()
        .skip(app.logs_scroll_offset)
        .take(visible_rows.max(1))
        .map(|line| ListItem::new(line.clone()))
        .collect::<Vec<_>>();

    let list = if items.is_empty() {
        List::new(vec![ListItem::new("No logs yet")])
    } else {
        List::new(items)
    };

    let title = format!(
        "Logs (offset {} / {})",
        app.logs_scroll_offset,
        app.log_lines.len()
    );
    frame.render_widget(list.block(panel_block(&title, COLOR_ACCENT)), sections[1]);
}

fn render_browser_modal(frame: &mut Frame<'_>, app: &App, layout: &mut LayoutInfo) {
    let Some(browser) = &app.browser else {
        return;
    };

    let area = centered_rect(88, 84, frame.area());
    frame.render_widget(Clear, area);

    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(5), Constraint::Length(4)])
        .split(area);

    let list_block = panel_block(
        format!(
            "Browse {}: {} (hidden: {})",
            match browser.target {
                crate::browser::BrowseTarget::Input => "input",
                crate::browser::BrowseTarget::Output => "output",
            },
            browser.current_dir().display(),
            browser.show_hidden(),
        ),
        COLOR_INFO,
    );
    let list_inner = list_block.inner(sections[0]);

    let visible = list_inner.height as usize;
    let selected = browser.selected();
    let start = if selected >= visible {
        selected - visible + 1
    } else {
        0
    };

    let mut items = Vec::new();
    for (offset, entry) in browser
        .entries()
        .iter()
        .enumerate()
        .skip(start)
        .take(visible.max(1))
    {
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

        let style = if offset == browser.selected() {
            Style::default().fg(Color::Black).bg(COLOR_ACCENT)
        } else if entry.is_dir {
            Style::default().fg(COLOR_ACCENT_ALT)
        } else {
            Style::default().fg(COLOR_TEXT)
        };

        items.push(ListItem::new(label).style(style));
    }

    if items.is_empty() {
        items.push(ListItem::new("(empty directory)"));
    }

    frame.render_widget(List::new(items).block(list_block), sections[0]);

    let controls_block = panel_block("Browser Actions", COLOR_ACCENT_ALT);
    let controls_inner = controls_block.inner(sections[1]);
    frame.render_widget(controls_block, sections[1]);

    let actions = [
        BrowserAction::OpenOrSelect,
        BrowserAction::SelectDir,
        BrowserAction::Parent,
        BrowserAction::ToggleHidden,
        BrowserAction::Refresh,
        BrowserAction::Close,
    ];

    let mut spans = Vec::new();
    let mut action_hits = Vec::new();
    let mut x = controls_inner.x;
    for action in actions {
        let text = format!("[{}]", action.label());
        let width = text.chars().count() as u16;
        if x.saturating_add(width) > controls_inner.x.saturating_add(controls_inner.width) {
            break;
        }
        let rect = Rect {
            x,
            y: controls_inner.y,
            width,
            height: 1,
        };
        action_hits.push((action, rect));
        spans.push(Span::styled(
            text,
            Style::default().fg(Color::Black).bg(COLOR_ACCENT_ALT),
        ));
        spans.push(Span::raw(" "));
        x = x.saturating_add(width + 1);
    }

    frame.render_widget(
        Paragraph::new(vec![
            Line::from(spans),
            Line::from("mouse: click row to select, wheel to move, buttons to apply"),
        ]),
        controls_inner,
    );

    layout.browser = Some(BrowserLayout {
        list_rect: list_inner,
        list_start_index: start,
        action_hits,
    });
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

fn rect_contains(rect: Rect, x: u16, y: u16) -> bool {
    x >= rect.x
        && y >= rect.y
        && x < rect.x.saturating_add(rect.width)
        && y < rect.y.saturating_add(rect.height)
}
