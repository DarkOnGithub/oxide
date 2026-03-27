use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use oxide_core::telemetry::{HistogramSnapshot, TelemetrySnapshot};
use oxide_core::{
    ArchiveReport, ArchiveSourceKind, BufferPool, CompressionAlgo, ExtractReport, ReportValue,
    ThreadReport, WorkerReport,
};

use crate::ui::{
    StreamTarget, Tone, format_bytes, format_bytes_f64, format_duration, format_duration_compact,
    format_rate, paint, progress_bar,
};

const CHART_WIDTH: usize = 28;

pub struct ArchiveReportSummary<'a> {
    pub input: &'a Path,
    pub output: &'a Path,
    pub profile_name: &'a str,
    pub compression: CompressionAlgo,
    pub compression_level: Option<i32>,
    pub compression_extreme: bool,
    pub report: &'a ArchiveReport,
    pub peak_read_bps: f64,
    pub peak_write_bps: f64,
    pub buffer_pool: &'a BufferPool,
    pub telemetry_details: bool,
}

pub struct ExtractReportSummary<'a> {
    pub archive_path: &'a Path,
    pub output_path: &'a Path,
    pub report: &'a ExtractReport,
    pub telemetry_details: bool,
}

pub fn print_archive_report_summary(summary: ArchiveReportSummary<'_>) {
    let ArchiveReportSummary {
        input,
        output,
        profile_name,
        compression,
        compression_level,
        compression_extreme,
        report,
        peak_read_bps,
        peak_write_bps,
        buffer_pool,
        telemetry_details,
    } = summary;
    let avg_block = if report.blocks_total > 0 {
        report.input_bytes_total / report.blocks_total as u64
    } else {
        0
    };

    print_report_title("Archive complete");
    print_key_value_table(
        "Overview",
        &[
            (
                "Source".to_string(),
                format!(
                    "{} ({})",
                    input.display(),
                    source_kind_label(report.source_kind)
                ),
            ),
            ("Archive".to_string(), output.display().to_string()),
            (
                "Profile".to_string(),
                format!(
                    "{} | {}",
                    profile_name,
                    compression_label(compression, compression_level, compression_extreme)
                ),
            ),
            ("Elapsed".to_string(), format_duration(report.elapsed)),
            (
                "Payload".to_string(),
                format!(
                    "{} -> {} | {} | ratio {:.3}x",
                    format_bytes(report.input_bytes_total),
                    format_bytes(report.output_bytes_total),
                    format_delta(report.input_bytes_total, report.output_bytes_total),
                    report.output_input_ratio,
                ),
            ),
            (
                "Blocks".to_string(),
                format!(
                    "{} total | avg {}",
                    report.blocks_total,
                    format_bytes(avg_block)
                ),
            ),
        ],
        Tone::Success,
    );

    let mut throughput_rows = vec![
        vec![
            "read avg".to_string(),
            format_rate_per_second(report.read_avg_bps),
        ],
        vec![
            "read peak".to_string(),
            format_rate_per_second(peak_read_bps),
        ],
        vec![
            "write avg".to_string(),
            format_rate_per_second(report.write_avg_bps),
        ],
        vec![
            "write peak".to_string(),
            format_rate_per_second(peak_write_bps),
        ],
    ];
    push_optional_rate_row(
        &mut throughput_rows,
        "compress cpu avg",
        extension_f64(&report.extensions, "throughput.compression_avg_bps"),
    );
    push_optional_rate_row(
        &mut throughput_rows,
        "compress wall avg",
        extension_f64(&report.extensions, "throughput.compression_wall_avg_bps"),
    );
    print_table(
        "Throughput table",
        &["Metric", "Rate"],
        &throughput_rows,
        Tone::Accent,
    );

    if telemetry_details {
        let mut throughput_chart = vec![
            (
                "read avg".to_string(),
                report.read_avg_bps,
                format_rate_per_second(report.read_avg_bps),
            ),
            (
                "write avg".to_string(),
                report.write_avg_bps,
                format_rate_per_second(report.write_avg_bps),
            ),
        ];
        if let Some(value) =
            extension_f64(&report.extensions, "throughput.compression_wall_avg_bps")
        {
            throughput_chart.push((
                "compress wall avg".to_string(),
                value,
                format_rate_per_second(value),
            ));
        }
        print_bar_chart("Throughput graph", &throughput_chart, Tone::Accent);

        let mut runtime_rows = vec![
            ("Scheduler".to_string(), scheduler_summary(&report.workers)),
            ("Buffer pool".to_string(), {
                let pool = buffer_pool.metrics();
                format!(
                    "created {} | recycled {} | dropped {}",
                    pool.created, pool.recycled, pool.dropped
                )
            }),
        ];
        push_optional_string_row(
            &mut runtime_rows,
            "Effective cores",
            extension_f64(&report.extensions, "runtime.effective_cores")
                .map(|value| format!("{value:.2}")),
        );
        push_optional_string_row(
            &mut runtime_rows,
            "Compress busy",
            extension_u64(&report.extensions, "runtime.compress_busy_us")
                .map(|value| format_duration(Duration::from_micros(value))),
        );
        push_optional_string_row(
            &mut runtime_rows,
            "Compression busy",
            extension_u64(&report.extensions, "runtime.compression_busy_us")
                .map(|value| format_duration(Duration::from_micros(value))),
        );
        push_optional_string_row(
            &mut runtime_rows,
            "LZMA dict",
            extension_u64(&report.extensions, "compression.lzma_dictionary_size").map(format_bytes),
        );
        push_optional_string_row(
            &mut runtime_rows,
            "Max inflight blocks",
            extension_u64(&report.extensions, "pipeline.max_inflight_blocks")
                .map(|value| value.to_string()),
        );
        push_optional_string_row(
            &mut runtime_rows,
            "Inflight / worker",
            extension_u64(
                &report.extensions,
                "pipeline.max_inflight_blocks_per_worker",
            )
            .map(|value| value.to_string()),
        );
        push_optional_string_row(
            &mut runtime_rows,
            "Configured inflight",
            extension_u64(&report.extensions, "pipeline.configured_inflight_bytes")
                .map(format_bytes),
        );
        push_optional_string_row(
            &mut runtime_rows,
            "Max inflight bytes",
            extension_u64(&report.extensions, "pipeline.max_inflight_bytes").map(format_bytes),
        );
        push_optional_string_row(
            &mut runtime_rows,
            "Writer queue",
            extension_u64(&report.extensions, "pipeline.writer_queue_capacity")
                .map(|value| value.to_string()),
        );
        push_optional_string_row(
            &mut runtime_rows,
            "Reorder limit",
            extension_u64(&report.extensions, "pipeline.reorder_pending_limit")
                .map(|value| value.to_string()),
        );
        push_optional_string_row(
            &mut runtime_rows,
            "Reorder peak",
            extension_u64(&report.extensions, "pipeline.pending_write_peak")
                .map(|value| value.to_string()),
        );
        push_optional_string_row(
            &mut runtime_rows,
            "Stages",
            thread_stage_summary(
                &report.main_thread,
                &[
                    ("discovery", "discovery"),
                    ("format_probe", "probe"),
                    ("producer_read", "read"),
                    ("producer_submit_blocked", "batch queue"),
                    ("submit_wait", "submit wait"),
                    ("result_wait", "result wait"),
                    ("writer_enqueue_blocked", "writer enqueue blocked"),
                    ("writer", "writer"),
                ],
            ),
        );
        print_key_value_table("Runtime", &runtime_rows, Tone::Info);
    }

    print_worker_table(&report.workers);
    print_telemetry_summary(report.telemetry.as_ref(), telemetry_details);
}

pub fn print_extract_report_summary(summary: ExtractReportSummary<'_>) {
    let ExtractReportSummary {
        archive_path,
        output_path,
        report,
        telemetry_details,
    } = summary;

    print_report_title("Extract complete");
    print_key_value_table(
        "Overview",
        &[
            ("Archive".to_string(), archive_path.display().to_string()),
            (
                "Output".to_string(),
                format!(
                    "{} ({})",
                    output_path.display(),
                    source_kind_label(report.source_kind)
                ),
            ),
            ("Elapsed".to_string(), format_duration(report.elapsed)),
            (
                "Payload".to_string(),
                format!(
                    "{} -> {} | {} | ratio {:.3}x",
                    format_bytes(report.archive_bytes_total),
                    format_bytes(report.output_bytes_total),
                    format_delta(report.archive_bytes_total, report.output_bytes_total),
                    report.output_archive_ratio,
                ),
            ),
            (
                "Decoded".to_string(),
                format_bytes(report.decoded_bytes_total),
            ),
            ("Blocks".to_string(), report.blocks_total.to_string()),
        ],
        Tone::Success,
    );

    let throughput_rows = vec![
        vec![
            "read avg".to_string(),
            format_rate_per_second(report.read_avg_bps),
        ],
        vec![
            "decode avg".to_string(),
            format_rate_per_second(report.decode_avg_bps),
        ],
        vec![
            "output avg".to_string(),
            format_rate_per_second(report.output_avg_bps),
        ],
    ];
    print_table(
        "Throughput table",
        &["Metric", "Rate"],
        &throughput_rows,
        Tone::Accent,
    );

    if telemetry_details {
        print_bar_chart(
            "Throughput graph",
            &[
                (
                    "read avg".to_string(),
                    report.read_avg_bps,
                    format_rate_per_second(report.read_avg_bps),
                ),
                (
                    "decode avg".to_string(),
                    report.decode_avg_bps,
                    format_rate_per_second(report.decode_avg_bps),
                ),
                (
                    "output avg".to_string(),
                    report.output_avg_bps,
                    format_rate_per_second(report.output_avg_bps),
                ),
            ],
            Tone::Accent,
        );

        let mut runtime_rows = vec![("Scheduler".to_string(), scheduler_summary(&report.workers))];
        push_optional_string_row(
            &mut runtime_rows,
            "Stages",
            thread_stage_summary(
                &report.main_thread,
                &[
                    ("archive_read", "archive read"),
                    ("decode_submit", "decode submit"),
                    ("decode_wait", "decode wait"),
                    ("writer_enqueue_blocked", "writer enqueue blocked"),
                    ("merge", "merge"),
                    ("directory_decode", "directory decode"),
                    ("output_prepare_directories", "prepare output dirs"),
                    ("output_write", "output write"),
                    ("output_create", "output create"),
                    ("output_create_directories", "create dirs"),
                    ("output_create_files", "create files"),
                    ("output_data", "output data"),
                    ("output_flush", "output flush"),
                    ("output_metadata", "output metadata"),
                    ("output_metadata_files", "metadata files"),
                    ("output_metadata_directories", "metadata dirs"),
                ],
            ),
        );
        push_optional_string_row(
            &mut runtime_rows,
            "Effective cores",
            extension_f64(&report.extensions, "runtime.effective_cores")
                .map(|value| format!("{value:.2}")),
        );
        push_optional_string_row(
            &mut runtime_rows,
            "Decode busy",
            extension_u64(&report.extensions, "runtime.decode_busy_us")
                .map(|value| format_duration(Duration::from_micros(value))),
        );
        push_optional_string_row(
            &mut runtime_rows,
            "Decode queue peak",
            extension_u64(&report.extensions, "pipeline.decode_task_queue_peak")
                .map(|value| value.to_string()),
        );
        push_optional_string_row(
            &mut runtime_rows,
            "Decode result queue peak",
            extension_u64(&report.extensions, "pipeline.decode_result_queue_peak")
                .map(|value| value.to_string()),
        );
        push_optional_string_row(
            &mut runtime_rows,
            "Ordered write queue peak",
            extension_u64(&report.extensions, "pipeline.ordered_write_queue_peak")
                .map(|value| value.to_string()),
        );
        push_optional_string_row(
            &mut runtime_rows,
            "Reorder limit",
            extension_u64(&report.extensions, "pipeline.reorder_pending_limit")
                .map(|value| value.to_string()),
        );
        push_optional_string_row(
            &mut runtime_rows,
            "Reorder peak",
            extension_u64(&report.extensions, "pipeline.reorder_pending_peak")
                .map(|value| value.to_string()),
        );
        push_optional_string_row(
            &mut runtime_rows,
            "Reorder bytes peak",
            extension_u64(&report.extensions, "pipeline.reorder_pending_bytes_peak")
                .map(format_bytes),
        );
        print_key_value_table("Runtime", &runtime_rows, Tone::Info);
    }

    print_worker_table(&report.workers);
    print_telemetry_summary(report.telemetry.as_ref(), telemetry_details);
}

fn print_worker_table(workers: &[WorkerReport]) {
    let rows = workers
        .iter()
        .map(|worker| {
            vec![
                format!("{:02}", worker.worker_id),
                worker.tasks_completed.to_string(),
                format!("{:.2}%", worker.utilization * 100.0),
                format_duration(worker.busy),
                format_duration(worker.idle),
                format_duration(worker.uptime),
            ]
        })
        .collect::<Vec<_>>();
    print_table(
        "Worker table",
        &["ID", "Tasks", "Util", "Busy", "Idle", "Uptime"],
        &rows,
        Tone::Info,
    );
}

fn print_telemetry_summary(snapshot: Option<&TelemetrySnapshot>, telemetry_details: bool) {
    let Some(snapshot) = snapshot else {
        return;
    };
    if snapshot.counters.is_empty() && snapshot.gauges.is_empty() && snapshot.histograms.is_empty()
    {
        return;
    }

    if !telemetry_details {
        return;
    }

    print_key_value_table(
        "Telemetry",
        &[(
            "Metrics".to_string(),
            format!(
                "{} counters | {} gauges | {} histograms",
                snapshot.counters.len(),
                snapshot.gauges.len(),
                snapshot.histograms.len(),
            ),
        )],
        Tone::Warning,
    );

    print_table(
        "Telemetry hotspots",
        &["Metric", "Value"],
        &rows_to_table(telemetry_hotspot_rows(snapshot)),
        Tone::Warning,
    );
    print_table(
        "Telemetry counters",
        &["Metric", "Value"],
        &rows_to_table(telemetry_scalar_rows(&snapshot.counters)),
        Tone::Warning,
    );
    print_table(
        "Telemetry gauges",
        &["Metric", "Value"],
        &rows_to_table(telemetry_scalar_rows(&snapshot.gauges)),
        Tone::Warning,
    );
    print_table(
        "Telemetry histograms",
        &["Metric", "Value"],
        &rows_to_table(telemetry_histogram_rows(&snapshot.histograms)),
        Tone::Warning,
    );
}

fn rows_to_table(rows: Vec<(String, String)>) -> Vec<Vec<String>> {
    rows.into_iter()
        .map(|(left, right)| vec![left, right])
        .collect()
}

fn telemetry_hotspot_rows(snapshot: &TelemetrySnapshot) -> Vec<(String, String)> {
    let mut rows = Vec::new();

    for (name, value) in &snapshot.counters {
        if is_duration_metric(name) {
            rows.push((
                telemetry_metric_label(name),
                format_telemetry_scalar(name, *value),
                *value as f64,
            ));
        }
    }

    for (name, value) in &snapshot.gauges {
        if is_duration_metric(name) {
            rows.push((
                telemetry_metric_label(name),
                format_telemetry_scalar(name, *value),
                *value as f64,
            ));
        }
    }

    for (name, histogram) in &snapshot.histograms {
        if is_duration_metric(name) {
            let label = telemetry_metric_label(name);
            rows.push((
                format!("{label} mean"),
                format_telemetry_mean(name, histogram.mean),
                histogram.mean,
            ));
            rows.push((
                format!("{label} max"),
                format_telemetry_scalar(name, histogram.max),
                histogram.max as f64,
            ));
            rows.push((
                format!("{label} total"),
                format_telemetry_scalar(name, histogram.total),
                histogram.total as f64,
            ));
        }
    }

    rows.sort_by(|left, right| {
        right
            .2
            .total_cmp(&left.2)
            .then_with(|| left.0.cmp(&right.0))
    });
    rows.truncate(12);
    rows.into_iter()
        .map(|(label, value, _)| (label, value))
        .collect()
}

fn telemetry_scalar_rows(values: &BTreeMap<String, u64>) -> Vec<(String, String)> {
    values
        .iter()
        .map(|(name, value)| {
            (
                telemetry_metric_label(name),
                format_telemetry_scalar(name, *value),
            )
        })
        .collect()
}

fn telemetry_histogram_rows(values: &BTreeMap<String, HistogramSnapshot>) -> Vec<(String, String)> {
    let mut rows = Vec::new();
    for (name, histogram) in values {
        let label = telemetry_metric_label(name);
        rows.push((format!("{label} count"), histogram.count.to_string()));
        rows.push((
            format!("{label} total"),
            format_telemetry_scalar(name, histogram.total),
        ));
        rows.push((
            format!("{label} mean"),
            format_telemetry_mean(name, histogram.mean),
        ));
        rows.push((
            format!("{label} min"),
            format_telemetry_scalar(name, histogram.min),
        ));
        rows.push((
            format!("{label} max"),
            format_telemetry_scalar(name, histogram.max),
        ));
    }
    rows
}

fn telemetry_metric_label(name: &str) -> String {
    let trimmed = name.strip_prefix("oxide.").unwrap_or(name);

    if let Some(base) = trimmed.strip_suffix(".count") {
        return humanize_metric_name(base);
    }
    if let Some(base) = trimmed.strip_suffix(".hist") {
        return humanize_metric_name(base);
    }
    if let Some(base) = trimmed.strip_suffix(".latency_us") {
        return format!("{} latency", humanize_metric_name(base));
    }
    if let Some(base) = trimmed.strip_suffix(".us") {
        return format!("{} time", humanize_metric_name(base));
    }

    humanize_metric_name(trimmed)
}

fn humanize_metric_name(name: &str) -> String {
    name.replace(['.', '_'], " ")
}

fn format_telemetry_scalar(name: &str, value: u64) -> String {
    if is_duration_metric(name) {
        format_duration_compact(Duration::from_micros(value))
    } else if is_bytes_metric(name) {
        format_bytes(value)
    } else {
        value.to_string()
    }
}

fn format_telemetry_mean(name: &str, value: f64) -> String {
    if !value.is_finite() || value <= 0.0 {
        return if is_bytes_metric(name) {
            "0 B".to_string()
        } else if is_duration_metric(name) {
            "0us".to_string()
        } else {
            "0".to_string()
        };
    }

    if is_duration_metric(name) {
        format_duration_compact(Duration::from_secs_f64(value / 1_000_000.0))
    } else if is_bytes_metric(name) {
        format_bytes_f64(value)
    } else {
        format!("{value:.2}")
    }
}

fn is_bytes_metric(name: &str) -> bool {
    name.contains("bytes")
}

fn is_duration_metric(name: &str) -> bool {
    name.ends_with(".us") || name.ends_with("_us")
}

fn print_report_title(title: &str) {
    let inner_width = title.len() + 8;
    let top = format!("╔{}╗", "═".repeat(inner_width));
    let middle = format!("║{}║", center_text(title, inner_width));
    let bottom = format!("╚{}╝", "═".repeat(inner_width));
    println!("{}", paint(StreamTarget::Stdout, Tone::Success, &top));
    println!("{}", paint(StreamTarget::Stdout, Tone::Success, &middle));
    println!("{}", paint(StreamTarget::Stdout, Tone::Success, &bottom));
}

fn center_text(value: &str, width: usize) -> String {
    let total_padding = width.saturating_sub(value.len());
    let left = total_padding / 2;
    let right = total_padding.saturating_sub(left);
    format!("{}{}{}", " ".repeat(left), value, " ".repeat(right))
}

fn print_key_value_table(title: &str, rows: &[(String, String)], tone: Tone) {
    let data = rows
        .iter()
        .map(|(left, right)| vec![left.clone(), right.clone()])
        .collect::<Vec<_>>();
    print_table(title, &["Field", "Value"], &data, tone);
}

fn print_bar_chart(title: &str, entries: &[(String, f64, String)], tone: Tone) {
    if entries.is_empty() {
        return;
    }

    println!();
    println!(
        "{}",
        paint(StreamTarget::Stdout, tone, &format!("▣ {title}"))
    );
    let label_width = entries
        .iter()
        .map(|(label, _, _)| label.len())
        .max()
        .unwrap_or(0);
    let max_value = entries
        .iter()
        .map(|(_, value, _)| *value)
        .fold(0.0, f64::max)
        .max(1.0);

    for (label, value, formatted) in entries {
        let percent = (*value / max_value) * 100.0;
        let bar = progress_bar(StreamTarget::Stdout, tone, percent, CHART_WIDTH);
        let label = paint(
            StreamTarget::Stdout,
            Tone::Muted,
            &format!("{label:<label_width$}"),
        );
        println!("  {label}  {bar}  {formatted}");
    }
}

fn print_table(title: &str, headers: &[&str], rows: &[Vec<String>], tone: Tone) {
    if headers.is_empty() || rows.is_empty() {
        return;
    }

    println!();
    let mut widths = headers
        .iter()
        .map(|header| header.len())
        .collect::<Vec<_>>();
    for row in rows {
        for (index, cell) in row.iter().enumerate() {
            if index < widths.len() {
                widths[index] = widths[index].max(cell.len());
            }
        }
    }

    let top = format!(
        "┌{}┐",
        widths
            .iter()
            .map(|width| "─".repeat(*width + 2))
            .collect::<Vec<_>>()
            .join("┬")
    );
    let divider = format!(
        "├{}┤",
        widths
            .iter()
            .map(|width| "─".repeat(*width + 2))
            .collect::<Vec<_>>()
            .join("┼")
    );
    let bottom = format!(
        "└{}┘",
        widths
            .iter()
            .map(|width| "─".repeat(*width + 2))
            .collect::<Vec<_>>()
            .join("┴")
    );

    println!(
        "{}",
        paint(StreamTarget::Stdout, tone, &format!("▣ {title}"))
    );
    println!("{}", paint(StreamTarget::Stdout, tone, &top));
    println!("{}", render_table_row(headers, &widths, Tone::Accent));
    println!("{}", paint(StreamTarget::Stdout, tone, &divider));
    for row in rows {
        let cells = row.iter().map(String::as_str).collect::<Vec<_>>();
        println!("{}", render_table_row(&cells, &widths, Tone::Muted));
    }
    println!("{}", paint(StreamTarget::Stdout, tone, &bottom));
}

fn render_table_row(cells: &[&str], widths: &[usize], tone: Tone) -> String {
    let mut parts = Vec::new();
    for (index, width) in widths.iter().enumerate() {
        let cell = cells.get(index).copied().unwrap_or("");
        let cell = pad_right(cell, *width);
        parts.push(format!(" {} ", paint(StreamTarget::Stdout, tone, &cell)));
    }
    format!("│{}│", parts.join("│"))
}

fn pad_right(value: &str, width: usize) -> String {
    let padding = width.saturating_sub(value.len());
    format!("{value}{}", " ".repeat(padding))
}

fn push_optional_rate_row(rows: &mut Vec<Vec<String>>, label: &str, value: Option<f64>) {
    if let Some(value) = value {
        rows.push(vec![label.to_string(), format_rate_per_second(value)]);
    }
}

fn push_optional_string_row(rows: &mut Vec<(String, String)>, label: &str, value: Option<String>) {
    if let Some(value) = value {
        rows.push((label.to_string(), value));
    }
}

fn scheduler_summary(workers: &[WorkerReport]) -> String {
    let worker_count = workers.len();
    let total_tasks: usize = workers.iter().map(|worker| worker.tasks_completed).sum();
    let max_tasks = workers
        .iter()
        .map(|worker| worker.tasks_completed)
        .max()
        .unwrap_or(0);
    let min_tasks = workers
        .iter()
        .map(|worker| worker.tasks_completed)
        .min()
        .unwrap_or(0);
    format!(
        "{worker_count} workers | task balance {min_tasks}/{max_tasks} | total tasks {total_tasks}"
    )
}

fn thread_stage_summary(thread: &ThreadReport, order: &[(&str, &str)]) -> Option<String> {
    let mut pieces = Vec::new();
    for (stage_key, label) in order {
        let value_us = thread.stage_us.get(*stage_key).copied().unwrap_or(0);
        if value_us > 0 {
            pieces.push(format!(
                "{label} {}",
                format_duration_compact(Duration::from_micros(value_us))
            ));
        }
    }
    (!pieces.is_empty()).then(|| pieces.join(" | "))
}

fn compression_label(compression: CompressionAlgo, level: Option<i32>, extreme: bool) -> String {
    let name = match compression {
        CompressionAlgo::Lz4 => "lz4",
        CompressionAlgo::Lzma => "lzma",
        CompressionAlgo::Zstd => "zstd",
    };

    match (level, compression == CompressionAlgo::Lzma && extreme) {
        (Some(level), true) => format!("{name}/{level}+extreme"),
        (Some(level), false) => format!("{name}/{level}"),
        (None, true) => format!("{name}+extreme"),
        (None, false) => name.to_string(),
    }
}

fn source_kind_label(source_kind: ArchiveSourceKind) -> &'static str {
    match source_kind {
        ArchiveSourceKind::File => "file",
        ArchiveSourceKind::Directory => "directory",
    }
}

fn format_delta(from: u64, to: u64) -> String {
    if to == from {
        return "no size change".to_string();
    }

    let delta = from.abs_diff(to);
    let percent = (delta as f64 / from.max(1) as f64) * 100.0;
    if to < from {
        format!("saved {} ({percent:.1}%)", format_bytes(delta))
    } else {
        format!("expanded {} ({percent:.1}%)", format_bytes(delta))
    }
}

fn format_rate_per_second(bytes_per_second: f64) -> String {
    format!("{}/s", format_rate(bytes_per_second))
}

fn extension_u64(extensions: &BTreeMap<String, ReportValue>, key: &str) -> Option<u64> {
    match extensions.get(key) {
        Some(ReportValue::U64(value)) => Some(*value),
        _ => None,
    }
}

fn extension_f64(extensions: &BTreeMap<String, ReportValue>, key: &str) -> Option<f64> {
    match extensions.get(key) {
        Some(ReportValue::F64(value)) => Some(*value),
        _ => None,
    }
}
