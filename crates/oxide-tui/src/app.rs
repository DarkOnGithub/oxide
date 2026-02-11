use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::Result;
use crossbeam_channel::{Receiver, Sender, unbounded};
use crossterm::event::{self, Event as CEvent, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use oxide_core::{
    ArchiveProgressEvent, ArchiveReport, CompressionAlgo, ExtractProgressEvent, ExtractReport,
    ProfileEvent, TelemetryEvent,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

use crate::browser::{BrowseTarget, FileBrowser};
use crate::formatters::{
    archive_output_filename, default_archive_output_path, default_extract_output_path,
    extract_output_filename, parse_size,
};
use crate::job::{JobConfig, JobEvent, JobMode, JobResult, spawn_job};
use crate::ui;

const EVENT_TICK_MS: u64 = 50;
const MIN_STATS_INTERVAL_MS: u64 = 50;
const MAX_HISTORY_POINTS: usize = 160;
const MAX_LOG_LINES: usize = 300;
const MAX_PROFILE_EVENTS: usize = 200;

pub fn run() -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_loop(&mut terminal);

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

fn run_loop(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> Result<()> {
    let (tx, rx) = unbounded::<JobEvent>();
    let mut app = App::default();

    loop {
        app.drain_job_events(&rx);

        terminal.draw(|frame| ui::draw(frame, &app))?;

        if event::poll(Duration::from_millis(EVENT_TICK_MS))? {
            let evt = event::read()?;
            if let CEvent::Key(key) = evt {
                if key.kind != KeyEventKind::Press {
                    continue;
                }

                if app.handle_key(key, &tx)? {
                    break;
                }
            }
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveField {
    Mode,
    Input,
    Output,
    Compression,
    Workers,
    BlockSize,
    StatsInterval,
}

impl ActiveField {
    pub const ALL: [Self; 7] = [
        Self::Mode,
        Self::Input,
        Self::Output,
        Self::Compression,
        Self::Workers,
        Self::BlockSize,
        Self::StatsInterval,
    ];

    pub fn next(self) -> Self {
        let idx = Self::ALL
            .iter()
            .position(|field| *field == self)
            .unwrap_or(0);
        Self::ALL[(idx + 1) % Self::ALL.len()]
    }

    pub fn prev(self) -> Self {
        let idx = Self::ALL
            .iter()
            .position(|field| *field == self)
            .unwrap_or(0);
        let next_idx = if idx == 0 {
            Self::ALL.len() - 1
        } else {
            idx - 1
        };
        Self::ALL[next_idx]
    }

    pub fn is_text(self) -> bool {
        matches!(
            self,
            Self::Input | Self::Output | Self::Workers | Self::BlockSize | Self::StatsInterval
        )
    }
}

#[derive(Debug, Clone, Default)]
pub struct ProfileAggregate {
    pub count: u64,
    pub total_us: u64,
    pub max_us: u64,
    pub last_result: String,
    pub last_message: String,
}

#[derive(Debug, Clone, Default)]
pub struct ArchiveLiveRates {
    last_elapsed: Duration,
    last_input_bytes: u64,
    last_output_bytes: u64,
    pub read_avg_bps: f64,
    pub read_inst_bps: f64,
    pub write_avg_bps: f64,
    pub write_inst_bps: f64,
    pub peak_read_bps: f64,
    pub peak_write_bps: f64,
}

impl ArchiveLiveRates {
    fn update(&mut self, event: &ArchiveProgressEvent) {
        let elapsed = event.elapsed;
        let done = event.input_bytes_completed.min(event.input_bytes_total);
        let written = event.output_bytes_completed;
        let elapsed_secs = elapsed.as_secs_f64().max(1e-6);

        self.read_avg_bps = done as f64 / elapsed_secs;
        self.write_avg_bps = written as f64 / elapsed_secs;

        let delta_read = done.saturating_sub(self.last_input_bytes);
        let delta_write = written.saturating_sub(self.last_output_bytes);
        let delta_elapsed = elapsed.saturating_sub(self.last_elapsed);
        let delta_secs = delta_elapsed.as_secs_f64();

        self.read_inst_bps = if delta_secs > 0.0 {
            delta_read as f64 / delta_secs
        } else {
            self.read_avg_bps
        };
        self.write_inst_bps = if delta_secs > 0.0 {
            delta_write as f64 / delta_secs
        } else {
            self.write_avg_bps
        };

        self.peak_read_bps = self.peak_read_bps.max(self.read_inst_bps);
        self.peak_write_bps = self.peak_write_bps.max(self.write_inst_bps);
        self.last_elapsed = elapsed;
        self.last_input_bytes = done;
        self.last_output_bytes = written;
    }

    fn reset(&mut self) {
        *self = Self::default();
    }
}

#[derive(Debug, Clone, Default)]
pub struct ExtractLiveRates {
    last_elapsed: Duration,
    last_archive_bytes: u64,
    last_decoded_bytes: u64,
    pub read_avg_bps: f64,
    pub read_inst_bps: f64,
    pub decode_avg_bps: f64,
    pub decode_inst_bps: f64,
    pub peak_read_bps: f64,
    pub peak_decode_bps: f64,
}

impl ExtractLiveRates {
    fn update(&mut self, event: &ExtractProgressEvent) {
        let elapsed_secs = event.elapsed.as_secs_f64().max(1e-6);
        self.read_avg_bps = event.archive_bytes_completed as f64 / elapsed_secs;
        self.decode_avg_bps = event.decoded_bytes_completed as f64 / elapsed_secs;

        let delta_archive = event
            .archive_bytes_completed
            .saturating_sub(self.last_archive_bytes);
        let delta_decoded = event
            .decoded_bytes_completed
            .saturating_sub(self.last_decoded_bytes);
        let delta_elapsed = event.elapsed.saturating_sub(self.last_elapsed);
        let delta_secs = delta_elapsed.as_secs_f64();

        self.read_inst_bps = if delta_secs > 0.0 {
            delta_archive as f64 / delta_secs
        } else {
            self.read_avg_bps
        };
        self.decode_inst_bps = if delta_secs > 0.0 {
            delta_decoded as f64 / delta_secs
        } else {
            self.decode_avg_bps
        };

        self.peak_read_bps = self.peak_read_bps.max(self.read_inst_bps);
        self.peak_decode_bps = self.peak_decode_bps.max(self.decode_inst_bps);
        self.last_elapsed = event.elapsed;
        self.last_archive_bytes = event.archive_bytes_completed;
        self.last_decoded_bytes = event.decoded_bytes_completed;
    }

    fn reset(&mut self) {
        *self = Self::default();
    }
}

#[derive(Debug, Clone)]
pub struct App {
    pub mode: JobMode,
    pub input_path: String,
    pub output_path: String,
    pub compression: CompressionAlgo,
    pub workers: String,
    pub block_size: String,
    pub stats_interval_ms: String,
    pub active_field: ActiveField,
    pub editing: bool,
    pub running: bool,
    pub status_line: String,
    pub log_lines: VecDeque<String>,
    pub browser: Option<FileBrowser>,
    pub archive_progress: Option<ArchiveProgressEvent>,
    pub extract_progress: Option<ExtractProgressEvent>,
    pub archive_report: Option<ArchiveReport>,
    pub extract_report: Option<ExtractReport>,
    pub read_rate_history: Vec<u64>,
    pub write_rate_history: Vec<u64>,
    pub decode_rate_history: Vec<u64>,
    pub archive_live: ArchiveLiveRates,
    pub extract_live: ExtractLiveRates,
    pub profile_events: VecDeque<ProfileEvent>,
    pub profile_aggregates: BTreeMap<String, ProfileAggregate>,
    pub run_started_at: Option<Instant>,
    pub last_finished_at: Option<Instant>,
}

impl Default for App {
    fn default() -> Self {
        Self {
            mode: JobMode::Archive,
            input_path: String::new(),
            output_path: String::new(),
            compression: CompressionAlgo::Lz4,
            workers: num_cpus::get().max(1).to_string(),
            block_size: "1M".to_string(),
            stats_interval_ms: "250".to_string(),
            active_field: ActiveField::Mode,
            editing: false,
            running: false,
            status_line: "Ready. Press r to run.".to_string(),
            log_lines: VecDeque::new(),
            browser: None,
            archive_progress: None,
            extract_progress: None,
            archive_report: None,
            extract_report: None,
            read_rate_history: Vec::new(),
            write_rate_history: Vec::new(),
            decode_rate_history: Vec::new(),
            archive_live: ArchiveLiveRates::default(),
            extract_live: ExtractLiveRates::default(),
            profile_events: VecDeque::new(),
            profile_aggregates: BTreeMap::new(),
            run_started_at: None,
            last_finished_at: None,
        }
    }
}

impl App {
    pub fn drain_job_events(&mut self, rx: &Receiver<JobEvent>) {
        while let Ok(event) = rx.try_recv() {
            self.handle_job_event(event);
        }
    }

    pub fn handle_key(&mut self, key: KeyEvent, tx: &Sender<JobEvent>) -> Result<bool> {
        if self.browser.is_some() {
            self.handle_browser_key(key);
            return Ok(false);
        }

        if self.editing {
            self.handle_edit_key(key);
            return Ok(false);
        }

        match key.code {
            KeyCode::Char('q') => return Ok(true),
            KeyCode::Tab => self.active_field = self.active_field.next(),
            KeyCode::BackTab => self.active_field = self.active_field.prev(),
            KeyCode::Enter => {
                if self.active_field.is_text() {
                    self.editing = true;
                } else {
                    self.apply_non_text_adjustment(1);
                }
            }
            KeyCode::Left => self.apply_non_text_adjustment(-1),
            KeyCode::Right => self.apply_non_text_adjustment(1),
            KeyCode::Char('r') => self.start_job(tx),
            KeyCode::Char('b') => self.open_browser_for_active_field(),
            KeyCode::Char('c') => self.clear_runtime_data(),
            KeyCode::Char('d') => self.fill_default_output_if_possible(),
            _ => {
                if matches!(key.code, KeyCode::Char(_))
                    && self.active_field.is_text()
                    && !key.modifiers.contains(KeyModifiers::CONTROL)
                {
                    self.editing = true;
                    self.handle_edit_key(key);
                }
            }
        }

        Ok(false)
    }

    fn handle_edit_key(&mut self, key: KeyEvent) {
        let Some(field) = self.active_text_field_mut() else {
            self.editing = false;
            return;
        };

        match key.code {
            KeyCode::Enter | KeyCode::Esc => self.editing = false,
            KeyCode::Tab => {
                self.editing = false;
                self.active_field = self.active_field.next();
            }
            KeyCode::BackTab => {
                self.editing = false;
                self.active_field = self.active_field.prev();
            }
            KeyCode::Backspace => {
                field.pop();
            }
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => field.clear(),
            KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                field.push(ch);
            }
            _ => {}
        }
    }

    fn handle_browser_key(&mut self, key: KeyEvent) {
        let Some(browser) = self.browser.as_mut() else {
            return;
        };

        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => {
                self.browser = None;
                self.status_line = "Closed browser".to_string();
            }
            KeyCode::Up => browser.move_prev(),
            KeyCode::Down => browser.move_next(),
            KeyCode::Left | KeyCode::Backspace => browser.go_parent(),
            KeyCode::Char('h') => browser.toggle_hidden(),
            KeyCode::Char('r') => browser.reload(),
            KeyCode::Enter => {
                let target = browser.target;
                if let Some(path) = browser.enter_selected() {
                    self.apply_browser_selection(target, path);
                }
            }
            KeyCode::Char('s') => {
                let target = browser.target;
                if let Some(path) = browser.choose_selected() {
                    self.apply_browser_selection(target, path);
                }
            }
            KeyCode::Char('d') => {
                let target = browser.target;
                let path = browser.choose_current_dir();
                self.apply_browser_selection(target, path);
            }
            _ => {}
        }
    }

    fn apply_browser_selection(&mut self, target: BrowseTarget, mut selected: PathBuf) {
        if target == BrowseTarget::Output && selected.is_dir() {
            selected = self.default_output_in_directory(&selected);
        }

        match target {
            BrowseTarget::Input => {
                self.input_path = selected.to_string_lossy().into_owned();
                if self.output_path.trim().is_empty() {
                    self.fill_default_output_if_possible();
                }
            }
            BrowseTarget::Output => {
                self.output_path = selected.to_string_lossy().into_owned();
            }
        }

        self.log_line(format!("selected {}", selected.display()));
        self.status_line = format!("selected {}", selected.display());
        self.browser = None;
    }

    fn open_browser_for_active_field(&mut self) {
        let target = match self.active_field {
            ActiveField::Output => BrowseTarget::Output,
            _ => BrowseTarget::Input,
        };

        let start_path = match target {
            BrowseTarget::Input => as_path_opt(&self.input_path),
            BrowseTarget::Output => as_path_opt(&self.output_path),
        };

        self.browser = Some(FileBrowser::new(target, start_path.as_deref()));
        self.status_line =
            "Browser: Enter opens dir/selects file, s selects, d selects dir".to_string();
    }

    fn apply_non_text_adjustment(&mut self, direction: i32) {
        match self.active_field {
            ActiveField::Mode => {
                self.mode = match (self.mode, direction >= 0) {
                    (JobMode::Archive, true) => JobMode::Extract,
                    (JobMode::Archive, false) => JobMode::Extract,
                    (JobMode::Extract, true) => JobMode::Archive,
                    (JobMode::Extract, false) => JobMode::Archive,
                };
                self.fill_default_output_if_possible();
            }
            ActiveField::Compression => {
                self.compression = match (self.compression, direction >= 0) {
                    (CompressionAlgo::Lz4, true) => CompressionAlgo::Lzma,
                    (CompressionAlgo::Lz4, false) => CompressionAlgo::Deflate,
                    (CompressionAlgo::Lzma, true) => CompressionAlgo::Deflate,
                    (CompressionAlgo::Lzma, false) => CompressionAlgo::Lz4,
                    (CompressionAlgo::Deflate, true) => CompressionAlgo::Lz4,
                    (CompressionAlgo::Deflate, false) => CompressionAlgo::Lzma,
                };
            }
            _ => {}
        }
    }

    fn start_job(&mut self, tx: &Sender<JobEvent>) {
        if self.running {
            self.status_line = "run already in progress".to_string();
            return;
        }

        let config = match self.build_job_config() {
            Ok(config) => config,
            Err(error) => {
                self.status_line = error.clone();
                self.log_line(error);
                return;
            }
        };

        self.running = true;
        self.archive_progress = None;
        self.extract_progress = None;
        self.archive_live.reset();
        self.extract_live.reset();
        self.read_rate_history.clear();
        self.write_rate_history.clear();
        self.decode_rate_history.clear();
        self.run_started_at = Some(Instant::now());
        self.last_finished_at = None;
        self.archive_report = None;
        self.extract_report = None;

        self.output_path = config.output_path.to_string_lossy().into_owned();
        self.status_line = format!(
            "starting {:?}: {} -> {}",
            config.mode,
            config.input_path.display(),
            config.output_path.display()
        );
        self.log_line(self.status_line.clone());
        spawn_job(config, tx.clone());
    }

    fn build_job_config(&self) -> Result<JobConfig, String> {
        let input_path =
            as_path_opt(&self.input_path).ok_or_else(|| "input path is required".to_string())?;
        let workers = self
            .workers
            .trim()
            .parse::<usize>()
            .map_err(|_| "workers must be a positive integer".to_string())?
            .max(1);
        let block_size = parse_size(self.block_size.trim())?;
        let stats_interval_ms = self
            .stats_interval_ms
            .trim()
            .parse::<u64>()
            .map_err(|_| "stats interval must be a positive integer (ms)".to_string())?
            .max(MIN_STATS_INTERVAL_MS);

        let output_path = if let Some(output) = as_path_opt(&self.output_path) {
            if output.is_dir() {
                self.default_output_in_directory(&output)
            } else {
                output
            }
        } else {
            match self.mode {
                JobMode::Archive => default_archive_output_path(&input_path),
                JobMode::Extract => default_extract_output_path(&input_path),
            }
        };

        Ok(JobConfig {
            mode: self.mode,
            input_path,
            output_path,
            workers,
            block_size,
            compression: self.compression,
            stats_interval: Duration::from_millis(stats_interval_ms),
        })
    }

    fn default_output_in_directory(&self, output_dir: &Path) -> PathBuf {
        let input_path = as_path_opt(&self.input_path);

        match self.mode {
            JobMode::Archive => {
                let name = input_path
                    .as_deref()
                    .map(archive_output_filename)
                    .unwrap_or_else(|| "archive.oxz".to_string());
                output_dir.join(name)
            }
            JobMode::Extract => {
                let name = input_path
                    .as_deref()
                    .map(extract_output_filename)
                    .unwrap_or_else(|| "extract.out".to_string());
                output_dir.join(name)
            }
        }
    }

    fn fill_default_output_if_possible(&mut self) {
        let Some(input_path) = as_path_opt(&self.input_path) else {
            return;
        };

        let output = match self.mode {
            JobMode::Archive => default_archive_output_path(&input_path),
            JobMode::Extract => default_extract_output_path(&input_path),
        };
        self.output_path = output.to_string_lossy().into_owned();
    }

    fn clear_runtime_data(&mut self) {
        self.archive_progress = None;
        self.extract_progress = None;
        self.archive_report = None;
        self.extract_report = None;
        self.archive_live.reset();
        self.extract_live.reset();
        self.read_rate_history.clear();
        self.write_rate_history.clear();
        self.decode_rate_history.clear();
        self.profile_events.clear();
        self.profile_aggregates.clear();
        self.log_lines.clear();
        self.status_line = "cleared telemetry + reports".to_string();
    }

    fn active_text_field_mut(&mut self) -> Option<&mut String> {
        match self.active_field {
            ActiveField::Input => Some(&mut self.input_path),
            ActiveField::Output => Some(&mut self.output_path),
            ActiveField::Workers => Some(&mut self.workers),
            ActiveField::BlockSize => Some(&mut self.block_size),
            ActiveField::StatsInterval => Some(&mut self.stats_interval_ms),
            _ => None,
        }
    }

    fn handle_job_event(&mut self, event: JobEvent) {
        match event {
            JobEvent::Telemetry(evt) => self.handle_telemetry_event(evt),
            JobEvent::JobStarted {
                mode,
                input_path,
                output_path,
            } => {
                self.status_line = format!(
                    "running {:?}: {} -> {}",
                    mode,
                    input_path.display(),
                    output_path.display()
                );
                self.log_line(self.status_line.clone());
            }
            JobEvent::JobFinished { result } => {
                self.running = false;
                self.last_finished_at = Some(Instant::now());

                match result {
                    Ok(JobResult::Archive(report)) => {
                        let elapsed = report.elapsed;
                        self.archive_report = Some(report);
                        self.status_line = format!(
                            "archive completed in {}",
                            crate::formatters::format_duration(elapsed)
                        );
                        self.log_line(self.status_line.clone());
                    }
                    Ok(JobResult::Extract(report)) => {
                        let elapsed = report.elapsed;
                        self.extract_report = Some(report);
                        self.status_line = format!(
                            "extract completed in {}",
                            crate::formatters::format_duration(elapsed)
                        );
                        self.log_line(self.status_line.clone());
                    }
                    Err(error) => {
                        self.status_line = format!("run failed: {error}");
                        self.log_line(self.status_line.clone());
                    }
                }
            }
        }
    }

    fn handle_telemetry_event(&mut self, event: TelemetryEvent) {
        match event {
            TelemetryEvent::ArchiveProgress(progress) => {
                self.archive_live.update(&progress);
                push_history_point(&mut self.read_rate_history, self.archive_live.read_inst_bps);
                push_history_point(
                    &mut self.write_rate_history,
                    self.archive_live.write_inst_bps,
                );
                self.archive_progress = Some(progress);
            }
            TelemetryEvent::ExtractProgress(progress) => {
                self.extract_live.update(&progress);
                push_history_point(&mut self.read_rate_history, self.extract_live.read_inst_bps);
                push_history_point(
                    &mut self.decode_rate_history,
                    self.extract_live.decode_inst_bps,
                );
                self.extract_progress = Some(progress);
            }
            TelemetryEvent::ArchiveCompleted(report) => {
                self.archive_report = Some(report);
            }
            TelemetryEvent::ExtractCompleted(report) => {
                self.extract_report = Some(report);
            }
            TelemetryEvent::Profile(profile) => {
                self.push_profile_event(profile);
            }
        }
    }

    fn push_profile_event(&mut self, event: ProfileEvent) {
        let key = format!("{}::{}", event.target, event.op);
        let aggregate = self.profile_aggregates.entry(key).or_default();
        aggregate.count = aggregate.count.saturating_add(1);
        aggregate.total_us = aggregate.total_us.saturating_add(event.elapsed_us);
        aggregate.max_us = aggregate.max_us.max(event.elapsed_us);
        aggregate.last_result = event.result.to_string();
        aggregate.last_message = event.message.to_string();

        self.profile_events.push_back(event);
        while self.profile_events.len() > MAX_PROFILE_EVENTS {
            self.profile_events.pop_front();
        }
    }

    fn log_line(&mut self, line: String) {
        self.log_lines.push_back(line);
        while self.log_lines.len() > MAX_LOG_LINES {
            self.log_lines.pop_front();
        }
    }
}

fn as_path_opt(value: &str) -> Option<PathBuf> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(PathBuf::from(trimmed))
    }
}

fn push_history_point(history: &mut Vec<u64>, rate_bps: f64) {
    let point = rate_bps.max(0.0) as u64;
    history.push(point);
    if history.len() > MAX_HISTORY_POINTS {
        let extra = history.len() - MAX_HISTORY_POINTS;
        history.drain(0..extra);
    }
}
