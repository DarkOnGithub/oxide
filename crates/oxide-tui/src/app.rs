use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::Result;
use crossbeam_channel::{Receiver, Sender, unbounded};
use crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event as CEvent, KeyCode, KeyEvent,
    KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use oxide_core::telemetry::tags;
use oxide_core::{
    ArchiveProgressEvent, ArchiveReport, CompressionAlgo, ExtractProgressEvent, ExtractReport,
    PoolRuntimeSnapshot, ProfileEvent, TelemetryEvent, WorkerRuntimeSnapshot,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

use crate::browser::{BrowseTarget, FileBrowser};
use crate::formatters::{
    EPHEMERAL_OUTPUT_TOKEN, NULL_OUTPUT_TOKEN, archive_output_filename,
    default_archive_output_path, default_extract_output_path, extract_output_filename,
    is_ephemeral_output_value, is_null_output_value, parse_size,
};
use crate::job::{JobConfig, JobEvent, JobMode, JobResult, spawn_job};
use crate::ui;

const EVENT_TICK_MS: u64 = 50;
const MIN_STATS_INTERVAL_MS: u64 = 50;
const MAX_HISTORY_POINTS: usize = 160;
const MAX_LOG_LINES: usize = 500;
const MAX_PROFILE_EVENTS: usize = 300;
const MAX_PROFILE_SAMPLES_PER_KEY: usize = 4096;
const DEFAULT_PROFILE_WINDOW_SECONDS: u64 = 10;

const PROFILE_TARGET_FILTERS: [&str; 8] = [
    "all",
    tags::PROFILE_MMAP,
    tags::PROFILE_FORMAT,
    tags::PROFILE_BUFFER,
    tags::PROFILE_SCANNER,
    tags::PROFILE_WORKER,
    tags::PROFILE_PIPELINE,
    "oxide.profile",
];

const PROFILE_OP_FILTERS: [&str; 17] = [
    "all",
    "open",
    "slice",
    "acquire",
    "recycle",
    "detect",
    "scan_file",
    "queue_depth",
    "task_start",
    "task_finish",
    "archive_run",
    "extract_run",
    "stage_discovery",
    "stage_writer",
    "stage_decode_wait",
    "stage_output_write",
    "task",
];

const PROFILE_TAG_FILTERS: [&str; 8] = [
    "all",
    tags::TAG_SYSTEM,
    tags::TAG_MMAP,
    tags::TAG_FORMAT,
    tags::TAG_BUFFER,
    tags::TAG_SCANNER,
    tags::TAG_WORKER,
    tags::TAG_PIPELINE,
];

pub fn run() -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();

    let mouse_enabled = execute!(stdout, EnterAlternateScreen, EnableMouseCapture).is_ok();
    if !mouse_enabled {
        execute!(stdout, EnterAlternateScreen)?;
    }

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let result = run_loop(&mut terminal, mouse_enabled);

    disable_raw_mode()?;
    if mouse_enabled {
        execute!(
            terminal.backend_mut(),
            DisableMouseCapture,
            LeaveAlternateScreen
        )?;
    } else {
        execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    }
    terminal.show_cursor()?;

    result
}

fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    mouse_enabled: bool,
) -> Result<()> {
    let (tx, rx) = unbounded::<JobEvent>();
    let mut app = App {
        mouse_enabled,
        ..App::default()
    };
    if !mouse_enabled {
        app.status_line = "mouse capture unavailable; keyboard mode active".to_string();
    }

    loop {
        app.drain_job_events(&rx);
        app.tick();

        let mut layout = ui::LayoutInfo::default();
        terminal.draw(|frame| {
            layout = ui::draw(frame, &app);
        })?;

        if event::poll(Duration::from_millis(EVENT_TICK_MS))? {
            let evt = event::read()?;
            match evt {
                CEvent::Key(key) => {
                    if key.kind == KeyEventKind::Press && app.handle_key(key, &tx)? {
                        break;
                    }
                }
                CEvent::Mouse(mouse) => {
                    app.handle_mouse(mouse, &layout, &tx);
                }
                CEvent::Resize(_, _) => {
                    // Redraw on next loop.
                }
                _ => {}
            }
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UiTab {
    Live,
    Profile,
    Logs,
}

impl UiTab {
    pub const ALL: [Self; 3] = [Self::Live, Self::Profile, Self::Logs];

    pub fn label(self) -> &'static str {
        match self {
            Self::Live => "Live",
            Self::Profile => "Profile",
            Self::Logs => "Logs",
        }
    }

    pub fn next(self) -> Self {
        let idx = Self::ALL.iter().position(|tab| *tab == self).unwrap_or(0);
        Self::ALL[(idx + 1) % Self::ALL.len()]
    }

    pub fn prev(self) -> Self {
        let idx = Self::ALL.iter().position(|tab| *tab == self).unwrap_or(0);
        let prev_idx = if idx == 0 {
            Self::ALL.len() - 1
        } else {
            idx - 1
        };
        Self::ALL[prev_idx]
    }
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
        let prev_idx = if idx == 0 {
            Self::ALL.len() - 1
        } else {
            idx - 1
        };
        Self::ALL[prev_idx]
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Mode => "mode",
            Self::Input => "input",
            Self::Output => "output",
            Self::Compression => "compression",
            Self::Workers => "workers",
            Self::BlockSize => "block size",
            Self::StatsInterval => "stats interval",
        }
    }

    pub fn is_text(self) -> bool {
        matches!(
            self,
            Self::Input | Self::Output | Self::Workers | Self::BlockSize | Self::StatsInterval
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunAction {
    StartRun,
    BrowseInput,
    BrowseOutput,
    EphemeralOutput,
    NullOutput,
    DefaultOutput,
    Clear,
}

impl RunAction {
    pub fn label(self) -> &'static str {
        match self {
            Self::StartRun => "Run",
            Self::BrowseInput => "Browse Input",
            Self::BrowseOutput => "Browse Output",
            Self::EphemeralOutput => "Ephemeral",
            Self::NullOutput => "Null Output",
            Self::DefaultOutput => "Default Output",
            Self::Clear => "Clear",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrowserAction {
    OpenOrSelect,
    SelectDir,
    Parent,
    ToggleHidden,
    Refresh,
    Close,
}

impl BrowserAction {
    pub fn label(self) -> &'static str {
        match self {
            Self::OpenOrSelect => "Open/Select",
            Self::SelectDir => "Select Dir",
            Self::Parent => "Parent",
            Self::ToggleHidden => "Hidden",
            Self::Refresh => "Refresh",
            Self::Close => "Close",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileSortMode {
    Total,
    Avg,
    Max,
    Min,
    Count,
    P95,
    Rate,
}

impl ProfileSortMode {
    pub const ALL: [Self; 7] = [
        Self::Total,
        Self::Avg,
        Self::Max,
        Self::Min,
        Self::Count,
        Self::P95,
        Self::Rate,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::Total => "total",
            Self::Avg => "avg",
            Self::Max => "max",
            Self::Min => "min",
            Self::Count => "count",
            Self::P95 => "p95",
            Self::Rate => "rate",
        }
    }

    pub fn next(self) -> Self {
        let idx = Self::ALL.iter().position(|mode| *mode == self).unwrap_or(0);
        Self::ALL[(idx + 1) % Self::ALL.len()]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerSortMode {
    Utilization,
    Busy,
    Tasks,
    WorkerId,
}

impl WorkerSortMode {
    pub const ALL: [Self; 4] = [Self::Utilization, Self::Busy, Self::Tasks, Self::WorkerId];

    pub fn label(self) -> &'static str {
        match self {
            Self::Utilization => "utilization",
            Self::Busy => "busy",
            Self::Tasks => "tasks",
            Self::WorkerId => "worker id",
        }
    }

    pub fn next(self) -> Self {
        let idx = Self::ALL.iter().position(|mode| *mode == self).unwrap_or(0);
        Self::ALL[(idx + 1) % Self::ALL.len()]
    }
}

#[derive(Debug, Clone)]
pub struct ViewConfig {
    pub profile_sort_mode: ProfileSortMode,
    pub worker_sort_mode: WorkerSortMode,
    pub profile_rows_limit: usize,
    pub worker_rows_limit: usize,
    pub profile_window_seconds: u64,
    pub profile_target_filter: String,
    pub profile_op_filter: String,
    pub profile_tag_filter: String,
}

impl Default for ViewConfig {
    fn default() -> Self {
        Self {
            profile_sort_mode: ProfileSortMode::Total,
            worker_sort_mode: WorkerSortMode::Utilization,
            profile_rows_limit: 12,
            worker_rows_limit: 10,
            profile_window_seconds: DEFAULT_PROFILE_WINDOW_SECONDS,
            profile_target_filter: "all".to_string(),
            profile_op_filter: "all".to_string(),
            profile_tag_filter: "all".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProfileAggregate {
    pub key: String,
    pub target: String,
    pub op: String,
    pub count: u64,
    pub total_us: u64,
    pub min_us: u64,
    pub max_us: u64,
    pub avg_us: f64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub events_per_sec: f64,
    pub last_result: String,
    pub last_message: String,
    pub tags_seen: BTreeSet<String>,
}

impl ProfileAggregate {
    fn new(key: String, target: &str, op: &str) -> Self {
        Self {
            key,
            target: target.to_string(),
            op: op.to_string(),
            count: 0,
            total_us: 0,
            min_us: u64::MAX,
            max_us: 0,
            avg_us: 0.0,
            p50_us: 0,
            p95_us: 0,
            events_per_sec: 0.0,
            last_result: String::new(),
            last_message: String::new(),
            tags_seen: BTreeSet::new(),
        }
    }

    pub fn min_display(&self) -> u64 {
        if self.count == 0 { 0 } else { self.min_us }
    }
}

#[derive(Debug, Clone)]
struct ProfileSample {
    at: Instant,
    elapsed_us: u64,
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
    pub output_input_ratio: f64,
    pub compression_ratio: f64,
}

impl ArchiveLiveRates {
    fn update(&mut self, event: &ArchiveProgressEvent) {
        let done = event.input_bytes_completed.min(event.input_bytes_total);
        let written = event.output_bytes_completed;
        let delta_read = done.saturating_sub(self.last_input_bytes);
        let delta_write = written.saturating_sub(self.last_output_bytes);
        let delta_elapsed = event.elapsed.saturating_sub(self.last_elapsed);
        let delta_secs = delta_elapsed.as_secs_f64();

        self.read_avg_bps = event.read_avg_bps;
        self.write_avg_bps = event.write_avg_bps;
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
        self.output_input_ratio = event.output_input_ratio;
        self.compression_ratio = event.compression_ratio;

        self.last_elapsed = event.elapsed;
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
    pub decode_archive_ratio: f64,
}

impl ExtractLiveRates {
    fn update(&mut self, event: &ExtractProgressEvent) {
        let delta_archive = event
            .archive_bytes_completed
            .saturating_sub(self.last_archive_bytes);
        let delta_decoded = event
            .decoded_bytes_completed
            .saturating_sub(self.last_decoded_bytes);
        let delta_elapsed = event.elapsed.saturating_sub(self.last_elapsed);
        let delta_secs = delta_elapsed.as_secs_f64();

        self.read_avg_bps = event.read_avg_bps;
        self.decode_avg_bps = event.decode_avg_bps;
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
        self.decode_archive_ratio = event.decode_archive_ratio;

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
    pub active_tab: UiTab,
    pub editing: bool,
    pub running: bool,
    pub mouse_enabled: bool,
    pub status_line: String,
    pub log_lines: VecDeque<String>,
    pub logs_scroll_offset: usize,
    pub profile_scroll_offset: usize,
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
    profile_samples: BTreeMap<String, VecDeque<ProfileSample>>,
    pub view: ViewConfig,
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
            active_tab: UiTab::Live,
            editing: false,
            running: false,
            mouse_enabled: true,
            status_line: "Ready. Press r to run.".to_string(),
            log_lines: VecDeque::new(),
            logs_scroll_offset: 0,
            profile_scroll_offset: 0,
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
            profile_samples: BTreeMap::new(),
            view: ViewConfig::default(),
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

    pub fn tick(&mut self) {
        self.refresh_profile_windows();
        self.clamp_scroll_offsets();
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
            KeyCode::Char(ch) => match ch {
                'r' => self.start_job(tx),
                'b' => self.open_browser_for_active_field(),
                'e' => self.enable_ephemeral_output(),
                'n' => self.enable_null_output(),
                'c' => self.clear_runtime_data(),
                'd' => self.fill_default_output_if_possible(),
                'm' => self.active_tab = self.active_tab.next(),
                'M' => self.active_tab = self.active_tab.prev(),
                'p' => self.view.profile_sort_mode = self.view.profile_sort_mode.next(),
                'w' => self.view.worker_sort_mode = self.view.worker_sort_mode.next(),
                't' => cycle_filter(
                    &mut self.view.profile_target_filter,
                    &PROFILE_TARGET_FILTERS,
                ),
                'o' => cycle_filter(&mut self.view.profile_op_filter, &PROFILE_OP_FILTERS),
                'g' => cycle_filter(&mut self.view.profile_tag_filter, &PROFILE_TAG_FILTERS),
                '+' | '=' => self.bump_visible_rows(1),
                '-' => self.bump_visible_rows(-1),
                '.' => {
                    self.view.profile_window_seconds =
                        self.view.profile_window_seconds.saturating_add(1)
                }
                ',' => {
                    self.view.profile_window_seconds =
                        self.view.profile_window_seconds.saturating_sub(1).max(1)
                }
                _ => {
                    if self.active_field.is_text() && !key.modifiers.contains(KeyModifiers::CONTROL)
                    {
                        self.editing = true;
                        self.handle_edit_key(key);
                    }
                }
            },
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
            KeyCode::Up => self.scroll_active_tab(-1),
            KeyCode::Down => self.scroll_active_tab(1),
            KeyCode::PageUp => self.scroll_active_tab(-8),
            KeyCode::PageDown => self.scroll_active_tab(8),
            _ => {}
        }

        Ok(false)
    }

    pub fn handle_mouse(
        &mut self,
        mouse: MouseEvent,
        layout: &ui::LayoutInfo,
        tx: &Sender<JobEvent>,
    ) {
        if self.browser.is_some() {
            self.handle_browser_mouse(mouse, layout);
            return;
        }

        match mouse.kind {
            MouseEventKind::Down(MouseButton::Left) => {
                if let Some(action) = layout.run_action_at(mouse.column, mouse.row) {
                    self.apply_run_action(action, tx);
                    return;
                }

                if let Some(field) = layout.run_field_at(mouse.column, mouse.row) {
                    self.active_field = field;
                    if field.is_text() {
                        self.editing = true;
                    } else {
                        self.apply_non_text_adjustment(1);
                    }
                }
            }
            MouseEventKind::ScrollUp => self.handle_mouse_scroll(-1, mouse, layout),
            MouseEventKind::ScrollDown => self.handle_mouse_scroll(1, mouse, layout),
            _ => {}
        }
    }

    pub fn current_runtime(&self) -> Option<&PoolRuntimeSnapshot> {
        if let Some(progress) = &self.archive_progress {
            return Some(&progress.runtime);
        }
        if let Some(progress) = &self.extract_progress {
            return Some(&progress.runtime);
        }
        None
    }

    pub fn filtered_sorted_profile_rows(&self) -> Vec<ProfileAggregate> {
        let mut rows = self
            .profile_aggregates
            .values()
            .filter(|aggregate| self.profile_filter_matches(aggregate))
            .cloned()
            .collect::<Vec<_>>();

        rows.sort_by(|left, right| {
            let order = match self.view.profile_sort_mode {
                ProfileSortMode::Total => right.total_us.cmp(&left.total_us),
                ProfileSortMode::Avg => right
                    .avg_us
                    .partial_cmp(&left.avg_us)
                    .unwrap_or(Ordering::Equal),
                ProfileSortMode::Max => right.max_us.cmp(&left.max_us),
                ProfileSortMode::Min => right.min_display().cmp(&left.min_display()),
                ProfileSortMode::Count => right.count.cmp(&left.count),
                ProfileSortMode::P95 => right.p95_us.cmp(&left.p95_us),
                ProfileSortMode::Rate => right
                    .events_per_sec
                    .partial_cmp(&left.events_per_sec)
                    .unwrap_or(Ordering::Equal),
            };

            if order == Ordering::Equal {
                left.key.cmp(&right.key)
            } else {
                order
            }
        });

        rows
    }

    pub fn sorted_worker_rows(&self) -> Vec<WorkerRuntimeSnapshot> {
        let Some(runtime) = self.current_runtime() else {
            return Vec::new();
        };

        let mut rows = runtime.workers.clone();
        rows.sort_by(|left, right| {
            let order = match self.view.worker_sort_mode {
                WorkerSortMode::Utilization => right
                    .utilization
                    .partial_cmp(&left.utilization)
                    .unwrap_or(Ordering::Equal),
                WorkerSortMode::Busy => right.busy.cmp(&left.busy),
                WorkerSortMode::Tasks => right.tasks_completed.cmp(&left.tasks_completed),
                WorkerSortMode::WorkerId => left.worker_id.cmp(&right.worker_id),
            };

            if order == Ordering::Equal {
                left.worker_id.cmp(&right.worker_id)
            } else {
                order
            }
        });

        rows
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

    fn handle_browser_mouse(&mut self, mouse: MouseEvent, layout: &ui::LayoutInfo) {
        let Some(browser_layout) = layout.browser.as_ref() else {
            return;
        };
        let Some(browser) = self.browser.as_mut() else {
            return;
        };

        match mouse.kind {
            MouseEventKind::Down(MouseButton::Left) => {
                if let Some(action) = browser_layout.action_at(mouse.column, mouse.row) {
                    self.apply_browser_action(action);
                    return;
                }

                if rect_contains(browser_layout.list_rect, mouse.column, mouse.row) {
                    let relative = mouse.row.saturating_sub(browser_layout.list_rect.y) as usize;
                    let index = browser_layout
                        .list_start_index
                        .saturating_add(relative)
                        .min(browser.len().saturating_sub(1));
                    browser.set_selected(index);
                }
            }
            MouseEventKind::ScrollUp => browser.scroll(-1),
            MouseEventKind::ScrollDown => browser.scroll(1),
            _ => {}
        }
    }

    fn apply_browser_action(&mut self, action: BrowserAction) {
        let Some(browser) = self.browser.as_mut() else {
            return;
        };

        match action {
            BrowserAction::OpenOrSelect => {
                let target = browser.target;
                if let Some(path) = browser.enter_selected() {
                    self.apply_browser_selection(target, path);
                }
            }
            BrowserAction::SelectDir => {
                let target = browser.target;
                let path = browser.choose_current_dir();
                self.apply_browser_selection(target, path);
            }
            BrowserAction::Parent => browser.go_parent(),
            BrowserAction::ToggleHidden => browser.toggle_hidden(),
            BrowserAction::Refresh => browser.reload(),
            BrowserAction::Close => {
                self.browser = None;
                self.status_line = "Closed browser".to_string();
            }
        }
    }

    fn handle_mouse_scroll(&mut self, delta: i32, mouse: MouseEvent, layout: &ui::LayoutInfo) {
        if let Some(rect) = layout.logs_rect {
            if rect_contains(rect, mouse.column, mouse.row) {
                self.logs_scroll_offset = scroll_with_floor(self.logs_scroll_offset, delta);
                return;
            }
        }

        if let Some(rect) = layout.profile_rect {
            if rect_contains(rect, mouse.column, mouse.row) {
                self.profile_scroll_offset = scroll_with_floor(self.profile_scroll_offset, delta);
                return;
            }
        }

        self.scroll_active_tab(delta);
    }

    fn scroll_active_tab(&mut self, delta: i32) {
        match self.active_tab {
            UiTab::Logs => {
                self.logs_scroll_offset = scroll_with_floor(self.logs_scroll_offset, delta)
            }
            UiTab::Profile => {
                self.profile_scroll_offset = scroll_with_floor(self.profile_scroll_offset, delta)
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
                let keep_special_output = if is_null_output_value(&self.output_path) {
                    Some(NULL_OUTPUT_TOKEN)
                } else if is_ephemeral_output_value(&self.output_path) {
                    Some(EPHEMERAL_OUTPUT_TOKEN)
                } else {
                    None
                };
                self.mode = match (self.mode, direction >= 0) {
                    (JobMode::Archive, true) => JobMode::Extract,
                    (JobMode::Archive, false) => JobMode::Extract,
                    (JobMode::Extract, true) => JobMode::Archive,
                    (JobMode::Extract, false) => JobMode::Archive,
                };
                if let Some(token) = keep_special_output {
                    self.output_path = token.to_string();
                } else {
                    self.fill_default_output_if_possible();
                }
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

    fn apply_run_action(&mut self, action: RunAction, tx: &Sender<JobEvent>) {
        match action {
            RunAction::StartRun => self.start_job(tx),
            RunAction::BrowseInput => {
                self.active_field = ActiveField::Input;
                self.open_browser_for_active_field();
            }
            RunAction::BrowseOutput => {
                self.active_field = ActiveField::Output;
                self.open_browser_for_active_field();
            }
            RunAction::EphemeralOutput => self.enable_ephemeral_output(),
            RunAction::NullOutput => self.enable_null_output(),
            RunAction::DefaultOutput => self.fill_default_output_if_possible(),
            RunAction::Clear => self.clear_runtime_data(),
        }
    }

    fn bump_visible_rows(&mut self, delta: i32) {
        if self.active_tab == UiTab::Profile {
            self.view.profile_rows_limit = bump_usize(self.view.profile_rows_limit, delta, 4, 100);
        } else if self.active_tab == UiTab::Live {
            self.view.worker_rows_limit = bump_usize(self.view.worker_rows_limit, delta, 4, 100);
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

        self.output_path = config.output_display.clone();
        self.status_line = format!(
            "starting {:?}: {} -> {}",
            config.mode,
            config.input_path.display(),
            config.output_display
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

        let output_raw = self.output_path.trim();
        let (output_path, output_display, ephemeral_output, null_output) =
            if is_null_output_value(output_raw) {
                (
                    PathBuf::from(NULL_OUTPUT_TOKEN),
                    NULL_OUTPUT_TOKEN.to_string(),
                    false,
                    true,
                )
            } else if is_ephemeral_output_value(output_raw) {
                (
                    PathBuf::from(EPHEMERAL_OUTPUT_TOKEN),
                    EPHEMERAL_OUTPUT_TOKEN.to_string(),
                    true,
                    false,
                )
            } else if let Some(output) = as_path_opt(&self.output_path) {
                if output.is_dir() {
                    let resolved = self.default_output_in_directory(&output);
                    (
                        resolved.clone(),
                        resolved.to_string_lossy().into_owned(),
                        false,
                        false,
                    )
                } else {
                    (
                        output.clone(),
                        output.to_string_lossy().into_owned(),
                        false,
                        false,
                    )
                }
            } else {
                let resolved = match self.mode {
                    JobMode::Archive => default_archive_output_path(&input_path),
                    JobMode::Extract => default_extract_output_path(&input_path),
                };
                (
                    resolved.clone(),
                    resolved.to_string_lossy().into_owned(),
                    false,
                    false,
                )
            };

        Ok(JobConfig {
            mode: self.mode,
            input_path,
            output_path,
            output_display,
            ephemeral_output,
            null_output,
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

    fn enable_ephemeral_output(&mut self) {
        self.output_path = EPHEMERAL_OUTPUT_TOKEN.to_string();
        self.status_line =
            "output set to ephemeral temp path (written for speed test, auto-deleted)".to_string();
        self.log_line(self.status_line.clone());
    }

    fn enable_null_output(&mut self) {
        self.output_path = NULL_OUTPUT_TOKEN.to_string();
        self.status_line =
            "output set to null sink (discard output, no persistent writes)".to_string();
        self.log_line(self.status_line.clone());
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
        self.profile_samples.clear();
        self.log_lines.clear();
        self.logs_scroll_offset = 0;
        self.profile_scroll_offset = 0;
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
                    output_path
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
        let now = Instant::now();
        let key = format!("{}::{}", event.target, event.op);
        let aggregate = self
            .profile_aggregates
            .entry(key.clone())
            .or_insert_with(|| ProfileAggregate::new(key.clone(), event.target, event.op));

        aggregate.count = aggregate.count.saturating_add(1);
        aggregate.total_us = aggregate.total_us.saturating_add(event.elapsed_us);
        aggregate.max_us = aggregate.max_us.max(event.elapsed_us);
        aggregate.min_us = aggregate.min_us.min(event.elapsed_us);
        aggregate.avg_us = aggregate.total_us as f64 / aggregate.count.max(1) as f64;
        aggregate.last_result = event.result.to_string();
        aggregate.last_message = event.message.to_string();
        for tag in &event.tags {
            aggregate.tags_seen.insert(tag.to_ascii_lowercase());
        }

        let samples = self.profile_samples.entry(key).or_default();
        samples.push_back(ProfileSample {
            at: now,
            elapsed_us: event.elapsed_us,
        });
        while samples.len() > MAX_PROFILE_SAMPLES_PER_KEY {
            samples.pop_front();
        }

        self.profile_events.push_back(event);
        while self.profile_events.len() > MAX_PROFILE_EVENTS {
            self.profile_events.pop_front();
        }

        self.refresh_profile_windows();
    }

    fn refresh_profile_windows(&mut self) {
        let now = Instant::now();
        let window = Duration::from_secs(self.view.profile_window_seconds.max(1));

        for (key, samples) in &mut self.profile_samples {
            while let Some(front) = samples.front() {
                if now.duration_since(front.at) > window {
                    samples.pop_front();
                } else {
                    break;
                }
            }

            let Some(aggregate) = self.profile_aggregates.get_mut(key) else {
                continue;
            };

            if samples.is_empty() {
                aggregate.p50_us = 0;
                aggregate.p95_us = 0;
                aggregate.events_per_sec = 0.0;
                continue;
            }

            let mut values = samples
                .iter()
                .map(|sample| sample.elapsed_us)
                .collect::<Vec<_>>();
            values.sort_unstable();
            aggregate.p50_us = percentile(&values, 50.0);
            aggregate.p95_us = percentile(&values, 95.0);
            aggregate.events_per_sec = samples.len() as f64 / window.as_secs_f64().max(1e-6);
        }
    }

    fn profile_filter_matches(&self, aggregate: &ProfileAggregate) -> bool {
        if !matches_filter(&aggregate.target, &self.view.profile_target_filter) {
            return false;
        }
        if !matches_filter(&aggregate.op, &self.view.profile_op_filter) {
            return false;
        }

        let tag_filter = self.view.profile_tag_filter.trim().to_ascii_lowercase();
        if tag_filter.is_empty() || tag_filter == "all" {
            return true;
        }

        aggregate
            .tags_seen
            .iter()
            .any(|tag| tag.contains(&tag_filter))
    }

    fn clamp_scroll_offsets(&mut self) {
        let max_logs = self.log_lines.len().saturating_sub(1);
        self.logs_scroll_offset = self.logs_scroll_offset.min(max_logs);

        let profile_len = self.filtered_sorted_profile_rows().len();
        self.profile_scroll_offset = self
            .profile_scroll_offset
            .min(profile_len.saturating_sub(1));
    }

    fn log_line(&mut self, line: String) {
        self.log_lines.push_back(line);
        while self.log_lines.len() > MAX_LOG_LINES {
            self.log_lines.pop_front();
        }
    }
}

fn rect_contains(rect: ratatui::layout::Rect, x: u16, y: u16) -> bool {
    x >= rect.x
        && y >= rect.y
        && x < rect.x.saturating_add(rect.width)
        && y < rect.y.saturating_add(rect.height)
}

fn bump_usize(current: usize, delta: i32, min: usize, max: usize) -> usize {
    if delta > 0 {
        current.saturating_add(delta as usize).clamp(min, max)
    } else if delta < 0 {
        current
            .saturating_sub(delta.unsigned_abs() as usize)
            .clamp(min, max)
    } else {
        current.clamp(min, max)
    }
}

fn scroll_with_floor(current: usize, delta: i32) -> usize {
    if delta > 0 {
        current.saturating_add(delta as usize)
    } else if delta < 0 {
        current.saturating_sub(delta.unsigned_abs() as usize)
    } else {
        current
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

fn matches_filter(value: &str, filter: &str) -> bool {
    let filter = filter.trim().to_ascii_lowercase();
    if filter.is_empty() || filter == "all" {
        return true;
    }

    value.to_ascii_lowercase().contains(&filter)
}

fn percentile(values: &[u64], percentile: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }

    let rank = ((percentile / 100.0) * (values.len().saturating_sub(1) as f64)).round() as usize;
    values[rank.min(values.len().saturating_sub(1))]
}

fn cycle_filter(current: &mut String, presets: &[&str]) {
    let normalized = current.trim().to_ascii_lowercase();
    let idx = presets
        .iter()
        .position(|value| value.eq_ignore_ascii_case(&normalized))
        .unwrap_or(0);
    let next = (idx + 1) % presets.len();
    *current = presets[next].to_string();
}

fn push_history_point(history: &mut Vec<u64>, rate_bps: f64) {
    let point = rate_bps.max(0.0) as u64;
    history.push(point);
    if history.len() > MAX_HISTORY_POINTS {
        let extra = history.len() - MAX_HISTORY_POINTS;
        history.drain(0..extra);
    }
}
