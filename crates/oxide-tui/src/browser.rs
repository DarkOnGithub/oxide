use std::cmp::Ordering;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrowseTarget {
    Input,
    Output,
}

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub name: String,
    pub path: PathBuf,
    pub is_dir: bool,
    pub is_parent: bool,
}

#[derive(Debug, Clone)]
pub struct FileBrowser {
    pub target: BrowseTarget,
    current_dir: PathBuf,
    entries: Vec<FileEntry>,
    selected: usize,
    show_hidden: bool,
}

impl FileBrowser {
    pub fn new(target: BrowseTarget, start_path: Option<&Path>) -> Self {
        let current_dir = start_path
            .map(normalize_start_dir)
            .filter(|path| path.is_dir())
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));

        let mut browser = Self {
            target,
            current_dir,
            entries: Vec::new(),
            selected: 0,
            show_hidden: false,
        };
        browser.reload();
        browser
    }

    pub fn current_dir(&self) -> &Path {
        &self.current_dir
    }

    pub fn entries(&self) -> &[FileEntry] {
        &self.entries
    }

    pub fn selected(&self) -> usize {
        self.selected
    }

    pub fn show_hidden(&self) -> bool {
        self.show_hidden
    }

    pub fn toggle_hidden(&mut self) {
        self.show_hidden = !self.show_hidden;
        self.reload();
    }

    pub fn move_next(&mut self) {
        if self.entries.is_empty() {
            self.selected = 0;
        } else if self.selected + 1 < self.entries.len() {
            self.selected += 1;
        }
    }

    pub fn move_prev(&mut self) {
        if self.selected > 0 {
            self.selected -= 1;
        }
    }

    pub fn go_parent(&mut self) {
        if let Some(parent) = self.current_dir.parent() {
            self.current_dir = parent.to_path_buf();
            self.reload();
        }
    }

    pub fn reload(&mut self) {
        self.entries.clear();

        if let Some(parent) = self.current_dir.parent() {
            self.entries.push(FileEntry {
                name: "..".to_string(),
                path: parent.to_path_buf(),
                is_dir: true,
                is_parent: true,
            });
        }

        let dir_entries = fs::read_dir(&self.current_dir)
            .ok()
            .into_iter()
            .flat_map(|iter| iter.filter_map(Result::ok))
            .filter_map(|entry| {
                let path = entry.path();
                let metadata = entry.metadata().ok()?;
                let file_name = entry.file_name().to_string_lossy().into_owned();
                if !self.show_hidden && file_name.starts_with('.') {
                    return None;
                }

                Some(FileEntry {
                    name: file_name,
                    path,
                    is_dir: metadata.is_dir(),
                    is_parent: false,
                })
            })
            .collect::<Vec<_>>();

        let mut dir_entries = dir_entries;
        dir_entries.sort_by(|a, b| match (a.is_dir, b.is_dir) {
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            _ => a
                .name
                .to_ascii_lowercase()
                .cmp(&b.name.to_ascii_lowercase()),
        });

        self.entries.extend(dir_entries);

        if self.entries.is_empty() {
            self.selected = 0;
        } else {
            self.selected = self.selected.min(self.entries.len().saturating_sub(1));
        }
    }

    pub fn selected_entry(&self) -> Option<&FileEntry> {
        self.entries.get(self.selected)
    }

    pub fn choose_selected(&self) -> Option<PathBuf> {
        self.selected_entry().map(|entry| entry.path.clone())
    }

    pub fn choose_current_dir(&self) -> PathBuf {
        self.current_dir.clone()
    }

    /// Enter selected directory. Returns a file path when the selection is a file.
    pub fn enter_selected(&mut self) -> Option<PathBuf> {
        let entry = self.selected_entry()?.clone();
        if entry.is_dir {
            self.current_dir = entry.path;
            self.reload();
            None
        } else {
            Some(entry.path)
        }
    }
}

fn normalize_start_dir(path: &Path) -> PathBuf {
    if path.is_dir() {
        path.to_path_buf()
    } else if path.exists() {
        path.parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."))
    } else {
        path.parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."))
    }
}
