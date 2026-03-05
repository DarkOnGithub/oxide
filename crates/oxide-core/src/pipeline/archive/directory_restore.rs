use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::types::Result;

use super::super::directory;
use super::reorder_writer::OrderedChunkWriter;

const DIRECTORY_HEADER_LEN: usize = 10;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct DirectoryRestoreStats {
    pub(crate) directory_decode: Duration,
    pub(crate) output_write: Duration,
    pub(crate) output_bytes_total: u64,
    pub(crate) entry_count: u64,
}

#[derive(Debug)]
enum RestoreState {
    Header,
    EntryKind,
    PathLen {
        kind: u8,
    },
    Path {
        kind: u8,
        len: usize,
    },
    FileSize {
        rel_path: String,
    },
    FileData {
        remaining: usize,
        writer: BufWriter<fs::File>,
    },
    Done,
}

#[derive(Debug)]
pub(crate) struct DirectoryRestoreWriter {
    root: PathBuf,
    pending: Vec<u8>,
    entries_remaining: u32,
    state: RestoreState,
    stats: DirectoryRestoreStats,
}

impl DirectoryRestoreWriter {
    pub(crate) fn create(root: &Path) -> Result<Self> {
        fs::create_dir_all(root)?;
        Ok(Self {
            root: root.to_path_buf(),
            pending: Vec::new(),
            entries_remaining: 0,
            state: RestoreState::Header,
            stats: DirectoryRestoreStats::default(),
        })
    }

    pub(crate) fn finish(&mut self) -> Result<DirectoryRestoreStats> {
        self.consume_pending()?;
        if !self.pending.is_empty() {
            return Err(crate::OxideError::InvalidFormat(
                "directory bundle has trailing data",
            ));
        }

        match self.state {
            RestoreState::Done if self.entries_remaining == 0 => Ok(self.stats),
            _ => Err(crate::OxideError::InvalidFormat(
                "truncated directory bundle during restore",
            )),
        }
    }

    fn consume_pending(&mut self) -> Result<()> {
        let mut cursor = 0usize;

        loop {
            if cursor == self.pending.len() {
                break;
            }

            match &mut self.state {
                RestoreState::Header => {
                    if self.pending.len() - cursor < DIRECTORY_HEADER_LEN {
                        break;
                    }

                    let decode_started = Instant::now();
                    let header = &self.pending[cursor..cursor + DIRECTORY_HEADER_LEN];
                    if header[..4] != directory::DIRECTORY_BUNDLE_MAGIC {
                        return Err(crate::OxideError::InvalidFormat(
                            "archive payload is not a directory bundle",
                        ));
                    }

                    let version = u16::from_le_bytes([header[4], header[5]]);
                    if version != directory::DIRECTORY_BUNDLE_VERSION {
                        return Err(crate::OxideError::InvalidFormat(
                            "unsupported directory bundle version",
                        ));
                    }

                    self.entries_remaining =
                        u32::from_le_bytes([header[6], header[7], header[8], header[9]]);
                    self.state = if self.entries_remaining == 0 {
                        RestoreState::Done
                    } else {
                        RestoreState::EntryKind
                    };
                    self.stats.directory_decode += decode_started.elapsed();
                    cursor += DIRECTORY_HEADER_LEN;
                }
                RestoreState::EntryKind => {
                    if self.entries_remaining == 0 {
                        self.state = RestoreState::Done;
                        continue;
                    }

                    let decode_started = Instant::now();
                    let kind = self.pending[cursor];
                    if kind > 1 {
                        return Err(crate::OxideError::InvalidFormat(
                            "invalid directory bundle entry kind",
                        ));
                    }
                    self.state = RestoreState::PathLen { kind };
                    self.stats.directory_decode += decode_started.elapsed();
                    cursor += 1;
                }
                RestoreState::PathLen { kind } => {
                    if self.pending.len() - cursor < 4 {
                        break;
                    }

                    let decode_started = Instant::now();
                    let len = u32::from_le_bytes([
                        self.pending[cursor],
                        self.pending[cursor + 1],
                        self.pending[cursor + 2],
                        self.pending[cursor + 3],
                    ]) as usize;
                    self.state = RestoreState::Path { kind: *kind, len };
                    self.stats.directory_decode += decode_started.elapsed();
                    cursor += 4;
                }
                RestoreState::Path { kind, len } => {
                    if self.pending.len() - cursor < *len {
                        break;
                    }

                    let decode_started = Instant::now();
                    let rel_path = std::str::from_utf8(&self.pending[cursor..cursor + *len])
                        .map_err(|_| {
                            crate::OxideError::InvalidFormat("directory path is not utf8")
                        })?
                        .to_string();
                    self.stats.directory_decode += decode_started.elapsed();
                    cursor += *len;

                    if *kind == 0 {
                        let output_started = Instant::now();
                        let out_path = directory::join_safe(&self.root, &rel_path)?;
                        fs::create_dir_all(out_path)?;
                        self.stats.output_write += output_started.elapsed();
                        self.complete_entry();
                    } else {
                        self.state = RestoreState::FileSize { rel_path };
                    }
                }
                RestoreState::FileSize { rel_path } => {
                    if self.pending.len() - cursor < 8 {
                        break;
                    }

                    let decode_started = Instant::now();
                    let size = u64::from_le_bytes([
                        self.pending[cursor],
                        self.pending[cursor + 1],
                        self.pending[cursor + 2],
                        self.pending[cursor + 3],
                        self.pending[cursor + 4],
                        self.pending[cursor + 5],
                        self.pending[cursor + 6],
                        self.pending[cursor + 7],
                    ]);
                    let remaining = usize::try_from(size)
                        .map_err(|_| crate::OxideError::InvalidFormat("file size overflow"))?;
                    self.stats.directory_decode += decode_started.elapsed();
                    cursor += 8;

                    let output_started = Instant::now();
                    let out_path = directory::join_safe(&self.root, rel_path)?;
                    if let Some(parent) = out_path.parent() {
                        fs::create_dir_all(parent)?;
                    }
                    let file = fs::File::create(out_path)?;
                    self.stats.output_write += output_started.elapsed();

                    if remaining == 0 {
                        self.complete_entry();
                    } else {
                        self.state = RestoreState::FileData {
                            remaining,
                            writer: BufWriter::new(file),
                        };
                    }
                }
                RestoreState::FileData { remaining, writer } => {
                    let available = self.pending.len() - cursor;
                    if available == 0 {
                        break;
                    }

                    let take = available.min(*remaining);
                    let output_started = Instant::now();
                    writer.write_all(&self.pending[cursor..cursor + take])?;
                    self.stats.output_write += output_started.elapsed();
                    self.stats.output_bytes_total =
                        self.stats.output_bytes_total.saturating_add(take as u64);
                    cursor += take;
                    *remaining -= take;

                    if *remaining == 0 {
                        let output_started = Instant::now();
                        writer.flush()?;
                        self.stats.output_write += output_started.elapsed();
                        self.complete_entry();
                    }
                }
                RestoreState::Done => {
                    return Err(crate::OxideError::InvalidFormat(
                        "directory bundle has trailing data",
                    ));
                }
            }
        }

        if cursor > 0 {
            self.pending.drain(..cursor);
        }

        Ok(())
    }

    fn complete_entry(&mut self) {
        self.stats.entry_count = self.stats.entry_count.saturating_add(1);
        self.entries_remaining = self.entries_remaining.saturating_sub(1);
        self.state = if self.entries_remaining == 0 {
            RestoreState::Done
        } else {
            RestoreState::EntryKind
        };
    }
}

impl OrderedChunkWriter for DirectoryRestoreWriter {
    fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        self.pending.extend_from_slice(bytes);
        self.consume_pending()
    }
}
