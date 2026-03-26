use std::collections::HashSet;
use std::fs;
use std::io::{BufWriter, Write};
use std::num::NonZeroUsize;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use regex::RegexSet;

use crate::OxideError;
use crate::format::ArchiveManifest;
use crate::types::Result;

use super::super::directory;
use super::reorder_writer::OrderedChunkWriter;

pub(crate) const OUTPUT_BUFFER_CAPACITY: usize = 1024 * 1024;
const PARALLEL_METADATA_MIN_ITEMS: usize = 8;
const MAX_METADATA_WORKERS: usize = 8;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct DirectoryRestoreStats {
    pub(crate) directory_decode: Duration,
    pub(crate) ordered_write_time: Duration,
    pub(crate) output_write: Duration,
    pub(crate) output_create: Duration,
    pub(crate) output_data: Duration,
    pub(crate) output_flush: Duration,
    pub(crate) output_metadata: Duration,
    pub(crate) output_bytes_total: u64,
    pub(crate) entry_count: u64,
}

impl DirectoryRestoreStats {
    fn record_output_create(&mut self, elapsed: Duration) {
        self.output_write += elapsed;
        self.output_create += elapsed;
    }

    fn record_output_data(&mut self, elapsed: Duration) {
        self.output_write += elapsed;
        self.output_data += elapsed;
    }

    fn record_output_flush(&mut self, elapsed: Duration) {
        self.output_write += elapsed;
        self.output_flush += elapsed;
    }

    fn record_output_metadata(&mut self, elapsed: Duration) {
        self.output_write += elapsed;
        self.output_metadata += elapsed;
    }
}

#[derive(Debug)]
pub(crate) struct DirectoryExtractSelection {
    manifest: ArchiveManifest,
    selected_ranges: Vec<Range<u64>>,
}

impl DirectoryExtractSelection {
    pub(crate) fn from_filters<S: AsRef<str>, T: AsRef<str>>(
        manifest: &ArchiveManifest,
        filters: &[S],
        regex_filters: &[T],
    ) -> Result<Self> {
        let normalized_filters = normalize_filters(filters)?;
        let regex_set = compile_regex_filters(regex_filters)?;
        if normalized_filters.is_empty() && regex_set.is_none() {
            return Err(anyhow!("path filters cannot be empty").into());
        }

        let mut entries = Vec::new();
        let mut selected_ranges = Vec::new();
        for entry in manifest.entries() {
            let selected = normalized_filters
                .iter()
                .any(|filter| path_matches_filter(&entry.path, filter))
                || regex_set
                    .as_ref()
                    .is_some_and(|patterns| patterns.is_match(&entry.path));

            if selected {
                entries.push(entry.clone());
                if matches!(entry.kind, crate::ArchiveEntryKind::File) && entry.size > 0 {
                    selected_ranges.push(entry.content_range());
                }
            }
        }

        if entries.is_empty() {
            return Err(anyhow!("path filters did not match any archive entries").into());
        }

        Ok(Self {
            manifest: ArchiveManifest::new(entries),
            selected_ranges,
        })
    }

    pub(crate) fn selected_ranges(&self) -> &[Range<u64>] {
        &self.selected_ranges
    }

    pub(crate) fn into_manifest(self) -> ArchiveManifest {
        self.manifest
    }
}

#[derive(Debug)]
struct PendingFile {
    remaining: u64,
    writer: BufWriter<fs::File>,
    path: PathBuf,
    entry: crate::ArchiveListingEntry,
}

#[derive(Debug)]
struct PendingDirectory {
    path: PathBuf,
    entry: crate::ArchiveListingEntry,
}

#[derive(Debug)]
struct PendingMetadata {
    path: PathBuf,
    entry: crate::ArchiveListingEntry,
}

#[derive(Debug)]
pub(crate) struct DirectoryRestoreWriter {
    root: PathBuf,
    entries: Vec<crate::ArchiveListingEntry>,
    next_entry: usize,
    pending_file: Option<PendingFile>,
    created_directories: HashSet<PathBuf>,
    pending_metadata: Vec<PendingMetadata>,
    pending_directories: Vec<PendingDirectory>,
    stats: DirectoryRestoreStats,
}

#[derive(Debug)]
pub(crate) struct FilteredDirectoryRestoreWriter {
    inner: DirectoryRestoreWriter,
    selected_ranges: Vec<Range<u64>>,
    next_range: usize,
    input_offset: u64,
}

impl DirectoryRestoreWriter {
    pub(crate) fn create(root: &Path, manifest: ArchiveManifest) -> Result<Self> {
        let mut writer = Self {
            root: root.to_path_buf(),
            entries: manifest.entries().to_vec(),
            next_entry: 0,
            pending_file: None,
            created_directories: HashSet::new(),
            pending_metadata: Vec::new(),
            pending_directories: Vec::new(),
            stats: DirectoryRestoreStats::default(),
        };
        writer.ensure_directory_exists(root)?;
        writer.prepare_output_directories()?;
        Ok(writer)
    }

    pub(crate) fn finish(&mut self) -> Result<DirectoryRestoreStats> {
        self.advance_entries()?;
        if let Some(pending) = self.pending_file.as_ref() {
            if pending.remaining > 0 {
                return Err(crate::OxideError::InvalidFormat(
                    "truncated file payload during directory restore",
                ));
            }
        }
        if self.pending_file.is_some() || self.next_entry != self.entries.len() {
            return Err(crate::OxideError::InvalidFormat(
                "directory restore ended before all entries completed",
            ));
        }
        self.finalize_files()?;
        self.finalize_directories()?;
        Ok(self.stats)
    }

    fn ensure_directory_exists(&mut self, path: &Path) -> Result<()> {
        if path.as_os_str().is_empty() || self.created_directories.contains(path) {
            return Ok(());
        }

        let started = Instant::now();
        fs::create_dir_all(path)?;
        self.stats.record_output_create(started.elapsed());

        let mut current = Some(path);
        while let Some(directory) = current {
            if !directory.starts_with(&self.root) {
                break;
            }
            if !self.created_directories.insert(directory.to_path_buf()) {
                break;
            }
            current = directory.parent();
        }

        Ok(())
    }

    fn defer_metadata(&mut self, path: PathBuf, entry: crate::ArchiveListingEntry) {
        self.pending_metadata.push(PendingMetadata { path, entry });
    }

    fn prepare_output_directories(&mut self) -> Result<()> {
        for entry in self.entries.clone() {
            let out_path = directory::join_safe(&self.root, &entry.path)?;
            match entry.kind {
                crate::ArchiveEntryKind::Directory => self.ensure_directory_exists(&out_path)?,
                crate::ArchiveEntryKind::File | crate::ArchiveEntryKind::Symlink => {
                    if let Some(parent) = out_path.parent() {
                        self.ensure_directory_exists(parent)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn advance_entries(&mut self) -> Result<()> {
        while self.pending_file.is_none() && self.next_entry < self.entries.len() {
            let entry = self.entries[self.next_entry].clone();
            self.next_entry += 1;
            self.stats.entry_count = self.stats.entry_count.saturating_add(1);

            match entry.kind {
                crate::ArchiveEntryKind::Directory => {
                    let out_path = directory::join_safe(&self.root, &entry.path)?;
                    self.ensure_directory_exists(&out_path)?;
                    self.pending_directories.push(PendingDirectory {
                        path: out_path,
                        entry,
                    });
                }
                crate::ArchiveEntryKind::File => {
                    let out_path = directory::join_safe(&self.root, &entry.path)?;
                    if let Some(parent) = out_path.parent() {
                        self.ensure_directory_exists(parent)?;
                    }

                    let output_started = Instant::now();
                    let file = fs::File::create(&out_path)?;
                    self.stats.record_output_create(output_started.elapsed());

                    if entry.size == 0 {
                        self.defer_metadata(out_path, entry);
                        continue;
                    }

                    self.pending_file = Some(PendingFile {
                        remaining: entry.size,
                        writer: BufWriter::with_capacity(OUTPUT_BUFFER_CAPACITY, file),
                        path: out_path,
                        entry,
                    });
                }
                crate::ArchiveEntryKind::Symlink => {
                    let out_path = directory::join_safe(&self.root, &entry.path)?;
                    if let Some(parent) = out_path.parent() {
                        self.ensure_directory_exists(parent)?;
                    }

                    let target = entry.target.as_deref().ok_or(OxideError::InvalidFormat(
                        "symlink manifest entry missing target",
                    ))?;
                    create_symlink(&out_path, target)?;
                    self.defer_metadata(out_path, entry);
                }
            }
        }

        Ok(())
    }

    fn finalize_pending_file(&mut self) -> Result<()> {
        let pending = self.pending_file.take().ok_or(OxideError::InvalidFormat(
            "directory restore missing pending file after write completion",
        ))?;

        let output_started = Instant::now();
        let PendingFile {
            writer,
            path,
            entry,
            ..
        } = pending;
        let _file = writer.into_inner().map_err(|error| error.into_error())?;
        self.stats.record_output_flush(output_started.elapsed());
        self.defer_metadata(path, entry);
        Ok(())
    }

    fn finalize_files(&mut self) -> Result<()> {
        let pending = self.pending_metadata.drain(..).collect::<Vec<_>>();
        if pending.is_empty() {
            return Ok(());
        }

        let started = Instant::now();
        apply_pending_metadata_in_parallel(&pending)?;
        self.stats.record_output_metadata(started.elapsed());
        Ok(())
    }

    fn finalize_directories(&mut self) -> Result<()> {
        while let Some(directory) = self.pending_directories.pop() {
            let started = Instant::now();
            apply_entry_metadata(&directory.path, &directory.entry)?;
            self.stats.record_output_metadata(started.elapsed());
        }
        Ok(())
    }
}

fn apply_pending_metadata_in_parallel(entries: &[PendingMetadata]) -> Result<()> {
    if entries.len() < PARALLEL_METADATA_MIN_ITEMS {
        for entry in entries {
            apply_entry_metadata(&entry.path, &entry.entry)?;
        }
        return Ok(());
    }

    let worker_count = available_metadata_workers(entries.len());
    if worker_count <= 1 {
        for entry in entries {
            apply_entry_metadata(&entry.path, &entry.entry)?;
        }
        return Ok(());
    }

    let chunk_size = entries.len().div_ceil(worker_count);
    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for chunk in entries.chunks(chunk_size) {
            handles.push(scope.spawn(move || -> Result<()> {
                for entry in chunk {
                    apply_entry_metadata(&entry.path, &entry.entry)?;
                }
                Ok(())
            }));
        }

        for handle in handles {
            match handle.join() {
                Ok(result) => result?,
                Err(payload) => {
                    let details = if let Some(message) = payload.downcast_ref::<&str>() {
                        (*message).to_string()
                    } else if let Some(message) = payload.downcast_ref::<String>() {
                        message.clone()
                    } else {
                        "unknown panic payload".to_string()
                    };
                    return Err(crate::OxideError::CompressionError(format!(
                        "metadata worker thread panicked: {details}"
                    )));
                }
            }
        }

        Ok(())
    })
}

fn available_metadata_workers(item_count: usize) -> usize {
    thread::available_parallelism()
        .unwrap_or(NonZeroUsize::MIN)
        .get()
        .min(MAX_METADATA_WORKERS)
        .min(item_count)
        .max(1)
}

impl FilteredDirectoryRestoreWriter {
    pub(crate) fn create(
        root: &Path,
        manifest: ArchiveManifest,
        selected_ranges: Vec<Range<u64>>,
    ) -> Result<Self> {
        Ok(Self {
            inner: DirectoryRestoreWriter::create(root, manifest)?,
            selected_ranges,
            next_range: 0,
            input_offset: 0,
        })
    }

    pub(crate) fn finish(&mut self) -> Result<DirectoryRestoreStats> {
        self.inner.finish()
    }
}

impl OrderedChunkWriter for DirectoryRestoreWriter {
    fn write_chunk(&mut self, mut bytes: &[u8]) -> Result<()> {
        let ordered_write_started = Instant::now();
        let decode_started = Instant::now();
        self.advance_entries()?;
        self.stats.directory_decode += decode_started.elapsed();

        while !bytes.is_empty() {
            let finished_file = {
                let pending = self.pending_file.as_mut().ok_or(OxideError::InvalidFormat(
                    "directory archive contains file data beyond manifest entries",
                ))?;

                let take = bytes.len().min(pending.remaining as usize);
                let output_started = Instant::now();
                pending.writer.write_all(&bytes[..take])?;
                self.stats.record_output_data(output_started.elapsed());
                self.stats.output_bytes_total =
                    self.stats.output_bytes_total.saturating_add(take as u64);
                pending.remaining -= take as u64;
                bytes = &bytes[take..];
                pending.remaining == 0
            };

            if finished_file {
                self.finalize_pending_file()?;
                let decode_started = Instant::now();
                self.advance_entries()?;
                self.stats.directory_decode += decode_started.elapsed();
            }
        }

        self.stats.ordered_write_time += ordered_write_started.elapsed();

        Ok(())
    }
}

impl OrderedChunkWriter for FilteredDirectoryRestoreWriter {
    fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        let chunk_start = self.input_offset;
        let chunk_end = chunk_start.saturating_add(bytes.len() as u64);

        while self.next_range < self.selected_ranges.len()
            && self.selected_ranges[self.next_range].end <= chunk_start
        {
            self.next_range += 1;
        }

        let mut range_index = self.next_range;
        while range_index < self.selected_ranges.len() {
            let range = &self.selected_ranges[range_index];
            if range.start >= chunk_end {
                break;
            }

            let write_start = range.start.max(chunk_start) - chunk_start;
            let write_end = range.end.min(chunk_end) - chunk_start;
            if write_start < write_end {
                self.inner
                    .write_chunk(&bytes[write_start as usize..write_end as usize])?;
            }

            if range.end <= chunk_end {
                range_index += 1;
            } else {
                break;
            }
        }

        self.next_range = range_index;
        self.input_offset = chunk_end;
        Ok(())
    }
}

fn normalize_filters<S: AsRef<str>>(filters: &[S]) -> Result<Vec<String>> {
    filters
        .iter()
        .map(|filter| normalize_filter(filter.as_ref()))
        .collect()
}

fn compile_regex_filters<S: AsRef<str>>(filters: &[S]) -> Result<Option<RegexSet>> {
    if filters.is_empty() {
        return Ok(None);
    }

    let patterns = filters
        .iter()
        .map(|filter| filter.as_ref().trim())
        .collect::<Vec<_>>();
    if patterns.iter().any(|pattern| pattern.is_empty()) {
        return Err(anyhow!("regex filters must be non-empty").into());
    }

    let set = RegexSet::new(&patterns)
        .map_err(|error| crate::OxideError::Other(anyhow!("invalid regex filter: {error}")))?;
    Ok(Some(set))
}

fn normalize_filter(raw: &str) -> Result<String> {
    let replaced = raw.trim().replace('\\', "/");
    if replaced.is_empty() {
        return Err(anyhow!("path filters must be non-empty relative paths").into());
    }

    let mut parts = Vec::new();
    for part in replaced.split('/') {
        match part {
            "" | "." => continue,
            ".." => {
                return Err(
                    anyhow!("path filters must not contain parent directory traversal").into(),
                );
            }
            _ => parts.push(part),
        }
    }

    if parts.is_empty() || raw.trim_start().starts_with('/') {
        return Err(anyhow!("path filters must be non-empty relative paths").into());
    }

    Ok(parts.join("/"))
}

fn path_matches_filter(path: &str, filter: &str) -> bool {
    path == filter
        || path
            .strip_prefix(filter)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

pub(crate) fn apply_entry_metadata(path: &Path, entry: &crate::ArchiveListingEntry) -> Result<()> {
    match entry.kind {
        crate::ArchiveEntryKind::Symlink => {
            apply_owner_nofollow(path, entry.uid, entry.gid)?;
            apply_mode_symlink(path, entry.mode)?;
            apply_mtime_nofollow(path, entry.mtime)?;
        }
        _ => {
            apply_owner(path, entry.uid, entry.gid)?;
            apply_mode(path, entry.mode)?;
            apply_mtime(path, entry.mtime)?;
        }
    }
    Ok(())
}

fn apply_mode(path: &Path, mode: u32) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
    }

    #[cfg(not(unix))]
    {
        let mut permissions = fs::metadata(path)?.permissions();
        permissions.set_readonly(mode & 0o200 == 0);
        fs::set_permissions(path, permissions)?;
    }

    Ok(())
}

#[cfg(unix)]
fn apply_owner(path: &Path, uid: u32, gid: u32) -> Result<()> {
    use std::ffi::CString;
    use std::io;
    use std::os::unix::ffi::OsStrExt;

    if should_apply_owner(uid, gid) == Some(false) {
        return Ok(());
    }

    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| OxideError::InvalidFormat("path contains interior nul byte"))?;
    let status = unsafe { libc::chown(path.as_ptr(), uid, gid) };
    if status == 0 {
        return Ok(());
    }

    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::EPERM | libc::EACCES | libc::ENOTSUP | libc::EINVAL) => Ok(()),
        _ => Err(error.into()),
    }
}

#[cfg(not(unix))]
fn apply_owner(_: &Path, _: u32, _: u32) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn apply_owner_nofollow(path: &Path, uid: u32, gid: u32) -> Result<()> {
    use std::ffi::CString;
    use std::io;
    use std::os::unix::ffi::OsStrExt;

    if should_apply_owner(uid, gid) == Some(false) {
        return Ok(());
    }

    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| OxideError::InvalidFormat("path contains interior nul byte"))?;
    let status = unsafe { libc::lchown(path.as_ptr(), uid, gid) };
    if status == 0 {
        return Ok(());
    }

    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::EPERM | libc::EACCES | libc::ENOTSUP | libc::EINVAL) => Ok(()),
        _ => Err(error.into()),
    }
}

#[cfg(not(unix))]
fn apply_owner_nofollow(_: &Path, _: u32, _: u32) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn should_apply_owner(uid: u32, gid: u32) -> Option<bool> {
    let effective_uid = unsafe { libc::geteuid() };
    let effective_gid = unsafe { libc::getegid() };

    if uid == effective_uid && gid == effective_gid {
        return Some(false);
    }

    if effective_uid != 0 && uid != effective_uid {
        return Some(false);
    }

    Some(true)
}

#[cfg(unix)]
fn apply_mtime(path: &Path, mtime: crate::ArchiveTimestamp) -> Result<()> {
    use std::ffi::CString;
    use std::io;
    use std::os::unix::ffi::OsStrExt;

    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| OxideError::InvalidFormat("path contains interior nul byte"))?;
    let times = [
        libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_OMIT,
        },
        libc::timespec {
            tv_sec: mtime.seconds as libc::time_t,
            tv_nsec: mtime.nanoseconds as libc::c_long,
        },
    ];
    let status = unsafe { libc::utimensat(libc::AT_FDCWD, path.as_ptr(), times.as_ptr(), 0) };
    if status == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error().into())
    }
}

#[cfg(not(unix))]
fn apply_mtime(_: &Path, _: crate::ArchiveTimestamp) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn apply_mtime_nofollow(path: &Path, mtime: crate::ArchiveTimestamp) -> Result<()> {
    use std::ffi::CString;
    use std::io;
    use std::os::unix::ffi::OsStrExt;

    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| OxideError::InvalidFormat("path contains interior nul byte"))?;
    let times = [
        libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_OMIT,
        },
        libc::timespec {
            tv_sec: mtime.seconds as libc::time_t,
            tv_nsec: mtime.nanoseconds as libc::c_long,
        },
    ];
    let status = unsafe {
        libc::utimensat(
            libc::AT_FDCWD,
            path.as_ptr(),
            times.as_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if status == 0 {
        Ok(())
    } else {
        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::EPERM | libc::EACCES | libc::ENOTSUP | libc::EINVAL) => Ok(()),
            _ => Err(error.into()),
        }
    }
}

#[cfg(not(unix))]
fn apply_mtime_nofollow(_: &Path, _: crate::ArchiveTimestamp) -> Result<()> {
    Ok(())
}

fn apply_mode_symlink(_: &Path, _: u32) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn create_symlink(path: &Path, target: &str) -> Result<()> {
    std::os::unix::fs::symlink(target, path)?;
    Ok(())
}

#[cfg(windows)]
fn create_symlink(path: &Path, target: &str) -> Result<()> {
    use std::fs;

    let resolved_target = path.parent().unwrap_or_else(|| Path::new("")).join(target);
    if fs::metadata(&resolved_target)
        .map(|metadata| metadata.is_dir())
        .unwrap_or(false)
    {
        std::os::windows::fs::symlink_dir(target, path)?;
    } else {
        std::os::windows::fs::symlink_file(target, path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::reorder_writer::OrderedChunkWriter;
    use super::DirectoryRestoreWriter;
    use crate::{ArchiveEntryKind, ArchiveListingEntry, ArchiveManifest, ArchiveTimestamp};
    use std::fs;
    use std::io::Read;
    use std::path::Path;
    use tempfile::tempdir;

    #[cfg(unix)]
    #[test]
    fn restore_creates_symlink_entries() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path();
        let manifest = ArchiveManifest::new(vec![ArchiveListingEntry {
            path: "link".to_string(),
            kind: ArchiveEntryKind::Symlink,
            target: Some("target.txt".to_string()),
            size: 0,
            mode: 0o777,
            mtime: ArchiveTimestamp::default(),
            uid: 0,
            gid: 0,
            content_offset: 0,
        }]);

        let mut writer = DirectoryRestoreWriter::create(root, manifest).expect("create writer");
        writer.finish().expect("finish restore");

        let link_path = root.join("link");
        let metadata = fs::symlink_metadata(&link_path).expect("symlink metadata");
        assert!(metadata.file_type().is_symlink());
        assert_eq!(
            fs::read_link(&link_path).expect("read link"),
            Path::new("target.txt")
        );
    }

    #[test]
    fn restore_creates_implicit_parent_directories() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path();
        let manifest = ArchiveManifest::new(vec![ArchiveListingEntry::file(
            "nested/deeper/file.txt".to_string(),
            4,
            0o644,
            ArchiveTimestamp::default(),
            0,
            0,
            0,
        )]);

        let mut writer = DirectoryRestoreWriter::create(root, manifest).expect("create writer");
        writer.write_chunk(b"test").expect("write chunk");
        writer.finish().expect("finish restore");

        let mut restored = String::new();
        fs::File::open(root.join("nested/deeper/file.txt"))
            .expect("open restored file")
            .read_to_string(&mut restored)
            .expect("read restored file");
        assert_eq!(restored, "test");
    }

    #[test]
    fn restore_multiple_files_completes_metadata_finalization() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path();
        let mut entries = Vec::new();
        let mut payload = Vec::new();
        for index in 0..8 {
            let name = format!("dir/file-{index}.txt");
            entries.push(ArchiveListingEntry::file(
                name,
                1,
                0o644,
                ArchiveTimestamp::default(),
                0,
                0,
                index as u64,
            ));
            payload.push(b'a' + index as u8);
        }

        let manifest = ArchiveManifest::new(entries);
        let mut writer = DirectoryRestoreWriter::create(root, manifest).expect("create writer");
        writer.write_chunk(&payload).expect("write chunk");
        writer.finish().expect("finish restore");

        for index in 0..8 {
            let path = root.join(format!("dir/file-{index}.txt"));
            assert_eq!(
                fs::read(&path).expect("read restored file"),
                vec![b'a' + index as u8]
            );
        }
    }
}
