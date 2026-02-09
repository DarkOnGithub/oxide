use std::fs;
use std::path::{Component, Path, PathBuf};

use bytes::Bytes;
use jwalk::WalkDir;
use rayon::prelude::*;

use crate::types::{Batch, FileFormat, Result};

use super::types::ArchiveSourceKind;

pub(super) const DIRECTORY_BUNDLE_MAGIC: [u8; 4] = *b"OXDB";
pub(super) const DIRECTORY_BUNDLE_VERSION: u16 = 1;
pub(super) const SOURCE_KIND_DIRECTORY_FLAG: u32 = 1 << 0;
const PARALLEL_DIRECTORY_READ_THRESHOLD: usize = 1024;
pub(super) const STREAM_READ_BUFFER_SIZE: usize = 4 * 1024 * 1024;

#[derive(Debug)]
pub(super) enum DirectoryBundleEntry {
    Directory { rel_path: String },
    File { rel_path: String, data: Vec<u8> },
}

#[derive(Debug, Clone)]
pub(super) struct DirectoryFileSpec {
    pub(super) rel_path: String,
    pub(super) full_path: PathBuf,
    pub(super) size: u64,
}

#[derive(Debug, Clone)]
pub(super) struct DirectoryDiscovery {
    pub(super) root: PathBuf,
    pub(super) directories: Vec<String>,
    pub(super) files: Vec<DirectoryFileSpec>,
    pub(super) input_bytes_total: u64,
}

pub(super) struct DirectoryBatchSubmitter {
    source_path: PathBuf,
    block_size: usize,
    next_block_id: usize,
    pending: Vec<u8>,
}

impl DirectoryBatchSubmitter {
    pub(super) fn new(source_path: PathBuf, block_size: usize) -> Self {
        Self {
            source_path,
            block_size: block_size.max(1),
            next_block_id: 0,
            pending: Vec::with_capacity(block_size.max(1)),
        }
    }

    pub(super) fn push_bytes<F>(&mut self, mut bytes: &[u8], mut submit: F) -> Result<()>
    where
        F: FnMut(Batch) -> Result<()>,
    {
        while !bytes.is_empty() {
            let room = self.block_size.saturating_sub(self.pending.len()).max(1);
            let take = room.min(bytes.len());
            self.pending.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];

            if self.pending.len() == self.block_size {
                self.flush_pending(&mut submit)?;
            }
        }

        Ok(())
    }

    pub(super) fn finish<F>(&mut self, submit: F) -> Result<()>
    where
        F: FnMut(Batch) -> Result<()>,
    {
        if !self.pending.is_empty() {
            self.flush_pending(submit)?;
        }
        Ok(())
    }

    fn flush_pending<F>(&mut self, mut submit: F) -> Result<()>
    where
        F: FnMut(Batch) -> Result<()>,
    {
        let chunk = std::mem::replace(&mut self.pending, Vec::with_capacity(self.block_size));
        let batch = Batch::with_hint(
            self.next_block_id,
            self.source_path.clone(),
            Bytes::from(chunk),
            FileFormat::Binary,
        );
        submit(batch)?;
        self.next_block_id += 1;
        Ok(())
    }
}

pub(super) fn decode_directory_entries(
    payload: &[u8],
    flags: u32,
) -> Result<Option<Vec<DirectoryBundleEntry>>> {
    if flags & SOURCE_KIND_DIRECTORY_FLAG != 0 {
        return decode_directory_bundle(payload).map(Some);
    }

    match decode_directory_bundle(payload) {
        Ok(entries) => Ok(Some(entries)),
        Err(_) => Ok(None),
    }
}

pub(super) fn write_directory_entries(
    root: &Path,
    entries: Vec<DirectoryBundleEntry>,
) -> Result<()> {
    fs::create_dir_all(root)?;

    for entry in entries {
        match entry {
            DirectoryBundleEntry::Directory { rel_path } => {
                let out_path = join_safe(root, &rel_path)?;
                fs::create_dir_all(out_path)?;
            }
            DirectoryBundleEntry::File { rel_path, data } => {
                let out_path = join_safe(root, &rel_path)?;
                if let Some(parent) = out_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(out_path, data)?;
            }
        }
    }

    Ok(())
}

pub(super) fn source_kind_flags(source_kind: ArchiveSourceKind) -> u32 {
    match source_kind {
        ArchiveSourceKind::File => 0,
        ArchiveSourceKind::Directory => SOURCE_KIND_DIRECTORY_FLAG,
    }
}

pub(super) fn discover_directory_tree(root: &Path) -> Result<DirectoryDiscovery> {
    if !root.is_dir() {
        return Err(crate::OxideError::InvalidFormat(
            "archive_directory expects a directory path",
        ));
    }

    let mut directory_paths = Vec::<PathBuf>::new();
    let mut file_paths = Vec::<PathBuf>::new();

    for entry in WalkDir::new(root) {
        let entry = entry.map_err(anyhow::Error::from)?;
        let path = entry.path();
        if path == root {
            continue;
        }

        let rel_path = path
            .strip_prefix(root)
            .map_err(|_| crate::OxideError::InvalidFormat("invalid relative path"))?
            .to_path_buf();

        if entry.file_type().is_dir() {
            directory_paths.push(rel_path);
        } else if entry.file_type().is_file() {
            file_paths.push(rel_path);
        } else {
            return Err(crate::OxideError::InvalidFormat(
                "directory archive supports regular files/directories only",
            ));
        }
    }

    directory_paths.sort();
    file_paths.sort();

    let mut input_bytes_total = 10u64;
    let mut directories = Vec::with_capacity(directory_paths.len());
    for directory_rel in directory_paths {
        let rel_path = relative_path_to_utf8(&directory_rel)?;
        input_bytes_total = accumulate_bundle_size(input_bytes_total, rel_path.len())?;
        directories.push(rel_path);
    }

    let mut files = Vec::with_capacity(file_paths.len());
    for file_rel in file_paths {
        let rel_path = relative_path_to_utf8(&file_rel)?;
        let full_path = root.join(&file_rel);
        let size = fs::metadata(&full_path)?.len();
        input_bytes_total = accumulate_bundle_size(input_bytes_total, rel_path.len())?;
        input_bytes_total = input_bytes_total
            .checked_add(8)
            .and_then(|total| total.checked_add(size))
            .ok_or(crate::OxideError::InvalidFormat(
                "directory bundle size overflow",
            ))?;
        files.push(DirectoryFileSpec {
            rel_path,
            full_path,
            size,
        });
    }

    Ok(DirectoryDiscovery {
        root: root.to_path_buf(),
        directories,
        files,
        input_bytes_total,
    })
}

fn accumulate_bundle_size(current: u64, path_len: usize) -> Result<u64> {
    current
        .checked_add(1)
        .and_then(|value| value.checked_add(4))
        .and_then(|value| value.checked_add(path_len as u64))
        .ok_or(crate::OxideError::InvalidFormat(
            "directory bundle size overflow",
        ))
}

pub(super) fn block_count_for_bytes(total_bytes: u64, block_size: usize) -> Result<u32> {
    let block_size = block_size.max(1) as u64;
    let blocks = if total_bytes == 0 {
        0
    } else {
        total_bytes
            .checked_add(block_size - 1)
            .ok_or(crate::OxideError::InvalidFormat("block count overflow"))?
            / block_size
    };
    u32::try_from(blocks).map_err(|_| crate::OxideError::InvalidFormat("too many blocks"))
}

pub(super) fn collect_directory_entries(root: &Path) -> Result<Vec<DirectoryBundleEntry>> {
    let mut directories = Vec::<PathBuf>::new();
    let mut files = Vec::<PathBuf>::new();

    for entry in WalkDir::new(root) {
        let entry = entry.map_err(anyhow::Error::from)?;
        let path = entry.path();
        if path == root {
            continue;
        }

        let rel_path = path
            .strip_prefix(root)
            .map_err(|_| crate::OxideError::InvalidFormat("invalid relative path"))?
            .to_path_buf();

        if entry.file_type().is_dir() {
            directories.push(rel_path);
        } else if entry.file_type().is_file() {
            files.push(rel_path);
        } else {
            return Err(crate::OxideError::InvalidFormat(
                "directory archive supports regular files/directories only",
            ));
        }
    }
    directories.sort();
    files.sort();

    let mut entries = Vec::with_capacity(directories.len() + files.len());
    for directory in directories {
        let rel_path = relative_path_to_utf8(&directory)?;
        entries.push(DirectoryBundleEntry::Directory { rel_path });
    }

    if files.len() >= PARALLEL_DIRECTORY_READ_THRESHOLD {
        let parallel_entries: std::result::Result<Vec<DirectoryBundleEntry>, crate::OxideError> =
            files
                .par_iter()
                .map(|file_rel| {
                    let rel_path = relative_path_to_utf8(file_rel)?;
                    let full_path = root.join(file_rel);
                    let data = fs::read(full_path)?;
                    Ok(DirectoryBundleEntry::File { rel_path, data })
                })
                .collect();
        entries.extend(parallel_entries?);
    } else {
        for file_rel in files {
            let rel_path = relative_path_to_utf8(&file_rel)?;
            let full_path = root.join(&file_rel);
            let data = fs::read(full_path)?;
            entries.push(DirectoryBundleEntry::File { rel_path, data });
        }
    }

    Ok(entries)
}

fn relative_path_to_utf8(path: &Path) -> Result<String> {
    let raw = path.to_str().ok_or(crate::OxideError::InvalidFormat(
        "non-utf8 path not supported",
    ))?;
    Ok(raw.replace('\\', "/"))
}

pub(super) fn encode_directory_bundle(entries: Vec<DirectoryBundleEntry>) -> Result<Vec<u8>> {
    let entry_count = u32::try_from(entries.len())
        .map_err(|_| crate::OxideError::InvalidFormat("directory entry count overflow"))?;

    let mut out = Vec::new();
    out.extend_from_slice(&DIRECTORY_BUNDLE_MAGIC);
    out.extend_from_slice(&DIRECTORY_BUNDLE_VERSION.to_le_bytes());
    out.extend_from_slice(&entry_count.to_le_bytes());

    for entry in entries {
        match entry {
            DirectoryBundleEntry::Directory { rel_path } => {
                out.push(0);
                encode_path(&mut out, &rel_path)?;
            }
            DirectoryBundleEntry::File { rel_path, data } => {
                out.push(1);
                encode_path(&mut out, &rel_path)?;
                out.extend_from_slice(
                    &(u64::try_from(data.len())
                        .map_err(|_| crate::OxideError::InvalidFormat("file size overflow"))?)
                    .to_le_bytes(),
                );
                out.extend_from_slice(&data);
            }
        }
    }

    Ok(out)
}

pub(super) fn encode_path(out: &mut Vec<u8>, rel_path: &str) -> Result<()> {
    let bytes = rel_path.as_bytes();
    let len = u32::try_from(bytes.len())
        .map_err(|_| crate::OxideError::InvalidFormat("path length overflow"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn decode_directory_bundle(payload: &[u8]) -> Result<Vec<DirectoryBundleEntry>> {
    if payload.len() < 10 {
        return Err(crate::OxideError::InvalidFormat(
            "directory bundle is too short",
        ));
    }

    if payload[..4] != DIRECTORY_BUNDLE_MAGIC {
        return Err(crate::OxideError::InvalidFormat(
            "archive payload is not a directory bundle",
        ));
    }

    let version = u16::from_le_bytes([payload[4], payload[5]]);
    if version != DIRECTORY_BUNDLE_VERSION {
        return Err(crate::OxideError::InvalidFormat(
            "unsupported directory bundle version",
        ));
    }

    let entry_count = u32::from_le_bytes([payload[6], payload[7], payload[8], payload[9]]);
    let mut cursor = 10usize;
    let mut entries = Vec::with_capacity(entry_count as usize);

    for _ in 0..entry_count {
        if cursor >= payload.len() {
            return Err(crate::OxideError::InvalidFormat(
                "truncated directory bundle entry kind",
            ));
        }
        let kind = payload[cursor];
        cursor += 1;

        let rel_path = decode_path(payload, &mut cursor)?;
        match kind {
            0 => entries.push(DirectoryBundleEntry::Directory { rel_path }),
            1 => {
                if cursor + 8 > payload.len() {
                    return Err(crate::OxideError::InvalidFormat(
                        "truncated directory bundle file size",
                    ));
                }
                let size = u64::from_le_bytes([
                    payload[cursor],
                    payload[cursor + 1],
                    payload[cursor + 2],
                    payload[cursor + 3],
                    payload[cursor + 4],
                    payload[cursor + 5],
                    payload[cursor + 6],
                    payload[cursor + 7],
                ]);
                cursor += 8;

                let size_usize = usize::try_from(size)
                    .map_err(|_| crate::OxideError::InvalidFormat("file size overflow"))?;
                let end =
                    cursor
                        .checked_add(size_usize)
                        .ok_or(crate::OxideError::InvalidFormat(
                            "directory bundle offset overflow",
                        ))?;
                if end > payload.len() {
                    return Err(crate::OxideError::InvalidFormat(
                        "truncated directory bundle file data",
                    ));
                }

                let data = payload[cursor..end].to_vec();
                cursor = end;
                entries.push(DirectoryBundleEntry::File { rel_path, data });
            }
            _ => {
                return Err(crate::OxideError::InvalidFormat(
                    "invalid directory bundle entry kind",
                ));
            }
        }
    }

    if cursor != payload.len() {
        return Err(crate::OxideError::InvalidFormat(
            "directory bundle has trailing data",
        ));
    }

    Ok(entries)
}

fn decode_path(payload: &[u8], cursor: &mut usize) -> Result<String> {
    if *cursor + 4 > payload.len() {
        return Err(crate::OxideError::InvalidFormat(
            "truncated directory bundle path length",
        ));
    }
    let len = u32::from_le_bytes([
        payload[*cursor],
        payload[*cursor + 1],
        payload[*cursor + 2],
        payload[*cursor + 3],
    ]) as usize;
    *cursor += 4;

    let end = cursor
        .checked_add(len)
        .ok_or(crate::OxideError::InvalidFormat(
            "directory path offset overflow",
        ))?;
    if end > payload.len() {
        return Err(crate::OxideError::InvalidFormat(
            "truncated directory bundle path data",
        ));
    }

    let rel_path = std::str::from_utf8(&payload[*cursor..end])
        .map_err(|_| crate::OxideError::InvalidFormat("directory path is not utf8"))?
        .to_string();
    *cursor = end;
    Ok(rel_path)
}

fn join_safe(root: &Path, rel_path: &str) -> Result<PathBuf> {
    let rel = Path::new(rel_path);
    if rel.is_absolute() {
        return Err(crate::OxideError::InvalidFormat(
            "absolute paths are not allowed in directory bundle",
        ));
    }

    for component in rel.components() {
        match component {
            Component::Normal(_) => {}
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(crate::OxideError::InvalidFormat(
                    "unsafe path component in directory bundle",
                ));
            }
        }
    }

    Ok(root.join(rel))
}
