use super::*;

#[derive(Debug)]
pub(super) struct PendingMetadata {
    pub(super) path: PathBuf,
    pub(super) entry: crate::ArchiveListingEntry,
    pub(super) depth: usize,
}

impl PendingMetadata {
    pub(super) fn new(path: PathBuf, entry: crate::ArchiveListingEntry) -> Self {
        Self {
            depth: path_depth(&path),
            path,
            entry,
        }
    }
}

pub(super) fn apply_pending_metadata_in_parallel(entries: &[PendingMetadata]) -> Result<()> {
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
pub(super) fn create_symlink(path: &Path, target: &str) -> Result<()> {
    std::os::unix::fs::symlink(target, path)?;
    Ok(())
}

#[cfg(windows)]
pub(super) fn create_symlink(path: &Path, target: &str) -> Result<()> {
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
