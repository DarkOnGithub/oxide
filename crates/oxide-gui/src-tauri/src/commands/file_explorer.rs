//! File explorer commands.

use serde::{Deserialize, Serialize};
use specta::Type;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct ExplorerDirectoryEntry {
    pub name: String,
    pub path: String,
    pub is_directory: bool,
    pub is_file: bool,
    pub is_symlink: bool,
    pub size: f64,
    pub modified_at: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct ExplorerPathMetadata {
    pub is_directory: bool,
    pub is_file: bool,
    pub is_symlink: bool,
    pub size: f64,
    pub modified_at: Option<f64>,
    pub accessed_at: Option<f64>,
    pub created_at: Option<f64>,
    pub readonly: bool,
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
}

fn system_time_to_millis(time: std::time::SystemTime) -> Option<f64> {
    time.duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis() as f64)
}

#[cfg(unix)]
fn metadata_mode(metadata: &std::fs::Metadata) -> Option<u32> {
    use std::os::unix::fs::MetadataExt;
    Some(metadata.mode())
}

#[cfg(not(unix))]
fn metadata_mode(_metadata: &std::fs::Metadata) -> Option<u32> {
    None
}

#[cfg(unix)]
fn metadata_uid(metadata: &std::fs::Metadata) -> Option<u32> {
    use std::os::unix::fs::MetadataExt;
    Some(metadata.uid())
}

#[cfg(not(unix))]
fn metadata_uid(_metadata: &std::fs::Metadata) -> Option<u32> {
    None
}

#[cfg(unix)]
fn metadata_gid(metadata: &std::fs::Metadata) -> Option<u32> {
    use std::os::unix::fs::MetadataExt;
    Some(metadata.gid())
}

#[cfg(not(unix))]
fn metadata_gid(_metadata: &std::fs::Metadata) -> Option<u32> {
    None
}

fn path_metadata(path: &Path) -> Result<ExplorerPathMetadata, String> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|e| format!("Failed to read metadata for {}: {e}", path.display()))?;

    let file_type = metadata.file_type();

    Ok(ExplorerPathMetadata {
        is_directory: file_type.is_dir(),
        is_file: file_type.is_file(),
        is_symlink: file_type.is_symlink(),
        size: metadata.len() as f64,
        modified_at: metadata.modified().ok().and_then(system_time_to_millis),
        accessed_at: metadata.accessed().ok().and_then(system_time_to_millis),
        created_at: metadata.created().ok().and_then(system_time_to_millis),
        readonly: metadata.permissions().readonly(),
        mode: metadata_mode(&metadata),
        uid: metadata_uid(&metadata),
        gid: metadata_gid(&metadata),
    })
}

/// Lists a directory and includes basic metadata for each entry.
#[tauri::command]
#[specta::specta]
pub async fn list_directory_entries(path: String) -> Result<Vec<ExplorerDirectoryEntry>, String> {
    let directory = Path::new(&path);

    let entries = std::fs::read_dir(directory)
        .map_err(|e| format!("Failed to read directory {}: {e}", directory.display()))?;

    let mut result = Vec::new();

    for entry in entries {
        let entry = entry.map_err(|e| format!("Failed to read directory entry: {e}"))?;
        let entry_path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();

        let metadata = match std::fs::symlink_metadata(&entry_path) {
            Ok(metadata) => metadata,
            Err(error) => {
                log::warn!(
                    "Failed to read entry metadata for {:?}: {}",
                    entry_path,
                    error
                );
                continue;
            }
        };

        let file_type = metadata.file_type();

        result.push(ExplorerDirectoryEntry {
            name,
            path: entry_path.to_string_lossy().to_string(),
            is_directory: file_type.is_dir(),
            is_file: file_type.is_file(),
            is_symlink: file_type.is_symlink(),
            size: metadata.len() as f64,
            modified_at: metadata.modified().ok().and_then(system_time_to_millis),
        });
    }

    Ok(result)
}

/// Returns metadata for a file or folder path.
#[tauri::command]
#[specta::specta]
pub async fn get_path_metadata(path: String) -> Result<ExplorerPathMetadata, String> {
    path_metadata(Path::new(&path))
}
