use std::ffi::OsStr;
use std::path::{Component, Path};

#[inline]
fn matches_force_raw_storage_label(label: &str) -> bool {
    matches!(
        label,
        "7z" | "aac"
            | "apk"
            | "avi"
            | "avif"
            | "br"
            | "bz2"
            | "cab"
            | "deb"
            | "dmg"
            | "docx"
            | "epub"
            | "flac"
            | "flv"
            | "gif"
            | "gz"
            | "heic"
            | "heif"
            | "jar"
            | "jpeg"
            | "jpg"
            | "jxl"
            | "lz4"
            | "lzma"
            | "m4a"
            | "m4v"
            | "mkv"
            | "mov"
            | "mp3"
            | "mp4"
            | "ogg"
            | "opus"
            | "oxz"
            | "png"
            | "pptx"
            | "rar"
            | "rpm"
            | "snap"
            | "sz"
            | "tgz"
            | "webm"
            | "webp"
            | "whl"
            | "wma"
            | "wmv"
            | "xlsx"
            | "xpi"
            | "xz"
            | "zip"
            | "zst"
            | "zstd"
    )
}

pub fn should_force_raw_storage_by_extension(path: &Path) -> bool {
    let Some(ext) = path.extension().and_then(|ext| ext.to_str()) else {
        return false;
    };

    matches_force_raw_storage_label(&ext.to_ascii_lowercase())
}

fn is_git_pack_artifact(path: &Path) -> bool {
    let Some(ext) = path.extension().and_then(|ext| ext.to_str()) else {
        return false;
    };

    if !matches!(ext.to_ascii_lowercase().as_str(), "idx" | "pack" | "rev") {
        return false;
    }

    let mut components = path.components();
    while let Some(component) = components.next() {
        if component != Component::Normal(OsStr::new(".git")) {
            continue;
        }

        return matches!(components.next(), Some(Component::Normal(part)) if part == OsStr::new("objects"))
            && matches!(components.next(), Some(Component::Normal(part)) if part == OsStr::new("pack"));
    }

    false
}

pub fn should_force_raw_storage(path: &Path) -> bool {
    should_force_raw_storage_by_extension(path) || is_git_pack_artifact(path)
}

#[cfg(test)]
#[path = "../../tests/format/raw_storage.rs"]
mod tests;
