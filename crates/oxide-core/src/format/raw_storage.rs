use std::path::Path;

#[inline]
fn matches_force_raw_storage_label(label: &str) -> bool {
    matches!(
        label,
        "7z" | "aac"
            | "apk"
            | "avif"
            | "br"
            | "bz2"
            | "docx"
            | "epub"
            | "flac"
            | "gif"
            | "gz"
            | "heic"
            | "heif"
            | "jar"
            | "jpeg"
            | "jpg"
            | "jxl"
            | "lz4"
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
            | "webm"
            | "webp"
            | "whl"
            | "xlsx"
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

pub fn should_force_raw_storage(path: &Path) -> bool {
    should_force_raw_storage_by_extension(path)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{should_force_raw_storage, should_force_raw_storage_by_extension};

    #[test]
    fn raw_storage_extension_policy_matches_known_compressed_types() {
        assert!(should_force_raw_storage_by_extension(Path::new(
            "photo.jpg"
        )));
        assert!(should_force_raw_storage_by_extension(Path::new(
            "bundle.ZIP"
        )));
        assert!(should_force_raw_storage_by_extension(Path::new(
            "archive.tar.zst"
        )));
        assert!(!should_force_raw_storage_by_extension(Path::new(
            "notes.txt"
        )));
        assert!(!should_force_raw_storage_by_extension(Path::new(
            "bitmap.bmp"
        )));
        assert!(!should_force_raw_storage_by_extension(Path::new("README")));
    }

    #[test]
    fn raw_storage_policy_uses_path_only() {
        assert!(should_force_raw_storage(Path::new("photo.jpg")));
        assert!(!should_force_raw_storage(Path::new("payload.bin")));
    }
}
