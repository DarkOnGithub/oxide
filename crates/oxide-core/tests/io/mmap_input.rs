use std::io::Write;

use oxide_core::MmapInput;
use tempfile::NamedTempFile;

#[test]
fn maps_file_and_reads_slices() -> Result<(), Box<dyn std::error::Error>> {
    let mut file = NamedTempFile::new()?;
    let data = b"hello memory map";
    file.write_all(data)?;
    file.flush()?;

    let mmap = MmapInput::open(file.path())?;
    assert_eq!(mmap.path(), file.path());
    assert_eq!(mmap.len_u64(), data.len() as u64);
    assert!(!mmap.is_empty());

    let head = mmap.slice(0, 5)?;
    assert_eq!(head.as_ref(), b"hello");

    let all = mmap.as_bytes()?;
    assert_eq!(all.as_ref(), data);

    Ok(())
}

#[test]
fn empty_file_is_supported() -> Result<(), Box<dyn std::error::Error>> {
    let file = NamedTempFile::new()?;
    let mmap = MmapInput::open(file.path())?;

    assert!(mmap.is_empty());
    assert_eq!(mmap.len_u64(), 0);
    assert_eq!(mmap.slice(0, 0)?.len(), 0);
    assert_eq!(mmap.as_bytes()?.len(), 0);

    Ok(())
}

#[test]
fn invalid_ranges_return_error() -> Result<(), Box<dyn std::error::Error>> {
    let mut file = NamedTempFile::new()?;
    file.write_all(b"abc")?;
    file.flush()?;

    let mmap = MmapInput::open(file.path())?;
    assert!(mmap.slice(3, 2).is_err());
    assert!(mmap.slice(2, 4).is_err());
    assert!(mmap.slice_u64(10, 11).is_err());

    Ok(())
}

#[test]
fn mapped_slice_survives_after_input_drop() -> Result<(), Box<dyn std::error::Error>> {
    let mut file = NamedTempFile::new()?;
    file.write_all(b"mapped payload")?;
    file.flush()?;

    let mapped = {
        let mmap = MmapInput::open(file.path())?;
        mmap.mapped_slice(1, 7)?
    };

    assert_eq!(mapped.as_slice(), b"apped ");

    Ok(())
}
