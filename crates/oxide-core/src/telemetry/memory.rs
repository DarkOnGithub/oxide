use std::fs;

/// Process-level memory usage sample.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProcessMemorySample {
    pub rss_bytes: Option<u64>,
    pub virtual_bytes: Option<u64>,
}

/// Samples current process memory usage.
pub fn sample_process_memory() -> ProcessMemorySample {
    #[cfg(target_os = "linux")]
    {
        let contents = match fs::read_to_string("/proc/self/status") {
            Ok(contents) => contents,
            Err(_) => return ProcessMemorySample::default(),
        };

        ProcessMemorySample {
            rss_bytes: parse_kib_field(&contents, "VmRSS:"),
            virtual_bytes: parse_kib_field(&contents, "VmSize:"),
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        ProcessMemorySample::default()
    }
}

#[cfg(target_os = "linux")]
fn parse_kib_field(status: &str, field: &str) -> Option<u64> {
    let line = status.lines().find(|line| line.starts_with(field))?;
    let value_kib = line
        .split_whitespace()
        .nth(1)
        .and_then(|value| value.parse::<u64>().ok())?;

    value_kib.checked_mul(1024)
}
