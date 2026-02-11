use std::time::Instant;

#[cfg(feature = "profiling")]
use crate::telemetry::events::{ProfileEvent, TelemetryEvent, emit_global};
#[cfg(feature = "profiling")]
use crate::telemetry::tags;
#[cfg(feature = "profiling")]
use std::collections::BTreeSet;
#[cfg(feature = "profiling")]
use std::sync::{OnceLock, RwLock};

/// Converts elapsed time since `started_at` to microseconds, clamped to `u64::MAX`.
#[inline]
pub fn elapsed_us(started_at: Instant) -> u64 {
    started_at.elapsed().as_micros().min(u64::MAX as u128) as u64
}

#[cfg(feature = "profiling")]
const PROFILE_TAGS_ENV: &str = "OXIDE_PROFILE_TAGS";

#[cfg(feature = "profiling")]
#[derive(Debug, Clone, Default)]
struct TagFilter {
    // None => all tags enabled.
    enabled: Option<BTreeSet<String>>,
}

#[cfg(feature = "profiling")]
fn parse_tags(raw: &str) -> Option<BTreeSet<String>> {
    let mut tags = BTreeSet::new();
    for token in raw
        .split(',')
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        let normalized = token.to_ascii_lowercase();
        if normalized == "*" || normalized == "all" {
            return None;
        }
        tags.insert(normalized);
    }

    if tags.is_empty() { None } else { Some(tags) }
}

#[cfg(feature = "profiling")]
fn filter_state() -> &'static RwLock<TagFilter> {
    static STATE: OnceLock<RwLock<TagFilter>> = OnceLock::new();
    STATE.get_or_init(|| {
        let filter = std::env::var(PROFILE_TAGS_ENV)
            .ok()
            .map(|raw| TagFilter {
                enabled: parse_tags(&raw),
            })
            .unwrap_or_default();
        RwLock::new(filter)
    })
}

#[cfg(feature = "profiling")]
fn lock_read() -> std::sync::RwLockReadGuard<'static, TagFilter> {
    match filter_state().read() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(feature = "profiling")]
fn lock_write() -> std::sync::RwLockWriteGuard<'static, TagFilter> {
    match filter_state().write() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Enables all profiling tags.
#[cfg(feature = "profiling")]
pub fn enable_all_tags() {
    lock_write().enabled = None;
}

#[cfg(not(feature = "profiling"))]
pub fn enable_all_tags() {}

/// Sets the complete list of enabled profiling tags.
///
/// Passing an empty slice enables all tags.
#[cfg(feature = "profiling")]
pub fn set_enabled_tags(tags: &[&str]) {
    if tags.is_empty() {
        enable_all_tags();
        return;
    }

    let mut enabled = BTreeSet::new();
    for tag in tags {
        let normalized = tag.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            continue;
        }
        if normalized == "*" || normalized == "all" {
            enable_all_tags();
            return;
        }
        enabled.insert(normalized);
    }

    lock_write().enabled = if enabled.is_empty() {
        None
    } else {
        Some(enabled)
    };
}

#[cfg(not(feature = "profiling"))]
pub fn set_enabled_tags(_tags: &[&str]) {}

/// Reloads enabled tags from `OXIDE_PROFILE_TAGS`.
#[cfg(feature = "profiling")]
pub fn reload_enabled_tags_from_env() {
    let parsed = std::env::var(PROFILE_TAGS_ENV)
        .ok()
        .map(|raw| parse_tags(&raw))
        .unwrap_or(None);
    lock_write().enabled = parsed;
}

#[cfg(not(feature = "profiling"))]
pub fn reload_enabled_tags_from_env() {}

/// Returns true when at least one tag in the stack is enabled.
#[cfg(feature = "profiling")]
pub fn is_tag_stack_enabled(tag_stack: &[&str]) -> bool {
    let state = lock_read();
    match &state.enabled {
        None => true,
        Some(enabled) => tag_stack
            .iter()
            .map(|tag| tag.to_ascii_lowercase())
            .any(|tag| enabled.contains(&tag)),
    }
}

#[cfg(not(feature = "profiling"))]
pub fn is_tag_stack_enabled(_tag_stack: &[&str]) -> bool {
    false
}

#[cfg(feature = "profiling")]
#[inline]
pub fn event(
    target: &'static str,
    tag_stack: &[&str],
    op: &'static str,
    result: &'static str,
    elapsed_us: u64,
    message: &'static str,
) {
    if !is_tag_stack_enabled(tag_stack) {
        return;
    }

    emit_global(TelemetryEvent::Profile(ProfileEvent {
        target,
        op,
        result,
        elapsed_us,
        tags: tag_stack.iter().map(|tag| (*tag).to_string()).collect(),
        message,
    }));

    match target {
        tags::PROFILE_MMAP => {
            tracing::debug!(target: tags::PROFILE_MMAP, op, result, elapsed_us, tags = ?tag_stack, "{message}");
        }
        tags::PROFILE_FORMAT => {
            tracing::debug!(target: tags::PROFILE_FORMAT, op, result, elapsed_us, tags = ?tag_stack, "{message}");
        }
        tags::PROFILE_BUFFER => {
            tracing::debug!(target: tags::PROFILE_BUFFER, op, result, elapsed_us, tags = ?tag_stack, "{message}");
        }
        tags::PROFILE_SCANNER => {
            tracing::debug!(target: tags::PROFILE_SCANNER, op, result, elapsed_us, tags = ?tag_stack, "{message}");
        }
        tags::PROFILE_WORKER => {
            tracing::debug!(target: tags::PROFILE_WORKER, op, result, elapsed_us, tags = ?tag_stack, "{message}");
        }
        tags::PROFILE_PIPELINE => {
            tracing::debug!(target: tags::PROFILE_PIPELINE, op, result, elapsed_us, tags = ?tag_stack, "{message}");
        }
        _ => {
            tracing::debug!(target: "oxide.profile", op, result, elapsed_us, original_target = target, tags = ?tag_stack, "{message}");
        }
    }
}

#[cfg(not(feature = "profiling"))]
#[inline]
pub fn event(
    _target: &'static str,
    _tag_stack: &[&str],
    _op: &'static str,
    _result: &'static str,
    _elapsed_us: u64,
    _message: &'static str,
) {
}
