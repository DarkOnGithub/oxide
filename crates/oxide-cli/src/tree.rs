use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::path::Path;

use oxide_core::{ArchiveEntryKind, ArchiveListingEntry, ArchiveTimestamp};

use crate::ui::{StreamTarget, Tone, format_bytes, paint};

#[derive(Debug, Default)]
struct TreeNode {
    entry: Option<ArchiveListingEntry>,
    size: u64,
    children: BTreeMap<String, TreeNode>,
}

pub fn print_archive_tree(archive_path: &Path, entries: &[ArchiveListingEntry]) {
    let root_label = archive_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_else(|| archive_path.as_os_str().to_str().unwrap_or("<archive>"));
    let file_count = entries
        .iter()
        .filter(|entry| matches!(entry.kind, ArchiveEntryKind::File))
        .count();
    let directory_count = entries
        .iter()
        .filter(|entry| matches!(entry.kind, ArchiveEntryKind::Directory))
        .count();
    let payload_bytes = entries
        .iter()
        .filter(|entry| matches!(entry.kind, ArchiveEntryKind::File))
        .map(|entry| entry.size)
        .sum::<u64>();

    let mut root = build_tree(entries);
    finalize_tree_sizes(&mut root);

    println!(
        "{}",
        paint(StreamTarget::Stdout, Tone::Accent, "Archive tree")
    );
    println!(
        "{} [{}]",
        paint(StreamTarget::Stdout, Tone::Success, root_label),
        format_bytes(root.size)
    );
    println!(
        "{}",
        paint(
            StreamTarget::Stdout,
            Tone::Muted,
            &format!(
                "{} entries | {} directories | {} files | {} payload",
                entries.len(),
                directory_count,
                file_count,
                format_bytes(payload_bytes)
            )
        )
    );
    render_tree_children(&root, "");
}

fn build_tree(entries: &[ArchiveListingEntry]) -> TreeNode {
    let mut root = TreeNode::default();
    for entry in entries {
        insert_tree_entry(&mut root, entry);
    }
    root
}

fn insert_tree_entry(root: &mut TreeNode, entry: &ArchiveListingEntry) {
    let mut node = root;
    for segment in entry.path.split('/').filter(|segment| !segment.is_empty()) {
        node = node.children.entry(segment.to_string()).or_default();
    }
    node.entry = Some(entry.clone());
    node.size = entry.size;
}

fn finalize_tree_sizes(node: &mut TreeNode) -> u64 {
    if node.children.is_empty() {
        return node.size;
    }

    let mut total = 0u64;
    for child in node.children.values_mut() {
        total = total.saturating_add(finalize_tree_sizes(child));
    }
    if matches!(node.entry_kind(), Some(ArchiveEntryKind::File)) {
        total = total.saturating_add(node.size);
    }
    node.size = total;
    total
}

fn render_tree_children(node: &TreeNode, prefix: &str) {
    let children = sorted_children(node);
    let len = children.len();

    for (index, (name, child)) in children.iter().enumerate() {
        let is_last = index + 1 == len;
        let branch = if is_last { "└──" } else { "├──" };
        let next_prefix = if is_last {
            format!("{prefix}    ")
        } else {
            format!("{prefix}│   ")
        };
        let label = if is_directory_node(child) {
            format!("{name}/")
        } else {
            name.to_string()
        };
        let label = if is_directory_node(child) {
            paint(StreamTarget::Stdout, Tone::Info, &label)
        } else {
            label
        };
        println!(
            "{prefix}{branch} {label} [{} | {} | {}]",
            format_bytes(child.size),
            child.mode_label(),
            child.timestamp_label(),
        );
        if is_directory_node(child) {
            render_tree_children(child, &next_prefix);
        }
    }
}

fn sorted_children(node: &TreeNode) -> Vec<(&String, &TreeNode)> {
    let mut children = node.children.iter().collect::<Vec<_>>();
    children.sort_by(|(left_name, left_node), (right_name, right_node)| {
        match (is_directory_node(left_node), is_directory_node(right_node)) {
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            _ => left_name.cmp(right_name),
        }
    });
    children
}

fn format_mode(entry: &ArchiveListingEntry) -> String {
    let prefix = match entry.kind {
        ArchiveEntryKind::Directory => 'd',
        ArchiveEntryKind::File => '-',
    };
    let mode = entry.mode;
    let bits = [
        0o400, 0o200, 0o100, 0o040, 0o020, 0o010, 0o004, 0o002, 0o001,
    ];
    let chars = ['r', 'w', 'x', 'r', 'w', 'x', 'r', 'w', 'x'];
    let mut out = String::with_capacity(10);
    out.push(prefix);
    for (bit, ch) in bits.into_iter().zip(chars) {
        out.push(if mode & bit != 0 { ch } else { '-' });
    }
    out
}

fn format_timestamp(timestamp: ArchiveTimestamp) -> String {
    let total_seconds = timestamp.seconds;
    let seconds_per_day = 86_400i64;
    let days = total_seconds.div_euclid(seconds_per_day);
    let seconds_of_day = total_seconds.rem_euclid(seconds_per_day);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;

    if timestamp.nanoseconds == 0 {
        format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}")
    } else {
        format!(
            "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{:09}",
            timestamp.nanoseconds
        )
    }
}

fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    if month <= 2 {
        year += 1;
    }
    (year, month as u32, day as u32)
}

fn is_directory_node(node: &TreeNode) -> bool {
    matches!(node.entry_kind(), Some(ArchiveEntryKind::Directory)) || !node.children.is_empty()
}

impl TreeNode {
    fn entry_kind(&self) -> Option<ArchiveEntryKind> {
        self.entry.as_ref().map(|entry| entry.kind)
    }

    fn mode_label(&self) -> String {
        self.entry
            .as_ref()
            .map(format_mode)
            .unwrap_or_else(|| "d---------".to_string())
    }

    fn timestamp_label(&self) -> String {
        self.entry
            .as_ref()
            .map(|entry| format_timestamp(entry.mtime))
            .unwrap_or_else(|| "unknown time".to_string())
    }
}
