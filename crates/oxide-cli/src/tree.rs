use std::collections::BTreeMap;
use std::path::Path;

use oxide_core::{ArchiveEntryKind, ArchiveListingEntry};

use crate::ui::{StreamTarget, Tone, format_bytes, paint};

#[derive(Debug, Default)]
struct TreeNode {
    kind: Option<ArchiveEntryKind>,
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
    render_tree_children(&root, "");
    println!();
    println!(
        "{} directories | {} files | {} payload",
        directory_count,
        file_count,
        format_bytes(root.size)
    );
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
    node.kind = Some(entry.kind);
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
    if matches!(node.kind, Some(ArchiveEntryKind::File)) {
        total = total.saturating_add(node.size);
    }
    node.size = total;
    total
}

fn render_tree_children(node: &TreeNode, prefix: &str) {
    let len = node.children.len();
    for (index, (name, child)) in node.children.iter().enumerate() {
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
        println!("{prefix}{branch} {label} [{}]", format_bytes(child.size));
        if is_directory_node(child) {
            render_tree_children(child, &next_prefix);
        }
    }
}

fn is_directory_node(node: &TreeNode) -> bool {
    matches!(node.kind, Some(ArchiveEntryKind::Directory)) || !node.children.is_empty()
}
