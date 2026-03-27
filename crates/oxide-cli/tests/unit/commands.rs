use super::{resolve_archive_producer_threads, resolve_archive_workers};

#[test]
fn explicit_worker_count_is_preserved() {
    assert_eq!(resolve_archive_workers(16), 16);
    assert_eq!(resolve_archive_workers(3), 3);
}

#[test]
fn auto_worker_count_uses_logical_core_count() {
    assert_eq!(resolve_archive_workers(0), num_cpus::get().max(1));
}

#[test]
fn explicit_producer_thread_count_is_preserved() {
    assert_eq!(resolve_archive_producer_threads(4, 16, true, true), 4);
    assert_eq!(resolve_archive_producer_threads(2, 4, false, true), 2);
}

#[test]
fn preset_producer_threads_shrink_when_explicit_workers_consume_all_logical_cores() {
    let logical_cores = num_cpus::get().max(1);
    assert_eq!(
        resolve_archive_producer_threads(3, logical_cores, true, false),
        1
    );
}

#[test]
fn preset_producer_threads_are_preserved_when_workers_are_auto() {
    assert_eq!(resolve_archive_producer_threads(3, 8, false, false), 3);
}
