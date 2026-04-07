use super::*;

pub(super) fn spawn_restore_planner(
    entries: Vec<RestoreEntry>,
    window_config: PreparedEntryWindowConfig,
) -> (
    Receiver<Result<RestorePlannerMessage>>,
    thread::JoinHandle<Result<()>>,
) {
    // Phase 3 split: the planner owns manifest-order coordination and emits
    // only data-bearing file work to the executor. Non-data entries and
    // deferred metadata stay off the write hot path and are merged at finish.
    let (tx, rx) =
        bounded::<Result<RestorePlannerMessage>>(window_config.ready_entry_window_capacity);
    let handle = thread::spawn(move || -> Result<()> {
        let (prepared_entry_rx, prepared_entry_handle) =
            spawn_prepared_restore_entries(entries, window_config);
        let mut outcome = RestorePlannerOutcome::default();

        let planner_result = (|| -> Result<bool> {
            while let Ok(prepared) = prepared_entry_rx.recv() {
                let prepared = prepared?;
                outcome.stats.entry_count = outcome.stats.entry_count.saturating_add(1);

                match prepared {
                    PreparedRestoreEntry::Directory { path, entry } => {
                        outcome
                            .pending_directory_metadata
                            .push(PendingMetadata::new(path, entry));
                    }
                    PreparedRestoreEntry::File { .. } => {
                        let _ = tx.send(Err(crate::OxideError::CompressionError(
                            "prepared file entry reached planner before in-order file open"
                                .to_string(),
                        )));
                        return Ok(false);
                    }
                    PreparedRestoreEntry::FileReady {
                        path,
                        entry,
                        writer,
                        permit,
                        permit_wait_elapsed,
                        open_elapsed,
                    } => {
                        outcome
                            .stats
                            .record_prepared_file_permit_wait(permit_wait_elapsed);
                        outcome.stats.record_prepared_file_open(open_elapsed);

                        if entry.size == 0 {
                            let started = Instant::now();
                            let _file = writer.into_inner()?;
                            outcome.stats.record_output_flush(started.elapsed());
                            outcome
                                .pending_file_metadata
                                .push(PendingMetadata::new(path, entry));
                            drop(permit);
                            continue;
                        }

                        outcome.data_file_count_total =
                            outcome.data_file_count_total.saturating_add(1);
                        if tx
                            .send(Ok(RestorePlannerMessage::ReadyFile(ReadyFileWrite {
                                path,
                                entry,
                                writer,
                                permit,
                            })))
                            .is_err()
                        {
                            return Ok(false);
                        }
                        outcome.stats.record_planner_ready_queue_peak(tx.len());
                    }
                    PreparedRestoreEntry::Symlink {
                        path,
                        entry,
                        create_elapsed,
                    } => {
                        outcome.stats.record_output_create_file(create_elapsed);
                        outcome
                            .pending_file_metadata
                            .push(PendingMetadata::new(path, entry));
                    }
                }
            }

            Ok(true)
        })();

        let join_result = join_prepared_restore_entries_handle(prepared_entry_handle);
        match (planner_result, join_result) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(true), Ok(prepared_entry_outcome)) => {
                outcome
                    .stats
                    .record_active_files_peak(prepared_entry_outcome.active_files_peak);
                let _ = tx.send(Ok(RestorePlannerMessage::Finished(outcome)));
                Ok(())
            }
            (Ok(false), Ok(_)) => Ok(()),
        }
    });

    (rx, handle)
}

fn spawn_prepared_restore_entries(
    entries: Vec<RestoreEntry>,
    window_config: PreparedEntryWindowConfig,
) -> (
    Receiver<Result<PreparedRestoreEntry>>,
    thread::JoinHandle<Result<PreparedRestoreWorkerOutcome>>,
) {
    let (tx, rx) = bounded::<Result<PreparedRestoreEntry>>(PREPARED_ENTRY_QUEUE_CAPACITY);
    let handle = thread::spawn(move || -> Result<PreparedRestoreWorkerOutcome> {
        let (open_file_permit_tx, open_file_permit_rx) =
            bounded::<()>(window_config.preopened_file_window_capacity);
        for _ in 0..window_config.preopened_file_window_capacity {
            open_file_permit_tx.send(()).map_err(|_| {
                crate::OxideError::CompressionError(
                    "pre-opened file permit queue closed during initialization".to_string(),
                )
            })?;
        }

        let total_entries = entries.len();
        let worker_count = available_prepared_entry_workers(total_entries);
        if worker_count <= 1 {
            let mut outcome = PreparedRestoreWorkerOutcome::default();
            for restore_entry in entries {
                if matches!(restore_entry.entry.kind, crate::ArchiveEntryKind::File) {
                    outcome.active_files_peak = outcome.active_files_peak.max(1);
                }
                let prepared = prepare_restore_entry_with_open(
                    restore_entry,
                    &open_file_permit_rx,
                    &open_file_permit_tx,
                );
                let stop_after_send = prepared.is_err();
                if tx.send(prepared).is_err() {
                    return Ok(outcome);
                }
                if stop_after_send {
                    return Ok(outcome);
                }
            }

            return Ok(outcome);
        }

        let cancelled = Arc::new(AtomicBool::new(false));
        let (work_tx, work_rx) = bounded::<PreparedRestoreWorkItem>(PREPARED_ENTRY_QUEUE_CAPACITY);
        let (result_tx, result_rx) =
            bounded::<(usize, Result<PreparedRestoreEntry>)>(PREPARED_ENTRY_QUEUE_CAPACITY);
        let file_work_items = entries
            .iter()
            .map(|restore_entry| matches!(restore_entry.entry.kind, crate::ArchiveEntryKind::File))
            .collect::<Vec<_>>();

        let mut worker_handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let work_rx = work_rx.clone();
            let result_tx = result_tx.clone();
            let cancelled = Arc::clone(&cancelled);
            let open_file_permit_rx = open_file_permit_rx.clone();
            let open_file_permit_tx = open_file_permit_tx.clone();
            worker_handles.push(thread::spawn(move || -> Result<()> {
                while !cancelled.load(Ordering::Relaxed) {
                    let Ok(work_item) = work_rx.recv() else {
                        return Ok(());
                    };
                    if cancelled.load(Ordering::Relaxed) {
                        return Ok(());
                    }

                    let index = work_item.index;
                    let prepared = prepare_restore_work_item_with_open(
                        work_item,
                        &open_file_permit_rx,
                        &open_file_permit_tx,
                    );
                    if prepared.is_err() {
                        cancelled.store(true, Ordering::Relaxed);
                    }
                    if result_tx.send((index, prepared)).is_err() {
                        return Ok(());
                    }
                }

                Ok(())
            }));
        }
        drop(result_tx);

        let mut pending_work = entries
            .into_iter()
            .enumerate()
            .map(|(index, restore_entry)| PreparedRestoreWorkItem {
                index,
                restore_entry,
                reserved_open_permit: None,
                permit_wait_started: None,
                active_file_counted: false,
            })
            .collect::<VecDeque<_>>();

        let mut next_index = 0usize;
        let mut pending: BTreeMap<usize, Result<PreparedRestoreEntry>> = BTreeMap::new();
        let mut in_flight = 0usize;
        let mut active_files = 0usize;
        let mut active_files_peak = 0usize;
        let mut sent_error = false;
        while next_index < total_entries && !sent_error {
            while let Some(prepared) = pending.remove(&next_index) {
                if file_work_items.get(next_index).copied().unwrap_or(false) {
                    active_files = active_files.saturating_sub(1);
                }
                sent_error = prepared.is_err();
                if tx.send(prepared).is_err() {
                    cancelled.store(true, Ordering::Relaxed);
                    sent_error = true;
                    break;
                }
                next_index += 1;
                if sent_error {
                    break;
                }
            }

            if next_index >= total_entries || sent_error {
                break;
            }

            while let Some(work_item) = pending_work.pop_front() {
                if cancelled.load(Ordering::Relaxed) {
                    break;
                }

                let mut work_item = work_item;
                if matches!(
                    work_item.restore_entry.entry.kind,
                    crate::ArchiveEntryKind::File
                ) && work_item.reserved_open_permit.is_none()
                {
                    match try_reserve_open_file_permit_for_work_item(
                        &mut work_item,
                        &open_file_permit_rx,
                        &open_file_permit_tx,
                    ) {
                        Ok(true) => {
                            mark_active_file_counted(
                                &mut work_item,
                                &mut active_files,
                                &mut active_files_peak,
                            );
                        }
                        Ok(false) => {
                            if in_flight == 0 {
                                if let Err(error) = reserve_open_file_permit_for_work_item(
                                    &mut work_item,
                                    &open_file_permit_rx,
                                    &open_file_permit_tx,
                                ) {
                                    cancelled.store(true, Ordering::Relaxed);
                                    sent_error = true;
                                    if tx.send(Err(error)).is_err() {
                                        return Ok(PreparedRestoreWorkerOutcome {
                                            active_files_peak,
                                        });
                                    }
                                    break;
                                }
                                mark_active_file_counted(
                                    &mut work_item,
                                    &mut active_files,
                                    &mut active_files_peak,
                                );
                            } else {
                                pending_work.push_front(work_item);
                                break;
                            }
                        }
                        Err(error) => {
                            cancelled.store(true, Ordering::Relaxed);
                            sent_error = true;
                            if tx.send(Err(error)).is_err() {
                                return Ok(PreparedRestoreWorkerOutcome { active_files_peak });
                            }
                            break;
                        }
                    }
                }

                match work_tx.try_send(work_item) {
                    Ok(()) => {
                        in_flight += 1;
                    }
                    Err(TrySendError::Full(work_item)) => {
                        pending_work.push_front(work_item);
                        break;
                    }
                    Err(TrySendError::Disconnected(_work_item)) => {
                        cancelled.store(true, Ordering::Relaxed);
                        sent_error = true;
                        break;
                    }
                }
            }

            while let Some(prepared) = pending.remove(&next_index) {
                if file_work_items.get(next_index).copied().unwrap_or(false) {
                    active_files = active_files.saturating_sub(1);
                }
                sent_error = prepared.is_err();
                if tx.send(prepared).is_err() {
                    cancelled.store(true, Ordering::Relaxed);
                    sent_error = true;
                    break;
                }
                next_index += 1;
                if sent_error {
                    break;
                }
            }

            if next_index >= total_entries || sent_error {
                break;
            }

            let (index, prepared) = result_rx.recv().map_err(|_| {
                crate::OxideError::CompressionError(
                    "prepared entry workers exited before completion".to_string(),
                )
            })?;
            in_flight = in_flight.saturating_sub(1);
            pending.insert(index, prepared);
        }

        drop(work_tx);
        cancelled.store(true, Ordering::Relaxed);
        drop(pending_work);
        drop(pending);
        drop(result_rx);

        for handle in worker_handles {
            handle.join().map_err(|payload| {
                let details = if let Some(message) = payload.downcast_ref::<&str>() {
                    (*message).to_string()
                } else if let Some(message) = payload.downcast_ref::<String>() {
                    message.clone()
                } else {
                    "unknown panic payload".to_string()
                };
                crate::OxideError::CompressionError(format!(
                    "prepared entry worker thread panicked: {details}"
                ))
            })??;
        }

        Ok(PreparedRestoreWorkerOutcome { active_files_peak })
    });

    (rx, handle)
}

fn join_prepared_restore_entries_handle(
    handle: thread::JoinHandle<Result<PreparedRestoreWorkerOutcome>>,
) -> Result<PreparedRestoreWorkerOutcome> {
    handle.join().map_err(|payload| {
        let details = if let Some(message) = payload.downcast_ref::<&str>() {
            (*message).to_string()
        } else if let Some(message) = payload.downcast_ref::<String>() {
            message.clone()
        } else {
            "unknown panic payload".to_string()
        };
        crate::OxideError::CompressionError(format!(
            "prepared entry worker thread panicked: {details}"
        ))
    })?
}

fn prepare_restore_entry(restore_entry: RestoreEntry) -> Result<PreparedRestoreEntry> {
    let RestoreEntry { path, entry } = restore_entry;
    match entry.kind {
        crate::ArchiveEntryKind::Directory => Ok(PreparedRestoreEntry::Directory { path, entry }),
        crate::ArchiveEntryKind::File => Ok(PreparedRestoreEntry::File { path, entry }),
        crate::ArchiveEntryKind::Symlink => {
            let target = entry.target.as_deref().ok_or(OxideError::InvalidFormat(
                "symlink manifest entry missing target",
            ))?;
            let create_started = Instant::now();
            super::metadata::create_symlink(&path, target)?;
            Ok(PreparedRestoreEntry::Symlink {
                path,
                entry,
                create_elapsed: create_started.elapsed(),
            })
        }
    }
}

pub(super) fn prepare_restore_entry_with_open(
    restore_entry: RestoreEntry,
    open_file_permit_rx: &Receiver<()>,
    open_file_permit_tx: &Sender<()>,
) -> Result<PreparedRestoreEntry> {
    prepare_prepared_entry_with_open(
        prepare_restore_entry(restore_entry),
        None,
        open_file_permit_rx,
        open_file_permit_tx,
    )
}

fn prepare_restore_work_item_with_open(
    work_item: PreparedRestoreWorkItem,
    open_file_permit_rx: &Receiver<()>,
    open_file_permit_tx: &Sender<()>,
) -> Result<PreparedRestoreEntry> {
    let PreparedRestoreWorkItem {
        restore_entry,
        reserved_open_permit,
        ..
    } = work_item;
    prepare_prepared_entry_with_open(
        prepare_restore_entry(restore_entry),
        reserved_open_permit,
        open_file_permit_rx,
        open_file_permit_tx,
    )
}

fn prepare_prepared_entry_with_open(
    prepared: Result<PreparedRestoreEntry>,
    reserved_open_permit: Option<ReservedOpenFilePermit>,
    open_file_permit_rx: &Receiver<()>,
    open_file_permit_tx: &Sender<()>,
) -> Result<PreparedRestoreEntry> {
    let prepared = prepared?;
    match prepared {
        PreparedRestoreEntry::File { path, entry } => {
            let (permit, permit_wait_elapsed) = match reserved_open_permit {
                Some(reserved) => (reserved.permit, reserved.wait_elapsed),
                None => {
                    let permit_wait_started = Instant::now();
                    let permit =
                        acquire_open_file_permit(open_file_permit_rx, open_file_permit_tx)?;
                    (permit, permit_wait_started.elapsed())
                }
            };
            let open_started = Instant::now();
            let file = fs::File::create(&path)?;
            let writer = PendingFileWriter::new(file, entry.size);
            Ok(PreparedRestoreEntry::FileReady {
                path,
                entry,
                writer,
                permit,
                permit_wait_elapsed,
                open_elapsed: open_started.elapsed(),
            })
        }
        other => Ok(other),
    }
}

fn mark_active_file_counted(
    work_item: &mut PreparedRestoreWorkItem,
    active_files: &mut usize,
    active_files_peak: &mut usize,
) {
    if work_item.active_file_counted {
        return;
    }

    work_item.active_file_counted = true;
    *active_files += 1;
    *active_files_peak = (*active_files_peak).max(*active_files);
}

pub(super) fn try_reserve_open_file_permit_for_work_item(
    work_item: &mut PreparedRestoreWorkItem,
    open_file_permit_rx: &Receiver<()>,
    open_file_permit_tx: &Sender<()>,
) -> Result<bool> {
    debug_assert!(matches!(
        work_item.restore_entry.entry.kind,
        crate::ArchiveEntryKind::File
    ));

    let wait_started = work_item
        .permit_wait_started
        .get_or_insert_with(Instant::now);
    match open_file_permit_rx.try_recv() {
        Ok(()) => {
            work_item.reserved_open_permit = Some(ReservedOpenFilePermit {
                permit: OpenFilePermit {
                    release_tx: open_file_permit_tx.clone(),
                },
                wait_elapsed: wait_started.elapsed(),
            });
            work_item.permit_wait_started = None;
            Ok(true)
        }
        Err(TryRecvError::Empty) => Ok(false),
        Err(TryRecvError::Disconnected) => Err(crate::OxideError::CompressionError(
            "pre-opened file permit queue closed before completion".to_string(),
        )),
    }
}

fn reserve_open_file_permit_for_work_item(
    work_item: &mut PreparedRestoreWorkItem,
    open_file_permit_rx: &Receiver<()>,
    open_file_permit_tx: &Sender<()>,
) -> Result<()> {
    debug_assert!(matches!(
        work_item.restore_entry.entry.kind,
        crate::ArchiveEntryKind::File
    ));

    if work_item.reserved_open_permit.is_some() {
        return Ok(());
    }

    let wait_started = work_item
        .permit_wait_started
        .get_or_insert_with(Instant::now);
    let permit = acquire_open_file_permit(open_file_permit_rx, open_file_permit_tx)?;
    work_item.reserved_open_permit = Some(ReservedOpenFilePermit {
        permit,
        wait_elapsed: wait_started.elapsed(),
    });
    work_item.permit_wait_started = None;
    Ok(())
}

fn acquire_open_file_permit(
    open_file_permit_rx: &Receiver<()>,
    open_file_permit_tx: &Sender<()>,
) -> Result<OpenFilePermit> {
    open_file_permit_rx.recv().map_err(|_| {
        crate::OxideError::CompressionError(
            "pre-opened file permit queue closed before completion".to_string(),
        )
    })?;
    Ok(OpenFilePermit {
        release_tx: open_file_permit_tx.clone(),
    })
}
