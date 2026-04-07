use super::*;

impl WriteShardWorker {
    pub(super) fn spawn(shard_count: usize) -> Self {
        let (completion_tx, completion_rx) = unbounded::<Result<WriteShardCompletion>>();
        let mut task_txs = Vec::with_capacity(shard_count);
        let mut handles = Vec::with_capacity(shard_count);

        for shard in 0..shard_count {
            let (task_tx, task_rx) = bounded::<WriteShardTask>(WRITE_SHARD_QUEUE_CAPACITY);
            let completion_tx = completion_tx.clone();
            let handle = thread::spawn(move || run_write_shard(shard, task_rx, completion_tx));
            task_txs.push(task_tx);
            handles.push(handle);
        }

        Self {
            shard_count,
            next_file_id: 0,
            next_shard_hint: 0,
            shard_load_bytes: vec![0; shard_count],
            completion_rx,
            task_txs,
            handles,
            pending: BTreeMap::new(),
            aborted: false,
        }
    }

    pub(super) fn reserve_file(&mut self, file_size: u64) -> (usize, usize) {
        let file_id = self.next_file_id;
        let mut shard = 0usize;
        let mut best = (u64::MAX, usize::MAX, usize::MAX);
        for offset in 0..self.shard_count.max(1) {
            let candidate = (self.next_shard_hint + offset) % self.shard_count.max(1);
            let candidate_load = self.shard_load_bytes[candidate];
            let candidate_queue = self.task_txs[candidate].len();
            let candidate_key = (candidate_load, candidate_queue, offset);
            if candidate_key < best {
                best = candidate_key;
                shard = candidate;
            }
        }
        self.next_file_id = self.next_file_id.saturating_add(1);
        self.next_shard_hint = (shard + 1) % self.shard_count.max(1);
        self.shard_load_bytes[shard] = self.shard_load_bytes[shard].saturating_add(file_size);
        (file_id, shard)
    }

    pub(super) fn release_file(&mut self, shard: usize, file_size: u64) {
        if let Some(load) = self.shard_load_bytes.get_mut(shard) {
            *load = load.saturating_sub(file_size);
        }
    }

    fn record_queue_peak(&self, stats: &mut DirectoryRestoreStats, shard: usize) {
        if let Some(tx) = self.task_txs.get(shard) {
            stats.record_write_shard_queue_peak(shard, tx.len());
        }
    }

    pub(super) fn send_task(
        &mut self,
        shard: usize,
        task: WriteShardTask,
        stats: &mut DirectoryRestoreStats,
        pending_file_metadata: &mut Vec<PendingMetadata>,
        completed_data_files: &mut usize,
    ) -> Result<()> {
        self.try_receive_completions(stats, pending_file_metadata, completed_data_files)?;
        let Some(tx) = self.task_txs.get(shard) else {
            return Err(crate::OxideError::CompressionError(format!(
                "write shard {shard} is not available"
            )));
        };

        match tx.try_send(task) {
            Ok(()) => {
                self.record_queue_peak(stats, shard);
                Ok(())
            }
            Err(TrySendError::Full(task)) => {
                let blocked_started = Instant::now();
                tx.send(task).map_err(|_| {
                    crate::OxideError::CompressionError(format!(
                        "write shard {shard} queue closed before completion"
                    ))
                })?;
                stats.record_write_shard_blocked(shard, blocked_started.elapsed());
                self.record_queue_peak(stats, shard);
                Ok(())
            }
            Err(TrySendError::Disconnected(_)) => Err(crate::OxideError::CompressionError(
                format!("write shard {shard} disconnected before completion"),
            )),
        }
    }

    pub(super) fn register_pending_completion(
        &mut self,
        file_id: usize,
        shard: usize,
        path: PathBuf,
        entry: crate::ArchiveListingEntry,
    ) {
        self.pending
            .insert(file_id, PendingShardCompletion { path, entry, shard });
    }

    pub(super) fn forget_pending_completion(&mut self, file_id: usize) {
        let _ = self.pending.remove(&file_id);
    }

    pub(super) fn try_receive_completions(
        &mut self,
        stats: &mut DirectoryRestoreStats,
        pending_file_metadata: &mut Vec<PendingMetadata>,
        completed_data_files: &mut usize,
    ) -> Result<()> {
        loop {
            match self.completion_rx.try_recv() {
                Ok(completion) => self.handle_completion(
                    completion,
                    stats,
                    pending_file_metadata,
                    completed_data_files,
                )?,
                Err(TryRecvError::Empty) => return Ok(()),
                Err(TryRecvError::Disconnected) => {
                    return Err(crate::OxideError::CompressionError(
                        "write shard completion queue closed before completion".to_string(),
                    ));
                }
            }
        }
    }

    pub(super) fn finish(
        &mut self,
        stats: &mut DirectoryRestoreStats,
        pending_file_metadata: &mut Vec<PendingMetadata>,
        completed_data_files: &mut usize,
    ) -> Result<()> {
        while !self.pending.is_empty() {
            let completion = self.completion_rx.recv().map_err(|_| {
                crate::OxideError::CompressionError(
                    "write shard completion queue closed before all files finished".to_string(),
                )
            })?;
            self.handle_completion(
                completion,
                stats,
                pending_file_metadata,
                completed_data_files,
            )?;
        }

        self.join(stats)
    }

    fn abort(&mut self) {
        if self.aborted {
            return;
        }
        self.aborted = true;
        for tx in &self.task_txs {
            let _ = tx.try_send(WriteShardTask::Abort);
        }
    }

    pub(super) fn shutdown(&mut self) {
        self.abort();
        let mut stats = DirectoryRestoreStats::default();
        let _ = self.join_outputs_only(&mut stats);
    }

    fn handle_completion(
        &mut self,
        completion: Result<WriteShardCompletion>,
        stats: &mut DirectoryRestoreStats,
        pending_file_metadata: &mut Vec<PendingMetadata>,
        completed_data_files: &mut usize,
    ) -> Result<()> {
        let completion = completion?;
        let pending = self.pending.remove(&completion.file_id).ok_or_else(|| {
            crate::OxideError::CompressionError("write shard completed unknown file".to_string())
        })?;
        self.shard_load_bytes[pending.shard] =
            self.shard_load_bytes[pending.shard].saturating_sub(pending.entry.size);
        stats.record_output_flush(completion.flush_elapsed);
        pending_file_metadata.push(PendingMetadata::new(pending.path, pending.entry));
        *completed_data_files = completed_data_files.saturating_add(1);
        Ok(())
    }

    fn join(&mut self, stats: &mut DirectoryRestoreStats) -> Result<()> {
        self.join_outputs_only(stats)
    }

    fn join_outputs_only(&mut self, stats: &mut DirectoryRestoreStats) -> Result<()> {
        for tx in self.task_txs.drain(..) {
            drop(tx);
        }

        for handle in self.handles.drain(..) {
            let outcome = handle.join().map_err(|payload| {
                let details = if let Some(message) = payload.downcast_ref::<&str>() {
                    (*message).to_string()
                } else if let Some(message) = payload.downcast_ref::<String>() {
                    message.clone()
                } else {
                    "unknown panic payload".to_string()
                };
                crate::OxideError::CompressionError(format!(
                    "write shard thread panicked: {details}"
                ))
            })??;
            stats.record_output_data(outcome.output_data);
            stats.record_write_shard_output_data(outcome.shard, outcome.output_data);
        }
        Ok(())
    }
}

fn run_write_shard(
    shard: usize,
    task_rx: Receiver<WriteShardTask>,
    completion_tx: Sender<Result<WriteShardCompletion>>,
) -> Result<WriteShardOutcome> {
    let result = run_write_shard_inner(shard, task_rx, &completion_tx);
    if let Err(error) = &result {
        let _ = completion_tx.send(Err(crate::OxideError::CompressionError(error.to_string())));
    }
    result
}

fn run_write_shard_inner(
    shard: usize,
    task_rx: Receiver<WriteShardTask>,
    completion_tx: &Sender<Result<WriteShardCompletion>>,
) -> Result<WriteShardOutcome> {
    let mut outcome = WriteShardOutcome {
        shard,
        ..WriteShardOutcome::default()
    };
    let mut pending: Option<ShardPendingFile> = None;

    while let Ok(task) = task_rx.recv() {
        match task {
            WriteShardTask::OpenFile {
                file_id,
                writer,
                permit,
            } => {
                if pending.is_some() {
                    return Err(crate::OxideError::CompressionError(format!(
                        "write shard {shard} received a new file before finishing the previous one"
                    )));
                }
                pending = Some(ShardPendingFile {
                    file_id,
                    writer,
                    permit,
                });
            }
            WriteShardTask::WriteData {
                file_id,
                chunk,
                range,
                finish,
            } => {
                let current = pending.as_mut().ok_or_else(|| {
                    crate::OxideError::CompressionError(format!(
                        "write shard {shard} received data without an open file"
                    ))
                })?;
                if current.file_id != file_id {
                    return Err(crate::OxideError::CompressionError(format!(
                        "write shard {shard} received out-of-order file data"
                    )));
                }

                let write_started = Instant::now();
                current.writer.write_all(&chunk.as_ref()[range])?;
                outcome.output_data += write_started.elapsed();

                if finish {
                    let pending_file = pending.take().ok_or_else(|| {
                        crate::OxideError::CompressionError(
                            "write shard lost pending file during finalization".to_string(),
                        )
                    })?;
                    let ShardPendingFile {
                        file_id,
                        writer,
                        permit,
                    } = pending_file;
                    let flush_started = Instant::now();
                    let _file = writer.into_inner()?;
                    let flush_elapsed = flush_started.elapsed();
                    drop(permit);
                    if completion_tx
                        .send(Ok(WriteShardCompletion {
                            file_id,
                            flush_elapsed,
                        }))
                        .is_err()
                    {
                        return Ok(outcome);
                    }
                }
            }
            WriteShardTask::Abort => return Ok(outcome),
        }
    }

    if pending.is_some() {
        return Err(crate::OxideError::CompressionError(format!(
            "write shard {shard} closed with a pending file"
        )));
    }

    Ok(outcome)
}
