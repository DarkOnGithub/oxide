use super::*;

impl DirectoryRestoreWriter {
    pub(crate) fn create_with_performance(
        root: &Path,
        manifest: ArchiveManifest,
        performance: &crate::pipeline::types::PipelinePerformanceOptions,
    ) -> Result<Self> {
        Self::create_with_shards(root, manifest, performance.extract_write_shards.max(1))
    }

    fn create_with_shards(
        root: &Path,
        manifest: ArchiveManifest,
        write_shards: usize,
    ) -> Result<Self> {
        let entries = build_restore_entries(root, manifest)?;
        let window_config = prepared_entry_window_config(&entries);
        let write_shard_count = write_shards.max(1);
        let mut writer = Self {
            root: root.to_path_buf(),
            data_file_count_total: 0,
            completed_data_files: 0,
            pending_file: None,
            write_shards: (write_shard_count > 1)
                .then(|| WriteShardWorker::spawn(write_shard_count)),
            created_directories: HashSet::new(),
            pending_file_metadata: Vec::new(),
            pending_directory_metadata: Vec::new(),
            ready_files: VecDeque::new(),
            planner_finished: false,
            planner_rx: never(),
            planner_handle: None,
            stats: DirectoryRestoreStats::default(),
        };
        writer.stats.ensure_write_shards(write_shard_count);
        writer.ensure_directory_exists(root)?;
        writer.prepare_output_directories(&entries)?;
        let (planner_rx, planner_handle) = spawn_restore_planner(entries, window_config);
        writer.planner_rx = planner_rx;
        writer.planner_handle = Some(planner_handle);
        Ok(writer)
    }

    pub(crate) fn finish(&mut self) -> Result<DirectoryRestoreStats> {
        if let Some(pending) = self.pending_file.as_ref()
            && pending.remaining > 0
        {
            return Err(crate::OxideError::InvalidFormat(
                "truncated file payload during directory restore",
            ));
        }

        self.wait_for_planner_completion()?;
        self.finish_write_shards()?;

        if self.pending_file.is_some() || self.completed_data_files != self.data_file_count_total {
            return Err(crate::OxideError::InvalidFormat(
                "directory restore ended before all entries completed",
            ));
        }
        self.finalize_file_metadata()?;
        self.finalize_directories()?;
        self.join_planner()?;
        Ok(self.stats.clone())
    }

    fn ensure_directory_exists(&mut self, path: &Path) -> Result<()> {
        if path.as_os_str().is_empty() || self.created_directories.contains(path) {
            return Ok(());
        }

        let started = Instant::now();
        fs::create_dir_all(path)?;
        self.stats.record_output_create_directory(started.elapsed());

        let mut current = Some(path);
        while let Some(directory) = current {
            if !directory.starts_with(&self.root) {
                break;
            }
            if !self.created_directories.insert(directory.to_path_buf()) {
                break;
            }
            current = directory.parent();
        }

        Ok(())
    }

    fn defer_file_metadata(&mut self, path: PathBuf, entry: crate::ArchiveListingEntry) {
        self.pending_file_metadata
            .push(PendingMetadata::new(path, entry));
    }

    fn prepare_output_directories(&mut self, entries: &[RestoreEntry]) -> Result<()> {
        let started = Instant::now();
        let directories = entries
            .iter()
            .filter_map(|restore_entry| match restore_entry.entry.kind {
                crate::ArchiveEntryKind::Directory => Some(restore_entry.path.clone()),
                crate::ArchiveEntryKind::File | crate::ArchiveEntryKind::Symlink => restore_entry
                    .path
                    .parent()
                    .filter(|path| !path.as_os_str().is_empty())
                    .map(Path::to_path_buf),
            })
            .collect::<Vec<_>>();

        for directory in directories {
            self.ensure_directory_exists(&directory)?;
        }

        self.stats
            .record_output_prepare_directories(started.elapsed());
        Ok(())
    }

    fn ensure_pending_file_for_write(&mut self) -> Result<bool> {
        self.try_receive_write_shard_completions()?;
        if self.pending_file.is_some() {
            return Ok(true);
        }

        self.pending_file = self.next_pending_file(true)?;
        Ok(self.pending_file.is_some())
    }

    fn ready_file_frontier_depth(&self) -> usize {
        self.ready_files
            .len()
            .saturating_add(self.planner_rx.len())
            .max(1)
    }

    fn drain_planner_ready_files(&mut self) -> Result<()> {
        loop {
            match self.planner_rx.try_recv() {
                Ok(message) => {
                    self.stats
                        .record_planner_ready_queue_peak(self.planner_rx.len());
                    self.consume_planner_message(message?)?;
                    if self.planner_finished {
                        return Ok(());
                    }
                }
                Err(TryRecvError::Empty) => return Ok(()),
                Err(TryRecvError::Disconnected) => {
                    let _ = self.handle_planner_disconnect()?;
                    return Ok(());
                }
            }
        }
    }

    fn next_pending_file(
        &mut self,
        track_file_transition_wait: bool,
    ) -> Result<Option<PendingFile>> {
        if let Some(ready_file) = self.ready_files.pop_front() {
            return self.pending_file_from_ready_file(ready_file).map(Some);
        }

        self.drain_planner_ready_files()?;
        if let Some(ready_file) = self.ready_files.pop_front() {
            return self.pending_file_from_ready_file(ready_file).map(Some);
        }

        if self.planner_finished {
            return Ok(None);
        }

        let wait_started = Instant::now();
        loop {
            match self.planner_rx.recv() {
                Ok(message) => {
                    let wait_elapsed = wait_started.elapsed();
                    self.stats.record_prepared_file_wait(wait_elapsed);
                    if track_file_transition_wait {
                        self.stats.record_file_transition_wait(wait_elapsed);
                    }
                    self.stats
                        .record_planner_ready_queue_peak(self.planner_rx.len());
                    self.consume_planner_message(message?)?;
                    self.drain_planner_ready_files()?;
                    if let Some(ready_file) = self.ready_files.pop_front() {
                        return self.pending_file_from_ready_file(ready_file).map(Some);
                    }
                    if self.planner_finished {
                        return Ok(None);
                    }
                }
                Err(_) => {
                    let wait_elapsed = wait_started.elapsed();
                    self.stats.record_prepared_file_wait(wait_elapsed);
                    if track_file_transition_wait {
                        self.stats.record_file_transition_wait(wait_elapsed);
                    }
                    let _ = self.handle_planner_disconnect()?;
                    return match self.ready_files.pop_front() {
                        Some(ready_file) => self.pending_file_from_ready_file(ready_file).map(Some),
                        None => Ok(None),
                    };
                }
            }
        }
    }

    fn consume_planner_message(&mut self, message: RestorePlannerMessage) -> Result<()> {
        match message {
            RestorePlannerMessage::ReadyFile(ready_file) => {
                self.stats
                    .record_ready_file_frontier(self.ready_file_frontier_depth());
                self.ready_files.push_back(ready_file);
                Ok(())
            }
            RestorePlannerMessage::Finished(outcome) => {
                self.apply_planner_outcome(outcome);
                Ok(())
            }
        }
    }

    fn pending_file_from_ready_file(&mut self, ready_file: ReadyFileWrite) -> Result<PendingFile> {
        if let Some(write_shards) = self.write_shards.as_mut() {
            let file_size = ready_file.entry.size;
            let (file_id, shard) = write_shards.reserve_file(file_size);
            if let Err(error) = write_shards.send_task(
                shard,
                WriteShardTask::OpenFile {
                    file_id,
                    writer: ready_file.writer,
                    permit: ready_file.permit,
                },
                &mut self.stats,
                &mut self.pending_file_metadata,
                &mut self.completed_data_files,
            ) {
                write_shards.release_file(shard, file_size);
                return Err(error);
            }
            Ok(PendingFile {
                remaining: ready_file.entry.size,
                path: ready_file.path,
                entry: ready_file.entry,
                state: PendingFileState::Sharded { file_id, shard },
            })
        } else {
            Ok(PendingFile {
                remaining: ready_file.entry.size,
                path: ready_file.path,
                entry: ready_file.entry,
                state: PendingFileState::Local {
                    writer: ready_file.writer,
                    permit: ready_file.permit,
                },
            })
        }
    }

    fn apply_planner_outcome(&mut self, outcome: RestorePlannerOutcome) {
        self.planner_finished = true;
        self.data_file_count_total = outcome.data_file_count_total;
        self.pending_file_metadata
            .extend(outcome.pending_file_metadata);
        self.pending_directory_metadata
            .extend(outcome.pending_directory_metadata);
        self.stats.merge(outcome.stats);
    }

    fn wait_for_planner_completion(&mut self) -> Result<()> {
        while !self.planner_finished {
            self.try_receive_write_shard_completions()?;
            self.drain_planner_ready_files()?;
            if self.planner_finished {
                break;
            }

            let wait_started = Instant::now();
            match self.planner_rx.recv() {
                Ok(message) => {
                    self.stats.record_prepared_file_wait(wait_started.elapsed());
                    self.stats
                        .record_planner_ready_queue_peak(self.planner_rx.len());
                    self.consume_planner_message(message?)?;
                }
                Err(_) => {
                    self.stats.record_prepared_file_wait(wait_started.elapsed());
                    let _ = self.handle_planner_disconnect()?;
                }
            }
        }

        Ok(())
    }

    fn try_receive_write_shard_completions(&mut self) -> Result<()> {
        if let Some(write_shards) = self.write_shards.as_mut() {
            write_shards.try_receive_completions(
                &mut self.stats,
                &mut self.pending_file_metadata,
                &mut self.completed_data_files,
            )?;
        }
        Ok(())
    }

    fn finish_write_shards(&mut self) -> Result<()> {
        if let Some(write_shards) = self.write_shards.as_mut() {
            write_shards.finish(
                &mut self.stats,
                &mut self.pending_file_metadata,
                &mut self.completed_data_files,
            )?;
        }
        Ok(())
    }

    fn handle_planner_disconnect(&mut self) -> Result<Option<PendingFile>> {
        self.join_planner()?;
        if self.planner_finished {
            Ok(None)
        } else {
            Err(crate::OxideError::CompressionError(
                "restore planner queue closed before completion".to_string(),
            ))
        }
    }

    fn join_planner(&mut self) -> Result<()> {
        let Some(handle) = self.planner_handle.take() else {
            return Ok(());
        };

        handle.join().map_err(|payload| {
            let details = if let Some(message) = payload.downcast_ref::<&str>() {
                (*message).to_string()
            } else if let Some(message) = payload.downcast_ref::<String>() {
                message.clone()
            } else {
                "unknown panic payload".to_string()
            };
            crate::OxideError::CompressionError(format!(
                "restore planner thread panicked: {details}"
            ))
        })?
    }

    fn finalize_pending_file(&mut self) -> Result<()> {
        let pending = self.pending_file.take().ok_or(OxideError::InvalidFormat(
            "directory restore missing pending file after write completion",
        ))?;
        let PendingFile {
            path, entry, state, ..
        } = pending;
        match state {
            PendingFileState::Local { writer, permit } => {
                let started = Instant::now();
                let _file = writer.into_inner()?;
                self.stats.record_output_flush(started.elapsed());
                drop(permit);
                self.defer_file_metadata(path, entry);
                self.completed_data_files = self.completed_data_files.saturating_add(1);
            }
            PendingFileState::Sharded { file_id, .. } => {
                let _ = (file_id, path, entry);
            }
        }
        Ok(())
    }

    fn write_shared_chunk(&mut self, chunk: SharedChunk, range: Range<usize>) -> Result<()> {
        let bytes = &chunk.as_ref()[range.clone()];
        self.write_chunk_range(bytes, Some(&chunk), range.start)
    }

    fn write_chunk_range(
        &mut self,
        mut bytes: &[u8],
        shared_chunk: Option<&SharedChunk>,
        shared_offset: usize,
    ) -> Result<()> {
        let ordered_write_started = Instant::now();
        self.try_receive_write_shard_completions()?;
        if !bytes.is_empty() && self.pending_file.is_none() {
            let decode_started = Instant::now();
            if !self.ensure_pending_file_for_write()? {
                return Err(OxideError::InvalidFormat(
                    "directory archive contains file data beyond manifest entries",
                ));
            }
            self.stats.directory_decode += decode_started.elapsed();
        }

        let mut chunk_offset = 0usize;
        while !bytes.is_empty() {
            let (take, finished_file) = {
                let pending = self.pending_file.as_mut().ok_or(OxideError::InvalidFormat(
                    "directory archive contains file data beyond manifest entries",
                ))?;

                let take = bytes.len().min(pending.remaining as usize);
                pending.remaining -= take as u64;
                let write_slice = &bytes[..take];
                match &mut pending.state {
                    PendingFileState::Local { writer, .. } => {
                        let started = Instant::now();
                        writer.write_all(write_slice)?;
                        let write_elapsed = started.elapsed();
                        self.stats.record_output_data(write_elapsed);
                        self.stats.record_write_shard_output_data(0, write_elapsed);
                    }
                    PendingFileState::Sharded { file_id, shard } => {
                        let write_shards = self.write_shards.as_mut().ok_or_else(|| {
                            crate::OxideError::CompressionError(
                                "directory restore missing write shards during data dispatch"
                                    .to_string(),
                            )
                        })?;
                        let finish_file = pending.remaining == 0;
                        if finish_file {
                            write_shards.register_pending_completion(
                                *file_id,
                                *shard,
                                pending.path.clone(),
                                pending.entry.clone(),
                            );
                        }
                        let task = if let Some(shared) = shared_chunk {
                            WriteShardTask::WriteData {
                                file_id: *file_id,
                                chunk: shared.clone(),
                                range: (shared_offset + chunk_offset)
                                    ..(shared_offset + chunk_offset + take),
                                finish: finish_file,
                            }
                        } else {
                            WriteShardTask::WriteData {
                                file_id: *file_id,
                                chunk: OwnedChunk::Owned(write_slice.to_vec()).into_shared(),
                                range: 0..take,
                                finish: finish_file,
                            }
                        };
                        if let Err(error) = write_shards.send_task(
                            *shard,
                            task,
                            &mut self.stats,
                            &mut self.pending_file_metadata,
                            &mut self.completed_data_files,
                        ) {
                            if finish_file {
                                write_shards.forget_pending_completion(*file_id);
                            }
                            write_shards.release_file(*shard, pending.entry.size);
                            return Err(error);
                        }
                    }
                }
                (take, pending.remaining == 0)
            };

            self.stats.output_bytes_total =
                self.stats.output_bytes_total.saturating_add(take as u64);
            bytes = &bytes[take..];
            chunk_offset = chunk_offset.saturating_add(take);

            if finished_file {
                self.finalize_pending_file()?;
                if !bytes.is_empty() {
                    let decode_started = Instant::now();
                    if !self.ensure_pending_file_for_write()? {
                        return Err(OxideError::InvalidFormat(
                            "directory archive contains file data beyond manifest entries",
                        ));
                    }
                    self.stats.directory_decode += decode_started.elapsed();
                }
            }
        }

        self.stats.ordered_write_time += ordered_write_started.elapsed();

        Ok(())
    }

    fn finalize_file_metadata(&mut self) -> Result<()> {
        let pending = self.pending_file_metadata.drain(..).collect::<Vec<_>>();
        if pending.is_empty() {
            return Ok(());
        }

        let started = Instant::now();
        apply_pending_metadata_in_parallel(&pending)?;
        self.stats.record_output_metadata_file(started.elapsed());
        Ok(())
    }

    fn finalize_directories(&mut self) -> Result<()> {
        if self.pending_directory_metadata.is_empty() {
            return Ok(());
        }

        self.pending_directory_metadata
            .sort_unstable_by_key(|directory| Reverse(directory.depth));

        let started = Instant::now();
        let mut range_start = 0usize;
        while range_start < self.pending_directory_metadata.len() {
            let depth = self.pending_directory_metadata[range_start].depth;
            let mut range_end = range_start + 1;
            while range_end < self.pending_directory_metadata.len()
                && self.pending_directory_metadata[range_end].depth == depth
            {
                range_end += 1;
            }

            apply_pending_metadata_in_parallel(
                &self.pending_directory_metadata[range_start..range_end],
            )?;
            range_start = range_end;
        }

        self.pending_directory_metadata.clear();
        self.stats
            .record_output_metadata_directory(started.elapsed());
        Ok(())
    }
}

impl Drop for DirectoryRestoreWriter {
    fn drop(&mut self) {
        if let Some(write_shards) = self.write_shards.as_mut() {
            write_shards.shutdown();
        }
        let _ = self.join_planner();
    }
}

fn build_restore_entries(root: &Path, manifest: ArchiveManifest) -> Result<Vec<RestoreEntry>> {
    manifest
        .entries()
        .iter()
        .map(|entry| {
            Ok(RestoreEntry {
                path: directory::join_safe(root, &entry.path)?,
                entry: entry.clone(),
            })
        })
        .collect()
}

impl FilteredDirectoryRestoreWriter {
    #[allow(dead_code)]
    pub(crate) fn create(
        root: &Path,
        manifest: ArchiveManifest,
        selected_ranges: Vec<Range<u64>>,
    ) -> Result<Self> {
        Self::create_with_performance(
            root,
            manifest,
            selected_ranges,
            &crate::pipeline::types::PipelinePerformanceOptions::default(),
        )
    }

    pub(crate) fn create_with_performance(
        root: &Path,
        manifest: ArchiveManifest,
        selected_ranges: Vec<Range<u64>>,
        performance: &crate::pipeline::types::PipelinePerformanceOptions,
    ) -> Result<Self> {
        Ok(Self {
            inner: DirectoryRestoreWriter::create_with_performance(root, manifest, performance)?,
            selected_ranges,
            next_range: 0,
            input_offset: 0,
        })
    }

    pub(crate) fn finish(&mut self) -> Result<DirectoryRestoreStats> {
        self.inner.finish()
    }
}

impl OrderedChunkWriter for DirectoryRestoreWriter {
    fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        self.write_chunk_range(bytes, None, 0)
    }

    fn write_owned_chunk(&mut self, chunk: OwnedChunk) -> Result<()> {
        let shared = chunk.into_shared();
        let len = shared.len();
        self.write_shared_chunk(shared, 0..len)
    }
}

impl OrderedChunkWriter for FilteredDirectoryRestoreWriter {
    fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        let chunk_start = self.input_offset;
        let chunk_end = chunk_start.saturating_add(bytes.len() as u64);

        while self.next_range < self.selected_ranges.len()
            && self.selected_ranges[self.next_range].end <= chunk_start
        {
            self.next_range += 1;
        }

        let mut range_index = self.next_range;
        while range_index < self.selected_ranges.len() {
            let range = &self.selected_ranges[range_index];
            if range.start >= chunk_end {
                break;
            }

            let write_start = range.start.max(chunk_start) - chunk_start;
            let write_end = range.end.min(chunk_end) - chunk_start;
            if write_start < write_end {
                self.inner
                    .write_chunk(&bytes[write_start as usize..write_end as usize])?;
            }

            if range.end <= chunk_end {
                range_index += 1;
            } else {
                break;
            }
        }

        self.next_range = range_index;
        self.input_offset = chunk_end;
        Ok(())
    }

    fn write_owned_chunk(&mut self, chunk: OwnedChunk) -> Result<()> {
        let shared = chunk.into_shared();
        let chunk_start = self.input_offset;
        let chunk_end = chunk_start.saturating_add(shared.len() as u64);

        while self.next_range < self.selected_ranges.len()
            && self.selected_ranges[self.next_range].end <= chunk_start
        {
            self.next_range += 1;
        }

        let mut range_index = self.next_range;
        while range_index < self.selected_ranges.len() {
            let range = &self.selected_ranges[range_index];
            if range.start >= chunk_end {
                break;
            }

            let write_start = range.start.max(chunk_start) - chunk_start;
            let write_end = range.end.min(chunk_end) - chunk_start;
            if write_start < write_end {
                self.inner
                    .write_shared_chunk(shared.clone(), write_start as usize..write_end as usize)?;
            }

            if range.end <= chunk_end {
                range_index += 1;
            } else {
                break;
            }
        }

        self.next_range = range_index;
        self.input_offset = chunk_end;
        Ok(())
    }
}
