use super::*;

#[derive(Default)]
pub(super) struct VecChunkWriter {
    output: Vec<u8>,
}

impl VecChunkWriter {
    pub(super) fn into_inner(self) -> Vec<u8> {
        self.output
    }
}

impl OrderedChunkWriter for VecChunkWriter {
    fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        self.output.extend_from_slice(bytes);
        Ok(())
    }
}

pub(super) struct FileChunkWriter {
    writer: BufWriter<fs::File>,
    path: PathBuf,
    entry: crate::ArchiveListingEntry,
    stats: FileRestoreStats,
}

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct FileRestoreStats {
    ordered_write_time: Duration,
    output_write: Duration,
    output_create: Duration,
    output_create_directories: Duration,
    output_create_files: Duration,
    output_data: Duration,
    output_flush: Duration,
    output_metadata: Duration,
    output_metadata_files: Duration,
    write_shard_output_data: Duration,
}

impl FileRestoreStats {
    fn record_output_create_directories(&mut self, elapsed: Duration) {
        self.output_write += elapsed;
        self.output_create += elapsed;
        self.output_create_directories += elapsed;
    }

    fn record_output_create_file(&mut self, elapsed: Duration) {
        self.output_write += elapsed;
        self.output_create += elapsed;
        self.output_create_files += elapsed;
    }

    fn record_output_data(&mut self, elapsed: Duration) {
        self.output_write += elapsed;
        self.output_data += elapsed;
        self.write_shard_output_data += elapsed;
    }

    fn record_output_flush(&mut self, elapsed: Duration) {
        self.output_write += elapsed;
        self.output_flush += elapsed;
    }

    fn record_output_metadata_file(&mut self, elapsed: Duration) {
        self.output_write += elapsed;
        self.output_metadata += elapsed;
        self.output_metadata_files += elapsed;
    }
}

impl FileChunkWriter {
    pub(super) fn create(path: &Path, entry: crate::ArchiveListingEntry) -> Result<Self> {
        let mut stats = FileRestoreStats::default();
        if let Some(parent) = path.parent().filter(|path| !path.as_os_str().is_empty()) {
            let output_started = Instant::now();
            fs::create_dir_all(parent)?;
            stats.record_output_create_directories(output_started.elapsed());
        }

        let output_started = Instant::now();
        let file = fs::File::create(path)?;
        stats.record_output_create_file(output_started.elapsed());
        Ok(Self {
            writer: BufWriter::with_capacity(OUTPUT_BUFFER_CAPACITY, file),
            path: path.to_path_buf(),
            entry,
            stats,
        })
    }

    pub(super) fn flush_and_apply_metadata(&mut self) -> Result<()> {
        let flush_started = Instant::now();
        self.writer.flush()?;
        self.stats.record_output_flush(flush_started.elapsed());

        let metadata_started = Instant::now();
        apply_entry_metadata(&self.path, &self.entry)?;
        self.stats
            .record_output_metadata_file(metadata_started.elapsed());
        Ok(())
    }

    pub(super) fn stats(&self) -> FileRestoreStats {
        self.stats
    }
}

impl OrderedChunkWriter for FileChunkWriter {
    fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        let write_started = Instant::now();
        self.writer.write_all(bytes)?;
        let elapsed = write_started.elapsed();
        self.stats.ordered_write_time += elapsed;
        self.stats.record_output_data(elapsed);
        Ok(())
    }
}

pub(super) fn apply_file_restore_stats(
    stage_timings: &mut ExtractStageTimings,
    stats: FileRestoreStats,
) {
    stage_timings.ordered_write = stage_timings
        .ordered_write
        .saturating_sub(stats.ordered_write_time);
    stage_timings.output_write += stats.output_write;
    stage_timings.output_create += stats.output_create;
    stage_timings.output_create_directories += stats.output_create_directories;
    stage_timings.output_create_files += stats.output_create_files;
    stage_timings.output_data += stats.output_data;
    stage_timings.output_flush += stats.output_flush;
    stage_timings.output_metadata += stats.output_metadata;
    stage_timings.output_metadata_files += stats.output_metadata_files;
    stage_timings.record_write_shard_output_data(0, stats.write_shard_output_data);
}
