use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) struct ReadRequest {
    pub(super) index: usize,
    pub(super) block_index: u32,
    pub(super) encoded_len: usize,
}

pub(super) trait DecodeReadBackend: Send + 'static {
    fn source_kind(&self) -> ArchiveSourceKind;

    fn block_descriptors(&self) -> &[ChunkDescriptor];

    fn global_header(&self) -> GlobalHeader;

    fn dictionary_bank(&self) -> ArchiveDictionaryBank;

    fn spawn(
        self,
        read_request_rx: Receiver<ReadRequest>,
        task_tx: Sender<DecodeTask>,
        result_tx: Sender<(usize, Duration, Result<DecodedBlock>)>,
        buffer_pool: Arc<BufferPool>,
        reader_threads: usize,
    ) -> Vec<thread::JoinHandle<Result<()>>>;
}

pub(super) struct SequentialDecodeReadBackend<R: Read + Seek> {
    archive: ArchiveReader<R>,
}

impl<R: Read + Seek> SequentialDecodeReadBackend<R> {
    pub(super) fn new(archive: ArchiveReader<R>) -> Self {
        Self { archive }
    }
}

impl<R> DecodeReadBackend for SequentialDecodeReadBackend<R>
where
    R: Read + Seek + Send + 'static,
{
    fn source_kind(&self) -> ArchiveSourceKind {
        self.archive.source_kind()
    }

    fn block_descriptors(&self) -> &[ChunkDescriptor] {
        self.archive.block_descriptors()
    }

    fn global_header(&self) -> GlobalHeader {
        self.archive.global_header()
    }

    fn dictionary_bank(&self) -> ArchiveDictionaryBank {
        self.archive.manifest().dictionary_bank().clone()
    }

    fn spawn(
        self,
        read_request_rx: Receiver<ReadRequest>,
        task_tx: Sender<DecodeTask>,
        result_tx: Sender<(usize, Duration, Result<DecodedBlock>)>,
        buffer_pool: Arc<BufferPool>,
        _reader_threads: usize,
    ) -> Vec<thread::JoinHandle<Result<()>>> {
        vec![thread::spawn(move || {
            spawn_io_reader(
                self.archive,
                read_request_rx,
                task_tx,
                result_tx,
                buffer_pool,
            )
        })]
    }
}

pub(super) struct ParallelFileDecodeReadBackend {
    archive: ArchiveReader<fs::File>,
    file: Arc<fs::File>,
    resolved_descriptors: Arc<Vec<ChunkDescriptor>>,
}

impl ParallelFileDecodeReadBackend {
    pub(super) fn new(archive: ArchiveReader<fs::File>, file: fs::File) -> Result<Self> {
        let mut resolved_descriptors = Vec::with_capacity(archive.block_count() as usize);
        for block_index in 0..archive.block_count() {
            resolved_descriptors.push(archive.resolved_block_descriptor(block_index)?);
        }

        Ok(Self {
            archive,
            file: Arc::new(file),
            resolved_descriptors: Arc::new(resolved_descriptors),
        })
    }
}

impl DecodeReadBackend for ParallelFileDecodeReadBackend {
    fn source_kind(&self) -> ArchiveSourceKind {
        self.archive.source_kind()
    }

    fn block_descriptors(&self) -> &[ChunkDescriptor] {
        self.archive.block_descriptors()
    }

    fn global_header(&self) -> GlobalHeader {
        self.archive.global_header()
    }

    fn dictionary_bank(&self) -> ArchiveDictionaryBank {
        self.archive.manifest().dictionary_bank().clone()
    }

    fn spawn(
        self,
        read_request_rx: Receiver<ReadRequest>,
        task_tx: Sender<DecodeTask>,
        result_tx: Sender<(usize, Duration, Result<DecodedBlock>)>,
        buffer_pool: Arc<BufferPool>,
        reader_threads: usize,
    ) -> Vec<thread::JoinHandle<Result<()>>> {
        let reader_count = reader_threads.max(1);
        let mut handles = Vec::with_capacity(reader_count);
        for _ in 0..reader_count {
            let local_rx = read_request_rx.clone();
            let local_task_tx = task_tx.clone();
            let local_result_tx = result_tx.clone();
            let local_pool = Arc::clone(&buffer_pool);
            let local_file = Arc::clone(&self.file);
            let local_resolved = Arc::clone(&self.resolved_descriptors);
            handles.push(thread::spawn(move || {
                spawn_offset_io_reader(
                    local_file,
                    local_resolved,
                    local_rx,
                    local_task_tx,
                    local_result_tx,
                    local_pool,
                )
            }));
        }
        handles
    }
}

fn spawn_io_reader<R>(
    mut archive: ArchiveReader<R>,
    read_request_rx: Receiver<ReadRequest>,
    task_tx: Sender<DecodeTask>,
    result_tx: Sender<(usize, Duration, Result<DecodedBlock>)>,
    buffer_pool: Arc<BufferPool>,
) -> Result<()>
where
    R: Read + Seek,
{
    while let Ok(request) = read_request_rx.recv() {
        let read_started = Instant::now();
        let mut block_data = buffer_pool.acquire_with_capacity(request.encoded_len);
        let read_result = archive.read_block_into(request.block_index, block_data.as_mut_vec());
        let read_elapsed = read_started.elapsed();

        match read_result {
            Ok(header) => {
                task_tx
                    .send(DecodeTask {
                        index: request.index,
                        header,
                        block_data,
                        read_elapsed,
                    })
                    .map_err(|_| {
                        crate::OxideError::CompressionError(
                            "decode queue closed before I/O submission completed".to_string(),
                        )
                    })?;
            }
            Err(error) => {
                result_tx
                    .send((request.index, read_elapsed, Err(error)))
                    .map_err(|_| {
                        crate::OxideError::CompressionError(
                            "decode result channel closed before I/O error delivery".to_string(),
                        )
                    })?;
            }
        }
    }

    archive.finish_sequential_extract_validation()
}

fn spawn_offset_io_reader(
    file: Arc<fs::File>,
    resolved_descriptors: Arc<Vec<ChunkDescriptor>>,
    read_request_rx: Receiver<ReadRequest>,
    task_tx: Sender<DecodeTask>,
    result_tx: Sender<(usize, Duration, Result<DecodedBlock>)>,
    buffer_pool: Arc<BufferPool>,
) -> Result<()> {
    while let Ok(request) = read_request_rx.recv() {
        let read_started = Instant::now();
        let header = *resolved_descriptors
            .get(request.block_index as usize)
            .ok_or(crate::OxideError::InvalidFormat(
                "decode read block index out of range",
            ))?;
        let mut block_data = buffer_pool.acquire_with_capacity(header.encoded_len as usize);
        block_data
            .as_mut_vec()
            .resize(header.encoded_len as usize, 0);
        let read_result = read_exact_file_at(
            file.as_ref(),
            header.payload_offset,
            block_data.as_mut_vec().as_mut_slice(),
        )
        .map_err(crate::OxideError::from);
        let read_elapsed = read_started.elapsed();

        match read_result {
            Ok(()) => {
                task_tx
                    .send(DecodeTask {
                        index: request.index,
                        header,
                        block_data,
                        read_elapsed,
                    })
                    .map_err(|_| {
                        crate::OxideError::CompressionError(
                            "decode queue closed before I/O submission completed".to_string(),
                        )
                    })?;
            }
            Err(error) => {
                result_tx
                    .send((request.index, read_elapsed, Err(error)))
                    .map_err(|_| {
                        crate::OxideError::CompressionError(
                            "decode result channel closed before I/O error delivery".to_string(),
                        )
                    })?;
            }
        }
    }

    Ok(())
}
