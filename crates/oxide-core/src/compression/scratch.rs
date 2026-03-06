use super::lz4::Lz4Scratch;

#[derive(Debug, Default)]
pub(crate) struct CompressionScratchArena {
    lz4: Lz4Scratch,
}

impl CompressionScratchArena {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn lz4(&mut self) -> &mut Lz4Scratch {
        &mut self.lz4
    }

    pub(crate) fn allocated_bytes(&self) -> usize {
        self.lz4.allocated_bytes()
    }
}
