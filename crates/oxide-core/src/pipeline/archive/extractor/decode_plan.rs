use super::*;

#[derive(Debug)]
pub(super) struct DecodePlan {
    pub(super) selected_blocks: Vec<bool>,
    pub(super) block_count: usize,
}

impl DecodePlan {
    pub(super) fn all(block_count: usize) -> Self {
        Self {
            selected_blocks: vec![true; block_count],
            block_count,
        }
    }

    pub(super) fn from_ranges(headers: &[ChunkDescriptor], ranges: &[Range<u64>]) -> Self {
        let mut selected_blocks = vec![false; headers.len()];
        let mut block_count = 0usize;
        let mut range_index = 0usize;
        let mut decoded_offset = 0u64;

        for (block_index, header) in headers.iter().enumerate() {
            let block_start = decoded_offset;
            let block_end = block_start.saturating_add(header.raw_len as u64);
            decoded_offset = block_end;

            while range_index < ranges.len() && ranges[range_index].end <= block_start {
                range_index += 1;
            }

            let Some(range) = ranges.get(range_index) else {
                break;
            };

            if range.start < block_end && block_start < range.end {
                selected_blocks[block_index] = true;
                block_count += 1;
            }
        }

        Self {
            selected_blocks,
            block_count,
        }
    }

    pub(super) fn block_count(&self) -> usize {
        self.block_count
    }

    pub(super) fn includes(&self, block_index: usize) -> bool {
        self.selected_blocks[block_index]
    }

    pub(super) fn project_ranges(
        &self,
        headers: &[ChunkDescriptor],
        ranges: &[Range<u64>],
    ) -> Vec<Range<u64>> {
        let mut projected: Vec<Range<u64>> = Vec::new();
        let mut range_index = 0usize;
        let mut decoded_offset = 0u64;
        let mut selected_offset = 0u64;

        for (block_index, header) in headers.iter().enumerate() {
            let block_start = decoded_offset;
            let block_end = block_start.saturating_add(header.raw_len as u64);
            decoded_offset = block_end;

            if !self.includes(block_index) {
                continue;
            }

            while range_index < ranges.len() && ranges[range_index].end <= block_start {
                range_index += 1;
            }

            let mut local_index = range_index;
            while let Some(range) = ranges.get(local_index) {
                if range.start >= block_end {
                    break;
                }

                let overlap_start = range.start.max(block_start);
                let overlap_end = range.end.min(block_end);
                if overlap_start < overlap_end {
                    let projected_start =
                        selected_offset + overlap_start.saturating_sub(block_start);
                    let projected_end = selected_offset + overlap_end.saturating_sub(block_start);

                    if let Some(last) = projected.last_mut() {
                        if last.end == projected_start {
                            last.end = projected_end;
                        } else {
                            projected.push(projected_start..projected_end);
                        }
                    } else {
                        projected.push(projected_start..projected_end);
                    }
                }

                if range.end <= block_end {
                    local_index += 1;
                } else {
                    break;
                }
            }

            range_index = local_index.min(ranges.len());
            selected_offset = selected_offset.saturating_add(header.raw_len as u64);
        }

        projected
    }
}
