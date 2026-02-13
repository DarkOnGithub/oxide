use std::collections::BTreeMap;

use crate::{OxideError, Result};

use super::DEFAULT_REORDER_PENDING_LIMIT;

/// A buffer that reorders items submitted out-of-order into a sequential stream.
///
/// Items are stored in a `BTreeMap` until the next expected sequential ID is pushed,
/// at which point all contiguous items starting from that ID are released.
#[derive(Debug)]
pub struct ReorderBuffer<T> {
    next_id: usize,
    pending: BTreeMap<usize, T>,
    max_pending: usize,
}

impl<T> Default for ReorderBuffer<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ReorderBuffer<T> {
    /// Creates a new reorder buffer with the default capacity.
    pub fn new() -> Self {
        Self::with_limit(DEFAULT_REORDER_PENDING_LIMIT)
    }

    /// Creates a new reorder buffer with a specific capacity.
    pub fn with_limit(max_pending: usize) -> Self {
        Self {
            next_id: 0,
            pending: BTreeMap::new(),
            max_pending: max_pending.max(1),
        }
    }

    /// Pushes an item into the buffer.
    ///
    /// If the `id` matches the next expected ID, it and any subsequent
    /// contiguous ready items are returned in a vector.
    ///
    /// # Errors
    /// Returns an error if the `id` has already been processed or if the
    /// buffer's capacity is exceeded.
    pub fn push(&mut self, id: usize, item: T) -> Result<Vec<T>> {
        if id < self.next_id {
            return Err(OxideError::InvalidBlockId {
                expected: self.next_id as u64,
                actual: id as u64,
            });
        }

        if self.pending.contains_key(&id) {
            return Err(OxideError::InvalidFormat(
                "duplicate block id in reorder buffer",
            ));
        }

        if id != self.next_id && self.pending.len() >= self.max_pending {
            return Err(OxideError::InvalidFormat(
                "reorder buffer capacity exceeded",
            ));
        }

        self.pending.insert(id, item);

        let mut ready = Vec::new();
        while let Some(item) = self.pending.remove(&self.next_id) {
            ready.push(item);
            self.next_id += 1;
        }
        Ok(ready)
    }

    /// Returns the next expected ID.
    pub fn next_expected(&self) -> usize {
        self.next_id
    }

    /// Returns the number of items currently pending.
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// Returns the maximum number of pending items allowed.
    pub fn max_pending(&self) -> usize {
        self.max_pending
    }
}
