use std::collections::BTreeMap;

use crate::{OxideError, Result};

use super::DEFAULT_REORDER_PENDING_LIMIT;

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
    pub fn new() -> Self {
        Self::with_limit(DEFAULT_REORDER_PENDING_LIMIT)
    }

    pub fn with_limit(max_pending: usize) -> Self {
        Self {
            next_id: 0,
            pending: BTreeMap::new(),
            max_pending: max_pending.max(1),
        }
    }

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

    pub fn next_expected(&self) -> usize {
        self.next_id
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub fn max_pending(&self) -> usize {
        self.max_pending
    }
}
