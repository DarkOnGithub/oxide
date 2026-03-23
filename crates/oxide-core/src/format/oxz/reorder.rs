use crate::{OxideError, Result};

use super::DEFAULT_REORDER_PENDING_LIMIT;

#[derive(Debug)]
struct PendingSlot<T> {
    id: usize,
    item: T,
}

/// A buffer that reorders items submitted out-of-order into a sequential stream.
///
/// Items are stored in a bounded dense window keyed by `id - next_id` until the
/// next expected sequential ID is pushed, at which point all contiguous items
/// starting from that ID are released.
#[derive(Debug)]
pub struct ReorderBuffer<T> {
    next_id: usize,
    pending: Vec<Option<PendingSlot<T>>>,
    pending_len: usize,
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
        let max_pending = max_pending.max(1);
        Self {
            next_id: 0,
            pending: std::iter::repeat_with(|| None)
                .take(max_pending.saturating_add(1))
                .collect(),
            pending_len: 0,
            max_pending,
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

        let offset = id - self.next_id;
        if offset > self.max_pending {
            return Err(OxideError::InvalidFormat(
                "reorder buffer capacity exceeded",
            ));
        }

        let slot_index = id % self.pending.len();
        if let Some(existing) = self.pending[slot_index].as_ref() {
            if existing.id == id {
                return Err(OxideError::InvalidFormat(
                    "duplicate block id in reorder buffer",
                ));
            }
            return Err(OxideError::InvalidFormat(
                "reorder buffer capacity exceeded",
            ));
        }

        self.pending[slot_index] = Some(PendingSlot { id, item });
        self.pending_len += 1;

        let mut ready = Vec::new();
        loop {
            let next_slot = self.next_id % self.pending.len();
            match self.pending[next_slot].take() {
                Some(slot) if slot.id == self.next_id => {
                    ready.push(slot.item);
                    self.pending_len = self.pending_len.saturating_sub(1);
                    self.next_id += 1;
                }
                Some(slot) => {
                    self.pending[next_slot] = Some(slot);
                    break;
                }
                None => break,
            }
        }

        Ok(ready)
    }

    /// Clears all pending items and returns how many were discarded.
    pub fn clear(&mut self) -> usize {
        let cleared = self.pending_len;
        for slot in &mut self.pending {
            *slot = None;
        }
        self.pending_len = 0;
        cleared
    }

    /// Returns the number of items currently pending.
    pub fn pending_len(&self) -> usize {
        self.pending_len
    }
}

#[cfg(test)]
mod tests {
    use super::ReorderBuffer;
    use crate::OxideError;

    #[test]
    fn releases_contiguous_items_once_gap_is_filled() {
        let mut reorder = ReorderBuffer::with_limit(4);

        assert_eq!(reorder.push(1, 11).expect("push id=1"), Vec::<i32>::new());
        assert_eq!(reorder.push(3, 33).expect("push id=3"), Vec::<i32>::new());
        assert_eq!(reorder.push(2, 22).expect("push id=2"), Vec::<i32>::new());
        assert_eq!(reorder.push(0, 0).expect("push id=0"), vec![0, 11, 22, 33]);
        assert_eq!(reorder.pending_len(), 0);
    }

    #[test]
    fn rejects_ids_outside_bounded_window() {
        let mut reorder = ReorderBuffer::with_limit(1);

        let error = reorder
            .push(2, 22)
            .expect_err("gap beyond limit should fail");
        assert!(matches!(
            error,
            OxideError::InvalidFormat("reorder buffer capacity exceeded")
        ));
    }

    #[test]
    fn clear_discards_pending_items() {
        let mut reorder = ReorderBuffer::with_limit(4);
        reorder.push(2, 22).expect("push id=2");
        reorder.push(1, 11).expect("push id=1");

        assert_eq!(reorder.clear(), 2);
        assert_eq!(reorder.pending_len(), 0);
        assert_eq!(reorder.push(0, 0).expect("push id=0"), vec![0]);
    }
}
