//! This module contains a basic memory tracking system

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A simple memory registry that tracks the total memory consumption
/// across a set of MemTrackers
#[derive(Debug, Default)]
pub struct MemRegistry {
    inner: Arc<MemTrackerShared>,
}

#[derive(Debug, Default)]
struct MemTrackerShared {
    /// The total bytes across all registered trackers
    bytes: AtomicUsize,
}

impl MemRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bytes(&self) -> usize {
        self.inner.bytes.load(Ordering::Relaxed)
    }

    pub fn register(&self) -> MemTracker {
        MemTracker {
            shared: Arc::clone(&self.inner),
            bytes: 0,
        }
    }
}

/// A MemTracker is created with a reference to a MemRegistry
/// The memory "allocation" associated with a specific MemTracker
/// can be increased or decreased and this will update the totals
/// on the MemRegistry
///
/// On Drop the "allocated" bytes associated with the MemTracker
/// will be decremented from the MemRegistry's total
///
/// Note: this purposefully does not implement Clone as the semantics
/// of such a construct are unclear
///
/// Note: this purposefully does not implement Default to avoid
/// accidentally creating untracked objects
#[derive(Debug)]
pub struct MemTracker {
    shared: Arc<MemTrackerShared>,
    bytes: usize,
}

impl MemTracker {
    /// Creates a new empty tracker registered to
    /// the same registry as this tracker
    pub fn clone_empty(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            bytes: 0,
        }
    }

    /// Set the number of bytes associated with this tracked instance
    pub fn set_bytes(&mut self, new: usize) {
        if new > self.bytes {
            self.shared
                .bytes
                .fetch_add(new - self.bytes, Ordering::Relaxed);
        } else {
            self.shared
                .bytes
                .fetch_sub(self.bytes - new, Ordering::Relaxed);
        }
        self.bytes = new;
    }
}

impl Drop for MemTracker {
    fn drop(&mut self) {
        self.shared.bytes.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tracker() {
        let registry = MemRegistry::new();
        let mut t1 = registry.register();
        let mut t2 = registry.register();

        t1.set_bytes(200);

        assert_eq!(registry.bytes(), 200);

        t1.set_bytes(100);

        assert_eq!(registry.bytes(), 100);

        t2.set_bytes(300);

        assert_eq!(registry.bytes(), 400);

        t2.set_bytes(400);
        assert_eq!(registry.bytes(), 500);

        std::mem::drop(t2);
        assert_eq!(registry.bytes(), 100);

        std::mem::drop(t1);
        assert_eq!(registry.bytes(), 0);
    }
}
