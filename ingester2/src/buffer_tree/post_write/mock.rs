use std::sync::Arc;

use parking_lot::Mutex;

use crate::buffer_tree::partition::PartitionData;

use super::PostWriteObserver;

#[derive(Debug, Default)]
pub(crate) struct MockPostWriteObserver {
    saw: Mutex<Vec<Arc<Mutex<PartitionData>>>>,
}

impl PostWriteObserver for MockPostWriteObserver {
    fn observe(
        &self,
        partition: Arc<Mutex<PartitionData>>,
        _guard: parking_lot::MutexGuard<'_, PartitionData>,
    ) {
        self.saw.lock().push(partition);
    }
}
