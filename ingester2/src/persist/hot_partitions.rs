use std::{fmt::Debug, sync::Arc};

use observability_deps::tracing::info;
use parking_lot::{Mutex, MutexGuard};

use crate::buffer_tree::{partition::PartitionData, post_write::PostWriteObserver};

use super::queue::PersistQueue;

#[derive(Debug)]
pub(crate) struct HotPartitionPersister<P> {
    persist_handle: P,
    max_estimated_persist_cost: usize,
}

impl<P> HotPartitionPersister<P>
where
    P: PersistQueue + Clone + Sync + 'static,
{
    pub fn new(persist_handle: P, max_estimated_persist_cost: usize) -> Self {
        Self {
            persist_handle,
            max_estimated_persist_cost,
        }
    }

    #[cold]
    fn persist(
        &self,
        cost_estimate: usize,
        partition: Arc<Mutex<PartitionData>>,
        mut guard: MutexGuard<'_, PartitionData>,
    ) {
        info!(
            partition_id = guard.partition_id().get(),
            cost_estimate, "marking hot partition for persistence"
        );

        let data = guard
            .mark_persisting()
            .expect("failed to transition buffer fsm to persisting state");

        // Perform the enqueue in a separate task, to avoid blocking this
        // writer if the persist system is saturated.
        let persist_handle = self.persist_handle.clone();
        tokio::spawn(async move {
            // There is no need to await on the completion handle.
            let _ = persist_handle.enqueue(partition, data).await;
        });
    }
}

impl<P> PostWriteObserver for HotPartitionPersister<P>
where
    P: PersistQueue + Clone + Sync + 'static,
{
    #[inline(always)]
    fn observe(&self, partition: Arc<Mutex<PartitionData>>, guard: MutexGuard<'_, PartitionData>) {
        // Without releasing the lock, obtain the new persist cost estimate.
        let cost_estimate = guard.persist_cost_estimate();

        // This observer is called after a successful write, therefore
        // persisting the partition MUST have a non-zero cost.
        assert!(cost_estimate > 0);

        // If the estimated persist cost is over the limit, mark the
        // partition as persisting.
        //
        // This SHOULD happen atomically with the above write, to ensure
        // accurate buffer costing - if the lock were to be released, more
        // writes could be added to the buffer in parallel, exceeding the
        // limit before it was marked as persisting.
        if cost_estimate >= self.max_estimated_persist_cost {
            self.persist(cost_estimate, partition, guard)
        }
    }
}
