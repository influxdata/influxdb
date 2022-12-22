use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::buffer_tree::partition::{persisting::PersistingData, PartitionData};

/// An abstract logical queue into which [`PersistingData`] (and their matching
/// [`PartitionData`]) are placed to be persisted.
///
/// Implementations MAY reorder persist jobs placed in this queue, and MAY block
/// indefinitely.
///
/// It is a logical error to enqueue a [`PartitionData`] with a
/// [`PersistingData`] from another instance.
#[async_trait]
pub(crate) trait PersistQueue: Clone + Send + Sync + Debug {
    /// Place `data` from `partition` into the persistence queue,
    /// (asynchronously) blocking until enqueued.
    async fn enqueue(
        &self,
        partition: Arc<Mutex<PartitionData>>,
        data: PersistingData,
    ) -> oneshot::Receiver<()>;
}
