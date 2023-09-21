use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use data_types::ParquetFile;
use gossip_parquet_file::tx::ParquetFileTx;
use observability_deps::tracing::info;

use crate::persist::completion_observer::{CompletedPersist, PersistCompletionObserver};

/// An abstract [`ParquetFile`] broadcast primitive.
trait BroadcastHandle: Debug + Send + Sync {
    fn broadcast(&self, file: ParquetFile);
}

impl BroadcastHandle for ParquetFileTx<ParquetFile> {
    fn broadcast(&self, file: ParquetFile) {
        Self::broadcast(self, file)
    }
}

/// Gossip persistence completion notifications to listening peers.
///
/// This type acts as [`PersistCompletionObserver`], broadcasting persisted
/// [`ParquetFile`] records to listening gossip peers. The gossip messages are
/// delegated to [`ParquetFileTx`], and sent over the [`Topic::NewParquetFiles`]
/// topic.
///
/// This happens strictly after the [`ParquetFile`] record has been added to the
/// catalog. This handler does not block, and serialisation/dispatch happens
/// asynchronously.
///
/// [`Topic::NewParquetFiles`]:
///     generated_types::influxdata::iox::gossip::Topic::NewParquetFiles
#[derive(Debug)]
pub struct ParquetFileNotification<T, H = ParquetFileTx<ParquetFile>> {
    inner: T,
    tx: H,
}

impl<T, H> ParquetFileNotification<T, H> {
    pub fn new(inner: T, gossip: H) -> Self {
        info!("gossiping persisted parquet file notifications");

        Self { inner, tx: gossip }
    }
}

#[async_trait]
impl<T, H> PersistCompletionObserver for ParquetFileNotification<T, H>
where
    T: PersistCompletionObserver,
    H: BroadcastHandle,
{
    async fn persist_complete(&self, note: Arc<CompletedPersist>) {
        // Broadcast the file creation notification.
        let file = note.file_record().clone();
        self.tx.broadcast(file);

        // Forward on the notification to the next handler.
        self.inner.persist_complete(note).await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use parking_lot::Mutex;

    use crate::{
        persist::completion_observer::mock::MockCompletionObserver,
        test_util::new_persist_notification,
    };

    use super::*;

    #[derive(Debug, Default)]
    struct MockBroadcastHandle {
        calls: Mutex<Vec<ParquetFile>>,
    }

    impl MockBroadcastHandle {
        pub fn calls(&self) -> Vec<ParquetFile> {
            self.calls.lock().clone()
        }
    }

    impl BroadcastHandle for Arc<MockBroadcastHandle> {
        fn broadcast(&self, file: ParquetFile) {
            self.calls.lock().push(file);
        }
    }

    #[tokio::test]
    async fn test_broadcast() {
        let inner = Arc::new(MockCompletionObserver::default());
        let handle = Arc::new(MockBroadcastHandle::default());

        // Mock persist completion notification
        let note = new_persist_notification([1, 2, 3, 42]);

        // Drive the handler
        let observer = ParquetFileNotification::new(Arc::clone(&inner), Arc::clone(&handle));
        observer.persist_complete(Arc::clone(&note)).await;

        // Ensure the notification was passed through (and not cloned)
        assert_matches!(inner.calls().as_slice(), [got] => {
            assert!(Arc::ptr_eq(&note, got));
        });

        // Ensure the ParquetFile was broadcast.
        assert_matches!(handle.calls().as_slice(), [got] => {
            assert_eq!(note.file_record(), got);
        });
    }
}
