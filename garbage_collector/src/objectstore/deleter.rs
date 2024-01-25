use futures::{FutureExt, StreamExt, TryStreamExt};
use object_store::{DynObjectStore, ObjectMeta};
use observability_deps::tracing::info;
use snafu::prelude::*;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub(crate) async fn perform(
    shutdown: CancellationToken,
    object_store: Arc<DynObjectStore>,
    dry_run: bool,
    items: mpsc::Receiver<ObjectMeta>,
) -> Result<()> {
    let locations = tokio_stream::wrappers::ReceiverStream::new(items).map(|item| item.location);

    let stream_fu = if dry_run {
        async move {
            locations
                .map(|path| {
                    info!(?path, "Not deleting due to dry run");
                })
                .collect::<()>()
                .await;
            Ok(())
        }
        .boxed()
    } else {
        async move {
            object_store
                .delete_stream(
                    locations
                        .map(|path| {
                            info!(%path, "Deleting");
                            Ok(path)
                        })
                        .boxed(),
                )
                .map_ok(|_| ())
                .map_err(|e: object_store::Error| Error::Deleting { source: e })
                .try_collect()
                .await
        }
        .boxed()
    };

    tokio::select! {
        _ = shutdown.cancelled() => {
            // Exit gracefully
        }
        res = stream_fu => {
            // Propagate error
            res?;
        }
    }

    Ok(())
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("could not be delete: {source}"))]
    Deleting { source: object_store::Error },
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use chrono::Utc;
    use data_types::{NamespaceId, ObjectStoreId, PartitionId, TableId, TransitionPartitionId};
    use object_store::path::Path;
    use parquet_file::ParquetFilePath;
    use std::time::Duration;

    #[tokio::test]
    async fn perform_shutdown_gracefully() {
        let shutdown = CancellationToken::new();
        let nitems = 3;
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let items = populate_os_with_items(&object_store, nitems).await;

        assert_eq!(count_os_element(&object_store).await, nitems);

        let dry_run = false;
        let (tx, rx) = mpsc::channel(1000);

        tokio::spawn({
            let shutdown = shutdown.clone();

            async move {
                for item in items {
                    tx.send(item.clone()).await.unwrap();
                }

                // Send a shutdown signal
                shutdown.cancel();

                // Prevent this thread from exiting. Exiting this thread will
                // close the channel, which in turns close the processing stream.
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        });

        // This call should terminate because we send shutdown signal, but
        // nothing can be said about the number of elements in object store.
        // The processing stream may or may not have chance to process the
        // items for deletion.
        let perform_fu = perform(shutdown, Arc::clone(&object_store), dry_run, rx);
        // Unusual test because there is no assertion but the call below should
        // not panic which verifies that the deleter task shutdown gracefully.
        tokio::time::timeout(Duration::from_secs(3), perform_fu)
            .await
            .unwrap()
            .unwrap();
    }

    async fn count_os_element(os: &Arc<DynObjectStore>) -> usize {
        let objects = os.list(None);
        objects.fold(0, |acc, _| async move { acc + 1 }).await
    }

    async fn populate_os_with_items(os: &Arc<DynObjectStore>, nitems: usize) -> Vec<ObjectMeta> {
        let mut items = vec![];
        for i in 0..nitems {
            let object_meta = ObjectMeta {
                location: new_object_meta_location(),
                last_modified: Utc::now(),
                size: 0,
                e_tag: None,
                version: None,
            };
            os.put(&object_meta.location, Bytes::from(i.to_string()))
                .await
                .unwrap();
            items.push(object_meta);
        }
        items
    }

    fn new_object_meta_location() -> Path {
        ParquetFilePath::new(
            NamespaceId::new(1),
            TableId::new(2),
            &TransitionPartitionId::Deprecated(PartitionId::new(4)),
            ObjectStoreId::new(),
        )
        .object_store_path()
    }
}
