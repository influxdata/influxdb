use futures::{StreamExt, TryStreamExt};
use object_store::{DynObjectStore, ObjectMeta};
use observability_deps::tracing::info;
use snafu::prelude::*;
use std::sync::Arc;
use tokio::sync::mpsc;

pub(crate) async fn perform(
    object_store: Arc<DynObjectStore>,
    dry_run: bool,
    concurrent_deletes: usize,
    items: mpsc::Receiver<ObjectMeta>,
) -> Result<()> {
    tokio_stream::wrappers::ReceiverStream::new(items)
        .map(|item| {
            let object_store = Arc::clone(&object_store);

            async move {
                let path = item.location;
                if dry_run {
                    info!(?path, "Not deleting due to dry run");
                    Ok(())
                } else {
                    info!("Deleting {path}");
                    object_store
                        .delete(&path)
                        .await
                        .context(DeletingSnafu { path })
                }
            }
        })
        .buffer_unordered(concurrent_deletes)
        .try_collect()
        .await?;

    Ok(())
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("{path} could not be deleted"))]
    Deleting {
        source: object_store::Error,
        path: object_store::path::Path,
    },
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;
