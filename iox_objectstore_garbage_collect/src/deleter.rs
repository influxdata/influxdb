use object_store::{DynObjectStore, ObjectMeta};
use observability_deps::tracing::*;
use snafu::prelude::*;
use std::sync::Arc;
use tokio::sync::mpsc;

pub(crate) async fn perform(
    object_store: Arc<DynObjectStore>,
    dry_run: bool,
    mut items: mpsc::Receiver<ObjectMeta>,
) -> Result<()> {
    while let Some(item) = items.recv().await {
        let path = item.location;
        if dry_run {
            eprintln!("Not deleting {path} due to dry run");
        } else {
            object_store
                .delete(&path)
                .await
                .with_context(|_| DeletingSnafu { path: path.clone() })?;
            info!(
                location = %path,
                deleted = true,
            );
        }
    }

    Ok(())
}

#[derive(Debug, Snafu)]
pub(crate) enum Error {
    #[snafu(display("{path} could not be deleted"))]
    Deleting {
        source: object_store::Error,
        path: object_store::path::Path,
    },
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;
