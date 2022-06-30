use object_store::ObjectMeta;
use observability_deps::tracing::*;
use snafu::prelude::*;
use std::sync::Arc;
use tokio::sync::mpsc;

pub(crate) async fn perform(
    args: Arc<crate::Args>,
    mut items: mpsc::Receiver<ObjectMeta>,
) -> Result<()> {
    let object_store = args
        .object_store()
        .await
        .context(CreatingObjectStoreSnafu)?;
    let dry_run = args.dry_run;

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
    #[snafu(display("Could not create the object store"))]
    CreatingObjectStore {
        source: clap_blocks::object_store::ParseError,
    },

    #[snafu(display("{path} could not be deleted"))]
    Deleting {
        source: object_store::Error,
        path: object_store::path::Path,
    },
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;
