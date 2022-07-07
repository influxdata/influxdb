use futures::prelude::*;
use object_store::{DynObjectStore, ObjectMeta};
use observability_deps::tracing::*;
use snafu::prelude::*;
use std::sync::Arc;
use tokio::sync::mpsc;

pub(crate) async fn perform(
    object_store: Arc<DynObjectStore>,
    checker: mpsc::Sender<ObjectMeta>,
) -> Result<()> {
    let mut items = object_store.list(None).await.context(ListingSnafu)?;

    while let Some(item) = items.next().await {
        let item = item.context(MalformedSnafu)?;
        info!(location = %item.location);
        checker.send(item).await?;
    }

    Ok(())
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("The prefix could not be listed"))]
    Listing { source: object_store::Error },

    #[snafu(display("The object could not be listed"))]
    Malformed { source: object_store::Error },

    #[snafu(display("The checker task exited unexpectedly"))]
    #[snafu(context(false))]
    CheckerExited {
        source: tokio::sync::mpsc::error::SendError<ObjectMeta>,
    },
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;
