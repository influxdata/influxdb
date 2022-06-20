use futures::prelude::*;
use object_store::ObjectMeta;
use snafu::prelude::*;
use std::sync::Arc;
use tokio::sync::mpsc;

pub(crate) async fn perform(
    args: Arc<crate::Args>,
    checker: mpsc::Sender<ObjectMeta>,
) -> Result<()> {
    let object_store = args
        .object_store()
        .await
        .context(CreatingObjectStoreSnafu)?;

    let mut items = object_store.list(None).await.context(ListingSnafu)?;

    while let Some(item) = items.next().await {
        let item = item.context(MalformedSnafu)?;
        checker.send(item).await?;
    }

    Ok(())
}

#[derive(Debug, Snafu)]
pub(crate) enum Error {
    #[snafu(display("Could not create the object store"))]
    CreatingObjectStore {
        source: clap_blocks::object_store::ParseError,
    },

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
