use futures::prelude::*;
use object_store::{DynObjectStore, ObjectMeta};
use observability_deps::tracing::*;
use snafu::prelude::*;
use std::{sync::Arc, time::Duration};
use tokio::{select, sync::mpsc, time::sleep};
use tokio_util::sync::CancellationToken;

pub(crate) async fn perform(
    shutdown: CancellationToken,
    object_store: Arc<DynObjectStore>,
    checker: mpsc::Sender<ObjectMeta>,
    sleep_interval_minutes: u64,
) -> Result<()> {
    let mut items = object_store.list(None).await.context(ListingSnafu)?;

    loop {
        select! {
            _ = shutdown.cancelled() => {
                break
            },
            item = items.next() => {
                match item {
                    Some(item) => {
                        let item = item.context(MalformedSnafu)?;
                        debug!(location = %item.location, "Object store item");
                        checker.send(item).await?;
                    }
                    None => {
                        // sleep for the configured time, then list again and go around the loop
                        // again
                        select! {
                            _ = shutdown.cancelled() => {
                                break;
                            }
                            _ = sleep(Duration::from_secs(60 * sleep_interval_minutes)) => {
                                items = object_store.list(None).await.context(ListingSnafu)?;
                            }
                        }
                    }
                }
            }
        }
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
