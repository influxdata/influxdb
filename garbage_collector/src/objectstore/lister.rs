use backoff::*;
use futures::prelude::*;
use futures::stream::BoxStream;
use object_store::{DynObjectStore, ObjectMeta};
use observability_deps::tracing::*;
use snafu::prelude::*;
use std::{sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::sleep};

/// perform a object store list, limiting to 1000 files per loop iteration, waiting sleep interval
/// per loop.
pub(crate) async fn perform(
    object_store: Arc<DynObjectStore>,
    checker: mpsc::Sender<ObjectMeta>,
    sleep_interval_minutes: u64,
) -> Result<()> {
    loop {
        // backoff retry to avoid issues with immediately polling the object store at startup
        let mut backoff = Backoff::new(&BackoffConfig::default());

        let mut items = backoff
            .retry_all_errors("list_os_files", || object_store.list(None))
            .await
            .expect("backoff retries forever");

        // ignore the result, if it was successful, sleep; if there was an error, sleep still to
        // make sure we don't loop onto the same error repeatedly
        // (todo: maybe improve in the future based on error).
        let ret = process_item_list(&mut items, &checker).await;
        match ret {
            Ok(_) => {}
            Err(e) => {
                info!("error processing items from object store, continuing: {e}")
            }
        }

        sleep(Duration::from_secs(60 * sleep_interval_minutes)).await;
    }
}

async fn process_item_list(
    items: &mut BoxStream<'_, object_store::Result<ObjectMeta>>,
    checker: &mpsc::Sender<ObjectMeta>,
) -> Result<()> {
    while let Some(item) = items.take(1000).next().await {
        let item = item.context(MalformedSnafu)?;
        debug!(location = %item.location, "Object store item");
        checker.send(item).await?;
    }
    debug!("end of object store item list");
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
