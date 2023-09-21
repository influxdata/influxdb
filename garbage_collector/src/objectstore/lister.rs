use backoff::*;
use futures::prelude::*;
use object_store::{DynObjectStore, ObjectMeta};
use observability_deps::tracing::*;
use snafu::prelude::*;
use std::{sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::sleep};

/// Object store implementations will generally list all objects in the bucket/prefix. This limits
/// the total items pulled (assuming lazy streams) at a time to limit impact on the catalog.
/// Consider increasing this if throughput is an issue or shortening the loop/list sleep intervals.
/// Listing will list all files, including those not to be deleted, which may be a very large number.
const MAX_ITEMS_PROCESSED_PER_LOOP: usize = 10_000;

/// perform a object store list, limiting to ['MAX_ITEMS_PROCESSED_PER_LOOP'] files at a time,
/// waiting sleep interval before listing afresh.
pub(crate) async fn perform(
    object_store: Arc<DynObjectStore>,
    checker: mpsc::Sender<ObjectMeta>,
    sleep_interval_iteration_minutes: u64,
    sleep_interval_list_page_milliseconds: u64,
) -> Result<()> {
    info!("beginning object store listing");

    loop {
        let mut backoff = Backoff::new(&BackoffConfig::default());

        // there are issues with the service immediately hitting the os api (credentials, etc) on
        // startup. Retry as needed.
        let items = backoff
            .retry_all_errors("list_os_files", || object_store.list(None))
            .await
            .expect("backoff retries forever");

        let mut chunked_items = items.chunks(MAX_ITEMS_PROCESSED_PER_LOOP);

        let mut count = 0;
        while let Some(v) = chunked_items.next().await {
            // relist and sleep on an error to allow time for transient errors to dissipate
            // todo(pjb): react differently to different errors
            match process_item_list(v, &checker).await {
                Err(e) => {
                    warn!("error processing items from object store, continuing: {e}");
                    // go back to start of loop to list again, hopefully to get past error.
                    break;
                }
                Ok(i) => {
                    count += i;
                }
            }
            sleep(Duration::from_millis(sleep_interval_list_page_milliseconds)).await;
            debug!("starting next chunk of listed files");
        }
        info!("end of object store item list; listed {count} files: will relist in {sleep_interval_iteration_minutes} minutes");
        sleep(Duration::from_secs(60 * sleep_interval_iteration_minutes)).await;
    }
}

async fn process_item_list(
    items: Vec<object_store::Result<ObjectMeta>>,
    checker: &mpsc::Sender<ObjectMeta>,
) -> Result<i32> {
    let mut i = 0;
    for item in items {
        let item = item.context(MalformedSnafu)?;
        debug!(location = %item.location, "Object store item");
        checker.send(item).await?;
        i += 1;
    }
    debug!("processed {i} files of listed chunk");
    Ok(i)
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("The prefix could not be listed: {source}"))]
    Listing { source: object_store::Error },

    #[snafu(display("The object could not be listed: {source}"))]
    Malformed { source: object_store::Error },

    #[snafu(display("The checker task exited unexpectedly: {source}"))]
    #[snafu(context(false))]
    CheckerExited {
        source: tokio::sync::mpsc::error::SendError<ObjectMeta>,
    },
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;
