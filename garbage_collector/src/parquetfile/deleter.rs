use data_types::Timestamp;
use iox_catalog::interface::Catalog;
use observability_deps::tracing::*;
use snafu::prelude::*;
use std::time::Instant;
use std::{sync::Arc, time::Duration};
use tokio::{select, time::sleep};
use tokio_util::sync::CancellationToken;

pub(crate) async fn perform(
    shutdown: CancellationToken,
    catalog: Arc<dyn Catalog>,
    cutoff: Duration,
    sleep_interval: Duration,
) -> Result<()> {
    loop {
        let start = Instant::now();
        let older_than = Timestamp::from(catalog.time_provider().now() - cutoff);
        // do the delete, returning the deleted files
        let deleted = catalog
            .repositories()
            .parquet_files()
            .delete_old_ids_only(older_than) // read/write
            .await
            .context(DeletingSnafu)?;

        let elapsed = start.elapsed();
        info!(delete_count = %deleted.len(), ?elapsed, "iox_catalog::delete_old()");

        select! {
            _ = shutdown.cancelled() => {
                break
            },
            _ = sleep(sleep_interval) => (),
        }
    }
    Ok(())
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Failed to delete old parquet files in catalog"))]
    Deleting {
        source: iox_catalog::interface::Error,
    },
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;
