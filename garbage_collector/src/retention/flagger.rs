use iox_catalog::interface::Catalog;
use observability_deps::tracing::*;
use snafu::prelude::*;
use std::{sync::Arc, time::Duration};
use tokio::{select, time::sleep};
use tokio_util::sync::CancellationToken;

pub(crate) async fn perform(
    shutdown: CancellationToken,
    catalog: Arc<dyn Catalog>,
    sleep_interval_minutes: u64,
) -> Result<()> {
    loop {
        let parquet_file_flagged = catalog
            .repositories()
            .await
            .parquet_files()
            .flag_for_delete_by_retention()
            .await
            .context(ParquetFileFlaggingSnafu)?;
        info!(parquet_file_flagged_count = %parquet_file_flagged.len(), "iox_catalog::parquet_file::flag_for_delete_by_retention()");

        let partition_flagged = catalog
            .repositories()
            .await
            .partitions()
            .flag_for_delete_by_retention()
            .await
            .context(PartitionFlaggingSnafu)?;
        info!(partition_flagged_count = %partition_flagged.len(), "iox_catalog::partition::flag_for_delete_by_retention()");

        if parquet_file_flagged.is_empty() && partition_flagged.is_empty() {
            select! {
                _ = shutdown.cancelled() => {
                    break
                },
                _ = sleep(Duration::from_secs(60 * sleep_interval_minutes)) => (),
            }
        } else if shutdown.is_cancelled() {
            break;
        }
    }
    Ok(())
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Failed to flag parquet files for deletion by retention policy"))]
    ParquetFileFlagging {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Failed to flag partitions for deletion by retention policy"))]
    PartitionFlagging {
        source: iox_catalog::interface::Error,
    },
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;
