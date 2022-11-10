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
        let flagged = catalog
            .repositories()
            .await
            .parquet_files()
            .flag_for_delete_by_retention()
            .await
            .context(FlaggingSnafu)?;
        info!(flagged_count = %flagged.len(), "iox_catalog::flag_for_delete_by_retention()");

        if flagged.is_empty() {
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
    Flagging {
        source: iox_catalog::interface::Error,
    },
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;
