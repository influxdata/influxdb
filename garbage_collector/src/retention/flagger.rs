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
    dry_run: bool,
) -> Result<()> {
    loop {
        if !dry_run {
            let flagged = catalog
                .repositories()
                .await
                .parquet_files()
                .flag_for_delete_by_retention() //read/write
                .await
                .context(FlaggingSnafu)?;
            info!(flagged_count = %flagged.len(), "iox_catalog::flag_for_delete_by_retention()");
        } else {
            debug!("dry run enabled for parquet retention flagger");
        };

        select! {
            _ = shutdown.cancelled() => {
                break
            },
            _ = sleep(Duration::from_secs(60 * sleep_interval_minutes)) => (),
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
