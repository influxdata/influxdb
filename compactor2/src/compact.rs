//! Actual compaction routine.
use std::sync::Arc;

use data_types::ParquetFile;
use iox_catalog::interface::Catalog;
use snafu::Snafu;

/// Compaction errors.
#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations)]
pub enum Error {}

/// Perform compaction on given files including catalog transaction.
///
/// This MUST use all files. No further filtering is performed here.
pub async fn compact_files(
    _files: &[ParquetFile],
    _catalog: &Arc<dyn Catalog>,
) -> Result<(), Error> {
    // TODO: implement this
    // TODO: split this into catalog actual DF execution and catalog bookkeeping
    Ok(())
}
