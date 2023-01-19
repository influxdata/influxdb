//! Actual compaction routine.
use std::sync::Arc;

use data_types::{ParquetFile, ParquetFileParams};
use iox_catalog::interface::Catalog;
use snafu::Snafu;

/// Compaction errors.
#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Not implemented"))]
    NotImplemented,
}

/// Perform compaction on given files including catalog transaction.
///
/// This MUST use all files. No further filtering is performed here.
pub async fn compact_files(
    _files: &[ParquetFile],
    _catalog: &Arc<dyn Catalog>,
) -> Result<Vec<ParquetFileParams>, Error> {
    // TODO: implement this
    Err(Error::NotImplemented)
}
