//! This module contains the definition of a "SeriesSet" a plan that when run produces
//! rows that can be logically divided into "Series"
//!
//! Specifically, this thing can produce represents a set of "tables",
//! and each table is sorted on a set of "tag" columns, meaning the
//! groups / series will be contiguous.
//!
//! For example, the output columns of such a plan would be:
//! (tag col0) (tag col1) ... (tag colN) (field val1) (field val2) ... (field valN) .. (timestamps)
//!
//! Note that the data will come out ordered by the tag keys (ORDER BY
//! (tag col0) (tag col1) ... (tag colN))
//!
//! NOTE: We think the influx storage engine returns series sorted by
//! the tag values, but the order of the columns is also sorted. So
//! for example, if you have `region`, `host`, and `service` as tags,
//! the columns would be ordered `host`, `region`, and `service` as
//! well.

use std::sync::Arc;

use delorean_arrow::arrow::{self, array::ArrayRef};
//use snafu::{ensure, OptionExt, Snafu};
use snafu::Snafu;

#[derive(Debug, Snafu)]
/// Opaque error type
pub enum Error {
    #[snafu(display("Plan Execution Error: {}", source))]
    Execution {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display(
        "Error reading record batch while converting from SeriesSet: {:?}",
        source
    ))]
    ReadingRecordBatch { source: arrow::error::ArrowError },
}

#[allow(dead_code)]
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct SeriesSet {
    /// key = value pairs
    keys: Vec<(String, String)>,
    /// Arrow array for this series.
    values: ArrayRef,
}
pub type SeriesSetRef = Arc<SeriesSet>;

/// Trait to convert RecordBatch'y things into `StringSetRef`s. Can
/// return errors, so don't use `std::convert::From`
pub trait IntoSeriesSet {
    /// Convert this thing into a stringset
    fn into_seriesset(self) -> Result<SeriesSet>;
}
