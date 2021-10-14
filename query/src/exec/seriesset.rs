//! This module contains the definition of a "SeriesSet" a plan that when run
//! produces rows that can be logically divided into "Series"
//!
//! Specifically, a SeriesSet wraps a "table", and each table is
//! sorted on a set of "tag" columns, meaning the data the series
//! series will be contiguous.
//!
//! For example, the output columns of such a plan would be:
//! (tag col0) (tag col1) ... (tag colN) (field val1) (field val2) ... (field
//! valN) .. (timestamps)
//!
//! Note that the data will come out ordered by the tag keys (ORDER BY
//! (tag col0) (tag col1) ... (tag colN))
//!
//! NOTE: The InfluxDB classic storage engine not only returns
//! series sorted by the tag values, but the order of the tag columns
//! (and thus the actual sort order) is also lexographically
//! sorted. So for example, if you have `region`, `host`, and
//! `service` as tags, the columns would be ordered `host`, `region`,
//! and `service` as well.

pub mod converter;

use arrow::{self, record_batch::RecordBatch};

use std::sync::Arc;

use super::field::FieldIndexes;

#[derive(Debug)]
/// Represents several logical timeseries that share the same
/// timestamps and name=value tag keys.
///
/// The heavy use of `Arc` is to avoid many duplicated Strings given
/// the the fact that many SeriesSets share the same tag keys and
/// table name.
pub struct SeriesSet {
    /// The table name this series came from
    pub table_name: Arc<str>,

    /// key = value pairs that define this series
    pub tags: Vec<(Arc<str>, Arc<str>)>,

    /// the column index of each "field" of the time series. For
    /// example, if there are two field indexes then this series set
    /// would result in two distinct series being sent back, one for
    /// each field.
    pub field_indexes: FieldIndexes,

    // The row in the record batch where the data starts (inclusive)
    pub start_row: usize,

    // The number of rows in the record batch that the data goes to
    pub num_rows: usize,

    // The underlying record batch data
    pub batch: RecordBatch,
}

/// Describes a group of series "group of series" series. Namely,
/// several logical timeseries that share the same timestamps and
/// name=value tag keys, grouped by some subset of the tag keys
///
/// TODO: this may also support computing an aggregation per group,
/// pending on what is required for the gRPC layer.
#[derive(Debug)]
pub struct GroupDescription {
    /// the names of all tags (not just the tags used for grouping)
    pub all_tags: Vec<Arc<str>>,

    /// the values of the group tags that defined the group.
    /// For example,
    ///
    /// If there were tags `t0`, `t1`, and `t2`, and the query had
    /// group_keys of `[t1, t2]` then this list would have the values
    /// of the t1 and t2 columns
    pub gby_vals: Vec<Arc<str>>,
}

#[derive(Debug)]
pub enum SeriesSetItem {
    GroupStart(GroupDescription),
    Data(SeriesSet),
}
