//! Write payload abstractions derived from [`MutableBatch`]

use crate::{column::ColumnData, MutableBatch, Result};
use data_types::{partition_template::TablePartitionTemplateOverride, PartitionKey};
use hashbrown::HashMap;
use schema::TIME_COLUMN_NAME;
use std::{num::NonZeroUsize, ops::Range};

pub use self::partition::PartitionKeyError;

mod filter;
mod partition;

/// A payload that can be written to a mutable batch
pub trait WritePayload {
    /// Write this payload to `batch`
    fn write_to_batch(&self, batch: &mut MutableBatch) -> Result<()>;
}

impl WritePayload for MutableBatch {
    fn write_to_batch(&self, batch: &mut MutableBatch) -> Result<()> {
        batch.extend_from(self)
    }
}

/// A [`MutableBatch`] with a non-zero set of row ranges to write
#[derive(Debug)]
pub struct PartitionWrite<'a> {
    batch: &'a MutableBatch,
    ranges: Vec<Range<usize>>,
    min_timestamp: i64,
    max_timestamp: i64,
    row_count: NonZeroUsize,
}

impl<'a> PartitionWrite<'a> {
    /// Create a new [`PartitionWrite`] with the entire range of the provided batch
    ///
    /// # Panic
    ///
    /// Panics if the batch has no rows
    pub fn new(batch: &'a MutableBatch) -> Self {
        let row_count = NonZeroUsize::new(batch.row_count).unwrap();
        let time = get_time_column(batch);
        let (min_timestamp, max_timestamp) = min_max_time(time);

        // This `allow` can be removed when this issue is fixed and released:
        // <https://github.com/rust-lang/rust-clippy/issues/11086>
        #[allow(clippy::single_range_in_vec_init)]
        Self {
            batch,
            ranges: vec![0..batch.row_count],
            min_timestamp,
            max_timestamp,
            row_count,
        }
    }

    /// Returns the minimum timestamp in the write
    pub fn min_timestamp(&self) -> i64 {
        self.min_timestamp
    }

    /// Returns the maximum timestamp in the write
    pub fn max_timestamp(&self) -> i64 {
        self.max_timestamp
    }

    /// Returns the number of rows in the write
    pub fn rows(&self) -> NonZeroUsize {
        self.row_count
    }

    /// Returns a [`PartitionWrite`] containing just the rows of `Self` that pass
    /// the provided time predicate, or None if no rows
    pub fn filter(&self, predicate: impl Fn(i64) -> bool) -> Option<PartitionWrite<'a>> {
        let mut min_timestamp = i64::MAX;
        let mut max_timestamp = i64::MIN;
        let mut row_count = 0_usize;

        // Construct a predicate that lets us inspect the timestamps as they are filtered
        let inspect = |t| match predicate(t) {
            true => {
                min_timestamp = min_timestamp.min(t);
                max_timestamp = max_timestamp.max(t);
                row_count += 1;
                true
            }
            false => false,
        };

        let ranges: Vec<_> = filter::filter_time(self.batch, &self.ranges, inspect);
        let row_count = NonZeroUsize::new(row_count)?;

        Some(PartitionWrite {
            batch: self.batch,
            ranges,
            min_timestamp,
            max_timestamp,
            row_count,
        })
    }

    /// Create a collection of [`PartitionWrite`] indexed by partition key
    /// from a [`MutableBatch`] and [`TablePartitionTemplateOverride`]
    pub fn partition(
        batch: &'a MutableBatch,
        partition_template: &TablePartitionTemplateOverride,
    ) -> Result<HashMap<PartitionKey, Self>, PartitionKeyError> {
        use hashbrown::hash_map::Entry;
        let time = get_time_column(batch);

        let mut partition_ranges = HashMap::new();
        for (partition, range) in partition::partition_batch(batch, partition_template) {
            let row_count = NonZeroUsize::new(range.end - range.start).unwrap();
            let (min_timestamp, max_timestamp) = min_max_time(&time[range.clone()]);

            match partition_ranges.entry(PartitionKey::from(partition?)) {
                Entry::Vacant(v) => {
                    v.insert(PartitionWrite {
                        batch,
                        ranges: vec![range],
                        min_timestamp,
                        max_timestamp,
                        row_count,
                    });
                }
                Entry::Occupied(mut o) => {
                    let pw = o.get_mut();
                    pw.min_timestamp = pw.min_timestamp.min(min_timestamp);
                    pw.max_timestamp = pw.max_timestamp.max(max_timestamp);
                    pw.row_count = NonZeroUsize::new(pw.row_count.get() + row_count.get()).unwrap();
                    pw.ranges.push(range);
                }
            }
        }
        Ok(partition_ranges)
    }
}

impl<'a> WritePayload for PartitionWrite<'a> {
    fn write_to_batch(&self, batch: &mut MutableBatch) -> Result<()> {
        batch.extend_from_ranges(self.batch, &self.ranges)
    }
}

fn get_time_column(batch: &MutableBatch) -> &[i64] {
    let time_column = batch.column(TIME_COLUMN_NAME).expect("time column");
    match &time_column.data {
        ColumnData::I64(col_data, _) => col_data,
        x => unreachable!("expected i64 got {} for time column", x),
    }
}

fn min_max_time(col: &[i64]) -> (i64, i64) {
    let mut min_timestamp = i64::MAX;
    let mut max_timestamp = i64::MIN;
    for t in col {
        min_timestamp = min_timestamp.min(*t);
        max_timestamp = max_timestamp.max(*t);
    }
    (min_timestamp, max_timestamp)
}
