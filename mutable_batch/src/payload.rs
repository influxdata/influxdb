//! Write payload abstractions derived from [`MutableBatch`]

use crate::column::ColumnData;
use crate::{MutableBatch, Result};
use data_types::database_rules::PartitionTemplate;
use data_types::sequence::Sequence;
use hashbrown::HashMap;
use schema::TIME_COLUMN_NAME;
use std::num::NonZeroUsize;
use std::ops::Range;
use time::Time;
use trace::ctx::SpanContext;

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
    /// from a [`MutableBatch`] and [`PartitionTemplate`]
    pub fn partition(
        table_name: &str,
        batch: &'a MutableBatch,
        partition_template: &PartitionTemplate,
    ) -> HashMap<String, Self> {
        use hashbrown::hash_map::Entry;
        let time = get_time_column(batch);

        let mut partition_ranges = HashMap::new();
        for (partition, range) in partition::partition_batch(batch, table_name, partition_template)
        {
            let row_count = NonZeroUsize::new(range.end - range.start).unwrap();
            let (min_timestamp, max_timestamp) = min_max_time(&time[range.clone()]);

            match partition_ranges.entry(partition) {
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
        partition_ranges
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

/// Metadata information about a write
#[derive(Debug, Default, Clone)]
pub struct WriteMeta {
    /// The sequence number associated with this write
    sequence: Option<Sequence>,

    /// When this write was ingested into the write buffer
    producer_ts: Option<Time>,

    // Optional span context associated w/ this write
    span_ctx: Option<SpanContext>,
}

impl WriteMeta {
    /// Create a new [`WriteMeta`]
    pub fn new(
        sequence: Option<Sequence>,
        producer_ts: Option<Time>,
        span_ctx: Option<SpanContext>,
    ) -> Self {
        Self {
            sequence,
            producer_ts,
            span_ctx,
        }
    }

    /// Gets the sequence number associated with the write if any
    pub fn sequence(&self) -> Option<&Sequence> {
        self.sequence.as_ref()
    }
}

/// A collection of writes to potentially multiple tables within the same database
#[derive(Debug)]
pub struct DbWrite {
    /// Writes to individual tables keyed by table name
    tables: HashMap<String, MutableBatch>,
    /// Write metadata
    meta: WriteMeta,
}

impl DbWrite {
    /// Create a new [`DbWrite`]
    pub fn new(tables: HashMap<String, MutableBatch>, meta: WriteMeta) -> Self {
        Self { tables, meta }
    }

    /// Metadata associated with this write
    pub fn meta(&self) -> &WriteMeta {
        &self.meta
    }

    /// Returns an iterator over the per-table writes within this [`DbWrite`]
    /// in no particular order
    pub fn tables(&self) -> impl Iterator<Item = (&str, &MutableBatch)> + '_ {
        self.tables.iter().map(|(k, v)| (k.as_str(), v))
    }
}
