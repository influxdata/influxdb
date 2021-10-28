#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

//! A mutable data structure for a collection of writes.
//!
//! Can be viewed as a mutable version of [`RecordBatch`] that remains the exclusive
//! owner of its buffers, permitting mutability. The in-memory layout is similar, however,
//! permitting fast conversion to [`RecordBatch`]
//!

use crate::column::{Column, ColumnData};
use arrow::record_batch::RecordBatch;
use data_types::database_rules::PartitionTemplate;
use data_types::write_summary::TimestampSummary;
use entry::TableBatch;
use hashbrown::HashMap;
use schema::selection::Selection;
use schema::{builder::SchemaBuilder, Schema, TIME_COLUMN_NAME};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::num::NonZeroUsize;
use std::ops::Range;

pub mod column;
mod filter;
mod partition;
pub mod writer;

#[allow(missing_docs)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Column error on column {}: {}", column, source))]
    ColumnError {
        column: String,
        source: column::Error,
    },

    #[snafu(display("Column {} had {} rows, expected {}", column, expected, actual))]
    IncorrectRowCount {
        column: String,
        expected: usize,
        actual: usize,
    },

    #[snafu(display("arrow conversion error: {}", source))]
    ArrowError { source: arrow::error::ArrowError },

    #[snafu(display("Internal error converting schema: {}", source))]
    InternalSchema { source: schema::builder::Error },

    #[snafu(display("Column not found: {}", column))]
    ColumnNotFound { column: String },

    #[snafu(display("Mask had {} rows, expected {}", expected, actual))]
    IncorrectMaskLength { expected: usize, actual: usize },

    #[snafu(context(false))]
    WriterError { source: writer::Error },
}

/// A specialized `Error` for [`MutableBatch`] errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Represents a mutable batch of rows (a horizontal subset of a table) which
/// can be appended to and converted into an Arrow `RecordBatch`
#[derive(Debug, Default)]
pub struct MutableBatch {
    /// Map of column name to index in `MutableBatch::columns`
    column_names: HashMap<String, usize>,

    /// Columns contained within this MutableBatch
    columns: Vec<Column>,

    /// The number of rows in this MutableBatch
    row_count: usize,
}

impl MutableBatch {
    /// Create a new empty batch
    pub fn new() -> Self {
        Self {
            column_names: Default::default(),
            columns: Default::default(),
            row_count: 0,
        }
    }

    /// Write the contents of a [`TableBatch`] into this MutableBatch.
    ///
    /// If `mask` is provided, only entries that are marked w/ `true` are written.
    ///
    /// Panics if the batch specifies a different name for the table in this Chunk
    pub fn write_table_batch(
        &mut self,
        batch: TableBatch<'_>,
        mask: Option<&[bool]>,
    ) -> Result<()> {
        self.write_columns(batch.columns(), mask)
    }

    /// Returns the schema for a given selection
    ///
    /// If Selection::All the returned columns are sorted by name
    pub fn schema(&self, selection: Selection<'_>) -> Result<Schema> {
        let mut schema_builder = SchemaBuilder::new();
        let schema = match selection {
            Selection::All => {
                for (column_name, column_idx) in self.column_names.iter() {
                    let column = &self.columns[*column_idx];
                    schema_builder.influx_column(column_name, column.influx_type());
                }

                schema_builder
                    .build()
                    .context(InternalSchema)?
                    .sort_fields_by_name()
            }
            Selection::Some(cols) => {
                for col in cols {
                    let column = self.column(col)?;
                    schema_builder.influx_column(col, column.influx_type());
                }
                schema_builder.build().context(InternalSchema)?
            }
        };

        Ok(schema)
    }

    /// Convert all the data in this `MutableBatch` into a `RecordBatch`
    pub fn to_arrow(&self, selection: Selection<'_>) -> Result<RecordBatch> {
        let schema = self.schema(selection)?;
        let columns = schema
            .iter()
            .map(|(_, field)| {
                let column = self
                    .column(field.name())
                    .expect("schema contains non-existent column");

                column.to_arrow().context(ColumnError {
                    column: field.name(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        RecordBatch::try_new(schema.into(), columns).context(ArrowError {})
    }

    /// Returns an iterator over the columns in this batch in no particular order
    pub fn columns(&self) -> impl Iterator<Item = (&String, &Column)> + '_ {
        self.column_names
            .iter()
            .map(move |(name, idx)| (name, &self.columns[*idx]))
    }

    /// Return the number of rows in this chunk
    pub fn rows(&self) -> usize {
        self.row_count
    }

    /// Returns a summary of the write timestamps in this chunk if a
    /// time column exists
    pub fn timestamp_summary(&self) -> Option<TimestampSummary> {
        let time = self.column_names.get(TIME_COLUMN_NAME)?;
        let mut summary = TimestampSummary::default();
        match &self.columns[*time].data {
            ColumnData::I64(col_data, _) => {
                for t in col_data {
                    summary.record_nanos(*t)
                }
            }
            _ => unreachable!(),
        }
        Some(summary)
    }

    /// Extend this [`MutableBatch`] with the contents of `other`
    pub fn extend_from(&mut self, other: &Self) -> Result<()> {
        let mut writer = writer::Writer::new(self, other.row_count);
        writer.write_batch(other)?;
        writer.commit();
        Ok(())
    }

    /// Extend this [`MutableBatch`] with `range` rows from `other`
    pub fn extend_from_range(&mut self, other: &Self, range: Range<usize>) -> Result<()> {
        let mut writer = writer::Writer::new(self, range.end - range.start);
        writer.write_batch_range(other, range)?;
        writer.commit();
        Ok(())
    }

    /// Extend this [`MutableBatch`] with `ranges` rows from `other`
    pub fn extend_from_ranges(&mut self, other: &Self, ranges: &[Range<usize>]) -> Result<()> {
        let to_insert = ranges.iter().map(|x| x.end - x.start).sum();

        let mut writer = writer::Writer::new(self, to_insert);
        writer.write_batch_ranges(other, ranges)?;
        writer.commit();
        Ok(())
    }

    /// Returns a reference to the specified column
    pub fn column(&self, column: &str) -> Result<&Column> {
        let idx = self
            .column_names
            .get(column)
            .context(ColumnNotFound { column })?;

        Ok(&self.columns[*idx])
    }

    /// Validates the schema of the passed in columns, then adds their values to
    /// the associated columns updates summary statistics.
    ///
    /// If `mask` is provided, only entries that are marked w/ `true` are written.
    fn write_columns(
        &mut self,
        columns: Vec<entry::Column<'_>>,
        mask: Option<&[bool]>,
    ) -> Result<()> {
        let row_count_before_insert = self.rows();
        let additional_rows = columns.first().map(|x| x.row_count).unwrap_or_default();
        let masked_values = if let Some(mask) = mask {
            ensure!(
                additional_rows == mask.len(),
                IncorrectMaskLength {
                    expected: additional_rows,
                    actual: mask.len(),
                }
            );
            mask.iter().filter(|x| !*x).count()
        } else {
            0
        };
        let final_row_count = row_count_before_insert + additional_rows - masked_values;

        // get the column ids and validate schema for those that already exist
        columns.iter().try_for_each(|column| {
            ensure!(
                column.row_count == additional_rows,
                IncorrectRowCount {
                    column: column.name(),
                    expected: additional_rows,
                    actual: column.row_count,
                }
            );

            if let Some(c_idx) = self.column_names.get(column.name()) {
                self.columns[*c_idx]
                    .validate_schema(column)
                    .context(ColumnError {
                        column: column.name(),
                    })?;
            }

            // https://github.com/shepmaster/snafu/issues/315
            Ok::<_, Error>(())
        })?;

        for fb_column in columns {
            let influx_type = fb_column.influx_type();
            let columns_len = self.columns.len();

            let column_idx = *self
                .column_names
                .raw_entry_mut()
                .from_key(fb_column.name())
                .or_insert_with(|| (fb_column.name().to_string(), columns_len))
                .1;

            if columns_len == column_idx {
                self.columns
                    .push(Column::new(row_count_before_insert, influx_type))
            }

            let column = &mut self.columns[column_idx];

            assert_eq!(column.len(), row_count_before_insert);

            column.append(&fb_column, mask).context(ColumnError {
                column: fb_column.name(),
            })?;

            assert_eq!(column.len(), final_row_count);
        }

        // Pad any columns that did not have values in this batch with NULLs
        for c in &mut self.columns {
            c.push_nulls_to_len(final_row_count);
        }
        self.row_count = final_row_count;

        Ok(())
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use entry::test_helpers::lp_to_entry;
    use schema::{InfluxColumnType, InfluxFieldType};

    #[test]
    fn write_columns_validates_schema() {
        let mut table = MutableBatch::new();
        let lp = "foo,t1=asdf iv=1i,uv=1u,fv=1.0,bv=true,sv=\"hi\" 1";
        let entry = lp_to_entry(lp);
        table
            .write_columns(
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
                None,
            )
            .unwrap();

        let lp = "foo t1=\"string\" 1";
        let entry = lp_to_entry(lp);
        let response = table
            .write_columns(
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
                None,
            )
            .err()
            .unwrap();
        assert!(
            matches!(
                &response,
                Error::ColumnError {
                    column,
                    source: column::Error::TypeMismatch {
                        existing: InfluxColumnType::Tag,
                        inserted: InfluxColumnType::Field(InfluxFieldType::String)
                    }
                } if column == "t1"
            ),
            "didn't match returned error: {:?}",
            response
        );

        let lp = "foo iv=1u 1";
        let entry = lp_to_entry(lp);
        let response = table
            .write_columns(
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
                None,
            )
            .err()
            .unwrap();
        assert!(
            matches!(
                &response,
                Error::ColumnError {
                    column,
                    source: column::Error::TypeMismatch {
                        inserted: InfluxColumnType::Field(InfluxFieldType::UInteger),
                        existing: InfluxColumnType::Field(InfluxFieldType::Integer)
                    }
                } if column == "iv"
            ),
            "didn't match returned error: {:?}",
            response
        );

        let lp = "foo fv=1i 1";
        let entry = lp_to_entry(lp);
        let response = table
            .write_columns(
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
                None,
            )
            .err()
            .unwrap();
        assert!(
            matches!(
                &response,
                Error::ColumnError {
                    column,
                    source: column::Error::TypeMismatch {
                        existing: InfluxColumnType::Field(InfluxFieldType::Float),
                        inserted: InfluxColumnType::Field(InfluxFieldType::Integer)
                    }
                } if column == "fv"
            ),
            "didn't match returned error: {:?}",
            response
        );

        let lp = "foo bv=1 1";
        let entry = lp_to_entry(lp);
        let response = table
            .write_columns(
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
                None,
            )
            .err()
            .unwrap();
        assert!(
            matches!(
                &response,
                Error::ColumnError {
                    column,
                    source: column::Error::TypeMismatch {
                        existing: InfluxColumnType::Field(InfluxFieldType::Boolean),
                        inserted: InfluxColumnType::Field(InfluxFieldType::Float)
                    }
                } if column == "bv"
            ),
            "didn't match returned error: {:?}",
            response
        );

        let lp = "foo sv=true 1";
        let entry = lp_to_entry(lp);
        let response = table
            .write_columns(
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
                None,
            )
            .err()
            .unwrap();
        assert!(
            matches!(
                &response,
                Error::ColumnError {
                    column,
                    source: column::Error::TypeMismatch {
                        existing: InfluxColumnType::Field(InfluxFieldType::String),
                        inserted: InfluxColumnType::Field(InfluxFieldType::Boolean),
                    }
                } if column == "sv"
            ),
            "didn't match returned error: {:?}",
            response
        );

        let lp = "foo,sv=\"bar\" f=3i 1";
        let entry = lp_to_entry(lp);
        let response = table
            .write_columns(
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
                None,
            )
            .err()
            .unwrap();
        assert!(
            matches!(
                &response,
                Error::ColumnError {
                    column,
                    source: column::Error::TypeMismatch {
                        existing: InfluxColumnType::Field(InfluxFieldType::String),
                        inserted: InfluxColumnType::Tag,
                    }
                } if column == "sv"
            ),
            "didn't match returned error: {:?}",
            response
        );
    }
}
