//! Implementation of Deduplication algorithm

use std::{ops::Range, sync::Arc};

use arrow::{
    array::{ArrayRef, UInt64Array},
    compute::TakeOptions,
    error::Result as ArrowResult,
    record_batch::RecordBatch,
};

use arrow_util::optimize::optimize_dictionaries;
use datafusion::physical_plan::{
    coalesce_batches::concat_batches, expressions::PhysicalSortExpr, PhysicalExpr, SQLMetric,
};
use observability_deps::tracing::trace;

// Handles the deduplication across potentially multiple
// [`RecordBatch`]es which are already sorted on a primary key,
// including primary keys which straddle RecordBatch boundaries
#[derive(Debug)]
pub(crate) struct RecordBatchDeduplicator {
    sort_keys: Vec<PhysicalSortExpr>,
    last_batch: Option<RecordBatch>,
    num_dupes: Arc<SQLMetric>,
}

#[derive(Debug)]
struct DuplicateRanges {
    ///  `is_sort_key[col_idx] = true` if the the input column at
    ///  `col_idx` is present in sort keys
    is_sort_key: Vec<bool>,

    /// ranges of row indices where the sort key columns have the
    /// same values
    ranges: Vec<Range<usize>>,
}

impl RecordBatchDeduplicator {
    pub fn new(sort_keys: Vec<PhysicalSortExpr>, num_dupes: Arc<SQLMetric>) -> Self {
        Self {
            sort_keys,
            last_batch: None,
            num_dupes,
        }
    }

    /// Push a new RecordBatch into the indexer. Returns a
    /// deduplicated RecordBatch and remembers any currently opened
    /// groups
    pub fn push(&mut self, batch: RecordBatch) -> ArrowResult<RecordBatch> {
        // If we had a previous batch of rows, add it in here
        //
        // Potential optimization would be to check if the sort key is actually the same
        // for the first row in the new batch and skip this concat if that is the case
        let batch = if let Some(last_batch) = self.last_batch.take() {
            let schema = last_batch.schema();
            let row_count = last_batch.num_rows() + batch.num_rows();
            concat_batches(&schema, &[last_batch, batch], row_count)?
        } else {
            batch
        };

        let mut dupe_ranges = self.compute_ranges(&batch)?;

        // The last partition may span batches so we can't emit it
        // until we have seen the next batch (or we are at end of
        // stream)
        let last_range = dupe_ranges.ranges.pop();

        let output_record_batch = self.output_from_ranges(&batch, &dupe_ranges)?;

        // Now, save the last bit of the pk
        if let Some(last_range) = last_range {
            let len = last_range.end - last_range.start;
            let last_batch = Self::slice_record_batch(&batch, last_range.start, len)?;
            self.last_batch = Some(last_batch);
        }

        Ok(output_record_batch)
    }

    /// Consume the indexer, returning any remaining record batches for output
    pub fn finish(mut self) -> ArrowResult<Option<RecordBatch>> {
        self.last_batch
            .take()
            .map(|last_batch| {
                let dupe_ranges = self.compute_ranges(&last_batch)?;
                self.output_from_ranges(&last_batch, &dupe_ranges)
            })
            .transpose()
    }

    /// Computes the ranges where the sort key has the same values
    fn compute_ranges(&self, batch: &RecordBatch) -> ArrowResult<DuplicateRanges> {
        let schema = batch.schema();
        // is_sort_key[col_idx] = true if it is present in sort keys
        let mut is_sort_key: Vec<bool> = vec![false; batch.columns().len()];

        // Figure out where the partitions are:
        let columns: Vec<_> = self
            .sort_keys
            .iter()
            .map(|skey| {
                // figure out what input column this is for
                let name = get_col_name(skey.expr.as_ref());
                let index = schema.index_of(name).unwrap();

                is_sort_key[index] = true;

                let array = batch.column(index);

                arrow::compute::SortColumn {
                    values: Arc::clone(array),
                    options: Some(skey.options),
                }
            })
            .collect();

        // Compute partitions (aka breakpoints between the ranges)
        let ranges = arrow::compute::lexicographical_partition_ranges(&columns)?;

        Ok(DuplicateRanges {
            is_sort_key,
            ranges,
        })
    }

    /// Compute the output record batch that includes the specified ranges
    fn output_from_ranges(
        &self,
        batch: &RecordBatch,
        dupe_ranges: &DuplicateRanges,
    ) -> ArrowResult<RecordBatch> {
        let ranges = &dupe_ranges.ranges;

        // each range is at least 1 large, so any that have more than
        // 1 are duplicates
        let num_dupes = ranges.iter().map(|r| r.end - r.start - 1).sum();

        self.num_dupes.add(num_dupes);

        // Special case when no ranges are duplicated (so just emit input as output)
        if num_dupes == 0 {
            trace!(num_rows = batch.num_rows(), "No dupes");
            Self::slice_record_batch(&batch, 0, ranges.len())
        } else {
            trace!(num_dupes, num_rows = batch.num_rows(), "dupes");

            // Use take kernel
            let sort_key_indices = self.compute_sort_key_indices(&ranges);

            let take_options = Some(TakeOptions {
                check_bounds: false,
            });

            // Form each new column by `take`ing the indices as needed
            let new_columns = batch
                .columns()
                .iter()
                .enumerate()
                .map(|(input_index, input_array)| {
                    if dupe_ranges.is_sort_key[input_index] {
                        arrow::compute::take(
                            input_array.as_ref(),
                            &sort_key_indices,
                            take_options.clone(),
                        )
                    } else {
                        // pick the last non null value
                        let field_indices = self.compute_field_indices(&ranges, input_array);

                        arrow::compute::take(
                            input_array.as_ref(),
                            &field_indices,
                            take_options.clone(),
                        )
                    }
                })
                .collect::<ArrowResult<Vec<ArrayRef>>>()?;

            let batch = RecordBatch::try_new(batch.schema(), new_columns)?;
            // At time of writing, `MutableArrayData` concatenates the
            // contents of dictionaries as well; Do a post pass to remove the
            // redundancy if possible
            optimize_dictionaries(&batch)
        }
    }

    /// Returns an array of indices, one for each input range (which
    /// index is arbitrary as all the values are the same for the sort
    /// column in each pk group)
    ///
    /// ranges: 0-1, 2-4, 5-6 --> Array[0, 2, 5]
    fn compute_sort_key_indices(&self, ranges: &[Range<usize>]) -> UInt64Array {
        ranges.iter().map(|r| Some(r.start as u64)).collect()
    }

    /// Returns an array of indices, one for each input range that
    /// return the first non-null value of `input_array` in that range
    /// (aka it will pick the index of the field value to use for each
    /// pk group)
    ///
    /// ranges: 0-1, 2-4, 5-6
    /// input array: A, NULL, NULL, C, NULL, NULL
    /// --> Array[0, 3, 5]
    fn compute_field_indices(
        &self,
        ranges: &[Range<usize>],
        input_array: &ArrayRef,
    ) -> UInt64Array {
        ranges
            .iter()
            .map(|r| {
                let value_index = r
                    .clone()
                    .filter(|&i| input_array.is_valid(i))
                    .last()
                    .map(|i| i as u64)
                    // if all field values are none, pick one arbitrarily
                    .unwrap_or(r.start as u64);
                Some(value_index)
            })
            .collect()
    }

    /// Create a new record batch from offset --> len
    ///
    /// https://github.com/apache/arrow-rs/issues/460 for adding this upstream
    fn slice_record_batch(
        batch: &RecordBatch,
        offset: usize,
        len: usize,
    ) -> ArrowResult<RecordBatch> {
        let schema = batch.schema();
        let new_columns: Vec<_> = batch
            .columns()
            .iter()
            .map(|old_column| old_column.slice(offset, len))
            .collect();

        let batch = RecordBatch::try_new(schema, new_columns)?;

        // At time of writing, `concat_batches` concatenates the
        // contents of dictionaries as well; Do a post pass to remove the
        // redundancy if possible
        optimize_dictionaries(&batch)
    }
}

/// Get column name out of the `expr`. TODO use
/// internal_types::schema::SortKey instead.
fn get_col_name(expr: &dyn PhysicalExpr) -> &str {
    expr.as_any()
        .downcast_ref::<datafusion::physical_plan::expressions::Column>()
        .expect("expected column reference")
        .name()
}
