//! This module contains code that "unpivots" annotated
//! [`RecordBatch`]es to [`Series`] and [`Group`]s for output by the
//! storage gRPC interface

use arrow::{
    self,
    array::{Array, BooleanArray, DictionaryArray, StringArray},
    compute,
    datatypes::{DataType, Int32Type, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::{
    error::DataFusionError,
    execution::memory_pool::{proxy::VecAllocExt, MemoryConsumer, MemoryPool, MemoryReservation},
    physical_plan::SendableRecordBatchStream,
};

use futures::{ready, Stream, StreamExt};
use predicate::rpc_predicate::{GROUP_KEY_SPECIAL_START, GROUP_KEY_SPECIAL_STOP};
use snafu::{OptionExt, Snafu};
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::exec::{
    field::{self, FieldColumns, FieldIndexes},
    seriesset::series::Group,
};

use super::{
    series::{Either, Series},
    SeriesSet,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Internal field error while converting series set: {}", source))]
    InternalField { source: field::Error },

    #[snafu(display("Internal error finding grouping colum: {}", column_name))]
    FindingGroupColumn { column_name: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

// Handles converting record batches into SeriesSets
#[derive(Debug, Default)]
pub struct SeriesSetConverter {}

impl SeriesSetConverter {
    /// Convert the results from running a DataFusion plan into the
    /// appropriate SeriesSetItems.
    ///
    /// The results must be in the logical format described in this
    /// module's documentation (i.e. ordered by tag keys)
    ///
    /// table_name: The name of the table
    ///
    /// tag_columns: The names of the columns that define tags
    ///
    /// field_columns: The names of the columns which are "fields"
    ///
    /// it: record batch iterator that produces data in the desired order
    pub async fn convert(
        &mut self,
        table_name: Arc<str>,
        tag_columns: Arc<Vec<Arc<str>>>,
        field_columns: FieldColumns,
        it: SendableRecordBatchStream,
    ) -> Result<impl Stream<Item = Result<SeriesSet, DataFusionError>>, DataFusionError> {
        assert_eq!(
            tag_columns.as_ref(),
            &{
                let mut tmp = tag_columns.as_ref().clone();
                tmp.sort();
                tmp
            },
            "Tag column sorted",
        );

        let schema = it.schema();

        let tag_indexes = FieldIndexes::names_to_indexes(&schema, &tag_columns).map_err(|e| {
            DataFusionError::Context(
                "Internal field error while converting series set".to_string(),
                Box::new(DataFusionError::External(Box::new(e))),
            )
        })?;
        let field_indexes =
            FieldIndexes::from_field_columns(&schema, &field_columns).map_err(|e| {
                DataFusionError::Context(
                    "Internal field error while converting series set".to_string(),
                    Box::new(DataFusionError::External(Box::new(e))),
                )
            })?;

        Ok(SeriesSetConverterStream {
            result_buffer: VecDeque::default(),
            open_batches: Vec::default(),
            need_new_batch: true,
            we_finished: false,
            schema,
            it: Some(it),
            tag_indexes,
            field_indexes,
            table_name,
            tag_columns,
        })
    }

    /// Returns the row indexes in `batch` where all of the values in the `tag_indexes` columns
    /// take on a new value.
    ///
    /// For example:
    ///
    /// ```text
    /// tags A, B
    /// ```
    ///
    /// If the input is:
    ///
    /// A | B | C
    /// - | - | -
    /// 1 | 2 | x
    /// 1 | 2 | y
    /// 2 | 2 | z
    /// 3 | 3 | q
    /// 3 | 3 | r
    ///
    /// Then this function will return `[3, 4]`:
    ///
    /// - The row at index 3 has values for A and B (2,2) different than the previous row (1,2).
    /// - Similarly the row at index 4 has values (3,3) which are different than (2,2).
    /// - However, the row at index 5 has the same values (3,3) so is NOT a transition point
    fn compute_changepoints(batch: &RecordBatch, tag_indexes: &[usize]) -> Vec<usize> {
        let tag_transitions = tag_indexes
            .iter()
            .map(|&col| Self::compute_transitions(batch, col))
            .collect::<Vec<_>>();

        // no tag columns, emit a single tagset
        if tag_transitions.is_empty() {
            vec![]
        } else {
            // OR bitsets together to to find all rows where the
            // keyset (values of the tag keys) changes
            let mut tag_transitions_it = tag_transitions.into_iter();
            let init = tag_transitions_it.next().expect("not empty");
            let intersections =
                tag_transitions_it.fold(init, |a, b| compute::or(&a, &b).expect("or operation"));

            intersections
                .iter()
                .enumerate()
                .filter(|(_idx, mask)| mask.unwrap_or(true))
                .map(|(idx, _mask)| idx)
                .collect()
        }
    }

    /// returns a bitset with all row indexes where the value of the
    /// batch `col_idx` changes.  Does not include row 0, always includes
    /// the last row, `batch.num_rows() - 1`
    ///
    /// Note: This may return false positives in the presence of dictionaries
    /// containing duplicates
    fn compute_transitions(batch: &RecordBatch, col_idx: usize) -> BooleanArray {
        let num_rows = batch.num_rows();

        if num_rows == 0 {
            return BooleanArray::builder(0).finish();
        }

        let col = batch.column(col_idx);

        let arr = compute::concat(&[
            &{
                let mut b = BooleanArray::builder(1);
                b.append_value(false);
                b.finish()
            },
            &compute::neq_dyn(&col.slice(0, col.len() - 1), &col.slice(1, col.len() - 1))
                .expect("cmp"),
        ])
        .expect("concat");

        // until https://github.com/apache/arrow-rs/issues/2901 is done, use a workaround
        // to get a `BooleanArray`
        BooleanArray::from(arr.data().clone())
    }

    /// Creates (column_name, column_value) pairs for each column
    /// named in `tag_column_name` at the corresponding index
    /// `tag_indexes`
    fn get_tag_keys(
        batch: &RecordBatch,
        row: usize,
        tag_column_names: &[Arc<str>],
        tag_indexes: &[usize],
    ) -> Vec<(Arc<str>, Arc<str>)> {
        assert_eq!(tag_column_names.len(), tag_indexes.len());

        let mut out = tag_column_names
            .iter()
            .zip(tag_indexes)
            .filter_map(|(column_name, column_index)| {
                let col = batch.column(*column_index);
                let tag_value = match col.data_type() {
                    DataType::Utf8 => {
                        let col = col.as_any().downcast_ref::<StringArray>().unwrap();

                        if col.is_valid(row) {
                            Some(col.value(row).to_string())
                        } else {
                            None
                        }
                    }
                    DataType::Dictionary(key, value)
                        if key.as_ref() == &DataType::Int32
                            && value.as_ref() == &DataType::Utf8 =>
                    {
                        let col = col
                            .as_any()
                            .downcast_ref::<DictionaryArray<Int32Type>>()
                            .expect("Casting column");

                        if col.is_valid(row) {
                            let key = col.keys().value(row);
                            let value = col
                                .values()
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .unwrap()
                                .value(key as _)
                                .to_string();
                            Some(value)
                        } else {
                            None
                        }
                    }
                    _ => unimplemented!(
                        "Series get_tag_keys not supported for type {:?} in column {:?}",
                        col.data_type(),
                        batch.schema().fields()[*column_index]
                    ),
                };

                tag_value.map(|tag_value| (Arc::clone(column_name), Arc::from(tag_value.as_str())))
            })
            .collect::<Vec<_>>();

        out.shrink_to_fit();
        out
    }
}

struct SeriesSetConverterStream {
    /// [`SeriesSet`]s that are ready to be emitted by this stream.
    ///
    /// These results must always be emitted before doing any additional work.
    result_buffer: VecDeque<SeriesSet>,

    /// Batches of data that have NO change point, i.e. they all belong to the same output set. However we have not yet
    /// found the next change point (or the end of the stream) so we need to keep them.
    ///
    /// We keep a list of batches instead of a giant concatenated batch to avoid `O(n^2)` complexity due to repeated mem-copies.
    open_batches: Vec<RecordBatch>,

    /// If `true`, we need to pull a new batch of `it`.
    need_new_batch: bool,

    /// We (i.e. [`SeriesSetConverterStream`]) completed its work. However there might be data available in
    /// [`result_buffer`](Self::result_buffer) which must be drained before returning `Ready(None)`.
    we_finished: bool,

    /// The schema of the input data.
    schema: SchemaRef,

    /// Indexes (within [`schema`](Self::schema)) of the tag columns.
    tag_indexes: Vec<usize>,

    /// Indexes (within [`schema`](Self::schema)) of the field columns.
    field_indexes: FieldIndexes,

    /// Name of the table we're operating on.
    ///
    /// This is required because this is part of the output [`SeriesSet`]s.
    table_name: Arc<str>,

    /// Name of the tag columns.
    ///
    /// This is kept in addition to [`tag_indexes`](Self::tag_indexes) because it is part of the output [`SeriesSet`]s.
    tag_columns: Arc<Vec<Arc<str>>>,

    /// Input data stream.
    ///
    ///
    /// This may be `None` when the stream was fully drained. We need to remember that fact so we don't pull a
    /// finished stream (which may panic).
    it: Option<SendableRecordBatchStream>,
}

impl Stream for SeriesSetConverterStream {
    type Item = Result<SeriesSet, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        loop {
            // drain results
            if let Some(sset) = this.result_buffer.pop_front() {
                return Poll::Ready(Some(Ok(sset)));
            }

            // early exit
            if this.we_finished {
                return Poll::Ready(None);
            }

            // do we need more input data?
            if this.need_new_batch {
                loop {
                    match ready!(this
                        .it
                        .as_mut()
                        .expect("need new input but input stream is already drained")
                        .poll_next_unpin(cx))
                    {
                        Some(Err(e)) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                        Some(Ok(batch)) => {
                            // skip empty batches (simplifies our code further down below because we can always assume that
                            // there's at least one row in the batch)
                            if batch.num_rows() == 0 {
                                continue;
                            }

                            this.open_batches.push(batch)
                        }
                        None => {
                            this.it = None;
                        }
                    }
                    break;
                }

                this.need_new_batch = false;
            }

            // do we only have a single batch or do we "overflow" from the last batch?
            let (batch_for_changepoints, extra_first_row) = match this.open_batches.len() {
                0 => {
                    assert!(
                        this.it.is_none(),
                        "We have no open batches left, so the input stream should be finished",
                    );
                    this.we_finished = true;
                    return Poll::Ready(None);
                }
                1 => (
                    this.open_batches.last().expect("checked length").clone(),
                    false,
                ),
                _ => {
                    // `open_batches` contains at least two batches. The last one was added just from the input stream.
                    // The prev. one was the end of the "open" interval and all before that belong to the same output
                    // set (because otherwise we would have flushed them earlier).
                    let batch_last = &this.open_batches[this.open_batches.len() - 2];
                    let batch_current = &this.open_batches[this.open_batches.len() - 1];
                    assert!(batch_last.num_rows() > 0);

                    let batch = match compute::concat_batches(
                        &this.schema,
                        &[
                            batch_last.slice(batch_last.num_rows() - 1, 1),
                            batch_current.clone(),
                        ],
                    ) {
                        Ok(batch) => batch,
                        Err(e) => {
                            // internal state is broken, end this stream
                            this.we_finished = true;
                            return Poll::Ready(Some(Err(DataFusionError::ArrowError(e))));
                        }
                    };

                    (batch, true)
                }
            };

            // compute changepoints
            let mut changepoints = SeriesSetConverter::compute_changepoints(
                &batch_for_changepoints,
                &this.tag_indexes,
            );
            if this.it.is_none() {
                // need to finish last SeriesSet
                changepoints.push(batch_for_changepoints.num_rows());
            }
            let prev_sizes = this.open_batches[..(this.open_batches.len() - 1)]
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>();
            let cp_delta = if extra_first_row {
                prev_sizes
                    .checked_sub(1)
                    .expect("at least one non-empty prev. batch")
            } else {
                prev_sizes
            };
            let changepoints = changepoints
                .into_iter()
                .map(|x| x + cp_delta)
                .collect::<Vec<_>>();

            // already change to "needs data" before we start emission
            this.need_new_batch = true;
            if this.it.is_none() {
                this.we_finished = true;
            }

            if !changepoints.is_empty() {
                // `batch_for_changepoints` only contains the last batch and the last row of the prev. one. However we
                // need to flush ALL rows in `open_batches` (and keep the ones after the last changepoint as a new open
                // batch). So concat again.
                let batch_for_flush =
                    match compute::concat_batches(&this.schema, &this.open_batches) {
                        Ok(batch) => batch,
                        Err(e) => {
                            // internal state is broken, end this stream
                            this.we_finished = true;
                            return Poll::Ready(Some(Err(DataFusionError::ArrowError(e))));
                        }
                    };

                let last_cp = *changepoints.last().expect("checked length");
                if last_cp == batch_for_flush.num_rows() {
                    // fully drained open batches
                    // This can ONLY happen when the input stream finished because `comput_changepoint` never returns
                    // the last row as changepoint (so we must have manually added that above).
                    assert!(
                        this.it.is_none(),
                        "Fully flushed all open batches but the input stream still has data?!"
                    );
                    this.open_batches.drain(..);
                } else {
                    // need to keep the open bit
                    // do NOT use `batch` here because it contains data for all open batches, we just need the last one
                    // (`slice` is zero-copy)
                    let offset = last_cp.checked_sub(prev_sizes).expect("underflow");
                    let last_batch = this.open_batches.last().expect("at least one batch");
                    let last_batch = last_batch.slice(
                        offset,
                        last_batch
                            .num_rows()
                            .checked_sub(offset)
                            .expect("underflow"),
                    );
                    this.open_batches.drain(..);
                    this.open_batches.push(last_batch);
                }

                // emit each series
                let mut start_row: usize = 0;
                assert!(this.result_buffer.is_empty());
                this.result_buffer = changepoints
                    .into_iter()
                    .map(|end_row| {
                        let series_set = SeriesSet {
                            table_name: Arc::clone(&this.table_name),
                            tags: SeriesSetConverter::get_tag_keys(
                                &batch_for_flush,
                                start_row,
                                &this.tag_columns,
                                &this.tag_indexes,
                            ),
                            field_indexes: this.field_indexes.clone(),
                            start_row,
                            num_rows: (end_row - start_row),
                            // batch clones are super cheap (in contrast to `slice` which has a way higher overhead!)
                            batch: batch_for_flush.clone(),
                        };

                        start_row = end_row;
                        series_set
                    })
                    .collect();
            }
        }
    }
}

/// Reorders and groups a sequence of Series is grouped correctly
#[derive(Debug)]
pub struct GroupGenerator {
    group_columns: Vec<Arc<str>>,
    memory_pool: Arc<dyn MemoryPool>,
    collector_buffered_size_max: usize,
}

impl GroupGenerator {
    pub fn new(group_columns: Vec<Arc<str>>, memory_pool: Arc<dyn MemoryPool>) -> Self {
        Self::new_with_buffered_size_max(
            group_columns,
            memory_pool,
            Collector::<()>::DEFAULT_ALLOCATION_BUFFER_SIZE,
        )
    }

    fn new_with_buffered_size_max(
        group_columns: Vec<Arc<str>>,
        memory_pool: Arc<dyn MemoryPool>,
        collector_buffered_size_max: usize,
    ) -> Self {
        Self {
            group_columns,
            memory_pool,
            collector_buffered_size_max,
        }
    }

    /// groups the set of `series` into SeriesOrGroups
    ///
    /// TODO: make this truly stream-based, see <https://github.com/influxdata/influxdb_iox/issues/6347>.
    pub async fn group<S>(
        self,
        series: S,
    ) -> Result<impl Stream<Item = Result<Either, DataFusionError>>, DataFusionError>
    where
        S: Stream<Item = Result<Series, DataFusionError>> + Send,
    {
        let series = Box::pin(series);
        let mut series = Collector::new(
            series,
            self.group_columns,
            self.memory_pool,
            self.collector_buffered_size_max,
        )
        .await?;

        // Potential optimization is to skip this sort if we are
        // grouping by a prefix of the tags for a single measurement
        //
        // Another potential optimization is if we are only grouping on
        // tag columns is to change the the actual plan output using
        // DataFusion to sort the data in the required group (likely
        // only possible with a single table)

        // Resort the data according to group key values
        series.sort();

        // now find the groups boundaries and emit the output
        let mut last_partition_key_vals: Option<Vec<Arc<str>>> = None;

        // Note that if there are no group columns, we still need to
        // sort by the tag keys, so that the output is sorted by tag
        // keys, and thus we can't bail out early here
        //
        // Interesting, it isn't clear flux requires this ordering, but
        // it is what TSM does so we preserve the behavior
        let mut output = vec![];

        // TODO make this more functional (issue is that sometimes the
        // loop inserts one item into `output` and sometimes it inserts 2)
        for SortableSeries {
            series,
            tag_vals,
            num_partition_keys,
        } in series.into_iter()
        {
            // keep only the values that form the group
            let mut partition_key_vals = tag_vals;
            partition_key_vals.truncate(num_partition_keys);

            // figure out if we are in a new group (partition key values have changed)
            let need_group_start = match &last_partition_key_vals {
                None => true,
                Some(last_partition_key_vals) => &partition_key_vals != last_partition_key_vals,
            };

            if need_group_start {
                last_partition_key_vals = Some(partition_key_vals.clone());

                let tag_keys = series.tags.iter().map(|tag| Arc::clone(&tag.key)).collect();

                let group = Group {
                    tag_keys,
                    partition_key_vals,
                };

                output.push(group.into());
            }

            output.push(series.into())
        }

        Ok(futures::stream::iter(output).map(Ok))
    }
}

#[derive(Debug)]
/// Wrapper around a Series that has the values of the group_by columns extracted
struct SortableSeries {
    series: Series,

    /// All the tag values, reordered so that the group_columns are first
    tag_vals: Vec<Arc<str>>,

    /// How many of the first N tag_values are used for the partition key
    num_partition_keys: usize,
}

impl PartialEq for SortableSeries {
    fn eq(&self, other: &Self) -> bool {
        self.tag_vals.eq(&other.tag_vals)
    }
}

impl Eq for SortableSeries {}

impl PartialOrd for SortableSeries {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.tag_vals.partial_cmp(&other.tag_vals)
    }
}

impl Ord for SortableSeries {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.tag_vals.cmp(&other.tag_vals)
    }
}

impl SortableSeries {
    fn try_new(series: Series, group_columns: &[Arc<str>]) -> Result<Self> {
        // Compute the order of new tag values
        let tags = &series.tags;

        // tag_used_set[i] is true if we have used the value in tag_columns[i]
        let mut tag_used_set = vec![false; tags.len()];

        // put the group columns first
        //
        // Note that this is an O(N^2) algorithm. We are assuming the
        // number of tag columns is reasonably small
        let mut tag_vals: Vec<_> = group_columns
            .iter()
            .map(|col| {
                tags.iter()
                    .enumerate()
                    // Searching for columns linearly is likely to be pretty slow....
                    .find(|(_i, tag)| tag.key == *col)
                    .map(|(i, tag)| {
                        assert!(!tag_used_set[i], "repeated group column");
                        tag_used_set[i] = true;
                        Arc::clone(&tag.value)
                    })
                    .or_else(|| {
                        // treat these specially and use value "" to mirror what TSM does
                        // see https://github.com/influxdata/influxdb_iox/issues/2693#issuecomment-947695442
                        // for more details
                        if col.as_ref() == GROUP_KEY_SPECIAL_START
                            || col.as_ref() == GROUP_KEY_SPECIAL_STOP
                        {
                            Some(Arc::from(""))
                        } else {
                            None
                        }
                    })
                    .context(FindingGroupColumnSnafu {
                        column_name: col.as_ref(),
                    })
            })
            .collect::<Result<Vec<_>>>()?;

        // Fill in all remaining tags
        tag_vals.extend(tags.iter().enumerate().filter_map(|(i, tag)| {
            let use_tag = !tag_used_set[i];
            use_tag.then(|| Arc::clone(&tag.value))
        }));

        // safe memory
        tag_vals.shrink_to_fit();

        Ok(Self {
            series,
            tag_vals,
            num_partition_keys: group_columns.len(),
        })
    }

    /// Memory usage in bytes, including `self`.
    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.series.size() - std::mem::size_of_val(&self.series)
            + (std::mem::size_of::<Arc<str>>() * self.tag_vals.capacity())
            + self.tag_vals.iter().map(|s| s.len()).sum::<usize>()
    }
}

/// [`Future`] that collects [`Series`] objects into a [`SortableSeries`] vector while registering/checking memory
/// allocations with a [`MemoryPool`].
///
/// This avoids unbounded memory growth when merging multiple `Series` in memory
struct Collector<S> {
    /// The inner stream was fully drained.
    inner_done: bool,

    /// This very future finished.
    outer_done: bool,

    /// Inner stream.
    inner: S,

    /// Group columns.
    ///
    /// These are required for [`SortableSeries::try_new`].
    group_columns: Vec<Arc<str>>,

    /// Already collected objects.
    collected: Vec<SortableSeries>,

    /// Buffered but not-yet-registered allocated size.
    ///
    /// We use an additional buffer here because in contrast to the normal DataFusion processing, the input stream is
    /// NOT batched and we want to avoid costly memory allocations checks with the [`MemoryPool`] for every single element.
    buffered_size: usize,

    /// Maximum [buffered size](Self::buffered_size). Decreasing this
    /// value causes allocations to be reported to the [`MemoryPool`]
    /// more frequently.
    buffered_size_max: usize,

    /// Our memory reservation.
    mem_reservation: MemoryReservation,
}

impl<S> Collector<S> {
    /// Default maximum [buffered size](Self::buffered_size) before updating [`MemoryPool`] reservation
    const DEFAULT_ALLOCATION_BUFFER_SIZE: usize = 1024 * 1024;
}

impl<S> Collector<S>
where
    S: Stream<Item = Result<Series, DataFusionError>> + Send + Unpin,
{
    fn new(
        inner: S,
        group_columns: Vec<Arc<str>>,
        memory_pool: Arc<dyn MemoryPool>,
        buffered_size_max: usize,
    ) -> Self {
        let mem_reservation = MemoryConsumer::new("SeriesSet Collector").register(&memory_pool);

        Self {
            inner_done: false,
            outer_done: false,
            inner,
            group_columns,
            collected: Vec::with_capacity(0),
            buffered_size: 0,
            buffered_size_max,
            mem_reservation,
        }
    }

    /// Registers all `self.buffered_size` with the MemoryPool,
    /// resetting self.buffered_size to zero. Returns an error if new
    /// memory can not be allocated from the pool.
    fn alloc(&mut self) -> Result<(), DataFusionError> {
        let bytes = std::mem::take(&mut self.buffered_size);
        self.mem_reservation.try_grow(bytes)
    }
}

impl<S> Future for Collector<S>
where
    S: Stream<Item = Result<Series, DataFusionError>> + Send + Unpin,
{
    type Output = Result<Vec<SortableSeries>, DataFusionError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;

        loop {
            assert!(!this.outer_done);
            // if the underlying stream is drained and the allocation future is ready (see above), we can finalize this future
            if this.inner_done {
                this.outer_done = true;
                return Poll::Ready(Ok(std::mem::take(&mut this.collected)));
            }

            match ready!(this.inner.poll_next_unpin(cx)) {
                Some(Ok(series)) => match SortableSeries::try_new(series, &this.group_columns) {
                    Ok(series) => {
                        // Note: the size of `SortableSeries` itself is already included in the vector allocation
                        this.buffered_size += series.size() - std::mem::size_of_val(&series);
                        this.collected
                            .push_accounted(series, &mut this.buffered_size);

                        // should we clear our allocation buffer?
                        if this.buffered_size > this.buffered_size_max {
                            if let Err(e) = this.alloc() {
                                return Poll::Ready(Err(e));
                            }
                            continue;
                        }
                    }
                    Err(e) => {
                        // poison this future
                        this.outer_done = true;
                        return Poll::Ready(Err(DataFusionError::External(Box::new(e))));
                    }
                },
                Some(Err(e)) => {
                    // poison this future
                    this.outer_done = true;
                    return Poll::Ready(Err(e));
                }
                None => {
                    // underlying stream drained. now register the final allocation and then we're done
                    this.inner_done = true;
                    if this.buffered_size > 0 {
                        if let Err(e) = this.alloc() {
                            return Poll::Ready(Err(e));
                        }
                    }
                    continue;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{ArrayRef, Float64Array, Int64Array, TimestampNanosecondArray},
        csv,
        datatypes::DataType,
        datatypes::Field,
        datatypes::{Schema, SchemaRef},
        record_batch::RecordBatch,
    };
    use arrow_util::assert_batches_eq;
    use assert_matches::assert_matches;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion_util::{stream_from_batch, stream_from_batches, stream_from_schema};
    use futures::TryStreamExt;
    use itertools::Itertools;
    use test_helpers::str_vec_to_arc_vec;

    use crate::exec::seriesset::series::{Data, Tag};

    use super::*;

    #[tokio::test]
    async fn test_convert_empty() {
        let schema = test_schema();
        let empty_iterator = stream_from_schema(schema);

        let table_name = "foo";
        let tag_columns = [];
        let field_columns = [];

        let results = convert(table_name, &tag_columns, &field_columns, empty_iterator).await;
        assert_eq!(results.len(), 0);
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("tag_a", DataType::Utf8, true),
            Field::new("tag_b", DataType::Utf8, true),
            Field::new("float_field", DataType::Float64, true),
            Field::new("int_field", DataType::Int64, true),
            Field::new("time", DataType::Int64, false),
        ]))
    }

    #[tokio::test]
    async fn test_convert_single_series_no_tags() {
        // single series
        let schema = test_schema();
        let inputs = parse_to_iterators(schema, &["one,ten,10.0,1,1000", "one,ten,10.1,2,2000"]);
        for (i, input) in inputs.into_iter().enumerate() {
            println!("Stream {i}");

            let table_name = "foo";
            let tag_columns = [];
            let field_columns = ["float_field"];
            let results = convert(table_name, &tag_columns, &field_columns, input).await;

            assert_eq!(results.len(), 1);

            assert_series_set(
                &results[0],
                "foo",
                [],
                FieldIndexes::from_timestamp_and_value_indexes(4, &[2]),
                [
                    "+-------+-------+-------------+-----------+------+",
                    "| tag_a | tag_b | float_field | int_field | time |",
                    "+-------+-------+-------------+-----------+------+",
                    "| one   | ten   | 10          | 1         | 1000 |",
                    "| one   | ten   | 10.1        | 2         | 2000 |",
                    "+-------+-------+-------------+-----------+------+",
                ],
            );
        }
    }

    #[tokio::test]
    async fn test_convert_single_series_no_tags_nulls() {
        // single series
        let schema = test_schema();

        let inputs = parse_to_iterators(schema, &["one,ten,10.0,,1000", "one,ten,10.1,,2000"]);

        // send no values in the int_field colum
        for (i, input) in inputs.into_iter().enumerate() {
            println!("Stream {i}");

            let table_name = "foo";
            let tag_columns = [];
            let field_columns = ["float_field"];
            let results = convert(table_name, &tag_columns, &field_columns, input).await;

            assert_eq!(results.len(), 1);

            assert_series_set(
                &results[0],
                "foo",
                [],
                FieldIndexes::from_timestamp_and_value_indexes(4, &[2]),
                [
                    "+-------+-------+-------------+-----------+------+",
                    "| tag_a | tag_b | float_field | int_field | time |",
                    "+-------+-------+-------------+-----------+------+",
                    "| one   | ten   | 10          |           | 1000 |",
                    "| one   | ten   | 10.1        |           | 2000 |",
                    "+-------+-------+-------------+-----------+------+",
                ],
            );
        }
    }

    #[tokio::test]
    async fn test_convert_single_series_one_tag() {
        // single series
        let schema = test_schema();
        let inputs = parse_to_iterators(schema, &["one,ten,10.0,1,1000", "one,ten,10.1,2,2000"]);

        for (i, input) in inputs.into_iter().enumerate() {
            println!("Stream {i}");

            // test with one tag column, one series
            let table_name = "bar";
            let tag_columns = ["tag_a"];
            let field_columns = ["float_field"];
            let results = convert(table_name, &tag_columns, &field_columns, input).await;

            assert_eq!(results.len(), 1);

            assert_series_set(
                &results[0],
                "bar",
                [("tag_a", "one")],
                FieldIndexes::from_timestamp_and_value_indexes(4, &[2]),
                [
                    "+-------+-------+-------------+-----------+------+",
                    "| tag_a | tag_b | float_field | int_field | time |",
                    "+-------+-------+-------------+-----------+------+",
                    "| one   | ten   | 10          | 1         | 1000 |",
                    "| one   | ten   | 10.1        | 2         | 2000 |",
                    "+-------+-------+-------------+-----------+------+",
                ],
            );
        }
    }

    #[tokio::test]
    async fn test_convert_single_series_one_tag_more_rows() {
        // single series
        let schema = test_schema();
        let inputs = parse_to_iterators(
            schema,
            &[
                "one,ten,10.0,1,1000",
                "one,ten,10.1,2,2000",
                "one,ten,10.2,3,3000",
            ],
        );

        for (i, input) in inputs.into_iter().enumerate() {
            println!("Stream {i}");

            // test with one tag column, one series
            let table_name = "bar";
            let tag_columns = ["tag_a"];
            let field_columns = ["float_field"];
            let results = convert(table_name, &tag_columns, &field_columns, input).await;

            assert_eq!(results.len(), 1);

            assert_series_set(
                &results[0],
                "bar",
                [("tag_a", "one")],
                FieldIndexes::from_timestamp_and_value_indexes(4, &[2]),
                [
                    "+-------+-------+-------------+-----------+------+",
                    "| tag_a | tag_b | float_field | int_field | time |",
                    "+-------+-------+-------------+-----------+------+",
                    "| one   | ten   | 10          | 1         | 1000 |",
                    "| one   | ten   | 10.1        | 2         | 2000 |",
                    "| one   | ten   | 10.2        | 3         | 3000 |",
                    "+-------+-------+-------------+-----------+------+",
                ],
            );
        }
    }

    #[tokio::test]
    async fn test_convert_one_tag_multi_series() {
        let schema = test_schema();

        let inputs = parse_to_iterators(
            schema,
            &[
                "one,ten,10.0,1,1000",
                "one,ten,10.1,2,2000",
                "one,eleven,10.1,3,3000",
                "two,eleven,10.2,4,4000",
                "two,eleven,10.3,5,5000",
            ],
        );

        for (i, input) in inputs.into_iter().enumerate() {
            println!("Stream {i}");

            let table_name = "foo";
            let tag_columns = ["tag_a"];
            let field_columns = ["int_field"];
            let results = convert(table_name, &tag_columns, &field_columns, input).await;

            assert_eq!(results.len(), 2);

            assert_series_set(
                &results[0],
                "foo",
                [("tag_a", "one")],
                FieldIndexes::from_timestamp_and_value_indexes(4, &[3]),
                [
                    "+-------+--------+-------------+-----------+------+",
                    "| tag_a | tag_b  | float_field | int_field | time |",
                    "+-------+--------+-------------+-----------+------+",
                    "| one   | ten    | 10          | 1         | 1000 |",
                    "| one   | ten    | 10.1        | 2         | 2000 |",
                    "| one   | eleven | 10.1        | 3         | 3000 |",
                    "+-------+--------+-------------+-----------+------+",
                ],
            );
            assert_series_set(
                &results[1],
                "foo",
                [("tag_a", "two")],
                FieldIndexes::from_timestamp_and_value_indexes(4, &[3]),
                [
                    "+-------+--------+-------------+-----------+------+",
                    "| tag_a | tag_b  | float_field | int_field | time |",
                    "+-------+--------+-------------+-----------+------+",
                    "| two   | eleven | 10.2        | 4         | 4000 |",
                    "| two   | eleven | 10.3        | 5         | 5000 |",
                    "+-------+--------+-------------+-----------+------+",
                ],
            );
        }
    }

    // two tag columns, three series
    #[tokio::test]
    async fn test_convert_two_tag_multi_series() {
        let schema = test_schema();

        let inputs = parse_to_iterators(
            schema,
            &[
                "one,ten,10.0,1,1000",
                "one,ten,10.1,2,2000",
                "one,eleven,10.1,3,3000",
                "two,eleven,10.2,4,4000",
                "two,eleven,10.3,5,5000",
            ],
        );

        for (i, input) in inputs.into_iter().enumerate() {
            println!("Stream {i}");

            let table_name = "foo";
            let tag_columns = ["tag_a", "tag_b"];
            let field_columns = ["int_field"];
            let results = convert(table_name, &tag_columns, &field_columns, input).await;

            assert_eq!(results.len(), 3);

            assert_series_set(
                &results[0],
                "foo",
                [("tag_a", "one"), ("tag_b", "ten")],
                FieldIndexes::from_timestamp_and_value_indexes(4, &[3]),
                [
                    "+-------+-------+-------------+-----------+------+",
                    "| tag_a | tag_b | float_field | int_field | time |",
                    "+-------+-------+-------------+-----------+------+",
                    "| one   | ten   | 10          | 1         | 1000 |",
                    "| one   | ten   | 10.1        | 2         | 2000 |",
                    "+-------+-------+-------------+-----------+------+",
                ],
            );
            assert_series_set(
                &results[1],
                "foo",
                [("tag_a", "one"), ("tag_b", "eleven")],
                FieldIndexes::from_timestamp_and_value_indexes(4, &[3]),
                [
                    "+-------+--------+-------------+-----------+------+",
                    "| tag_a | tag_b  | float_field | int_field | time |",
                    "+-------+--------+-------------+-----------+------+",
                    "| one   | eleven | 10.1        | 3         | 3000 |",
                    "+-------+--------+-------------+-----------+------+",
                ],
            );
            assert_series_set(
                &results[2],
                "foo",
                [("tag_a", "two"), ("tag_b", "eleven")],
                FieldIndexes::from_timestamp_and_value_indexes(4, &[3]),
                [
                    "+-------+--------+-------------+-----------+------+",
                    "| tag_a | tag_b  | float_field | int_field | time |",
                    "+-------+--------+-------------+-----------+------+",
                    "| two   | eleven | 10.2        | 4         | 4000 |",
                    "| two   | eleven | 10.3        | 5         | 5000 |",
                    "+-------+--------+-------------+-----------+------+",
                ],
            );
        }
    }

    #[tokio::test]
    async fn test_convert_two_tag_with_null_multi_series() {
        let tag_a = StringArray::from(vec!["one", "one", "one"]);
        let tag_b = StringArray::from(vec![Some("ten"), Some("ten"), None]);
        let float_field = Float64Array::from(vec![10.0, 10.1, 10.1]);
        let int_field = Int64Array::from(vec![1, 2, 3]);
        let time = TimestampNanosecondArray::from(vec![1000, 2000, 3000]);

        let batch = RecordBatch::try_from_iter_with_nullable(vec![
            ("tag_a", Arc::new(tag_a) as ArrayRef, true),
            ("tag_b", Arc::new(tag_b), true),
            ("float_field", Arc::new(float_field), true),
            ("int_field", Arc::new(int_field), true),
            ("time", Arc::new(time), false),
        ])
        .unwrap();

        // Input has one row that has no value (NULL value) for tag_b, which is its own series
        let input = stream_from_batch(batch.schema(), batch);

        let table_name = "foo";
        let tag_columns = ["tag_a", "tag_b"];
        let field_columns = ["int_field"];
        let results = convert(table_name, &tag_columns, &field_columns, input).await;

        assert_eq!(results.len(), 2);

        assert_series_set(
            &results[0],
            "foo",
            [("tag_a", "one"), ("tag_b", "ten")],
            FieldIndexes::from_timestamp_and_value_indexes(4, &[3]),
            [
                "+-------+-------+-------------+-----------+-----------------------------+",
                "| tag_a | tag_b | float_field | int_field | time                        |",
                "+-------+-------+-------------+-----------+-----------------------------+",
                "| one   | ten   | 10          | 1         | 1970-01-01T00:00:00.000001Z |",
                "| one   | ten   | 10.1        | 2         | 1970-01-01T00:00:00.000002Z |",
                "+-------+-------+-------------+-----------+-----------------------------+",
            ],
        );
        assert_series_set(
            &results[1],
            "foo",
            [("tag_a", "one")], // note no value for tag_b, only one tag
            FieldIndexes::from_timestamp_and_value_indexes(4, &[3]),
            [
                "+-------+-------+-------------+-----------+-----------------------------+",
                "| tag_a | tag_b | float_field | int_field | time                        |",
                "+-------+-------+-------------+-----------+-----------------------------+",
                "| one   |       | 10.1        | 3         | 1970-01-01T00:00:00.000003Z |",
                "+-------+-------+-------------+-----------+-----------------------------+",
            ],
        );
    }

    /// Test helper: run conversion and return a Vec
    pub async fn convert<'a>(
        table_name: &'a str,
        tag_columns: &'a [&'a str],
        field_columns: &'a [&'a str],
        it: SendableRecordBatchStream,
    ) -> Vec<SeriesSet> {
        let mut converter = SeriesSetConverter::default();

        let table_name = Arc::from(table_name);
        let tag_columns = Arc::new(str_vec_to_arc_vec(tag_columns));
        let field_columns = FieldColumns::from(field_columns);

        converter
            .convert(table_name, tag_columns, field_columns, it)
            .await
            .expect("Conversion happened without error")
            .try_collect()
            .await
            .expect("Conversion happened without error")
    }

    /// Test helper: parses the csv content into a single record batch arrow
    /// arrays columnar ArrayRef according to the schema
    fn parse_to_record_batch(schema: SchemaRef, data: &str) -> RecordBatch {
        if data.is_empty() {
            return RecordBatch::new_empty(schema);
        }

        let has_header = false;
        let delimiter = Some(b',');
        let batch_size = 1000;
        let bounds = None;
        let projection = None;
        let datetime_format = None;
        let mut reader = csv::Reader::new(
            data.as_bytes(),
            schema,
            has_header,
            delimiter,
            batch_size,
            bounds,
            projection,
            datetime_format,
        );

        let first_batch = reader.next().expect("Reading first batch");
        assert!(
            first_batch.is_ok(),
            "Can not parse record batch from csv: {first_batch:?}"
        );
        assert!(
            reader.next().is_none(),
            "Unexpected batch while parsing csv"
        );

        println!("batch: \n{first_batch:#?}");

        first_batch.unwrap()
    }

    /// Parses a set of CSV lines into several `RecordBatchStream`s of varying sizes
    ///
    /// For example, with three input lines:
    /// line1
    /// line2
    /// line3
    ///
    /// This will produce two output streams:
    /// Stream1: (line1), (line2), (line3)
    /// Stream2: (line1, line2), (line3)
    fn parse_to_iterators(schema: SchemaRef, lines: &[&str]) -> Vec<SendableRecordBatchStream> {
        split_lines(lines)
            .into_iter()
            .map(|batches| {
                let batches = batches
                    .into_iter()
                    .map(|chunk| Arc::new(parse_to_record_batch(Arc::clone(&schema), &chunk)))
                    .collect::<Vec<_>>();

                stream_from_batches(Arc::clone(&schema), batches)
            })
            .collect()
    }

    fn split_lines(lines: &[&str]) -> Vec<Vec<String>> {
        println!("** Input data:\n{lines:#?}\n\n");
        if lines.is_empty() {
            return vec![vec![], vec![String::from("")]];
        }

        // potential split points for batches
        // we keep each split point twice so we may also produce empty batches
        let n_lines = lines.len();
        let mut split_points = (0..=n_lines).chain(0..=n_lines).collect::<Vec<_>>();
        split_points.sort();

        let mut split_point_sets = split_points
            .into_iter()
            .powerset()
            .map(|mut split_points| {
                split_points.sort();

                // ensure that "begin" and "end" are always split points
                if split_points.first() != Some(&0) {
                    split_points.insert(0, 0);
                }
                if split_points.last() != Some(&n_lines) {
                    split_points.push(n_lines);
                }

                split_points
            })
            .collect::<Vec<_>>();
        split_point_sets.sort();

        let variants = split_point_sets
            .into_iter()
            .unique()
            .map(|split_points| {
                let batches = split_points
                    .into_iter()
                    .tuple_windows()
                    .map(|(begin, end)| lines[begin..end].join("\n"))
                    .collect::<Vec<_>>();

                // stream from those batches
                assert!(!batches.is_empty());
                batches
            })
            .collect::<Vec<_>>();

        assert!(!variants.is_empty());
        variants
    }

    #[test]
    fn test_split_lines() {
        assert_eq!(split_lines(&[]), vec![vec![], vec![String::from("")],],);

        assert_eq!(
            split_lines(&["foo"]),
            vec![
                vec![String::from(""), String::from("foo")],
                vec![String::from(""), String::from("foo"), String::from("")],
                vec![String::from("foo")],
                vec![String::from("foo"), String::from("")],
            ],
        );

        assert_eq!(
            split_lines(&["foo", "bar"]),
            vec![
                vec![
                    String::from(""),
                    String::from("foo"),
                    String::from(""),
                    String::from("bar")
                ],
                vec![
                    String::from(""),
                    String::from("foo"),
                    String::from(""),
                    String::from("bar"),
                    String::from("")
                ],
                vec![String::from(""), String::from("foo"), String::from("bar")],
                vec![
                    String::from(""),
                    String::from("foo"),
                    String::from("bar"),
                    String::from("")
                ],
                vec![String::from(""), String::from("foo\nbar")],
                vec![String::from(""), String::from("foo\nbar"), String::from("")],
                vec![String::from("foo"), String::from(""), String::from("bar")],
                vec![
                    String::from("foo"),
                    String::from(""),
                    String::from("bar"),
                    String::from("")
                ],
                vec![String::from("foo"), String::from("bar")],
                vec![String::from("foo"), String::from("bar"), String::from("")],
                vec![String::from("foo\nbar")],
                vec![String::from("foo\nbar"), String::from("")],
            ],
        );

        assert_eq!(
            split_lines(&["foo", "bar", "xxx"]),
            vec![
                vec![
                    String::from(""),
                    String::from("foo"),
                    String::from(""),
                    String::from("bar"),
                    String::from(""),
                    String::from("xxx")
                ],
                vec![
                    String::from(""),
                    String::from("foo"),
                    String::from(""),
                    String::from("bar"),
                    String::from(""),
                    String::from("xxx"),
                    String::from("")
                ],
                vec![
                    String::from(""),
                    String::from("foo"),
                    String::from(""),
                    String::from("bar"),
                    String::from("xxx")
                ],
                vec![
                    String::from(""),
                    String::from("foo"),
                    String::from(""),
                    String::from("bar"),
                    String::from("xxx"),
                    String::from("")
                ],
                vec![
                    String::from(""),
                    String::from("foo"),
                    String::from(""),
                    String::from("bar\nxxx")
                ],
                vec![
                    String::from(""),
                    String::from("foo"),
                    String::from(""),
                    String::from("bar\nxxx"),
                    String::from("")
                ],
                vec![
                    String::from(""),
                    String::from("foo"),
                    String::from("bar"),
                    String::from(""),
                    String::from("xxx")
                ],
                vec![
                    String::from(""),
                    String::from("foo"),
                    String::from("bar"),
                    String::from(""),
                    String::from("xxx"),
                    String::from("")
                ],
                vec![
                    String::from(""),
                    String::from("foo"),
                    String::from("bar"),
                    String::from("xxx")
                ],
                vec![
                    String::from(""),
                    String::from("foo"),
                    String::from("bar"),
                    String::from("xxx"),
                    String::from("")
                ],
                vec![
                    String::from(""),
                    String::from("foo"),
                    String::from("bar\nxxx")
                ],
                vec![
                    String::from(""),
                    String::from("foo"),
                    String::from("bar\nxxx"),
                    String::from("")
                ],
                vec![
                    String::from(""),
                    String::from("foo\nbar"),
                    String::from(""),
                    String::from("xxx")
                ],
                vec![
                    String::from(""),
                    String::from("foo\nbar"),
                    String::from(""),
                    String::from("xxx"),
                    String::from("")
                ],
                vec![
                    String::from(""),
                    String::from("foo\nbar"),
                    String::from("xxx")
                ],
                vec![
                    String::from(""),
                    String::from("foo\nbar"),
                    String::from("xxx"),
                    String::from("")
                ],
                vec![String::from(""), String::from("foo\nbar\nxxx")],
                vec![
                    String::from(""),
                    String::from("foo\nbar\nxxx"),
                    String::from("")
                ],
                vec![
                    String::from("foo"),
                    String::from(""),
                    String::from("bar"),
                    String::from(""),
                    String::from("xxx")
                ],
                vec![
                    String::from("foo"),
                    String::from(""),
                    String::from("bar"),
                    String::from(""),
                    String::from("xxx"),
                    String::from("")
                ],
                vec![
                    String::from("foo"),
                    String::from(""),
                    String::from("bar"),
                    String::from("xxx")
                ],
                vec![
                    String::from("foo"),
                    String::from(""),
                    String::from("bar"),
                    String::from("xxx"),
                    String::from("")
                ],
                vec![
                    String::from("foo"),
                    String::from(""),
                    String::from("bar\nxxx")
                ],
                vec![
                    String::from("foo"),
                    String::from(""),
                    String::from("bar\nxxx"),
                    String::from("")
                ],
                vec![
                    String::from("foo"),
                    String::from("bar"),
                    String::from(""),
                    String::from("xxx")
                ],
                vec![
                    String::from("foo"),
                    String::from("bar"),
                    String::from(""),
                    String::from("xxx"),
                    String::from("")
                ],
                vec![
                    String::from("foo"),
                    String::from("bar"),
                    String::from("xxx")
                ],
                vec![
                    String::from("foo"),
                    String::from("bar"),
                    String::from("xxx"),
                    String::from("")
                ],
                vec![String::from("foo"), String::from("bar\nxxx")],
                vec![
                    String::from("foo"),
                    String::from("bar\nxxx"),
                    String::from("")
                ],
                vec![
                    String::from("foo\nbar"),
                    String::from(""),
                    String::from("xxx")
                ],
                vec![
                    String::from("foo\nbar"),
                    String::from(""),
                    String::from("xxx"),
                    String::from("")
                ],
                vec![String::from("foo\nbar"), String::from("xxx")],
                vec![
                    String::from("foo\nbar"),
                    String::from("xxx"),
                    String::from("")
                ],
                vec![String::from("foo\nbar\nxxx")],
                vec![String::from("foo\nbar\nxxx"), String::from("")]
            ]
        );
    }

    #[tokio::test]
    async fn test_group_generator_mem_limit() {
        let memory_pool = Arc::new(GreedyMemoryPool::new(1)) as _;

        let ggen = GroupGenerator::new(vec![Arc::from("g")], memory_pool);
        let input = futures::stream::iter([Ok(Series {
            tags: vec![Tag {
                key: Arc::from("g"),
                value: Arc::from("x"),
            }],
            data: Data::FloatPoints {
                timestamps: vec![],
                values: vec![],
            },
        })]);
        let err = match ggen.group(input).await {
            Ok(stream) => stream.try_collect::<Vec<_>>().await.unwrap_err(),
            Err(e) => e,
        };
        assert_matches!(err, DataFusionError::ResourcesExhausted(_));
    }

    #[tokio::test]
    async fn test_group_generator_no_mem_limit() {
        let memory_pool = Arc::new(GreedyMemoryPool::new(usize::MAX)) as _;
        // use a generator w/ a low buffered allocation to force multiple `alloc` calls
        let ggen = GroupGenerator::new_with_buffered_size_max(vec![Arc::from("g")], memory_pool, 1);
        let input = futures::stream::iter([
            Ok(Series {
                tags: vec![Tag {
                    key: Arc::from("g"),
                    value: Arc::from("x"),
                }],
                data: Data::IntegerPoints {
                    timestamps: vec![1],
                    values: vec![1],
                },
            }),
            Ok(Series {
                tags: vec![Tag {
                    key: Arc::from("g"),
                    value: Arc::from("y"),
                }],
                data: Data::IntegerPoints {
                    timestamps: vec![2],
                    values: vec![2],
                },
            }),
            Ok(Series {
                tags: vec![Tag {
                    key: Arc::from("g"),
                    value: Arc::from("x"),
                }],
                data: Data::IntegerPoints {
                    timestamps: vec![3],
                    values: vec![3],
                },
            }),
            Ok(Series {
                tags: vec![Tag {
                    key: Arc::from("g"),
                    value: Arc::from("x"),
                }],
                data: Data::IntegerPoints {
                    timestamps: vec![4],
                    values: vec![4],
                },
            }),
        ]);
        let actual = ggen
            .group(input)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let expected = vec![
            Either::Group(Group {
                tag_keys: vec![Arc::from("g")],
                partition_key_vals: vec![Arc::from("x")],
            }),
            Either::Series(Series {
                tags: vec![Tag {
                    key: Arc::from("g"),
                    value: Arc::from("x"),
                }],
                data: Data::IntegerPoints {
                    timestamps: vec![1],
                    values: vec![1],
                },
            }),
            Either::Series(Series {
                tags: vec![Tag {
                    key: Arc::from("g"),
                    value: Arc::from("x"),
                }],
                data: Data::IntegerPoints {
                    timestamps: vec![3],
                    values: vec![3],
                },
            }),
            Either::Series(Series {
                tags: vec![Tag {
                    key: Arc::from("g"),
                    value: Arc::from("x"),
                }],
                data: Data::IntegerPoints {
                    timestamps: vec![4],
                    values: vec![4],
                },
            }),
            Either::Group(Group {
                tag_keys: vec![Arc::from("g")],
                partition_key_vals: vec![Arc::from("y")],
            }),
            Either::Series(Series {
                tags: vec![Tag {
                    key: Arc::from("g"),
                    value: Arc::from("y"),
                }],
                data: Data::IntegerPoints {
                    timestamps: vec![2],
                    values: vec![2],
                },
            }),
        ];
        assert_eq!(actual, expected);
    }

    fn assert_series_set<const N: usize, const M: usize>(
        set: &SeriesSet,
        table_name: &'static str,
        tags: [(&'static str, &'static str); N],
        field_indexes: FieldIndexes,
        data: [&'static str; M],
    ) {
        assert_eq!(set.table_name.as_ref(), table_name);

        let set_tags = set
            .tags
            .iter()
            .map(|(a, b)| (a.as_ref(), b.as_ref()))
            .collect::<Vec<_>>();
        assert_eq!(set_tags.as_slice(), tags);

        assert_eq!(set.field_indexes, field_indexes);

        assert_batches_eq!(data, &[set.batch.slice(set.start_row, set.num_rows)]);
    }
}
