use crate::{
    metrics::Metrics,
    row_group::{ColumnName, Predicate, RowGroup},
    schema::{AggregateType, ResultSchema},
    table::{self, Table},
};
use arrow::{error::ArrowError, record_batch::RecordBatch};
use data_types::{chunk_metadata::ChunkColumnSummary, partition_metadata::TableSummary};

use observability_deps::tracing::debug;
use schema::selection::Selection;
use schema::{builder::Error as SchemaError, Schema};
use snafu::{ResultExt, Snafu};
use std::{
    collections::{BTreeMap, BTreeSet},
    convert::TryFrom,
};

// The desired minimum row group size, used as the default for the `ChunkBuilder`.
const DEFAULT_ROW_GROUP_MIN_ROWS: usize = 100_000;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("unsupported operation: {}", msg))]
    UnsupportedOperation { msg: String },

    #[snafu(display("error processing table: {}", source))]
    TableError { source: table::Error },

    #[snafu(display("error generating schema for table: {}", source))]
    TableSchemaError { source: SchemaError },

    #[snafu(display("table '{}' does not exist", table_name))]
    TableNotFound { table_name: String },

    #[snafu(display("no data to build chunk for table '{}'", table_name))]
    ChunkBuilderNoInput { table_name: String },

    #[snafu(display("error building chunk for table '{}': {}", table_name, source))]
    ChunkBuilderError {
        table_name: String,
        source: arrow::error::ArrowError,
    },

    #[snafu(display("column '{}' does not exist in table '{}'", column_name, table_name))]
    ColumnDoesNotExist {
        column_name: String,
        table_name: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A `Chunk` is a horizontal partition of data for a single table.
pub struct Chunk {
    // All metrics for the chunk.
    metrics: Metrics,

    // The table associated with the chunk.
    pub(crate) table: Table,
}

impl Chunk {
    /// Start a new Chunk from the given record batch.
    pub fn new(table_name: impl Into<String>, table_data: RecordBatch, metrics: Metrics) -> Self {
        let table_name = table_name.into();
        let row_group = record_batch_to_row_group(&table_name, table_data);

        Self::new_from_row_group(table_name, row_group, metrics)
    }

    // Only used in tests and benchmarks
    pub(crate) fn new_from_row_group(
        table_name: impl Into<String>,
        row_group: RowGroup,
        mut metrics: Metrics,
    ) -> Self {
        let storage_statistics = row_group.column_storage_statistics();
        metrics.update_column_storage_statistics(&storage_statistics);

        Self {
            metrics,
            table: Table::with_row_group(table_name, row_group),
        }
    }

    // The total size taken up by an empty instance of `Chunk`.
    fn base_size() -> usize {
        std::mem::size_of::<Self>()
    }

    /// The total estimated size in bytes of this `Chunk` and all contained
    /// data.
    pub fn size(&self) -> usize {
        Self::base_size() + self.table.size()
    }

    /// Return the estimated size for each column in the table.
    /// Note there may be multiple entries for each column.
    pub fn column_sizes(&self) -> Vec<ChunkColumnSummary> {
        self.table.column_sizes()
    }

    /// The total estimated size in bytes of this `Chunk` and all contained
    /// data if the data was not compressed but was stored contiguously in
    /// vectors. `include_nulls` allows the caller to factor in NULL values or
    /// to ignore them.
    pub fn size_raw(&self, include_nulls: bool) -> usize {
        self.table.size_raw(include_nulls)
    }

    /// The total number of rows in all row groups in all tables in this chunk.
    pub fn rows(&self) -> u64 {
        self.table.rows()
    }

    /// The total number of row groups in all tables in this chunk.
    pub fn row_groups(&self) -> usize {
        self.table.row_groups()
    }

    /// Add a row_group to a table in the chunk, updating all Chunk meta data.
    pub(crate) fn upsert_table_with_row_group(&mut self, row_group: RowGroup) {
        // track new row group statistics to update column-based metrics.
        let storage_statistics = row_group.column_storage_statistics();

        self.table.add_row_group(row_group);

        // update column metrics associated with column storage
        self.metrics
            .update_column_storage_statistics(&storage_statistics);
    }

    /// Add a record batch of data to to a `Table` in the chunk.
    ///
    /// The data is converted to a `RowGroup` outside of any locking so the
    /// caller does not need to be concerned about the size of the update.
    pub fn upsert_table(&mut self, table_data: RecordBatch) {
        let table_name = self.table.name();

        let row_group = record_batch_to_row_group(table_name, table_data);

        self.upsert_table_with_row_group(row_group)
    }

    //
    // Methods for executing queries.
    //

    /// Given a projection of column names, `read_filter` returns all rows that
    /// satisfy the provided predicate, subject to those rows also not
    /// satisfying any of the provided negation predicates.
    ///
    /// Effectively `read_filter` provides projection push-down, predicate
    /// push-down and the ability to express delete operations based on
    /// predicates that are then used to remove rows from the result set.
    ///
    /// Where possible all this work happens on compressed representations, such
    /// that the minimum amount of data is materialised.
    ///
    /// `read_filter` returns an iterator of Record Batches, where each Record
    /// Batch contains all matching rows for a _row group_ within the chunk.
    /// Each Record Batch is processed and materialised lazily.
    ///
    /// Expressing deletes with `read_filter`.
    ///
    /// An expectation of this API is that each distinct delete operation can be
    /// expressed as a predicate consisting of conjunctive expressions comparing
    /// a column to a literal value. For example you might express two delete
    /// operations like this:
    ///
    ///  DELETE FROM t WHERE "region" = 'west';
    ///  DELETE FROM t WHERE "region" = 'north' AND "env" = 'prod';
    ///
    /// In this case `read_filter` should _remove_ from _any_ result set rows
    /// that either:
    ///
    ///  (1) contain a value 'west' in the "region" column; OR
    ///  (2) contain a value 'east' in the "region column and also 'prod' in
    ///      the "env" column.
    ///
    /// It is important that separate delete operations are expressed as
    /// separate `Predicate` objects in the `negated_predicates` argument. This
    /// ensures that the matching rows are appropriately unioned. This final
    /// unioned set of rows is then removed from any rows matching the
    /// `predicate` argument.
    ///
    pub fn read_filter(
        &self,
        predicate: Predicate,
        select_columns: Selection<'_>,
        negated_predicates: Vec<Predicate>,
    ) -> Result<table::ReadFilterResults> {
        debug!(%predicate, ?select_columns, ?negated_predicates, "read_filter called");
        let now = std::time::Instant::now();
        let result = self
            .table
            .read_filter(&select_columns, &predicate, negated_predicates.as_slice())
            .context(TableSnafu);

        let row_groups = result
            .as_ref()
            .map(|result| result.row_groups())
            .unwrap_or(0);
        debug!(elapsed=?now.elapsed(), succeeded=result.is_ok(), ?row_groups, "read_filter completed");
        result
    }

    /// Returns an iterable collection of data in group columns and aggregate
    /// columns, optionally filtered by the provided predicate. Results are
    /// merged across all row groups.
    ///
    /// Note: `read_aggregate` currently only supports grouping on "tag"
    /// columns.
    pub(crate) fn read_aggregate(
        &self,
        predicate: Predicate,
        group_columns: &Selection<'_>,
        aggregates: &[(ColumnName<'_>, AggregateType)],
    ) -> Result<table::ReadAggregateResults> {
        self.table
            .read_aggregate(predicate, group_columns, aggregates)
            .context(TableSnafu)
    }

    //
    // ---- Schema queries
    //

    /// Validates if the predicate can be applied to the table based on the
    /// schema and the predicate's expressions. Returns an error if the
    /// predicate cannot be applied.
    pub fn validate_predicate(&self, predicate: Predicate) -> Result<Predicate, Error> {
        self.table.validate_predicate(predicate).context(TableSnafu)
    }

    /// Determines if one of more rows in the provided table could possibly
    /// match the provided predicate.
    ///
    /// If the provided table does not exist then `could_pass_predicate` returns
    /// `false`. If the predicate is incompatible with chunk's schema
    /// `could_pass_predicate` returns false.
    pub fn could_pass_predicate(&self, predicate: Predicate) -> bool {
        self.table.could_pass_predicate(&predicate)
    }

    /// Return table summaries or all tables in this chunk.
    /// Each table will be represented exactly once.
    ///
    /// TODO(edd): consider deprecating or changing to return information about
    /// the physical layout of the data in the chunk.
    pub fn table_summary(&self) -> TableSummary {
        self.table.table_summary()
    }

    /// Returns a schema object for a `read_filter` operation using the provided
    /// column selection. An error is returned if the specified columns do not
    /// exist.
    pub fn read_filter_table_schema(&self, columns: Selection<'_>) -> Result<Schema> {
        // Validate columns exist in table.
        let table_meta = self.table.meta();
        if let Selection::Some(cols) = columns {
            for column_name in cols {
                if !table_meta.has_column(column_name) {
                    return ColumnDoesNotExistSnafu {
                        column_name: column_name.to_string(),
                        table_name: self.table.name().to_owned(),
                    }
                    .fail();
                }
            }
        }

        // Build a table schema
        Schema::try_from(&ResultSchema {
            select_columns: match columns {
                Selection::All => table_meta.schema_for_all_columns(),
                Selection::Some(column_names) => table_meta.schema_for_column_names(column_names),
            },
            ..ResultSchema::default()
        })
        .context(TableSchemaSnafu)
    }

    /// Determines if at least one row in the Chunk satisfies the provided
    /// predicate. `satisfies_predicate` will return true if it is guaranteed
    /// that at least one row in the Chunk will satisfy the predicate.
    pub fn satisfies_predicate(&self, predicate: &Predicate) -> bool {
        self.table.satisfies_predicate(predicate)
    }

    /// Returns the distinct set of column names that contain data matching the
    /// provided predicate, which may be empty.
    ///
    /// Results can be further limited to a specific selection of columns.
    ///
    /// `dst` is a buffer that will be populated with results. `column_names` is
    /// smart enough to short-circuit processing on row groups when it
    /// determines that all the columns in the row group are already contained
    /// in the results buffer. Callers can skip this behaviour by passing in
    /// an empty `BTreeSet`.
    pub fn column_names(
        &self,
        predicate: Predicate,
        negated_predicates: Vec<Predicate>,
        only_columns: Selection<'_>,
        dst: BTreeSet<String>,
    ) -> Result<BTreeSet<String>> {
        self.table
            .column_names(&predicate, &negated_predicates, only_columns, dst)
            .context(TableSnafu)
    }

    /// Returns the distinct set of column values for each provided column,
    /// where each returned value lives in a row matching the provided
    /// predicate.
    ///
    /// If the predicate is empty then all distinct values are returned for the
    /// chunk.
    ///
    /// `dst` is intended to allow for some more sophisticated execution,
    /// wherein execution can be short-circuited for distinct values that have
    /// already been found. Callers can simply provide an empty `BTreeMap` to
    /// skip this behaviour.
    pub fn column_values(
        &self,
        predicate: Predicate,
        columns: Selection<'_>,
        dst: BTreeMap<String, BTreeSet<String>>,
    ) -> Result<BTreeMap<String, BTreeSet<String>>> {
        let columns = match columns {
            Selection::All => {
                return UnsupportedOperationSnafu {
                    msg: "column_values does not support All columns".to_owned(),
                }
                .fail();
            }
            Selection::Some(columns) => columns,
        };

        self.table
            .column_values(&predicate, columns, dst)
            .context(TableSnafu)
    }
}

fn record_batch_to_row_group(table_name: &str, rb: RecordBatch) -> RowGroup {
    let now = std::time::Instant::now();
    let row_group = RowGroup::from(rb);
    debug!(rows=row_group.rows(), columns=row_group.columns(), size_bytes=row_group.size(),
        raw_size_null=row_group.size_raw(true), raw_size_no_null=row_group.size_raw(true), table_name=?table_name, compressing_took=?now.elapsed(), "row group added");
    row_group
}

impl std::fmt::Debug for Chunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Chunk: rows: {:?}", self.rows())
    }
}

/// A ChunkBuilder builds up a chunk with row-groups whose row counts try to be
/// at least a specified number of rows. The row groups will be built as the row
/// count is met, reducing the overall memory footprint during the building
/// phase.
pub struct ChunkBuilder {
    table_name: String,
    schema: arrow::datatypes::SchemaRef,

    current_rows: usize, // current total rows of below vec
    record_batches: Vec<RecordBatch>,

    row_group_min_rows: usize,
    chunk_metrics: Option<Metrics>,
    row_groups: Vec<RowGroup>,
}

impl ChunkBuilder {
    pub fn new(table_name: impl Into<String>, schema: arrow::datatypes::SchemaRef) -> Self {
        Self {
            table_name: table_name.into(),
            schema,
            current_rows: 0,
            record_batches: vec![],
            row_group_min_rows: DEFAULT_ROW_GROUP_MIN_ROWS,
            chunk_metrics: None,
            row_groups: vec![],
        }
    }

    /// Provide metrics for the built `Chunk`. If no metrics are provided then
    /// no metrics will be tracked for the `Chunk`.
    pub fn with_metrics(mut self, metrics: Metrics) -> Self {
        self.chunk_metrics = Some(metrics);
        self
    }

    /// Specify the desired minimum number of rows in a row group. During build
    /// one final row group may be smaller than this size.
    pub fn set_row_group_min_size(mut self, size: usize) -> Self {
        self.row_group_min_rows = size;
        self
    }

    /// Add a new record batch to the chunk. If the minimum number of rows
    /// threshold is met then all the builder's record batches will be
    /// compressed into a row group and the record batches cleared.
    ///
    /// Note: pushing an empty record batch is currently treated as a no-op.
    pub fn push_record_batch(&mut self, rb: RecordBatch) -> Result<(), ArrowError> {
        if rb.num_rows() == 0 {
            return Ok(());
        }

        self.current_rows += rb.num_rows();
        self.record_batches.push(rb);

        if self.current_rows >= self.row_group_min_rows {
            return self.snapshot_rowgroup();
        }

        Ok(())
    }

    // Force a rowgroup snapshot.
    fn snapshot_rowgroup(&mut self) -> Result<(), ArrowError> {
        // PERF(edd): This will copy all record batches so that they are in a
        // contiguous block of memory. To reduce memory overhead we can avoid
        // the concatenation and teach the `RowGroup` to build itself from a
        // vec of record batches.
        let concat_batch =
            arrow::record_batch::RecordBatch::concat(&self.schema, &self.record_batches)?;

        self.row_groups
            .push(record_batch_to_row_group(&self.table_name, concat_batch));

        // clear pending batches
        self.record_batches.clear();
        self.current_rows = 0;
        Ok(())
    }

    /// Returns true if there are no pending record batches or snapshotted
    /// row groups; effectively the `Chunk` is empty.
    pub fn is_empty(&self) -> bool {
        self.record_batches.is_empty() && self.row_groups.is_empty()
    }

    /// Consume the builder and attempt to create a single `Chunk`. The resulting
    /// `Chunk` will have at most `n + 1` row groups, where `n` is number of
    /// already snapshotted row groups within the builder.
    pub fn build(mut self) -> Result<Chunk> {
        // No batches or row groups is an error because we have nothing to build
        // chunk with.
        if self.is_empty() {
            return ChunkBuilderNoInputSnafu {
                table_name: self.table_name.clone(),
            }
            .fail();
            // Snapshot remaining batches to a row group (accepting that it may
            // be smaller than desired)
        } else if !self.record_batches.is_empty() {
            self.snapshot_rowgroup().context(ChunkBuilderSnafu {
                table_name: self.table_name.clone(),
            })?;
        }

        // Create new chunk
        let mut chunk = Chunk::new_from_row_group(
            self.table_name.clone(),
            self.row_groups.remove(0),
            match self.chunk_metrics.take() {
                // avoid partial move of self
                Some(metrics) => metrics,
                None => Metrics::new_unregistered(),
            },
        );

        for rg in self.row_groups {
            chunk.upsert_table_with_row_group(rg);
        }
        Ok(chunk)
    }

    /*
     *
     * Test helpers
     *
     */

    #[cfg(test)]
    fn with_record_batch(mut self, rb: RecordBatch) -> Self {
        self.must_push_record_batch(rb);
        self
    }

    #[cfg(test)]
    fn must_push_record_batch(&mut self, rb: RecordBatch) {
        self.push_record_batch(rb).unwrap()
    }

    #[cfg(test)]
    fn must_snapshot_rowgroup(&mut self) {
        self.snapshot_rowgroup().unwrap();
    }

    #[cfg(test)]
    fn must_build(self) -> Chunk {
        self.build().unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        row_group::{ColumnType, RowGroup},
        value::Values,
        BinaryExpr,
    };
    use arrow::{
        array::{
            ArrayRef, BinaryArray, BooleanArray, DictionaryArray, Float64Array, Int64Array,
            StringArray, TimestampNanosecondArray, UInt64Array,
        },
        datatypes::{
            DataType::{Boolean, Float64, Int64, UInt64, Utf8},
            Int32Type,
        },
    };
    use data_types::partition_metadata::{ColumnSummary, InfluxDbType, StatValues, Statistics};
    use metric::{Attributes, MetricKind, Observation, ObservationSet, RawReporter};
    use schema::builder::SchemaBuilder;
    use std::iter::FromIterator;
    use std::{num::NonZeroU64, sync::Arc};

    // helper to make the `add_remove_tables` test simpler to read.
    fn gen_recordbatch() -> RecordBatch {
        let schema = SchemaBuilder::new()
            .non_null_tag("region")
            .non_null_field("counter", Float64)
            .non_null_field("active", Boolean)
            .timestamp()
            .field("sketchy_sensor", Float64)
            .build()
            .unwrap()
            .into();

        let data: Vec<ArrayRef> = vec![
            Arc::new(
                vec!["west", "west", "east"]
                    .into_iter()
                    .collect::<DictionaryArray<Int32Type>>(),
            ),
            Arc::new(Float64Array::from(vec![1.2, 3.3, 45.3])),
            Arc::new(BooleanArray::from(vec![true, false, true])),
            Arc::new(TimestampNanosecondArray::from_vec(
                vec![11111111, 222222, 3333],
                None,
            )),
            Arc::new(Float64Array::from(vec![Some(11.0), None, Some(12.0)])),
        ];

        RecordBatch::try_new(schema, data).unwrap()
    }

    // Helper function to assert the contents of a column on a record batch.
    fn assert_rb_column_equals(rb: &RecordBatch, col_name: &str, exp: &Values<'_>) {
        use arrow::datatypes::DataType;

        let got_column = rb.column(rb.schema().index_of(col_name).unwrap());

        match exp {
            Values::Dictionary(keys, values) => match got_column.data_type() {
                DataType::Dictionary(key, value)
                    if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8 =>
                {
                    // Record batch stores keys as i32
                    let keys = keys
                        .iter()
                        .map(|&x| i32::try_from(x).unwrap())
                        .collect::<Vec<_>>();

                    let dictionary = got_column
                        .as_any()
                        .downcast_ref::<DictionaryArray<Int32Type>>()
                        .unwrap();
                    let rb_values = dictionary.values();
                    let rb_values = rb_values.as_any().downcast_ref::<StringArray>().unwrap();

                    // Ensure string values are same
                    assert!(rb_values.iter().zip(values.iter()).all(|(a, b)| &a == b));

                    let rb_keys = dictionary.keys().values();
                    assert_eq!(rb_keys, keys.as_slice());
                }
                d => panic!("Unexpected type {:?}", d),
            },
            Values::String(exp_data) => match got_column.data_type() {
                DataType::Utf8 => {
                    let arr = got_column.as_any().downcast_ref::<StringArray>().unwrap();
                    assert_eq!(&arr.iter().collect::<Vec<_>>(), exp_data);
                }
                d => panic!("Unexpected type {:?}", d),
            },
            Values::I64(exp_data) => {
                if let Some(arr) = got_column.as_any().downcast_ref::<Int64Array>() {
                    assert_eq!(arr.values(), exp_data);
                } else if let Some(arr) = got_column
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                {
                    assert_eq!(arr.values(), exp_data);
                } else {
                    panic!("Unexpected type");
                }
            }
            Values::U64(exp_data) => {
                let arr: &UInt64Array = got_column.as_any().downcast_ref::<UInt64Array>().unwrap();
                assert_eq!(arr.values(), exp_data);
            }
            Values::F64(exp_data) => {
                let arr: &Float64Array =
                    got_column.as_any().downcast_ref::<Float64Array>().unwrap();
                assert_eq!(arr.values(), exp_data);
            }
            Values::I64N(exp_data) => {
                let arr: &Int64Array = got_column.as_any().downcast_ref::<Int64Array>().unwrap();
                let got_data = (0..got_column.len())
                    .map(|i| {
                        if got_column.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i))
                        }
                    })
                    .collect::<Vec<_>>();
                assert_eq!(&got_data, exp_data);
            }
            Values::U64N(exp_data) => {
                let arr: &UInt64Array = got_column.as_any().downcast_ref::<UInt64Array>().unwrap();
                let got_data = (0..got_column.len())
                    .map(|i| {
                        if got_column.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i))
                        }
                    })
                    .collect::<Vec<_>>();
                assert_eq!(&got_data, exp_data);
            }
            Values::F64N(exp_data) => {
                let arr: &Float64Array =
                    got_column.as_any().downcast_ref::<Float64Array>().unwrap();
                let got_data = (0..got_column.len())
                    .map(|i| {
                        if got_column.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i))
                        }
                    })
                    .collect::<Vec<_>>();
                assert_eq!(&got_data, exp_data);
            }
            Values::Bool(exp_data) => {
                let arr: &BooleanArray =
                    got_column.as_any().downcast_ref::<BooleanArray>().unwrap();
                let got_data = (0..got_column.len())
                    .map(|i| {
                        if got_column.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i))
                        }
                    })
                    .collect::<Vec<_>>();
                assert_eq!(&got_data, exp_data);
            }
            Values::ByteArray(exp_data) => {
                let arr: &BinaryArray = got_column.as_any().downcast_ref::<BinaryArray>().unwrap();
                let got_data = (0..got_column.len())
                    .map(|i| {
                        if got_column.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i))
                        }
                    })
                    .collect::<Vec<_>>();
                assert_eq!(&got_data, exp_data);
            }
        }
    }

    #[test]
    fn add_remove_tables() {
        let registry = metric::Registry::new();

        let rb = gen_recordbatch();
        let mut chunk = ChunkBuilder::new("mydb", rb.schema())
            .with_metrics(Metrics::new(&registry, "mydb"))
            .with_record_batch(rb)
            .must_build();

        assert_eq!(chunk.rows(), 3);
        assert_eq!(chunk.row_groups(), 1);
        assert!(chunk.size() > 0);

        // Add a row group to the same table in the Chunk.
        let last_chunk_size = chunk.size();
        chunk.upsert_table(gen_recordbatch());

        assert_eq!(chunk.rows(), 6);
        assert_eq!(chunk.row_groups(), 2);
        assert!(chunk.size() > last_chunk_size);

        let expected_observations = vec![
            ObservationSet {
                metric_name: "read_buffer_column_allocated_bytes",
                description: "The number of bytes used by all data in the Read Buffer including allocated by unused buffers",
                kind: MetricKind::U64Gauge,
                observations: vec![
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "BT_U32-FIXED"), ("log_data_type", "i64")]), Observation::U64Gauge(192)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "FBT_U8-FIXEDN"), ("log_data_type", "f64")]), Observation::U64Gauge(906)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "FIXED"), ("log_data_type", "f64")]), Observation::U64Gauge(186)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "FIXEDN"), ("log_data_type", "bool")]), Observation::U64Gauge(672)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "RLE"), ("log_data_type", "string")]), Observation::U64Gauge(784)),
                ]
            },
            ObservationSet {
                metric_name: "read_buffer_column_raw_bytes",
                description: "The number of bytes used by all columns if they were uncompressed in the Read Buffer",
                kind: MetricKind::U64Gauge,
                observations: vec![
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "BT_U32-FIXED"), ("log_data_type", "i64"), ("null", "false")]), Observation::U64Gauge(96)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "BT_U32-FIXED"), ("log_data_type", "i64"), ("null", "true")]), Observation::U64Gauge(0)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "FBT_U8-FIXEDN"), ("log_data_type", "f64"), ("null", "false")]), Observation::U64Gauge(80)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "FBT_U8-FIXEDN"), ("log_data_type", "f64"), ("null", "true")]), Observation::U64Gauge(16)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "FIXED"), ("log_data_type", "f64"), ("null", "false")]), Observation::U64Gauge(96)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "FIXED"), ("log_data_type", "f64"), ("null", "true")]), Observation::U64Gauge(0)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "FIXEDN"), ("log_data_type", "bool"), ("null", "false")]), Observation::U64Gauge(54)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "FIXEDN"), ("log_data_type", "bool"), ("null", "true")]), Observation::U64Gauge(0)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "RLE"), ("log_data_type", "string"), ("null", "false")]), Observation::U64Gauge(216)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "RLE"), ("log_data_type", "string"), ("null", "true")]), Observation::U64Gauge(0)),
                ]
            },
            ObservationSet {
                metric_name: "read_buffer_column_required_bytes",
                description: "The number of bytes currently required to store data in the Read Buffer excluding allocated by unused buffers",
                kind: MetricKind::U64Gauge,
                observations: vec![
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "BT_U32-FIXED"), ("log_data_type", "i64")]), Observation::U64Gauge(192)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "FBT_U8-FIXEDN"), ("log_data_type", "f64")]), Observation::U64Gauge(906)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "FIXED"), ("log_data_type", "f64")]), Observation::U64Gauge(186)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "FIXEDN"), ("log_data_type", "bool")]), Observation::U64Gauge(672)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "RLE"), ("log_data_type", "string")]), Observation::U64Gauge(352)),
                ]
            },
            ObservationSet {
                metric_name: "read_buffer_column_total",
                description: "The number of columns within the Read Buffer",
                kind: MetricKind::U64Gauge,
                observations: vec![
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "BT_U32-FIXED"), ("log_data_type", "i64")]), Observation::U64Gauge(2)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "FBT_U8-FIXEDN"), ("log_data_type", "f64")]), Observation::U64Gauge(2)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "FIXED"), ("log_data_type", "f64")]), Observation::U64Gauge(2)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "FIXEDN"), ("log_data_type", "bool")]), Observation::U64Gauge(2)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "RLE"), ("log_data_type", "string")]), Observation::U64Gauge(2)),
                ]
            },
            ObservationSet {
                metric_name: "read_buffer_column_values",
                description: "The number of values within columns in the Read Buffer",
                kind: MetricKind::U64Gauge,
                observations: vec![
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "BT_U32-FIXED"), ("log_data_type", "i64"), ("null", "false")]), Observation::U64Gauge(6)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "BT_U32-FIXED"), ("log_data_type", "i64"), ("null", "true")]), Observation::U64Gauge(0)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "FBT_U8-FIXEDN"), ("log_data_type", "f64"), ("null", "false")]), Observation::U64Gauge(4)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "FBT_U8-FIXEDN"), ("log_data_type", "f64"), ("null", "true")]), Observation::U64Gauge(2)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "FIXED"), ("log_data_type", "f64"), ("null", "false")]), Observation::U64Gauge(6)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "FIXED"), ("log_data_type", "f64"), ("null", "true")]), Observation::U64Gauge(0)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "FIXEDN"), ("log_data_type", "bool"), ("null", "false")]), Observation::U64Gauge(6)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "FIXEDN"), ("log_data_type", "bool"), ("null", "true")]), Observation::U64Gauge(0)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "RLE"), ("log_data_type", "string"), ("null", "false")]), Observation::U64Gauge(6)),
                    (Attributes::from(&[("db_name", "mydb"), ("encoding", "RLE"), ("log_data_type", "string"), ("null", "true")]), Observation::U64Gauge(0)),
                ]
            },
            ObservationSet {
                metric_name: "read_buffer_row_group_total",
                description: "The number of row groups within the Read Buffer",
                kind: MetricKind::U64Gauge,
                observations: vec![
                    (Attributes::from(&[("db_name", "mydb")]), Observation::U64Gauge(2)),
                ]
            },
        ];

        let mut reporter = RawReporter::default();
        registry.report(&mut reporter);
        assert_eq!(&expected_observations, reporter.observations());

        // when the chunk is dropped the metrics are all correctly decreased
        std::mem::drop(chunk);

        let expected_observations: Vec<_> = expected_observations
            .iter()
            .map(|set| ObservationSet {
                metric_name: set.metric_name,
                description: set.description,
                kind: set.kind,
                observations: set
                    .observations
                    .iter()
                    .map(|(attributes, _)| (attributes.clone(), Observation::U64Gauge(0)))
                    .collect(),
            })
            .collect();

        let mut reporter = RawReporter::default();
        registry.report(&mut reporter);
        assert_eq!(&expected_observations, reporter.observations());
    }

    #[test]
    fn read_filter_table_schema() {
        let rb = gen_recordbatch();
        let chunk = ChunkBuilder::new("mydb", rb.schema())
            .with_record_batch(rb)
            .must_build();

        let schema = chunk.read_filter_table_schema(Selection::All).unwrap();

        let exp_schema: Arc<Schema> = SchemaBuilder::new()
            .tag("region")
            .field("counter", Float64)
            .field("active", Boolean)
            .timestamp()
            .field("sketchy_sensor", Float64)
            .build()
            .unwrap()
            .into();
        assert_eq!(Arc::new(schema), exp_schema);

        let schema = chunk
            .read_filter_table_schema(Selection::Some(&["sketchy_sensor", "counter", "region"]))
            .unwrap();

        let exp_schema: Arc<Schema> = SchemaBuilder::new()
            .field("sketchy_sensor", Float64)
            .field("counter", Float64)
            .tag("region")
            .build()
            .unwrap()
            .into();
        assert_eq!(Arc::new(schema), exp_schema);

        // Verify error handling
        assert!(matches!(
            chunk.read_filter_table_schema(Selection::Some(&["random column name"])),
            Err(Error::ColumnDoesNotExist { .. })
        ));
    }

    #[test]
    fn table_summaries() {
        use std::iter::repeat;

        let schema = SchemaBuilder::new()
            .non_null_tag("env")
            .tag("host")
            .non_null_field("temp", Float64)
            .non_null_field("counter", UInt64)
            .non_null_field("icounter", Int64)
            .non_null_field("active", Boolean)
            .non_null_field("msg", Utf8)
            .field("zf64", Float64)
            .field("zu64", UInt64)
            .field("zi64", Int64)
            .field("zbool", Boolean)
            .field("zstr", Utf8)
            .timestamp()
            .build()
            .unwrap();

        let data: Vec<ArrayRef> = vec![
            Arc::new(
                vec!["prod", "dev", "prod"]
                    .into_iter()
                    .collect::<DictionaryArray<Int32Type>>(),
            ),
            Arc::new(
                (vec![Some("host a"), None, Some("host b")] as Vec<Option<&str>>)
                    .into_iter()
                    .collect::<DictionaryArray<Int32Type>>(),
            ),
            Arc::new(Float64Array::from(vec![10.0, 30000.0, 4500.0])),
            Arc::new(UInt64Array::from(vec![1000, 3000, 5000])),
            Arc::new(Int64Array::from(vec![1000, -1000, 4000])),
            Arc::new(BooleanArray::from(vec![true, true, false])),
            Arc::new(StringArray::from(vec![
                Some("msg a"),
                Some("msg b"),
                Some("msg b"),
            ])),
            // all null columns
            Arc::new(Float64Array::from_iter(repeat(None).take(3))),
            Arc::new(UInt64Array::from_iter(repeat(None).take(3))),
            Arc::new(Int64Array::from_iter(repeat(None).take(3))),
            Arc::new(BooleanArray::from_iter(repeat(None).take(3))),
            Arc::new(StringArray::from_iter(
                repeat::<Option<String>>(None).take(3),
            )),
            // timestamp column
            Arc::new(TimestampNanosecondArray::from_vec(
                vec![11111111, 222222, 3333],
                None,
            )),
        ];

        // Add a record batch to a single partition
        let rb = RecordBatch::try_new(schema.into(), data).unwrap();
        let mut chunk_builder = ChunkBuilder::new("a_table", rb.schema());
        chunk_builder.must_push_record_batch(rb);
        let chunk = chunk_builder.must_build();

        let summary = chunk.table_summary();
        assert_eq!("a_table", summary.name);

        let column_summaries = summary.columns;
        let expected_column_summaries = vec![
            ColumnSummary {
                name: "active".into(),
                influxdb_type: Some(InfluxDbType::Field),
                stats: Statistics::Bool(StatValues::new_non_null(Some(false), Some(true), 3)),
            },
            ColumnSummary {
                name: "counter".into(),
                influxdb_type: Some(InfluxDbType::Field),
                stats: Statistics::U64(StatValues::new_non_null(Some(1000), Some(5000), 3)),
            },
            ColumnSummary {
                name: "env".into(),
                influxdb_type: Some(InfluxDbType::Tag),
                stats: Statistics::String(StatValues {
                    min: Some("dev".into()),
                    max: Some("prod".into()),
                    total_count: 3,
                    null_count: 0,
                    distinct_count: Some(NonZeroU64::new(2).unwrap()),
                }),
            },
            ColumnSummary {
                name: "host".into(),
                influxdb_type: Some(InfluxDbType::Tag),
                stats: Statistics::String(StatValues {
                    min: Some("host a".into()),
                    max: Some("host b".into()),
                    total_count: 3,
                    null_count: 1,
                    distinct_count: Some(NonZeroU64::new(3).unwrap()),
                }),
            },
            ColumnSummary {
                name: "icounter".into(),
                influxdb_type: Some(InfluxDbType::Field),
                stats: Statistics::I64(StatValues::new_non_null(Some(-1000), Some(4000), 3)),
            },
            ColumnSummary {
                name: "msg".into(),
                influxdb_type: Some(InfluxDbType::Field),
                stats: Statistics::String(StatValues {
                    min: Some("msg a".into()),
                    max: Some("msg b".into()),
                    total_count: 3,
                    null_count: 0,
                    distinct_count: Some(NonZeroU64::new(2).unwrap()),
                }),
            },
            ColumnSummary {
                name: "temp".into(),
                influxdb_type: Some(InfluxDbType::Field),
                stats: Statistics::F64(StatValues::new_non_null(Some(10.0), Some(30000.0), 3)),
            },
            ColumnSummary {
                name: "time".into(),
                influxdb_type: Some(InfluxDbType::Timestamp),
                stats: Statistics::I64(StatValues::new_non_null(Some(3333), Some(11111111), 3)),
            },
            ColumnSummary {
                name: "zbool".into(),
                influxdb_type: Some(InfluxDbType::Field),
                stats: Statistics::Bool(StatValues::new_all_null(3, None)),
            },
            ColumnSummary {
                name: "zf64".into(),
                influxdb_type: Some(InfluxDbType::Field),
                stats: Statistics::F64(StatValues::new_all_null(3, None)),
            },
            ColumnSummary {
                name: "zi64".into(),
                influxdb_type: Some(InfluxDbType::Field),
                stats: Statistics::I64(StatValues::new_all_null(3, None)),
            },
            ColumnSummary {
                name: "zstr".into(),
                influxdb_type: Some(InfluxDbType::Field),
                stats: Statistics::String(StatValues::new_all_null(3, Some(1))),
            },
            ColumnSummary {
                name: "zu64".into(),
                influxdb_type: Some(InfluxDbType::Field),
                stats: Statistics::U64(StatValues::new_all_null(3, None)),
            },
        ];

        assert_eq!(
            expected_column_summaries, column_summaries,
            "expected:\n{:#?}\n\nactual:{:#?}\n\n",
            expected_column_summaries, column_summaries
        );
    }

    fn read_filter_setup() -> Chunk {
        let schema = SchemaBuilder::new()
            .non_null_tag("env")
            .non_null_tag("region")
            .non_null_field("counter", Float64)
            .field("sketchy_sensor", Int64)
            .non_null_field("active", Boolean)
            .field("msg", Utf8)
            .field("all_null", Utf8)
            .timestamp()
            .build()
            .unwrap();

        let mut chunk_builder = ChunkBuilder::new("Coolverine", schema.clone().into());

        // Add a bunch of row groups to a single table in a single chunk
        for &i in &[100, 200, 300] {
            let data: Vec<ArrayRef> = vec![
                Arc::new(
                    vec!["us-west", "us-east", "us-west"]
                        .into_iter()
                        .collect::<DictionaryArray<Int32Type>>(),
                ),
                Arc::new(
                    vec!["west", "west", "east"]
                        .into_iter()
                        .collect::<DictionaryArray<Int32Type>>(),
                ),
                Arc::new(Float64Array::from(vec![1.2, 300.3, 4500.3])),
                Arc::new(Int64Array::from(vec![None, Some(33), Some(44)])),
                Arc::new(BooleanArray::from(vec![true, false, false])),
                Arc::new(StringArray::from(vec![
                    Some("message a"),
                    Some("message b"),
                    None,
                ])),
                Arc::new(StringArray::from(vec![None, None, None])),
                Arc::new(TimestampNanosecondArray::from_vec(
                    vec![i, 2 * i, 3 * i],
                    None,
                )),
            ];

            // Add a record batch to a single partition
            let rb = RecordBatch::try_new(schema.clone().into(), data).unwrap();
            chunk_builder.must_push_record_batch(rb);
            chunk_builder.must_snapshot_rowgroup();
        }
        chunk_builder.must_build()
    }

    #[test]
    fn read_filter() {
        // Chunk should be initialized now.
        let chunk = read_filter_setup();

        // Build the operation equivalent to the following query:
        //
        //   SELECT * FROM "table_1"
        //   WHERE "env" = 'us-west' AND
        //   "time" >= 100 AND  "time" < 205
        //
        let predicate =
            Predicate::with_time_range(&[BinaryExpr::from(("env", "=", "us-west"))], 100, 205); // filter on time

        let mut itr = chunk
            .read_filter(predicate, Selection::All, vec![])
            .unwrap();

        let exp_env_values = Values::Dictionary(vec![0], vec![Some("us-west")]);
        let exp_region_values = Values::Dictionary(vec![0], vec![Some("west")]);
        let exp_counter_values = Values::F64(vec![1.2]);
        let exp_sketchy_sensor_values = Values::I64N(vec![None]);
        let exp_active_values = Values::Bool(vec![Some(true)]);
        let exp_msg_values = Values::String(vec![Some("message a")]);

        let first_row_group = itr.next().unwrap();
        assert_rb_column_equals(&first_row_group, "env", &exp_env_values);
        assert_rb_column_equals(&first_row_group, "region", &exp_region_values);
        assert_rb_column_equals(&first_row_group, "counter", &exp_counter_values);
        assert_rb_column_equals(
            &first_row_group,
            "sketchy_sensor",
            &exp_sketchy_sensor_values,
        );
        assert_rb_column_equals(&first_row_group, "active", &exp_active_values);
        assert_rb_column_equals(&first_row_group, "msg", &exp_msg_values);
        assert_rb_column_equals(&first_row_group, "all_null", &Values::String(vec![None]));
        assert_rb_column_equals(&first_row_group, "time", &Values::I64(vec![100])); // first row from first record batch

        let second_row_group = itr.next().unwrap();
        assert_rb_column_equals(&second_row_group, "env", &exp_env_values);
        assert_rb_column_equals(&second_row_group, "region", &exp_region_values);
        assert_rb_column_equals(&second_row_group, "counter", &exp_counter_values);
        assert_rb_column_equals(
            &first_row_group,
            "sketchy_sensor",
            &exp_sketchy_sensor_values,
        );
        assert_rb_column_equals(&first_row_group, "active", &exp_active_values);
        assert_rb_column_equals(&first_row_group, "all_null", &Values::String(vec![None]));
        assert_rb_column_equals(&second_row_group, "time", &Values::I64(vec![200])); // first row from second record batch

        // No rows returned when filtering on all_null column
        let predicate = Predicate::new(vec![BinaryExpr::from(("all_null", "!=", "a string"))]);
        let mut itr = chunk
            .read_filter(predicate, Selection::All, vec![])
            .unwrap();
        assert!(itr.next().is_none());

        // Error when predicate is invalid
        let predicate =
            Predicate::with_time_range(&[BinaryExpr::from(("env", "=", 22.3))], 100, 205);
        assert!(chunk
            .read_filter(predicate, Selection::All, vec![])
            .is_err());

        // No more data
        assert!(itr.next().is_none());
    }

    #[test]
    fn read_filter_with_deletes() {
        // Chunk should be initialized now.
        let chunk = read_filter_setup();

        // Build the operation equivalent to the following query:
        //
        //   SELECT * FROM "table_1" WHERE "env" = 'us-west';
        //
        // But also assume the following delete has been applied:
        //
        // DELETE FROM "table_1" WHERE "region" = "west"
        //
        let predicate = Predicate::new(vec![BinaryExpr::from(("env", "=", "us-west"))]);
        let delete_predicates = vec![Predicate::new(vec![BinaryExpr::from((
            "region", "=", "west",
        ))])];
        let mut itr = chunk
            .read_filter(predicate, Selection::All, delete_predicates)
            .unwrap();

        let exp_env_values = Values::Dictionary(vec![0], vec![Some("us-west")]);
        let exp_region_values = Values::Dictionary(vec![0], vec![Some("east")]);
        let exp_counter_values = Values::F64(vec![4500.3]);
        let exp_sketchy_sensor_values = Values::I64N(vec![Some(44)]);
        let exp_active_values = Values::Bool(vec![Some(false)]);
        let exp_msg_values = Values::String(vec![None]);

        let first_row_group = itr.next().unwrap();
        assert_rb_column_equals(&first_row_group, "env", &exp_env_values);
        assert_rb_column_equals(&first_row_group, "region", &exp_region_values);
        assert_rb_column_equals(&first_row_group, "counter", &exp_counter_values);
        assert_rb_column_equals(
            &first_row_group,
            "sketchy_sensor",
            &exp_sketchy_sensor_values,
        );
        assert_rb_column_equals(&first_row_group, "active", &exp_active_values);
        assert_rb_column_equals(&first_row_group, "msg", &exp_msg_values);
        assert_rb_column_equals(&first_row_group, "time", &Values::I64(vec![300])); // last row from first record batch

        let second_row_group = itr.next().unwrap();
        assert_rb_column_equals(&second_row_group, "env", &exp_env_values);
        assert_rb_column_equals(&second_row_group, "region", &exp_region_values);
        assert_rb_column_equals(&second_row_group, "counter", &exp_counter_values);
        assert_rb_column_equals(
            &first_row_group,
            "sketchy_sensor",
            &exp_sketchy_sensor_values,
        );
        assert_rb_column_equals(&first_row_group, "active", &exp_active_values);
        assert_rb_column_equals(&second_row_group, "time", &Values::I64(vec![600])); // last row from second record batch

        let third_row_group = itr.next().unwrap();
        assert_rb_column_equals(&third_row_group, "env", &exp_env_values);
        assert_rb_column_equals(&third_row_group, "region", &exp_region_values);
        assert_rb_column_equals(&third_row_group, "counter", &exp_counter_values);
        assert_rb_column_equals(
            &first_row_group,
            "sketchy_sensor",
            &exp_sketchy_sensor_values,
        );
        assert_rb_column_equals(&first_row_group, "active", &exp_active_values);
        assert_rb_column_equals(&third_row_group, "time", &Values::I64(vec![900])); // last row from third record batch

        // No more data
        assert!(itr.next().is_none());

        // Error when one of the negated predicates is invalid
        let predicate = Predicate::new(vec![BinaryExpr::from(("env", "=", "us-west"))]);
        let delete_predicates = vec![
            Predicate::new(vec![BinaryExpr::from(("region", "=", "west"))]),
            Predicate::new(vec![BinaryExpr::from(("time", "=", "not a number"))]),
        ];
        assert!(chunk
            .read_filter(predicate, Selection::All, delete_predicates)
            .is_err());
    }

    #[test]
    fn could_pass_predicate() {
        let rb = gen_recordbatch();
        let mut chunk_builder = ChunkBuilder::new("table", rb.schema());
        chunk_builder.must_push_record_batch(rb);
        let chunk = chunk_builder.must_build();

        assert!(
            chunk.could_pass_predicate(Predicate::new(vec![BinaryExpr::from((
                "region", "=", "east"
            ))]))
        );
    }

    #[test]
    fn satisfies_predicate() {
        let columns = vec![
            (
                "time".to_owned(),
                ColumnType::create_time(&[1_i64, 2, 3, 4, 5, 6]),
            ),
            (
                "region".to_owned(),
                ColumnType::create_tag(&["west", "west", "east", "west", "south", "north"]),
            ),
        ];
        let rg = RowGroup::new(6, columns);

        let chunk = Chunk::new_from_row_group("table_1", rg, Metrics::new_unregistered());

        // No predicate so at least one row matches
        assert!(chunk.satisfies_predicate(&Predicate::default()));

        // at least one row satisfies the predicate
        assert!(
            chunk.satisfies_predicate(&Predicate::new(vec![BinaryExpr::from((
                "region", ">=", "west"
            ))]),)
        );

        // no rows match the predicate
        assert!(
            !chunk.satisfies_predicate(&Predicate::new(vec![BinaryExpr::from((
                "region", ">", "west"
            ))]),)
        );

        // invalid predicate so no rows can match
        assert!(
            !chunk.satisfies_predicate(&Predicate::new(vec![BinaryExpr::from((
                "region", "=", 33.2
            ))]),)
        );
    }

    fn to_set(v: &[&str]) -> BTreeSet<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn column_names() {
        let schema = SchemaBuilder::new()
            .non_null_tag("region")
            .non_null_field("counter", Float64)
            .timestamp()
            .field("sketchy_sensor", Float64)
            .build()
            .unwrap()
            .into();

        let data: Vec<ArrayRef> = vec![
            Arc::new(
                vec!["west", "west", "east"]
                    .into_iter()
                    .collect::<DictionaryArray<Int32Type>>(),
            ),
            Arc::new(Float64Array::from(vec![1.2, 3.3, 45.3])),
            Arc::new(TimestampNanosecondArray::from_vec(
                vec![11111111, 222222, 3333],
                None,
            )),
            Arc::new(Float64Array::from(vec![Some(11.0), None, Some(12.0)])),
        ];

        // Create the chunk with the above table
        let rb = RecordBatch::try_new(schema, data).unwrap();
        let mut chunk_builder = ChunkBuilder::new("Utopia", rb.schema());
        chunk_builder.must_push_record_batch(rb);
        let chunk = chunk_builder.must_build();

        let result = chunk
            .column_names(
                Predicate::default(),
                vec![],
                Selection::All,
                BTreeSet::new(),
            )
            .unwrap();

        assert_eq!(
            result,
            to_set(&["counter", "region", "sketchy_sensor", "time"])
        );

        // Testing predicates
        let result = chunk
            .column_names(
                Predicate::new(vec![BinaryExpr::from(("time", "=", 222222_i64))]),
                vec![],
                Selection::All,
                BTreeSet::new(),
            )
            .unwrap();

        // sketchy_sensor won't be returned because it has a NULL value for the
        // only matching row.
        assert_eq!(result, to_set(&["counter", "region", "time"]));

        // Error when invalid predicate provided.
        assert!(matches!(
            chunk.column_names(
                Predicate::new(vec![BinaryExpr::from(("time", "=", "not a number"))]),
                vec![],
                Selection::Some(&["region", "env"]),
                BTreeSet::new()
            ),
            Err(Error::TableError { .. })
        ));
    }

    #[test]
    fn column_names_with_deletes() {
        let schema = SchemaBuilder::new()
            .non_null_tag("region")
            .non_null_field("counter", Float64)
            .timestamp()
            .field("sketchy_sensor", Float64)
            .build()
            .unwrap()
            .into();

        let data: Vec<ArrayRef> = vec![
            Arc::new(
                vec!["west", "west", "east"]
                    .into_iter()
                    .collect::<DictionaryArray<Int32Type>>(),
            ),
            Arc::new(Float64Array::from(vec![1.2, 3.3, 45.3])),
            Arc::new(TimestampNanosecondArray::from_vec(
                vec![11111111, 222222, 3333],
                None,
            )),
            Arc::new(Float64Array::from(vec![Some(11.0), None, Some(12.0)])),
        ];

        // Create the chunk with the above table
        let rb = RecordBatch::try_new(schema, data).unwrap();
        let mut chunk_builder = ChunkBuilder::new("Utopia", rb.schema());
        chunk_builder.must_push_record_batch(rb);
        let chunk = chunk_builder.must_build();

        let result = chunk
            .column_names(
                Predicate::default(),
                vec![Predicate::default()], // all rows deleted
                Selection::All,
                BTreeSet::new(),
            )
            .unwrap();
        assert_eq!(result, to_set(&[]));

        let result = chunk
            .column_names(
                Predicate::default(),
                vec![Predicate::new(vec![BinaryExpr::from((
                    "region", "!=", "west",
                ))])], // all rows deleted
                Selection::All,
                BTreeSet::new(),
            )
            .unwrap();
        assert_eq!(
            result,
            to_set(&["counter", "region", "sketchy_sensor", "time"])
        );

        let result = chunk
            .column_names(
                Predicate::default(),
                vec![Predicate::new(vec![BinaryExpr::from((
                    "sketchy_sensor",
                    ">",
                    10.0,
                ))])], // deletes all rows with non-null sketchy sensor values
                Selection::All,
                BTreeSet::new(),
            )
            .unwrap();
        assert_eq!(result, to_set(&["counter", "region", "time"]));
    }

    fn to_map(arr: Vec<(&str, &[&str])>) -> BTreeMap<String, BTreeSet<String>> {
        arr.iter()
            .map(|(k, values)| {
                (
                    k.to_string(),
                    values
                        .iter()
                        .map(|s| s.to_string())
                        .collect::<BTreeSet<_>>(),
                )
            })
            .collect::<BTreeMap<_, _>>()
    }

    #[test]
    fn column_values() {
        let schema = SchemaBuilder::new()
            .non_null_tag("region")
            .non_null_tag("env")
            .timestamp()
            .build()
            .unwrap()
            .into();

        let data: Vec<ArrayRef> = vec![
            Arc::new(
                vec!["north", "south", "east"]
                    .into_iter()
                    .collect::<DictionaryArray<Int32Type>>(),
            ),
            Arc::new(
                vec![Some("prod"), None, Some("stag")]
                    .into_iter()
                    .collect::<DictionaryArray<Int32Type>>(),
            ),
            Arc::new(TimestampNanosecondArray::from_vec(
                vec![11111111, 222222, 3333],
                None,
            )),
        ];

        // Create the chunk with the above table
        let rb = RecordBatch::try_new(schema, data).unwrap();
        let mut chunk_builder = ChunkBuilder::new("my_table", rb.schema());
        chunk_builder.must_push_record_batch(rb);
        let chunk = chunk_builder.must_build();

        let result = chunk
            .column_values(
                Predicate::default(),
                Selection::Some(&["region", "env"]),
                BTreeMap::new(),
            )
            .unwrap();

        assert_eq!(
            result,
            to_map(vec![
                ("region", &["north", "south", "east"]),
                ("env", &["prod", "stag"])
            ])
        );

        // With a predicate
        let result = chunk
            .column_values(
                Predicate::new(vec![
                    BinaryExpr::from(("time", ">=", 20_i64)),
                    BinaryExpr::from(("time", "<=", 3333_i64)),
                ]),
                Selection::Some(&["region", "env"]),
                BTreeMap::new(),
            )
            .unwrap();

        assert_eq!(
            result,
            to_map(vec![
                ("region", &["east"]),
                ("env", &["stag"]) // column_values returns non-null values.
            ])
        );

        // Error when All column selection provided.
        assert!(matches!(
            chunk.column_values(Predicate::default(), Selection::All, BTreeMap::new()),
            Err(Error::UnsupportedOperation { .. })
        ));

        // Error when invalid predicate provided.
        assert!(matches!(
            chunk.column_values(
                Predicate::new(vec![BinaryExpr::from(("time", "=", "not a number"))]),
                Selection::Some(&["region", "env"]),
                BTreeMap::new()
            ),
            Err(Error::TableError { .. })
        ));
    }

    #[test]
    fn chunk_builder() {
        // test row group configuration
        let rb = gen_recordbatch();
        let chunk = ChunkBuilder::new("table_a", rb.schema())
            .with_record_batch(rb)
            .must_build();

        assert_eq!(chunk.row_groups(), 1);
        assert_eq!(chunk.rows(), 3);

        let rb = gen_recordbatch();
        let mut builder = ChunkBuilder::new("table_a", rb.schema()).set_row_group_min_size(3);
        builder.must_push_record_batch(rb);
        builder.must_push_record_batch(gen_recordbatch());
        builder.must_push_record_batch(gen_recordbatch());

        let chunk = builder.must_build();
        assert_eq!(chunk.table.name(), "table_a");
        assert_eq!(chunk.row_groups(), 3);
        assert_eq!(chunk.rows(), 9);

        // when the chunk is empty an error is returned on build
        let rb = gen_recordbatch();
        let builder = ChunkBuilder::new("table_a", rb.schema());
        assert!(builder.build().is_err());

        // empty record batches are not stored for snapshotting
        let rb = gen_recordbatch();
        let mut builder = ChunkBuilder::new("table_a", rb.schema());
        assert!(builder
            .push_record_batch(RecordBatch::new_empty(rb.schema()))
            .is_ok());
        assert!(builder.is_empty());
    }
}
