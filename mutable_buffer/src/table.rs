use generated_types::wal as wb;
use query::{
    exec::{field::FieldColumns, make_schema_pivot, SeriesSetPlan},
    func::selectors::{selector_first, selector_last, selector_max, selector_min, SelectorOutput},
    func::window::make_window_bound_expr,
    group_by::{Aggregate, WindowDuration},
};
use tracing::debug;

use std::{collections::BTreeSet, collections::HashMap, sync::Arc};

use crate::{
    chunk::ChunkIdSet,
    chunk::{Chunk, ChunkPredicate},
    column,
    column::Column,
    dictionary::{Dictionary, Error as DictionaryError},
};
use data_types::{partition_metadata::Column as ColumnStats, TIME_COLUMN_NAME};
use snafu::{OptionExt, ResultExt, Snafu};

use arrow_deps::{
    arrow,
    arrow::{
        array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder},
        datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
        record_batch::RecordBatch,
    },
    datafusion::{
        self,
        logical_plan::{Expr, LogicalPlan, LogicalPlanBuilder},
        prelude::*,
    },
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Tag value ID {} not found in dictionary of chunk {}", value, chunk))]
    TagValueIdNotFoundInDictionary {
        value: u32,
        chunk: String,
        source: DictionaryError,
    },

    #[snafu(display("Column error on column {}: {}", column, source))]
    ColumnError {
        column: String,
        source: column::Error,
    },

    #[snafu(display(
        "Internal error: Expected column {} to be type {} but was {}",
        column_id,
        expected_column_type,
        actual_column_type
    ))]
    InternalColumnTypeMismatch {
        column_id: u32,
        expected_column_type: String,
        actual_column_type: String,
    },

    #[snafu(display("Internal error: unexpected aggregate request for None aggregate",))]
    InternalUnexpectedNoneAggregate {},

    #[snafu(display("Internal error: aggregate {:?} is not a selector", agg))]
    InternalAggregateNotSelector { agg: Aggregate },

    #[snafu(display(
        "Column name '{}' not found in dictionary of chunk {}",
        column_name,
        chunk
    ))]
    ColumnNameNotFoundInDictionary {
        column_name: String,
        chunk: String,
        source: DictionaryError,
    },

    #[snafu(display(
        "Internal: Column id '{}' not found in dictionary of chunk {}",
        column_id,
        chunk
    ))]
    ColumnIdNotFoundInDictionary {
        column_id: u32,
        chunk: String,
        source: DictionaryError,
    },

    #[snafu(display("Error building plan: {}", source))]
    BuildingPlan {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("arrow conversion error: {}", source))]
    ArrowError { source: arrow::error::ArrowError },

    #[snafu(display(
        "No index entry found for column {} with id {}",
        column_name,
        column_id
    ))]
    InternalNoColumnInIndex { column_name: String, column_id: u32 },

    #[snafu(display("Error creating column from wal for column {}: {}", column, source))]
    CreatingFromWal {
        column: u32,
        source: crate::column::Error,
    },

    #[snafu(display("Error evaluating column predicate for column {}: {}", column, source))]
    ColumnPredicateEvaluation {
        column: u32,
        source: crate::column::Error,
    },

    #[snafu(display("Row insert to table {} missing column name", table))]
    ColumnNameNotInRow { table: u32 },

    #[snafu(display(
        "Group column '{}' not found in tag columns: {}",
        column_name,
        all_tag_column_names
    ))]
    GroupColumnNotFound {
        column_name: String,
        all_tag_column_names: String,
    },

    #[snafu(display("Error creating aggregate expression:  {}", source))]
    CreatingAggregates { source: query::group_by::Error },

    #[snafu(display("Duplicate group column '{}'", column_name))]
    DuplicateGroupColumn { column_name: String },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Table {
    /// Name of the table as a u32 in the chunk dictionary
    pub id: u32,

    /// Maps column name (as a u32 in the chunk dictionary) to an index in
    /// self.columns
    pub column_id_to_index: HashMap<u32, usize>,

    /// Actual column storage
    pub columns: Vec<Column>,
}

type ArcStringVec = Vec<Arc<String>>;

impl Table {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            column_id_to_index: HashMap::new(),
            columns: Vec::new(),
        }
    }

    fn append_row(
        &mut self,
        dictionary: &mut Dictionary,
        values: &flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<wb::Value<'_>>>,
    ) -> Result<()> {
        let row_count = self.row_count();

        // insert new columns and validate existing ones
        for value in values {
            let column_name = value
                .column()
                .context(ColumnNameNotInRow { table: self.id })?;
            let column_id = dictionary.lookup_value_or_insert(column_name);

            let column = match self.column_id_to_index.get(&column_id) {
                Some(idx) => &mut self.columns[*idx],
                None => {
                    // Add the column and make all values for existing rows None
                    let idx = self.columns.len();
                    self.column_id_to_index.insert(column_id, idx);
                    self.columns.push(
                        Column::with_value(dictionary, row_count, value)
                            .context(CreatingFromWal { column: column_id })?,
                    );

                    continue;
                }
            };

            column.push(dictionary, &value).context(ColumnError {
                column: column_name,
            })?;
        }

        // make sure all the columns are of the same length
        for col in &mut self.columns {
            col.push_none_if_len_equal(row_count);
        }

        Ok(())
    }

    pub fn row_count(&self) -> usize {
        self.columns.first().map_or(0, |v| v.len())
    }

    /// Returns a reference to the specified column
    fn column(&self, column_id: u32) -> Result<&Column> {
        Ok(self
            .column_id_to_index
            .get(&column_id)
            .map(|&column_index| &self.columns[column_index])
            .expect("invalid column id"))
    }

    /// Returns a reference to the specified column as a slice of
    /// i64s. Errors if the type is not i64
    pub fn column_i64(&self, column_id: u32) -> Result<&[Option<i64>]> {
        let column = self.column(column_id)?;
        match column {
            Column::I64(vals, _) => Ok(vals),
            _ => InternalColumnTypeMismatch {
                column_id,
                expected_column_type: "i64",
                actual_column_type: column.type_description(),
            }
            .fail(),
        }
    }

    pub fn append_rows(
        &mut self,
        dictionary: &mut Dictionary,
        rows: &flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<wb::Row<'_>>>,
    ) -> Result<()> {
        for row in rows {
            if let Some(values) = row.values() {
                self.append_row(dictionary, &values)?;
            }
        }

        Ok(())
    }

    /// Creates and adds a datafuson filtering expression, if any out of the
    /// combination of predicate and timestamp. Returns the builder
    fn add_datafusion_predicate(
        plan_builder: LogicalPlanBuilder,
        chunk_predicate: &ChunkPredicate,
    ) -> Result<LogicalPlanBuilder> {
        match chunk_predicate.filter_expr() {
            Some(df_predicate) => plan_builder.filter(df_predicate).context(BuildingPlan),
            None => Ok(plan_builder),
        }
    }

    /// Creates a DataFusion LogicalPlan that returns column *names* as a
    /// single column of Strings
    ///
    /// The created plan looks like:
    ///
    ///  Extension(PivotSchema)
    ///    (Optional Projection to get rid of time)
    ///        Filter(predicate)
    ///          InMemoryScan
    pub fn tag_column_names_plan(
        &self,
        chunk_predicate: &ChunkPredicate,
        chunk: &Chunk,
    ) -> Result<LogicalPlan> {
        let need_time_column = chunk_predicate.range.is_some();

        let time_column_id = chunk_predicate.time_column_id;

        // figure out the tag columns
        let requested_columns_with_index = self
            .column_id_to_index
            .iter()
            .filter_map(|(&column_id, &column_index)| {
                // keep tag columns and the timestamp column, if needed to evaluate a timestamp
                // predicate
                let need_column = if let Column::Tag(_, _) = self.columns[column_index] {
                    true
                } else {
                    need_time_column && column_id == time_column_id
                };

                if need_column {
                    // the id came out of our map, so it should always be valid
                    let column_name = chunk.dictionary.lookup_id(column_id).unwrap();
                    Some((column_name, column_index))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // TODO avoid materializing here
        let data = self.to_arrow_impl(chunk, &requested_columns_with_index)?;

        let schema = data.schema();

        let projection = None;

        let plan_builder = LogicalPlanBuilder::scan_memory(vec![vec![data]], schema, projection)
            .context(BuildingPlan)?;

        let plan_builder = Self::add_datafusion_predicate(plan_builder, chunk_predicate)?;

        // add optional selection to remove time column
        let plan_builder = if !need_time_column {
            plan_builder
        } else {
            // Create expressions for all columns except time
            let select_exprs = requested_columns_with_index
                .iter()
                .filter_map(|&(column_name, _)| {
                    if column_name != TIME_COLUMN_NAME {
                        Some(col(column_name))
                    } else {
                        None
                    }
                })
                .collect();

            plan_builder.project(select_exprs).context(BuildingPlan)?
        };

        let plan = plan_builder.build().context(BuildingPlan)?;

        // And finally pivot the plan
        let plan = make_schema_pivot(plan);

        debug!(
            "Created column_name plan for table '{}':\n{}",
            chunk.dictionary.lookup_id(self.id).unwrap(),
            plan.display_indent_schema()
        );

        Ok(plan)
    }

    /// Creates a DataFusion LogicalPlan that returns column *values* as a
    /// single column of Strings
    ///
    /// The created plan looks like:
    ///
    ///    Projection
    ///        Filter(predicate)
    ///          InMemoryScan
    pub fn tag_values_plan(
        &self,
        column_name: &str,
        chunk_predicate: &ChunkPredicate,
        chunk: &Chunk,
    ) -> Result<LogicalPlan> {
        // Scan and Filter
        let plan_builder = self.scan_with_predicates(chunk_predicate, chunk)?;

        let select_exprs = vec![col(column_name)];

        plan_builder
            .project(select_exprs)
            .context(BuildingPlan)?
            .build()
            .context(BuildingPlan)
    }

    /// Creates a SeriesSet plan that produces an output table with rows that
    /// match the predicate
    ///
    /// The output looks like:
    /// (tag_col1, tag_col2, ... field1, field2, ... timestamp)
    ///
    /// The order of the tag_columns is orderd by name.
    ///
    /// The data is sorted on tag_col1, tag_col2, ...) so that all
    /// rows for a particular series (groups where all tags are the
    /// same) occur together in the plan
    pub fn series_set_plan(
        &self,
        chunk_predicate: &ChunkPredicate,
        chunk: &Chunk,
    ) -> Result<SeriesSetPlan> {
        self.series_set_plan_impl(chunk_predicate, None, chunk)
    }

    /// Creates the plans for computing series set, ensuring that
    /// prefix_columns, if any, are the prefix of the ordering.
    ///
    /// The created plan looks like:
    ///
    ///    Projection (select the columns columns needed)
    ///      Order by (tag_columns, timestamp_column)
    ///        Filter(predicate)
    ///          InMemoryScan
    pub fn series_set_plan_impl(
        &self,
        chunk_predicate: &ChunkPredicate,
        prefix_columns: Option<&[String]>,
        chunk: &Chunk,
    ) -> Result<SeriesSetPlan> {
        let (mut tag_columns, field_columns) =
            self.tag_and_field_column_names(chunk_predicate, chunk)?;

        // reorder tag_columns to have the prefix columns, if requested
        if let Some(prefix_columns) = prefix_columns {
            tag_columns = reorder_prefix(prefix_columns, tag_columns)?;
        }

        // TODO avoid materializing all the columns here (ideally we
        // would use a data source and then let DataFusion prune out
        // column references during its optimizer phase).
        let plan_builder = self.scan_with_predicates(chunk_predicate, chunk)?;

        let mut sort_exprs = Vec::new();
        sort_exprs.extend(tag_columns.iter().map(|c| c.into_sort_expr()));
        sort_exprs.push(TIME_COLUMN_NAME.into_sort_expr());

        // Order by
        let plan_builder = plan_builder.sort(sort_exprs).context(BuildingPlan)?;

        // Selection
        let mut select_exprs = Vec::new();
        select_exprs.extend(tag_columns.iter().map(|c| c.into_expr()));
        select_exprs.extend(field_columns.iter().map(|c| c.into_expr()));
        select_exprs.push(TIME_COLUMN_NAME.into_expr());

        let plan_builder = plan_builder.project(select_exprs).context(BuildingPlan)?;

        // and finally create the plan
        let plan = plan_builder.build().context(BuildingPlan)?;

        Ok(SeriesSetPlan::new_from_shared_timestamp(
            self.table_name(chunk),
            plan,
            tag_columns,
            field_columns,
        ))
    }

    /// Returns a LogialPlannBuilder which scans all columns in this
    /// Table and has applied any predicates in `chunk_predicate`
    ///
    ///  Filter(predicate)
    ///    InMemoryScan
    fn scan_with_predicates(
        &self,
        chunk_predicate: &ChunkPredicate,
        chunk: &Chunk,
    ) -> Result<LogicalPlanBuilder> {
        // TODO avoid materializing all the columns here (ideally
        // DataFusion can prune some of them out)
        let data = self.all_to_arrow(chunk)?;

        let schema = data.schema();

        let projection = None;

        // And build the plan from the bottom up
        let plan_builder = LogicalPlanBuilder::scan_memory(vec![vec![data]], schema, projection)
            .context(BuildingPlan)?;

        // Filtering
        Self::add_datafusion_predicate(plan_builder, chunk_predicate)
    }

    /// Look up this table's name as a string
    fn table_name(&self, chunk: &Chunk) -> Arc<String> {
        // I wonder if all this string creation will be too slow?
        let table_name = chunk
            .dictionary
            .lookup_id(self.id)
            .expect("looking up table name in dictionary")
            .to_string();

        Arc::new(table_name)
    }

    /// Creates a GroupedSeriesSet plan that produces an output table
    /// with rows that match the predicate. See documentation on
    /// series_set_plan_impl for more details.
    pub fn grouped_series_set_plan(
        &self,
        chunk_predicate: &ChunkPredicate,
        agg: Aggregate,
        group_columns: &[String],
        chunk: &Chunk,
    ) -> Result<SeriesSetPlan> {
        let num_prefix_tag_group_columns = group_columns.len();

        let plan = if let Aggregate::None = agg {
            self.series_set_plan_impl(chunk_predicate, Some(&group_columns), chunk)?
        } else {
            self.aggregate_series_set_plan(chunk_predicate, agg, group_columns, chunk)?
        };

        Ok(plan.grouped(num_prefix_tag_group_columns))
    }

    /// Creates a GroupedSeriesSet plan that produces an output table
    /// with rows grouped by an aggregate function. Note that we still
    /// group by all tags (so group within series) and the
    /// group_columns define the order of the result
    ///
    /// Equivalent to this SQL query for 'aggregates': sum, count, mean
    /// SELECT
    ///   tag1...tagN
    ///   agg_function(_val1) as _value1
    ///   ...
    ///   agg_function(_valN) as _valueN
    ///   agg_function(time) as time
    /// GROUP BY
    ///   group_key1, group_key2, remaining tags,
    /// ORDER BY
    ///   group_key1, group_key2, remaining tags
    ///
    /// Equivalent to this SQL query for 'selector' functions: first, last, min,
    /// max as they can have different values of the timestamp column
    ///
    /// SELECT
    ///   tag1...tagN
    ///   agg_function(_val1) as _value1
    ///   agg_function(time) as time1
    ///   ..
    ///   agg_function(_valN) as _valueN
    ///   agg_function(time) as timeN
    /// GROUP BY
    ///   group_key1, group_key2, remaining tags,
    /// ORDER BY
    ///   group_key1, group_key2, remaining tags
    ///
    /// The created plan looks like:
    ///
    ///  OrderBy(gby cols; agg)
    ///     GroupBy(gby cols, aggs, time cols)
    ///       Filter(predicate)
    ///          InMemoryScan
    pub fn aggregate_series_set_plan(
        &self,
        chunk_predicate: &ChunkPredicate,
        agg: Aggregate,
        group_columns: &[String],
        chunk: &Chunk,
    ) -> Result<SeriesSetPlan> {
        let (tag_columns, field_columns) =
            self.tag_and_field_column_names(chunk_predicate, chunk)?;

        // order the tag columns so that the group keys come first (we will group and
        // order in the same order)
        let tag_columns = reorder_prefix(group_columns, tag_columns)?;

        // Scan and Filter
        let plan_builder = self.scan_with_predicates(chunk_predicate, chunk)?;

        // Group by all tag columns
        let group_exprs = tag_columns
            .iter()
            .map(|tag_name| col(tag_name.as_ref()))
            .collect::<Vec<_>>();

        let AggExprs {
            agg_exprs,
            field_columns,
        } = AggExprs::new(agg, field_columns, |col_name| {
            let index = self.column_index(chunk, col_name)?;
            Ok(self.columns[index].data_type())
        })?;

        let sort_exprs = group_exprs
            .iter()
            .map(|expr| expr.into_sort_expr())
            .collect::<Vec<_>>();

        let plan_builder = plan_builder
            .aggregate(group_exprs, agg_exprs)
            .context(BuildingPlan)?
            .sort(sort_exprs)
            .context(BuildingPlan)?;

        // and finally create the plan
        let plan = plan_builder.build().context(BuildingPlan)?;

        Ok(SeriesSetPlan::new(
            self.table_name(chunk),
            plan,
            tag_columns,
            field_columns,
        ))
    }

    /// Creates a GroupedSeriesSet plan that produces an output table with rows
    /// that are grouped by window defintions
    ///
    /// The order of the tag_columns
    ///
    /// The data is sorted on tag_col1, tag_col2, ...) so that all
    /// rows for a particular series (groups where all tags are the
    /// same) occur together in the plan
    ///
    /// Equivalent to this SQL query
    ///
    /// SELECT tag1, ... tagN,
    ///   window_bound(time, every, offset) as time,
    ///   agg_function1(field), as field_name
    /// FROM measurement
    /// GROUP BY
    ///   tag1, ... tagN,
    ///   window_bound(time, every, offset) as time,
    /// ORDER BY
    ///   tag1, ... tagN,
    ///   window_bound(time, every, offset) as time
    ///
    /// The created plan looks like:
    ///
    ///  OrderBy(gby: tag columns, window_function; agg: aggregate(field)
    ///      GroupBy(gby: tag columns, window_function; agg: aggregate(field)
    ///        Filter(predicate)
    ///          InMemoryScan
    pub fn window_grouped_series_set_plan(
        &self,
        chunk_predicate: &ChunkPredicate,
        agg: Aggregate,
        every: &WindowDuration,
        offset: &WindowDuration,
        chunk: &Chunk,
    ) -> Result<SeriesSetPlan> {
        let (tag_columns, field_columns) =
            self.tag_and_field_column_names(chunk_predicate, chunk)?;

        // Scan and Filter
        let plan_builder = self.scan_with_predicates(chunk_predicate, chunk)?;

        // Group by all tag columns and the window bounds
        let mut group_exprs = tag_columns
            .iter()
            .map(|tag_name| col(tag_name.as_ref()))
            .collect::<Vec<_>>();
        // add window_bound() call
        let window_bound =
            make_window_bound_expr(col(TIME_COLUMN_NAME), every, offset).alias(TIME_COLUMN_NAME);
        group_exprs.push(window_bound);

        // aggregate each field
        let agg_exprs = field_columns
            .iter()
            .map(|field_name| make_agg_expr(agg, field_name))
            .collect::<Result<Vec<_>>>()?;

        // sort by the group by expressions as well
        let sort_exprs = group_exprs
            .iter()
            .map(|expr| expr.into_sort_expr())
            .collect::<Vec<_>>();

        let plan_builder = plan_builder
            .aggregate(group_exprs, agg_exprs)
            .context(BuildingPlan)?
            .sort(sort_exprs)
            .context(BuildingPlan)?;

        // and finally create the plan
        let plan = plan_builder.build().context(BuildingPlan)?;

        Ok(SeriesSetPlan::new_from_shared_timestamp(
            self.table_name(chunk),
            plan,
            tag_columns,
            field_columns,
        ))
    }

    /// Creates a plan that produces an output table with rows that
    /// match the predicate for all fields in the table.
    ///
    /// The output looks like (field0, field1, ..., time)
    ///
    /// The data is not sorted in any particular order
    ///
    /// The created plan looks like:
    ///
    ///    Projection (select the field columns needed)
    ///        Filter(predicate) [optional]
    ///          InMemoryScan
    pub fn field_names_plan(
        &self,
        chunk_predicate: &ChunkPredicate,
        chunk: &Chunk,
    ) -> Result<LogicalPlan> {
        // Scan and Filter
        let plan_builder = self.scan_with_predicates(chunk_predicate, chunk)?;

        // Selection
        let select_exprs = self
            .field_and_time_column_names(chunk_predicate, chunk)
            .into_iter()
            .map(|c| c.into_expr())
            .collect::<Vec<_>>();

        let plan_builder = plan_builder.project(select_exprs).context(BuildingPlan)?;

        // and finally create the plan
        plan_builder.build().context(BuildingPlan)
    }

    // Returns (tag_columns, field_columns) vectors with the names of
    // all tag and field columns, respectively, after any predicates
    // have been applied. The vectors are sorted by lexically by name.
    fn tag_and_field_column_names(
        &self,
        chunk_predicate: &ChunkPredicate,
        chunk: &Chunk,
    ) -> Result<(ArcStringVec, ArcStringVec)> {
        let mut tag_columns = Vec::with_capacity(self.column_id_to_index.len());
        let mut field_columns = Vec::with_capacity(self.column_id_to_index.len());

        for (&column_id, &column_index) in &self.column_id_to_index {
            let column_name = chunk
                .dictionary
                .lookup_id(column_id)
                .expect("Find column name in dictionary");

            if column_name != TIME_COLUMN_NAME {
                let column_name = Arc::new(column_name.to_string());

                match self.columns[column_index] {
                    Column::Tag(_, _) => tag_columns.push(column_name),
                    _ => {
                        if chunk_predicate.should_include_field(column_id) {
                            field_columns.push(column_name)
                        }
                    }
                }
            }
        }

        // tag columns are always sorted by name (aka sorted by tag
        // key) in the output schema, so ensure the columns are sorted
        // (the select exprs)
        tag_columns.sort();

        // Sort the field columns too so that the output always comes
        // out in a predictable order
        field_columns.sort();

        Ok((tag_columns, field_columns))
    }

    // Returns (field_columns and time) in sorted order
    fn field_and_time_column_names(
        &self,
        chunk_predicate: &ChunkPredicate,
        chunk: &Chunk,
    ) -> ArcStringVec {
        let mut field_columns = self
            .column_id_to_index
            .iter()
            .filter_map(|(&column_id, &column_index)| {
                match self.columns[column_index] {
                    Column::Tag(_, _) => None, // skip tags
                    _ => {
                        if chunk_predicate.should_include_field(column_id)
                            || chunk_predicate.is_time_column(column_id)
                        {
                            let column_name = chunk
                                .dictionary
                                .lookup_id(column_id)
                                .expect("Find column name in dictionary");
                            Some(Arc::new(column_name.to_string()))
                        } else {
                            None
                        }
                    }
                }
            })
            .collect::<Vec<_>>();

        // Sort the field columns too so that the output always comes
        // out in a predictable order
        field_columns.sort();

        field_columns
    }

    /// Converts this table to an arrow record batch.
    ///
    /// If requested_columns is empty (`[]`), retrieve all columns in
    /// the table
    pub fn to_arrow(&self, chunk: &Chunk, requested_columns: &[&str]) -> Result<RecordBatch> {
        if requested_columns.is_empty() {
            self.all_to_arrow(chunk)
        } else {
            let columns_with_index = self.column_names_with_index(chunk, requested_columns)?;

            self.to_arrow_impl(chunk, &columns_with_index)
        }
    }

    fn column_index(&self, chunk: &Chunk, column_name: &str) -> Result<usize> {
        let column_id =
            chunk
                .dictionary
                .lookup_value(column_name)
                .context(ColumnNameNotFoundInDictionary {
                    column_name,
                    chunk: &chunk.key,
                })?;

        self.column_id_to_index
            .get(&column_id)
            .copied()
            .context(InternalNoColumnInIndex {
                column_name,
                column_id,
            })
    }

    /// Returns (name, index) pairs for all named columns
    fn column_names_with_index<'a>(
        &self,
        chunk: &Chunk,
        columns: &[&'a str],
    ) -> Result<Vec<(&'a str, usize)>> {
        columns
            .iter()
            .map(|&column_name| {
                let column_index = self.column_index(chunk, column_name)?;

                Ok((column_name, column_index))
            })
            .collect()
    }

    /// Convert all columns to an arrow record batch
    pub fn all_to_arrow(&self, chunk: &Chunk) -> Result<RecordBatch> {
        let mut requested_columns_with_index = self
            .column_id_to_index
            .iter()
            .map(|(&column_id, &column_index)| {
                let column_name = chunk.dictionary.lookup_id(column_id).context(
                    ColumnIdNotFoundInDictionary {
                        column_id,
                        chunk: &chunk.key,
                    },
                )?;
                Ok((column_name, column_index))
            })
            .collect::<Result<Vec<_>>>()?;

        requested_columns_with_index.sort_by(|(a, _), (b, _)| a.cmp(b));

        self.to_arrow_impl(chunk, &requested_columns_with_index)
    }

    /// Converts this table to an arrow record batch,
    ///
    /// requested columns with index are tuples of column_name, column_index
    pub fn to_arrow_impl(
        &self,
        chunk: &Chunk,
        requested_columns_with_index: &[(&str, usize)],
    ) -> Result<RecordBatch> {
        let mut fields = Vec::with_capacity(requested_columns_with_index.len());
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(requested_columns_with_index.len());

        for &(column_name, column_index) in requested_columns_with_index.iter() {
            let arrow_col: ArrayRef = match &self.columns[column_index] {
                Column::String(vals, _) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Utf8, true));
                    let mut builder = StringBuilder::with_capacity(vals.len(), vals.len() * 10);

                    for v in vals {
                        match v {
                            None => builder.append_null(),
                            Some(s) => builder.append_value(s),
                        }
                        .context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                }
                Column::Tag(vals, _) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Utf8, true));
                    let mut builder = StringBuilder::with_capacity(vals.len(), vals.len() * 10);

                    for v in vals {
                        match v {
                            None => builder.append_null(),
                            Some(value_id) => {
                                let tag_value = chunk.dictionary.lookup_id(*value_id).context(
                                    TagValueIdNotFoundInDictionary {
                                        value: *value_id,
                                        chunk: &chunk.key,
                                    },
                                )?;
                                builder.append_value(tag_value)
                            }
                        }
                        .context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                }
                Column::F64(vals, _) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Float64, true));
                    let mut builder = Float64Builder::new(vals.len());

                    for v in vals {
                        builder.append_option(*v).context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                }
                Column::I64(vals, _) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Int64, true));
                    let mut builder = Int64Builder::new(vals.len());

                    for v in vals {
                        builder.append_option(*v).context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                }
                Column::Bool(vals, _) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Boolean, true));
                    let mut builder = BooleanBuilder::new(vals.len());

                    for v in vals {
                        builder.append_option(*v).context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                }
            };

            columns.push(arrow_col);
        }

        let schema = ArrowSchema::new(fields);

        RecordBatch::try_new(Arc::new(schema), columns).context(ArrowError {})
    }

    /// returns true if any row in this table could possible match the
    /// predicate. true does not mean any rows will *actually* match,
    /// just that the entire table can not be ruled out.
    ///
    /// false means that no rows in this table could possibly match
    pub fn could_match_predicate(&self, chunk_predicate: &ChunkPredicate) -> Result<bool> {
        Ok(
            self.matches_column_name_predicate(chunk_predicate.field_name_predicate.as_ref())
                && self.matches_table_name_predicate(chunk_predicate.table_name_predicate.as_ref())
                && self.matches_timestamp_predicate(chunk_predicate)?
                && self.has_columns(chunk_predicate.required_columns.as_ref()),
        )
    }

    /// Returns true if the table contains any of the field columns
    /// requested or there are no specific fields requested.
    fn matches_column_name_predicate(&self, column_selection: Option<&BTreeSet<u32>>) -> bool {
        match column_selection {
            Some(column_selection) => {
                self.column_id_to_index
                    .iter()
                    .any(|(column_id, &column_index)| {
                        column_selection.contains(column_id) && !self.columns[column_index].is_tag()
                    })
            }
            None => true, // no specific selection
        }
    }

    fn matches_table_name_predicate(&self, table_name_predicate: Option<&BTreeSet<u32>>) -> bool {
        match table_name_predicate {
            Some(table_name_predicate) => table_name_predicate.contains(&self.id),
            None => true, // no table predicate
        }
    }

    /// returns true if there are any timestamps in this table that
    /// fall within the timestamp range
    fn matches_timestamp_predicate(&self, chunk_predicate: &ChunkPredicate) -> Result<bool> {
        match &chunk_predicate.range {
            None => Ok(true),
            Some(range) => {
                let time_column_id = chunk_predicate.time_column_id;
                let time_column = self.column(time_column_id)?;
                time_column.has_i64_range(range.start, range.end).context(
                    ColumnPredicateEvaluation {
                        column: time_column_id,
                    },
                )
            }
        }
    }

    /// returns true if no columns are specified, or the table has all
    /// columns specified
    fn has_columns(&self, columns: Option<&ChunkIdSet>) -> bool {
        if let Some(columns) = columns {
            match columns {
                ChunkIdSet::AtLeastOneMissing => return false,
                ChunkIdSet::Present(symbols) => {
                    for symbol in symbols {
                        if !self.column_id_to_index.contains_key(symbol) {
                            return false;
                        }
                    }
                }
            }
        }
        true
    }

    /// returns true if there are any rows in column that are non-null
    /// and within the timestamp range specified by pred
    pub fn column_matches_predicate<T>(
        &self,
        column: &[Option<T>],
        chunk_predicate: &ChunkPredicate,
    ) -> Result<bool> {
        match chunk_predicate.range {
            None => Ok(true),
            Some(range) => {
                let time_column_id = chunk_predicate.time_column_id;
                let time_column = self.column(time_column_id)?;
                time_column
                    .has_non_null_i64_range(column, range.start, range.end)
                    .context(ColumnPredicateEvaluation {
                        column: time_column_id,
                    })
            }
        }
    }

    pub fn stats(&self) -> Vec<ColumnStats> {
        self.columns
            .iter()
            .map(|c| match c {
                Column::F64(_, stats) => ColumnStats::F64(stats.clone()),
                Column::I64(_, stats) => ColumnStats::I64(stats.clone()),
                Column::Bool(_, stats) => ColumnStats::Bool(stats.clone()),
                Column::String(_, stats) | Column::Tag(_, stats) => {
                    ColumnStats::String(stats.clone())
                }
            })
            .collect()
    }
}

/// Reorders tag_columns so that its prefix matches exactly
/// prefix_columns. Returns an error if there are duplicates, or other
/// untoward inputs
fn reorder_prefix(
    prefix_columns: &[String],
    tag_columns: Vec<Arc<String>>,
) -> Result<Vec<Arc<String>>> {
    // tag_used_set[i[ is true if we have used the value in tag_columns[i]
    let mut tag_used_set = vec![false; tag_columns.len()];

    // Note that this is an O(N^2) algorithm. We are assuming the
    // number of tag columns is reasonably small

    // map from prefix_column[idx] -> index in tag_columns
    let prefix_map = prefix_columns
        .iter()
        .map(|pc| {
            let found_location = tag_columns
                .iter()
                .enumerate()
                .find(|(_, c)| pc == c.as_ref());

            if let Some((index, _)) = found_location {
                if tag_used_set[index] {
                    DuplicateGroupColumn { column_name: pc }.fail()
                } else {
                    tag_used_set[index] = true;
                    Ok(index)
                }
            } else {
                GroupColumnNotFound {
                    column_name: pc,
                    all_tag_column_names: tag_columns
                        .iter()
                        .map(|s| s.as_ref() as &str)
                        .collect::<Vec<_>>()
                        .as_slice()
                        .join(", "),
                }
                .fail()
            }
        })
        .collect::<Result<Vec<_>>>()?;

    let mut new_tag_columns = prefix_map
        .iter()
        .map(|&i| tag_columns[i].clone())
        .collect::<Vec<_>>();

    new_tag_columns.extend(tag_columns.into_iter().enumerate().filter_map(|(i, c)| {
        // already used in prefix
        if tag_used_set[i] {
            None
        } else {
            Some(c)
        }
    }));

    Ok(new_tag_columns)
}

/// Traits to help creating DataFuson expressions from strings
trait IntoExpr {
    /// Creates a DataFuson expr
    fn into_expr(&self) -> Expr;

    /// creates a DataFusion SortExpr
    fn into_sort_expr(&self) -> Expr {
        Expr::Sort {
            expr: Box::new(self.into_expr()),
            asc: true, // Sort ASCENDING
            nulls_first: true,
        }
    }
}

impl IntoExpr for Arc<String> {
    fn into_expr(&self) -> Expr {
        col(self.as_ref())
    }
}

impl IntoExpr for str {
    fn into_expr(&self) -> Expr {
        col(self)
    }
}

impl IntoExpr for Expr {
    fn into_expr(&self) -> Expr {
        self.clone()
    }
}

struct AggExprs {
    agg_exprs: Vec<Expr>,
    field_columns: FieldColumns,
}

/// Creates aggregate and sort expressions for an aggregate plan,
/// according to the rules explained on
/// `aggregate_series_set_plan`
impl AggExprs {
    /// Create the appropriate aggregate expressions, based on the type of
    fn new<F>(agg: Aggregate, field_columns: Vec<Arc<String>>, field_type_lookup: F) -> Result<Self>
    where
        F: Fn(&str) -> Result<ArrowDataType>,
    {
        match agg {
            Aggregate::Sum | Aggregate::Count | Aggregate::Mean => {
                //  agg_function(_val1) as _value1
                //  ...
                //  agg_function(_valN) as _valueN
                //  agg_function(time) as time

                let mut agg_exprs = field_columns
                    .iter()
                    .map(|field_name| make_agg_expr(agg, field_name.as_ref()))
                    .collect::<Result<Vec<_>>>()?;

                agg_exprs.push(make_agg_expr(agg, TIME_COLUMN_NAME)?);

                let field_columns = field_columns.into();
                Ok(Self {
                    agg_exprs,
                    field_columns,
                })
            }
            Aggregate::First | Aggregate::Last | Aggregate::Min | Aggregate::Max => {
                //   agg_function(_val1) as _value1
                //   agg_function(time) as time1
                //   ..
                //   agg_function(_valN) as _valueN
                //   agg_function(time) as timeN

                // might be nice to use a more functional style here
                let mut agg_exprs = Vec::with_capacity(field_columns.len() * 2);
                let mut field_list = Vec::with_capacity(field_columns.len());

                for field_name in &field_columns {
                    let field_type = field_type_lookup(field_name.as_ref())?;

                    agg_exprs.push(make_selector_expr(
                        agg,
                        SelectorOutput::Value,
                        field_name.as_ref(),
                        &field_type,
                        field_name.as_ref(),
                    )?);

                    let time_column_name = Arc::new(format!("{}_{}", TIME_COLUMN_NAME, field_name));

                    agg_exprs.push(make_selector_expr(
                        agg,
                        SelectorOutput::Time,
                        field_name.as_ref(),
                        &field_type,
                        time_column_name.as_ref(),
                    )?);

                    field_list.push((
                        field_name.clone(), // value name
                        time_column_name,
                    ));
                }

                let field_columns = field_list.into();
                Ok(Self {
                    agg_exprs,
                    field_columns,
                })
            }
            Aggregate::None => InternalUnexpectedNoneAggregate.fail(),
        }
    }
}

/// Creates a DataFusion expression suitable for calculating an aggregate:
///
/// equivalent to `CAST agg(field) as field`
fn make_agg_expr(agg: Aggregate, field_name: &str) -> Result<Expr> {
    agg.to_datafusion_expr(col(field_name))
        .context(CreatingAggregates)
        .map(|agg| agg.alias(field_name))
}

/// Creates a DataFusion expression suitable for calculating the time
/// part of a selector:
///
/// equivalent to `CAST selector_time(field) as column_name`
fn make_selector_expr(
    agg: Aggregate,
    output: SelectorOutput,
    field_name: &str,
    data_type: &ArrowDataType,
    column_name: &str,
) -> Result<Expr> {
    let uda = match agg {
        Aggregate::First => selector_first(data_type, output),
        Aggregate::Last => selector_last(data_type, output),
        Aggregate::Min => selector_min(data_type, output),
        Aggregate::Max => selector_max(data_type, output),
        _ => return InternalAggregateNotSelector { agg }.fail(),
    };
    Ok(uda
        .call(vec![col(field_name), col(TIME_COLUMN_NAME)])
        .alias(column_name))
}

#[cfg(test)]
mod tests {

    use arrow::util::pretty::pretty_format_batches;
    use data_types::data::split_lines_into_write_entry_partitions;
    use influxdb_line_protocol::{parse_lines, ParsedLine};
    use query::{
        exec::Executor,
        predicate::{Predicate, PredicateBuilder},
    };
    use test_helpers::str_vec_to_arc_vec;

    use super::*;

    #[test]
    fn test_has_columns() {
        let mut chunk = Chunk::new("dummy_chunk_key", 42);
        let dictionary = &mut chunk.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
        ];

        write_lines_to_table(&mut table, dictionary, lp_lines);

        let state_symbol = dictionary.id("state").unwrap();
        let new_symbol = dictionary.lookup_value_or_insert("not_a_columns");

        assert!(table.has_columns(None));

        let pred = ChunkIdSet::AtLeastOneMissing;
        assert!(!table.has_columns(Some(&pred)));

        let set = BTreeSet::<u32>::new();
        let pred = ChunkIdSet::Present(set);
        assert!(table.has_columns(Some(&pred)));

        let mut set = BTreeSet::new();
        set.insert(state_symbol);
        let pred = ChunkIdSet::Present(set);
        assert!(table.has_columns(Some(&pred)));

        let mut set = BTreeSet::new();
        set.insert(new_symbol);
        let pred = ChunkIdSet::Present(set);
        assert!(!table.has_columns(Some(&pred)));

        let mut set = BTreeSet::new();
        set.insert(state_symbol);
        set.insert(new_symbol);
        let pred = ChunkIdSet::Present(set);
        assert!(!table.has_columns(Some(&pred)));
    }

    #[test]
    fn test_matches_table_name_predicate() {
        let mut chunk = Chunk::new("dummy_chunk_key", 42);
        let dictionary = &mut chunk.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("h2o"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
        ];
        write_lines_to_table(&mut table, dictionary, lp_lines);

        let h2o_symbol = dictionary.id("h2o").unwrap();

        assert!(table.matches_table_name_predicate(None));

        let set = BTreeSet::new();
        assert!(!table.matches_table_name_predicate(Some(&set)));

        let mut set = BTreeSet::new();
        set.insert(h2o_symbol);
        assert!(table.matches_table_name_predicate(Some(&set)));

        // Some symbol that is not the same as h2o_symbol
        assert_ne!(37377, h2o_symbol);
        let mut set = BTreeSet::new();
        set.insert(37377);
        assert!(!table.matches_table_name_predicate(Some(&set)));
    }

    #[test]
    fn test_matches_column_name_predicate() {
        let mut chunk = Chunk::new("dummy_chunk_key", 42);
        let dictionary = &mut chunk.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("h2o"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4,awesomeness=1000 100",
            "h2o,state=MA,city=Boston temp=72.4,awesomeness=2000 250",
        ];
        write_lines_to_table(&mut table, dictionary, lp_lines);

        let state_symbol = dictionary.id("state").unwrap();
        let temp_symbol = dictionary.id("temp").unwrap();
        let awesomeness_symbol = dictionary.id("awesomeness").unwrap();

        assert!(table.matches_column_name_predicate(None));

        let set = BTreeSet::new();
        assert!(!table.matches_column_name_predicate(Some(&set)));

        // tag columns should not count
        let mut set = BTreeSet::new();
        set.insert(state_symbol);
        assert!(!table.matches_column_name_predicate(Some(&set)));

        let mut set = BTreeSet::new();
        set.insert(temp_symbol);
        assert!(table.matches_column_name_predicate(Some(&set)));

        let mut set = BTreeSet::new();
        set.insert(temp_symbol);
        set.insert(awesomeness_symbol);
        assert!(table.matches_column_name_predicate(Some(&set)));

        let mut set = BTreeSet::new();
        set.insert(temp_symbol);
        set.insert(awesomeness_symbol);
        set.insert(1337); // some other symbol, but that is ok
        assert!(table.matches_column_name_predicate(Some(&set)));

        let mut set = BTreeSet::new();
        set.insert(1337);
        assert!(!table.matches_column_name_predicate(Some(&set)));
    }

    #[tokio::test]
    async fn test_series_set_plan() {
        let mut chunk = Chunk::new("dummy_chunk_key", 42);
        let dictionary = &mut chunk.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
            "h2o,state=CA,city=LA temp=90.0 200",
            "h2o,state=CA,city=LA temp=90.0 350",
        ];

        write_lines_to_table(&mut table, dictionary, lp_lines);

        let predicate = PredicateBuilder::default().build();
        let chunk_predicate = chunk.compile_predicate(&predicate).unwrap();
        let series_set_plan = table
            .series_set_plan(&chunk_predicate, &chunk)
            .expect("creating the series set plan");

        assert_eq!(series_set_plan.table_name.as_ref(), "table_name");
        assert_eq!(
            series_set_plan.tag_columns,
            *str_vec_to_arc_vec(&["city", "state"])
        );
        assert_eq!(series_set_plan.field_columns, vec!["temp"].into());

        // run the created plan, ensuring the output is as expected
        let results = run_plan(series_set_plan.plan).await;

        let expected = vec![
            "+--------+-------+------+------+",
            "| city   | state | temp | time |",
            "+--------+-------+------+------+",
            "| Boston | MA    | 70.4 | 100  |",
            "| Boston | MA    | 72.4 | 250  |",
            "| LA     | CA    | 90   | 200  |",
            "| LA     | CA    | 90   | 350  |",
            "+--------+-------+------+------+",
        ];
        assert_eq!(expected, results, "expected output");
    }

    #[tokio::test]
    async fn test_series_set_plan_order() {
        // test that the columns and rows come out in the right order (tags then
        // timestamp)

        let mut chunk = Chunk::new("dummy_chunk_key", 42);
        let dictionary = &mut chunk.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            "h2o,zz_tag=A,state=MA,city=Kingston temp=70.1 800",
            "h2o,state=MA,city=Kingston,zz_tag=B temp=70.2 100",
            "h2o,state=CA,city=Boston temp=70.3 250",
            "h2o,state=MA,city=Boston,zz_tag=A temp=70.4 1000",
            "h2o,state=MA,city=Boston temp=70.5,other=5.0 250",
        ];

        write_lines_to_table(&mut table, dictionary, lp_lines);

        let predicate = PredicateBuilder::default().build();
        let chunk_predicate = chunk.compile_predicate(&predicate).unwrap();
        let series_set_plan = table
            .series_set_plan(&chunk_predicate, &chunk)
            .expect("creating the series set plan");

        assert_eq!(series_set_plan.table_name.as_ref(), "table_name");
        assert_eq!(
            series_set_plan.tag_columns,
            *str_vec_to_arc_vec(&["city", "state", "zz_tag"])
        );
        assert_eq!(series_set_plan.field_columns, vec!["other", "temp"].into(),);

        // run the created plan, ensuring the output is as expected
        let results = run_plan(series_set_plan.plan).await;

        let expected = vec![
            "+----------+-------+--------+-------+------+------+",
            "| city     | state | zz_tag | other | temp | time |",
            "+----------+-------+--------+-------+------+------+",
            "| Boston   | CA    |        |       | 70.3 | 250  |",
            "| Boston   | MA    |        | 5     | 70.5 | 250  |",
            "| Boston   | MA    | A      |       | 70.4 | 1000 |",
            "| Kingston | MA    | A      |       | 70.1 | 800  |",
            "| Kingston | MA    | B      |       | 70.2 | 100  |",
            "+----------+-------+--------+-------+------+------+",
        ];

        assert_eq!(expected, results, "expected output");
    }

    #[tokio::test]
    async fn test_series_set_plan_filter() {
        // test that filters are applied reasonably

        let mut chunk = Chunk::new("dummy_chunk_key", 42);
        let dictionary = &mut chunk.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
            "h2o,state=CA,city=LA temp=90.0 200",
            "h2o,state=CA,city=LA temp=90.0 350",
        ];

        write_lines_to_table(&mut table, dictionary, lp_lines);

        let predicate = PredicateBuilder::default()
            .add_expr(col("city").eq(lit("LA")))
            .timestamp_range(190, 210)
            .build();

        let chunk_predicate = chunk.compile_predicate(&predicate).unwrap();

        let series_set_plan = table
            .series_set_plan(&chunk_predicate, &chunk)
            .expect("creating the series set plan");

        assert_eq!(series_set_plan.table_name.as_ref(), "table_name");
        assert_eq!(
            series_set_plan.tag_columns,
            *str_vec_to_arc_vec(&["city", "state"])
        );
        assert_eq!(series_set_plan.field_columns, vec!["temp"].into(),);

        // run the created plan, ensuring the output is as expected
        let results = run_plan(series_set_plan.plan).await;

        let expected = vec![
            "+------+-------+------+------+",
            "| city | state | temp | time |",
            "+------+-------+------+------+",
            "| LA   | CA    | 90   | 200  |",
            "+------+-------+------+------+",
        ];

        assert_eq!(expected, results, "expected output");
    }

    #[tokio::test]
    async fn test_grouped_series_set_plan_none() {
        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
            "h2o,state=CA,city=LA temp=90.0 200",
            "h2o,state=CA,city=LA temp=90.0 350",
        ];

        let mut fixture = TableFixture::new(lp_lines);

        let predicate = PredicateBuilder::default()
            .add_expr(col("city").eq(lit("LA")))
            .timestamp_range(190, 210)
            .build();

        // run the created plan, ensuring the output is as expected
        let results = fixture
            .grouped_series_set(predicate, Aggregate::Sum, &["state"])
            .await;

        let expected = vec![
            "+-------+------+------+------+",
            "| state | city | temp | time |",
            "+-------+------+------+------+",
            "| CA    | LA   | 90   | 200  |",
            "+-------+------+------+------+",
        ];

        assert_eq!(expected, results, "expected output");
    }

    #[tokio::test]
    async fn test_grouped_series_set_plan_sum() {
        let lp_lines = vec![
            "h2o,state=MA,city=Cambridge temp=80 50",
            "h2o,state=MA,city=Cambridge temp=81 100",
            "h2o,state=MA,city=Cambridge temp=82 200",
            "h2o,state=MA,city=Boston temp=70 300",
            "h2o,state=MA,city=Boston temp=71 400",
            "h2o,state=CA,city=LA temp=90,humidity=10 500",
            "h2o,state=CA,city=LA temp=91,humidity=11 600",
        ];

        let mut fixture = TableFixture::new(lp_lines);

        let predicate = PredicateBuilder::default()
            // city=Boston OR city=Cambridge (filters out LA rows)
            .add_expr(
                col("city")
                    .eq(lit("Boston"))
                    .or(col("city").eq(lit("Cambridge"))),
            )
            // fiter out first Cambridge row
            .timestamp_range(100, 1000)
            .build();

        // run the created plan, ensuring the output is as expected
        let results = fixture
            .grouped_series_set(predicate, Aggregate::Sum, &["state"])
            .await;

        // The null field (after predicates) are not sent as series
        // Note order of city key (boston --> cambridge)
        let expected = vec![
            "+-------+-----------+----------+------+------+",
            "| state | city      | humidity | temp | time |",
            "+-------+-----------+----------+------+------+",
            "| MA    | Boston    |          | 141  | 700  |",
            "| MA    | Cambridge |          | 163  | 300  |",
            "+-------+-----------+----------+------+------+",
        ];

        assert_eq!(expected, results, "expected output");
    }

    #[tokio::test]
    async fn test_grouped_series_set_plan_count() {
        let lp_lines = vec![
            "h2o,state=MA,city=Cambridge temp=80 50",
            "h2o,state=MA,city=Cambridge temp=81 100",
            "h2o,state=MA,city=Cambridge temp=82 200",
            "h2o,state=MA,city=Boston temp=70 300",
            "h2o,state=MA,city=Boston temp=71 400",
            "h2o,state=CA,city=LA temp=90,humidity=10 500",
            "h2o,state=CA,city=LA temp=91,humidity=11 600",
        ];

        let mut fixture = TableFixture::new(lp_lines);

        let predicate = PredicateBuilder::default()
            // city=Boston OR city=Cambridge (filters out LA rows)
            .add_expr(
                col("city")
                    .eq(lit("Boston"))
                    .or(col("city").eq(lit("Cambridge"))),
            )
            // fiter out first Cambridge row
            .timestamp_range(100, 1000)
            .build();

        // run the created plan, ensuring the output is as expected
        let results = fixture
            .grouped_series_set(predicate, Aggregate::Count, &["state"])
            .await;

        // The null field (after predicates) are not sent as series
        let expected = vec![
            "+-------+-----------+----------+------+------+",
            "| state | city      | humidity | temp | time |",
            "+-------+-----------+----------+------+------+",
            "| MA    | Boston    | 0        | 2    | 2    |",
            "| MA    | Cambridge | 0        | 2    | 2    |",
            "+-------+-----------+----------+------+------+",
        ];

        assert_eq!(expected, results, "expected output");
    }

    #[tokio::test]
    async fn test_grouped_series_set_plan_mean() {
        let lp_lines = vec![
            "h2o,state=MA,city=Cambridge temp=80 50",
            "h2o,state=MA,city=Cambridge temp=81 100",
            "h2o,state=MA,city=Cambridge temp=82 200",
            "h2o,state=MA,city=Boston temp=70 300",
            "h2o,state=MA,city=Boston temp=71 400",
            "h2o,state=CA,city=LA temp=90,humidity=10 500",
            "h2o,state=CA,city=LA temp=91,humidity=11 600",
        ];

        let mut fixture = TableFixture::new(lp_lines);

        let predicate = PredicateBuilder::default()
            // city=Boston OR city=Cambridge (filters out LA rows)
            .add_expr(
                col("city")
                    .eq(lit("Boston"))
                    .or(col("city").eq(lit("Cambridge"))),
            )
            // fiter out first Cambridge row
            .timestamp_range(100, 1000)
            .build();

        // run the created plan, ensuring the output is as expected
        let results = fixture
            .grouped_series_set(predicate, Aggregate::Mean, &["state"])
            .await;

        // The null field (after predicates) are not sent as series
        let expected = vec![
            "+-------+-----------+----------+------+------+",
            "| state | city      | humidity | temp | time |",
            "+-------+-----------+----------+------+------+",
            "| MA    | Boston    |          | 70.5 | 350  |",
            "| MA    | Cambridge |          | 81.5 | 150  |",
            "+-------+-----------+----------+------+------+",
        ];

        assert_eq!(expected, results, "expected output");
    }

    #[tokio::test]
    async fn test_grouped_series_set_plan_first() {
        let lp_lines = vec![
            "h2o,state=MA,city=Cambridge f=8.0,i=8i,b=true,s=\"d\" 1000",
            "h2o,state=MA,city=Cambridge f=7.0,i=7i,b=true,s=\"c\" 2000",
            "h2o,state=MA,city=Cambridge f=6.0,i=6i,b=false,s=\"b\" 3000",
            "h2o,state=MA,city=Cambridge f=5.0,i=5i,b=false,s=\"a\" 4000",
        ];

        let mut fixture = TableFixture::new(lp_lines);

        let predicate = PredicateBuilder::default()
            // fiter out first row (ts 1000)
            .timestamp_range(1001, 4001)
            .build();

        // run the created plan, ensuring the output is as expected
        let results = fixture
            .grouped_series_set(predicate, Aggregate::First, &["state"])
            .await;

        let expected = vec![
            "+-------+-----------+------+--------+---+--------+---+--------+---+--------+",
            "| state | city      | b    | time_b | f | time_f | i | time_i | s | time_s |",
            "+-------+-----------+------+--------+---+--------+---+--------+---+--------+",
            "| MA    | Cambridge | true | 2000   | 7 | 2000   | 7 | 2000   | c | 2000   |",
            "+-------+-----------+------+--------+---+--------+---+--------+---+--------+",
        ];

        assert_eq!(expected, results, "expected output");
    }

    #[tokio::test]
    async fn test_grouped_series_set_plan_last() {
        let lp_lines = vec![
            "h2o,state=MA,city=Cambridge f=8.0,i=8i,b=true,s=\"d\" 1000",
            "h2o,state=MA,city=Cambridge f=7.0,i=7i,b=true,s=\"c\" 2000",
            "h2o,state=MA,city=Cambridge f=6.0,i=6i,b=false,s=\"b\" 3000",
            "h2o,state=MA,city=Cambridge f=5.0,i=5i,b=false,s=\"a\" 4000",
        ];

        let mut fixture = TableFixture::new(lp_lines);

        let predicate = PredicateBuilder::default()
            // fiter out last row (ts 4000)
            .timestamp_range(100, 3999)
            .build();

        // run the created plan, ensuring the output is as expected
        let results = fixture
            .grouped_series_set(predicate, Aggregate::Last, &["state"])
            .await;

        let expected = vec![
            "+-------+-----------+-------+--------+---+--------+---+--------+---+--------+",
            "| state | city      | b     | time_b | f | time_f | i | time_i | s | time_s |",
            "+-------+-----------+-------+--------+---+--------+---+--------+---+--------+",
            "| MA    | Cambridge | false | 3000   | 6 | 3000   | 6 | 3000   | b | 3000   |",
            "+-------+-----------+-------+--------+---+--------+---+--------+---+--------+",
        ];

        assert_eq!(expected, results, "expected output");
    }

    #[tokio::test]
    async fn test_grouped_series_set_plan_min() {
        let lp_lines = vec![
            "h2o,state=MA,city=Cambridge f=8.0,i=8i,b=false,s=\"c\" 1000",
            "h2o,state=MA,city=Cambridge f=7.0,i=7i,b=false,s=\"a\" 2000",
            "h2o,state=MA,city=Cambridge f=6.0,i=6i,b=true,s=\"z\" 3000",
            "h2o,state=MA,city=Cambridge f=5.0,i=5i,b=true,s=\"c\" 4000",
        ];

        let mut fixture = TableFixture::new(lp_lines);

        let predicate = PredicateBuilder::default()
            // fiter out last row (ts 4000)
            .timestamp_range(100, 3999)
            .build();

        // run the created plan, ensuring the output is as expected
        let results = fixture
            .grouped_series_set(predicate, Aggregate::Min, &["state"])
            .await;

        let expected = vec![
            "+-------+-----------+-------+--------+---+--------+---+--------+---+--------+",
            "| state | city      | b     | time_b | f | time_f | i | time_i | s | time_s |",
            "+-------+-----------+-------+--------+---+--------+---+--------+---+--------+",
            "| MA    | Cambridge | false | 1000   | 6 | 3000   | 6 | 3000   | a | 2000   |",
            "+-------+-----------+-------+--------+---+--------+---+--------+---+--------+",
        ];

        assert_eq!(expected, results, "expected output");
    }

    #[tokio::test]
    async fn test_grouped_series_set_plan_max() {
        let lp_lines = vec![
            "h2o,state=MA,city=Cambridge f=8.0,i=8i,b=true,s=\"c\" 1000",
            "h2o,state=MA,city=Cambridge f=7.0,i=7i,b=false,s=\"d\" 2000",
            "h2o,state=MA,city=Cambridge f=6.0,i=6i,b=true,s=\"a\" 3000",
            "h2o,state=MA,city=Cambridge f=5.0,i=5i,b=true,s=\"z\" 4000",
        ];

        let mut fixture = TableFixture::new(lp_lines);

        let predicate = PredicateBuilder::default()
            // fiter out first row (ts 1000)
            .timestamp_range(1001, 4001)
            .build();

        // run the created plan, ensuring the output is as expected
        let results = fixture
            .grouped_series_set(predicate, Aggregate::Max, &["state"])
            .await;

        let expected = vec![
            "+-------+-----------+------+--------+---+--------+---+--------+---+--------+",
            "| state | city      | b    | time_b | f | time_f | i | time_i | s | time_s |",
            "+-------+-----------+------+--------+---+--------+---+--------+---+--------+",
            "| MA    | Cambridge | true | 3000   | 7 | 2000   | 7 | 2000   | z | 4000   |",
            "+-------+-----------+------+--------+---+--------+---+--------+---+--------+",
        ];

        assert_eq!(expected, results, "expected output");
    }

    #[tokio::test]
    async fn test_grouped_series_set_plan_group_by_keys() {
        let lp_lines = vec![
            "h2o,state=MA,city=Cambridge temp=80 50",
            "h2o,state=MA,city=Cambridge temp=81 100",
            "h2o,state=MA,city=Cambridge temp=82 200",
            "h2o,state=MA,city=Boston temp=70 300",
            "h2o,state=MA,city=Boston temp=71 400",
            "h2o,state=CA,city=LA temp=90,humidity=10 500",
            "h2o,state=CA,city=LA temp=91,humidity=11 600",
        ];

        let mut fixture = TableFixture::new(lp_lines);

        // no predicate
        let predicate = PredicateBuilder::default().build();

        // check that group_by state, city results in the right output ordering
        let group_keys = ["state", "city"];
        let results = fixture
            .grouped_series_set(predicate.clone(), Aggregate::Sum, &group_keys)
            .await;

        let expected = vec![
            "+-------+-----------+----------+------+------+",
            "| state | city      | humidity | temp | time |",
            "+-------+-----------+----------+------+------+",
            "| CA    | LA        | 21       | 181  | 1100 |",
            "| MA    | Boston    |          | 141  | 700  |",
            "| MA    | Cambridge |          | 243  | 350  |",
            "+-------+-----------+----------+------+------+",
        ];
        assert_eq!(expected, results, "expected output");

        // Test with alternate group key order
        let group_keys = ["city", "state"];
        let results = fixture
            .grouped_series_set(predicate, Aggregate::Sum, &group_keys)
            .await;

        let expected = vec![
            "+-----------+-------+----------+------+------+",
            "| city      | state | humidity | temp | time |",
            "+-----------+-------+----------+------+------+",
            "| Boston    | MA    |          | 141  | 700  |",
            "| Cambridge | MA    |          | 243  | 350  |",
            "| LA        | CA    | 21       | 181  | 1100 |",
            "+-----------+-------+----------+------+------+",
        ];
        assert_eq!(expected, results, "expected output");
    }

    #[tokio::test]
    async fn test_grouped_window_series_set_plan_nanoseconds() {
        let mut chunk = Chunk::new("dummy_chunk_key", 42);
        let dictionary = &mut chunk.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.0 100",
            "h2o,state=MA,city=Boston temp=71.0 200",
            "h2o,state=MA,city=Boston temp=72.0 300",
            "h2o,state=MA,city=Boston temp=73.0 400",
            "h2o,state=MA,city=Boston temp=74.0 500",
            "h2o,state=MA,city=Cambridge temp=80.0 100",
            "h2o,state=MA,city=Cambridge temp=81.0 200",
            "h2o,state=MA,city=Cambridge temp=82.0 300",
            "h2o,state=MA,city=Cambridge temp=83.0 400",
            "h2o,state=MA,city=Cambridge temp=84.0 500",
            "h2o,state=CA,city=LA temp=90.0 100",
            "h2o,state=CA,city=LA temp=91.0 200",
            "h2o,state=CA,city=LA temp=92.0 300",
            "h2o,state=CA,city=LA temp=93.0 400",
            "h2o,state=CA,city=LA temp=94.0 500",
        ];

        write_lines_to_table(&mut table, dictionary, lp_lines);

        let predicate = PredicateBuilder::default()
            // city=Boston or city=LA
            .add_expr(col("city").eq(lit("Boston")).or(col("city").eq(lit("LA"))))
            .timestamp_range(100, 450)
            .build();
        let chunk_predicate = chunk.compile_predicate(&predicate).unwrap();

        let agg = Aggregate::Mean;
        let every = WindowDuration::from_nanoseconds(200);
        let offset = WindowDuration::from_nanoseconds(0);

        let plan = table
            .window_grouped_series_set_plan(&chunk_predicate, agg, &every, &offset, &chunk)
            .expect("creating the grouped_series set plan");

        assert_eq!(plan.tag_columns, *str_vec_to_arc_vec(&["city", "state"]));
        assert_eq!(plan.field_columns, vec!["temp"].into());

        // run the created plan, ensuring the output is as expected
        let results = run_plan(plan.plan).await;

        // note the name of the field is "temp" even though it is the average
        let expected = vec![
            "+--------+-------+------+------+",
            "| city   | state | time | temp |",
            "+--------+-------+------+------+",
            "| Boston | MA    | 200  | 70   |",
            "| Boston | MA    | 400  | 71.5 |",
            "| Boston | MA    | 600  | 73   |",
            "| LA     | CA    | 200  | 90   |",
            "| LA     | CA    | 400  | 91.5 |",
            "| LA     | CA    | 600  | 93   |",
            "+--------+-------+------+------+",
        ];

        assert_eq!(expected, results, "expected output");
    }

    #[tokio::test]
    async fn test_grouped_window_series_set_plan_months() {
        let mut chunk = Chunk::new("dummy_chunk_key", 42);
        let dictionary = &mut chunk.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.0 1583020800000000000", // 2020-03-01T00:00:00Z
            "h2o,state=MA,city=Boston temp=71.0 1583107920000000000", // 2020-03-02T00:12:00Z
            "h2o,state=MA,city=Boston temp=72.0 1585699200000000000", // 2020-04-01T00:00:00Z
            "h2o,state=MA,city=Boston temp=73.0 1585785600000000000", // 2020-04-02T00:00:00Z
        ];

        write_lines_to_table(&mut table, dictionary, lp_lines);

        let predicate = PredicateBuilder::default().build();
        let chunk_predicate = chunk.compile_predicate(&predicate).unwrap();

        let agg = Aggregate::Mean;
        let every = WindowDuration::from_months(1, false);
        let offset = WindowDuration::from_months(0, false);

        let plan = table
            .window_grouped_series_set_plan(&chunk_predicate, agg, &every, &offset, &chunk)
            .expect("creating the grouped_series set plan");

        assert_eq!(plan.tag_columns, *str_vec_to_arc_vec(&["city", "state"]));
        assert_eq!(plan.field_columns, vec!["temp"].into());

        // run the created plan, ensuring the output is as expected
        let results = run_plan(plan.plan).await;

        // note the name of the field is "temp" even though it is the average
        let expected = vec![
            "+--------+-------+---------------------+------+",
            "| city   | state | time                | temp |",
            "+--------+-------+---------------------+------+",
            "| Boston | MA    | 1585699200000000000 | 70.5 |",
            "| Boston | MA    | 1588291200000000000 | 72.5 |",
            "+--------+-------+---------------------+------+",
        ];

        assert_eq!(expected, results, "expected output");
    }

    #[tokio::test]
    async fn test_field_name_plan() {
        let mut chunk = Chunk::new("dummy_chunk_key", 42);
        let dictionary = &mut chunk.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            // Order this so field3 comes before field2
            // (and thus the columns need to get reordered)
            "h2o,tag1=foo,tag2=bar field1=70.6,field3=2 100",
            "h2o,tag1=foo,tag2=bar field1=70.4,field2=\"ss\" 100",
            "h2o,tag1=foo,tag2=bar field1=70.5,field2=\"ss\" 100",
            "h2o,tag1=foo,tag2=bar field1=70.6,field4=true 1000",
        ];

        write_lines_to_table(&mut table, dictionary, lp_lines);

        let predicate = PredicateBuilder::default().timestamp_range(0, 200).build();

        let chunk_predicate = chunk.compile_predicate(&predicate).unwrap();

        let field_names_set_plan = table
            .field_names_plan(&chunk_predicate, &chunk)
            .expect("creating the field_name plan");

        // run the created plan, ensuring the output is as expected
        let results = run_plan(field_names_set_plan).await;

        let expected = vec![
            "+--------+--------+--------+--------+------+",
            "| field1 | field2 | field3 | field4 | time |",
            "+--------+--------+--------+--------+------+",
            "| 70.6   |        | 2      |        | 100  |",
            "| 70.4   | ss     |        |        | 100  |",
            "| 70.5   | ss     |        |        | 100  |",
            "+--------+--------+--------+--------+------+",
        ];

        assert_eq!(expected, results, "expected output");
    }

    #[test]
    fn test_reorder_prefix() {
        assert_eq!(reorder_prefix_ok(&[], &[]), &[] as &[&str]);

        assert_eq!(reorder_prefix_ok(&[], &["one"]), &["one"]);
        assert_eq!(reorder_prefix_ok(&["one"], &["one"]), &["one"]);

        assert_eq!(reorder_prefix_ok(&[], &["one", "two"]), &["one", "two"]);
        assert_eq!(
            reorder_prefix_ok(&["one"], &["one", "two"]),
            &["one", "two"]
        );
        assert_eq!(
            reorder_prefix_ok(&["two"], &["one", "two"]),
            &["two", "one"]
        );
        assert_eq!(
            reorder_prefix_ok(&["two", "one"], &["one", "two"]),
            &["two", "one"]
        );

        assert_eq!(
            reorder_prefix_ok(&[], &["one", "two", "three"]),
            &["one", "two", "three"]
        );
        assert_eq!(
            reorder_prefix_ok(&["one"], &["one", "two", "three"]),
            &["one", "two", "three"]
        );
        assert_eq!(
            reorder_prefix_ok(&["two"], &["one", "two", "three"]),
            &["two", "one", "three"]
        );
        assert_eq!(
            reorder_prefix_ok(&["three", "one"], &["one", "two", "three"]),
            &["three", "one", "two"]
        );

        // errors
        assert_eq!(
            reorder_prefix_err(&["one"], &[]),
            "Group column \'one\' not found in tag columns: "
        );
        assert_eq!(
            reorder_prefix_err(&["one"], &["two", "three"]),
            "Group column \'one\' not found in tag columns: two, three"
        );
        assert_eq!(
            reorder_prefix_err(&["two", "one", "two"], &["one", "two"]),
            "Duplicate group column \'two\'"
        );
    }

    fn reorder_prefix_ok(prefix: &[&str], table_columns: &[&str]) -> Vec<String> {
        let prefix = prefix.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let table_columns =
            Arc::try_unwrap(str_vec_to_arc_vec(table_columns)).expect("unwrap the arc");

        let res = reorder_prefix(&prefix, table_columns);
        let message = format!("Expected OK, got {:?}", res);
        let res = res.expect(&message);

        res.into_iter()
            .map(|a| Arc::try_unwrap(a).expect("unwrapping arc"))
            .collect()
    }

    // returns the error string or panics if `reorder_prefix` doesn't return an
    // error
    fn reorder_prefix_err(prefix: &[&str], table_columns: &[&str]) -> String {
        let prefix = prefix.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let table_columns =
            Arc::try_unwrap(str_vec_to_arc_vec(table_columns)).expect("unwrap the arc");

        let res = reorder_prefix(&prefix, table_columns);

        match res {
            Ok(r) => {
                panic!(
                    "Expected error result from reorder_prefix_err, but was OK: '{:?}'",
                    r
                );
            }
            Err(e) => format!("{}", e),
        }
    }

    /// Runs `plan` and returns the output as petty-formatted array of strings
    async fn run_plan(plan: LogicalPlan) -> Vec<String> {
        // run the created plan, ensuring the output is as expected
        let batches = Executor::new()
            .run_logical_plan(plan)
            .await
            .expect("ok running plan");

        pretty_format_batches(&batches)
            .expect("formatting results")
            .trim()
            .split('\n')
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
    }

    ///  Insert the line protocol lines in `lp_lines` into this table
    fn write_lines_to_table(table: &mut Table, dictionary: &mut Dictionary, lp_lines: Vec<&str>) {
        let lp_data = lp_lines.join("\n");

        let lines: Vec<_> = parse_lines(&lp_data).map(|l| l.unwrap()).collect();

        let data = split_lines_into_write_entry_partitions(chunk_key_func, &lines);

        let batch = flatbuffers::get_root::<wb::WriteBufferBatch<'_>>(&data);
        let entries = batch.entries().expect("at least one entry");

        for entry in entries {
            let table_batches = entry.table_batches().expect("there were table batches");
            for batch in table_batches {
                let rows = batch.rows().expect("Had rows in the batch");
                table
                    .append_rows(dictionary, &rows)
                    .expect("Appended the row");
            }
        }
    }

    fn chunk_key_func(_: &ParsedLine<'_>) -> String {
        String::from("the_chunk_key")
    }

    /// Pre-loaded Table for use in tests
    struct TableFixture {
        chunk: Chunk,
        table: Table,
    }

    impl TableFixture {
        /// Create an Table with the specified lines loaded
        fn new(lp_lines: Vec<&str>) -> Self {
            let mut chunk = Chunk::new("dummy_chunk_key", 42);
            let dictionary = &mut chunk.dictionary;
            let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

            write_lines_to_table(&mut table, dictionary, lp_lines);
            Self { chunk, table }
        }

        /// create a series set plan from the predicate ane aggregates
        /// and return the results as a vector of strings
        async fn grouped_series_set(
            &mut self,
            predicate: Predicate,
            agg: Aggregate,
            group_columns: &[&str],
        ) -> Vec<String> {
            let chunk_predicate = self.chunk.compile_predicate(&predicate).unwrap();

            let group_columns: Vec<_> = group_columns.iter().map(|s| String::from(*s)).collect();

            let grouped_series_set_plan = self
                .table
                .grouped_series_set_plan(&chunk_predicate, agg, &group_columns, &self.chunk)
                .expect("creating the grouped_series set plan");

            // ensure the group prefix got to the right place
            assert_eq!(
                grouped_series_set_plan.num_prefix_tag_group_columns,
                Some(group_columns.len())
            );

            // run the created plan, ensuring the output is as expected
            run_plan(grouped_series_set_plan.plan).await
        }
    }
}
