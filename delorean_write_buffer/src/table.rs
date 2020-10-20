use delorean_generated_types::wal as wb;
use delorean_storage::{
    exec::make_schema_pivot, exec::GroupedSeriesSetPlan, exec::SeriesSetPlan, Predicate,
    TimestampRange,
};

use std::{collections::BTreeSet, collections::HashMap, sync::Arc};

use crate::{
    column::Column,
    dictionary::{Dictionary, Error as DictionaryError},
    partition::Partition,
};
use delorean_data_types::{data::type_description, TIME_COLUMN_NAME};
use snafu::{OptionExt, ResultExt, Snafu};

use delorean_arrow::{
    arrow,
    arrow::{
        array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder},
        datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
        record_batch::RecordBatch,
    },
    datafusion,
    datafusion::logical_plan::Expr,
    datafusion::logical_plan::LogicalPlan,
    datafusion::logical_plan::LogicalPlanBuilder,
    datafusion::logical_plan::Operator,
    datafusion::scalar::ScalarValue,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Table {} not found", table))]
    TableNotFound { table: String },

    #[snafu(display(
        "Column {} said it was type {} but extracting a value of that type failed",
        column,
        expected
    ))]
    WalValueTypeMismatch { column: String, expected: String },

    #[snafu(display(
        "Tag value ID {} not found in dictionary of partition {}",
        value,
        partition
    ))]
    TagValueIdNotFoundInDictionary {
        value: u32,
        partition: String,
        source: DictionaryError,
    },

    #[snafu(display(
        "Column type mismatch for column {}: can't insert {} into column with type {}",
        column,
        inserted_value_type,
        existing_column_type
    ))]
    ColumnTypeMismatch {
        column: String,
        existing_column_type: String,
        inserted_value_type: String,
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

    #[snafu(display(
        "Column name '{}' not found in dictionary of partition {}",
        column_name,
        partition
    ))]
    ColumnNameNotFoundInDictionary {
        column_name: String,
        partition: String,
        source: DictionaryError,
    },

    #[snafu(display(
        "Internal: Column id '{}' not found in dictionary of partition {}",
        column_id,
        partition
    ))]
    ColumnIdNotFoundInDictionary {
        column_id: u32,
        partition: String,
        source: DictionaryError,
    },

    #[snafu(display(
        "Schema mismatch: for column {}: can't insert {} into column with type {}",
        column,
        inserted_value_type,
        existing_column_type
    ))]
    SchemaMismatch {
        column: u32,
        existing_column_type: String,
        inserted_value_type: String,
    },

    #[snafu(display("Error building plan: {}", source))]
    BuildingPlan {
        source: datafusion::error::ExecutionError,
    },

    #[snafu(display("arrow conversion error: {}", source))]
    ArrowError { source: arrow::error::ArrowError },

    #[snafu(display("Schema mismatch: for column {}: {}", column, source))]
    InternalSchemaMismatch {
        column: u32,
        source: crate::column::Error,
    },

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

    #[snafu(display("Duplicate group column '{}'", column_name))]
    DuplicateGroupColumn { column_name: String },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone, Copy)]
pub struct TimestampPredicate {
    pub time_column_id: u32,
    pub range: TimestampRange,
}

#[derive(Debug)]
/// Describes a list of columns used in a predicate
pub enum PredicateTableColumns {
    /// Predicate exists, but has no columns
    NoColumns,

    /// Predicate exists, has columns, but at least one of those columns are not in the table
    AtLeastOneMissing,

    /// Predicate exists, has columns, and all columns are in the table
    Present(BTreeSet<u32>),
}

#[derive(Debug)]
pub struct Table {
    /// Name of the table as a u32 in the partition dictionary
    pub id: u32,
    /// Maps column name (as a u32 in the partition dictionary) to an index in self.columns
    pub column_id_to_index: HashMap<u32, usize>,
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

            let mut column = match self.column_id_to_index.get(&column_id) {
                Some(idx) => &mut self.columns[*idx],
                None => {
                    // Add the column and make all values for existing rows None
                    let idx = self.columns.len();
                    self.column_id_to_index.insert(column_id, idx);
                    self.columns.push(
                        Column::new_from_wal(row_count, value.value_type())
                            .context(CreatingFromWal { column: column_id })?,
                    );

                    &mut self.columns[idx]
                }
            };

            if let (Column::Bool(vals), Some(v)) = (&mut column, value.value_as_bool_value()) {
                vals.push(Some(v.value()));
            } else if let (Column::I64(vals), Some(v)) = (&mut column, value.value_as_i64value()) {
                vals.push(Some(v.value()));
            } else if let (Column::F64(vals), Some(v)) = (&mut column, value.value_as_f64value()) {
                vals.push(Some(v.value()));
            } else if let (Column::String(vals), Some(v)) =
                (&mut column, value.value_as_string_value())
            {
                vals.push(Some(v.value().unwrap().to_string()));
            } else if let (Column::Tag(vals), Some(v)) = (&mut column, value.value_as_tag_value()) {
                let v_id = dictionary.lookup_value_or_insert(v.value().unwrap());

                vals.push(Some(v_id));
            } else {
                return ColumnTypeMismatch {
                    column: column_name,
                    existing_column_type: column.type_description(),
                    inserted_value_type: type_description(value.value_type()),
                }
                .fail();
            }
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
            Column::I64(vals) => Ok(vals),
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
        predicate: &Predicate,
        timestamp_predicate: Option<&TimestampPredicate>,
        partition: &Partition,
    ) -> Result<LogicalPlan> {
        // Note we also need to add a timestamp predicate to this
        // expression as some additional rows may be filtered out (and
        // we couldn't prune out the entire Table based on range)
        let df_predicate = predicate.expr.clone();
        let (time_column_id, df_predicate) = match timestamp_predicate {
            None => (None, df_predicate),
            Some(timestamp_predicate) => (
                Some(timestamp_predicate.time_column_id),
                Self::add_timestamp_predicate_expr(df_predicate, timestamp_predicate),
            ),
        };

        // figure out the tag columns
        let requested_columns_with_index = self
            .column_id_to_index
            .iter()
            .filter_map(|(&column_id, &column_index)| {
                // keep tag columns and the timestamp column, if needed
                let need_column = if let Column::Tag(_) = self.columns[column_index] {
                    true
                } else {
                    Some(column_id) == time_column_id
                };

                if need_column {
                    // the id came out of our map, so it should always be valid
                    let column_name = partition.dictionary.lookup_id(column_id).unwrap();
                    Some((column_name, column_index))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // TODO avoid materializing here
        let data = self.to_arrow_impl(partition, &requested_columns_with_index)?;

        let schema = data.schema();

        let projection = None;
        let projected_schema = schema.clone();

        let plan_builder = LogicalPlanBuilder::from(&LogicalPlan::InMemoryScan {
            data: vec![vec![data]],
            schema,
            projection,
            projected_schema,
        });
        let plan_builder = plan_builder.filter(df_predicate).context(BuildingPlan)?;

        // add optional selection to remove time column
        let plan_builder = match time_column_id {
            None => plan_builder,
            Some(_) => {
                // Create expressions for all columns except time
                let select_exprs = requested_columns_with_index
                    .iter()
                    .filter_map(|&(column_name, _)| {
                        if column_name != TIME_COLUMN_NAME {
                            Some(Expr::Column(column_name.into()))
                        } else {
                            None
                        }
                    })
                    .collect();

                plan_builder.project(select_exprs).context(BuildingPlan)?
            }
        };

        let plan = plan_builder.build().context(BuildingPlan)?;

        // And finally pivot the plan
        let plan = make_schema_pivot(plan);
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
        predicate: &Predicate,
        timestamp_predicate: Option<&TimestampPredicate>,
        partition: &Partition,
    ) -> Result<LogicalPlan> {
        // Note we also need to add a timestamp predicate to this
        // expression as some additional rows may be filtered out (and
        // we couldn't prune out the entire Table based on range)
        let df_predicate = predicate.expr.clone();
        let df_predicate = match timestamp_predicate {
            None => df_predicate,
            Some(timestamp_predicate) => {
                Self::add_timestamp_predicate_expr(df_predicate, timestamp_predicate)
            }
        };

        // TODO avoid materializing all the columns here (ideally
        // DataFusion can prune them out)
        let data = self.all_to_arrow(partition)?;

        let schema = data.schema();

        let projection = None;
        let projected_schema = schema.clone();
        let select_exprs = vec![Expr::Column(column_name.into())];

        // And build the plan!
        let plan_builder = LogicalPlanBuilder::from(&LogicalPlan::InMemoryScan {
            data: vec![vec![data]],
            schema,
            projection,
            projected_schema,
        });

        plan_builder
            .filter(df_predicate)
            .context(BuildingPlan)?
            .project(select_exprs)
            .context(BuildingPlan)?
            .build()
            .context(BuildingPlan)
    }

    /// Creates a SeriesSet plan that produces an output table with rows that match the predicate
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
        predicate: Option<&Predicate>,
        timestamp_predicate: Option<&TimestampPredicate>,
        partition: &Partition,
    ) -> Result<SeriesSetPlan> {
        self.series_set_plan_impl(predicate, timestamp_predicate, None, partition)
    }

    /// Creates the plans for computing series set, pulling prefix_columns, if any, as a prefix of the ordering
    /// The created plan looks like:
    ///
    ///    Projection (select the columns columns needed)
    ///      Order by (tag_columns, timestamp_column)
    ///        Filter(predicate)
    ///          InMemoryScan
    pub fn series_set_plan_impl(
        &self,
        predicate: Option<&Predicate>,
        timestamp_predicate: Option<&TimestampPredicate>,
        prefix_columns: Option<&[String]>,
        partition: &Partition,
    ) -> Result<SeriesSetPlan> {
        // Note we also need to add a timestamp predicate to this
        // expression as some additional rows may be filtered out (and
        // we couldn't prune out the entire Table based on range)

        let df_predicate = predicate
            .map(|predicate| predicate.expr.clone())
            .map(|predicate| match timestamp_predicate {
                None => predicate,
                Some(timestamp_predicate) => {
                    Self::add_timestamp_predicate_expr(predicate, timestamp_predicate)
                }
            });

        // I wonder if all this string creation will be too slow?
        let table_name = partition
            .dictionary
            .lookup_id(self.id)
            .expect("looking up table name in dictionary")
            .to_string();

        let table_name = Arc::new(table_name);
        let (mut tag_columns, field_columns) = self.tag_and_field_column_names(partition)?;

        // reorder tag_columns to have the prefix columns, if requested
        if let Some(prefix_columns) = prefix_columns {
            tag_columns = reorder_prefix(prefix_columns, tag_columns)?;
        }

        // TODO avoid materializing all the columns here (ideally
        // DataFusion can prune them out)
        let data = self.all_to_arrow(partition)?;

        let schema = data.schema();

        let projection = None;
        let projected_schema = schema.clone();

        // And build the plan from the bottom up
        let plan_builder = LogicalPlanBuilder::from(&LogicalPlan::InMemoryScan {
            data: vec![vec![data]],
            schema,
            projection,
            projected_schema,
        });

        // Filtering
        let plan_builder = match df_predicate {
            Some(df_predicate) => plan_builder.filter(df_predicate).context(BuildingPlan)?,
            None => plan_builder,
        };

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

        Ok(SeriesSetPlan {
            table_name,
            plan,
            tag_columns,
            field_columns,
        })
    }

    /// Creates a GroupedSeriesSet plan that produces an output table with rows that match the predicate
    ///
    /// The output looks like:
    /// (group_tag_column1, group_tag_column2, ... tag_col1, tag_col2, ... field1, field2, ... timestamp)
    ///
    /// The order of the tag_columns is ordered by name.
    ///
    /// The data is sorted on tag_col1, tag_col2, ...) so that all
    /// rows for a particular series (groups where all tags are the
    /// same) occur together in the plan
    ///
    /// The created plan looks like:
    ///
    ///    Projection (select the columns columns needed)
    ///      Order by (tag_columns, timestamp_column)
    ///        Filter(predicate)
    ///          InMemoryScan
    pub fn grouped_series_set_plan(
        &self,
        predicate: Option<&Predicate>,
        timestamp_predicate: Option<&TimestampPredicate>,
        group_columns: &[String],
        partition: &Partition,
    ) -> Result<GroupedSeriesSetPlan> {
        let series_set_plan = self.series_set_plan_impl(
            predicate,
            timestamp_predicate,
            Some(&group_columns),
            partition,
        )?;
        let num_prefix_tag_group_columns = group_columns.len();

        Ok(GroupedSeriesSetPlan {
            series_set_plan,
            num_prefix_tag_group_columns,
        })
    }

    // Returns (tag_columns, field_columns) vectors with the names of
    // all tag and field columns, respectively. The vectors are sorted
    // by name.
    fn tag_and_field_column_names(
        &self,
        partition: &Partition,
    ) -> Result<(ArcStringVec, ArcStringVec)> {
        let mut tag_columns = Vec::with_capacity(self.column_id_to_index.len());
        let mut field_columns = Vec::with_capacity(self.column_id_to_index.len());

        for (&column_id, &column_index) in &self.column_id_to_index {
            let column_name = partition
                .dictionary
                .lookup_id(column_id)
                .expect("Find column name in dictionary");

            if column_name != TIME_COLUMN_NAME {
                let column_name = Arc::new(column_name.to_string());

                match self.columns[column_index] {
                    Column::Tag(_) => tag_columns.push(column_name),
                    _ => field_columns.push(column_name),
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

    /// Creates a DataFusion predicate of the form:
    ///
    /// `expr AND (range.start <= time and time < range.end)`
    fn add_timestamp_predicate_expr(expr: Expr, timestamp_predicate: &TimestampPredicate) -> Expr {
        let range = timestamp_predicate.range;
        let ts_low = Expr::BinaryExpr {
            left: Box::new(Expr::Literal(ScalarValue::Int64(Some(range.start)))),
            op: Operator::LtEq,
            right: Box::new(Expr::Column(TIME_COLUMN_NAME.into())),
        };
        let ts_high = Expr::BinaryExpr {
            left: Box::new(Expr::Column(TIME_COLUMN_NAME.into())),
            op: Operator::Lt,
            right: Box::new(Expr::Literal(ScalarValue::Int64(Some(range.end)))),
        };
        let ts_pred = Expr::BinaryExpr {
            left: Box::new(ts_low),
            op: Operator::And,
            right: Box::new(ts_high),
        };

        Expr::BinaryExpr {
            left: Box::new(expr),
            op: Operator::And,
            right: Box::new(ts_pred),
        }
    }

    /// Converts this table to an arrow record batch.
    pub fn to_arrow(
        &self,
        partition: &Partition,
        requested_columns: &[&str],
    ) -> Result<RecordBatch> {
        // if requested columns is empty, retrieve all columns in the table
        if requested_columns.is_empty() {
            self.all_to_arrow(partition)
        } else {
            let columns_with_index = self.column_names_with_index(partition, requested_columns)?;

            self.to_arrow_impl(partition, &columns_with_index)
        }
    }

    fn column_names_with_index<'a>(
        &self,
        partition: &Partition,
        columns: &[&'a str],
    ) -> Result<Vec<(&'a str, usize)>> {
        columns
            .iter()
            .map(|&column_name| {
                let column_id = partition.dictionary.lookup_value(column_name).context(
                    ColumnNameNotFoundInDictionary {
                        column_name,
                        partition: &partition.key,
                    },
                )?;

                let column_index =
                    *self
                        .column_id_to_index
                        .get(&column_id)
                        .context(InternalNoColumnInIndex {
                            column_name,
                            column_id,
                        })?;

                Ok((column_name, column_index))
            })
            .collect()
    }

    /// Convert all columns to an arrow record batch
    pub fn all_to_arrow(&self, partition: &Partition) -> Result<RecordBatch> {
        let mut requested_columns_with_index = self
            .column_id_to_index
            .iter()
            .map(|(&column_id, &column_index)| {
                let column_name = partition.dictionary.lookup_id(column_id).context(
                    ColumnIdNotFoundInDictionary {
                        column_id,
                        partition: &partition.key,
                    },
                )?;
                Ok((column_name, column_index))
            })
            .collect::<Result<Vec<_>>>()?;

        requested_columns_with_index.sort_by(|(a, _), (b, _)| a.cmp(b));

        self.to_arrow_impl(partition, &requested_columns_with_index)
    }

    /// Converts this table to an arrow record batch,
    ///
    /// requested columns with index are tuples of column_name, column_index
    pub fn to_arrow_impl(
        &self,
        partition: &Partition,
        requested_columns_with_index: &[(&str, usize)],
    ) -> Result<RecordBatch> {
        let mut fields = Vec::with_capacity(requested_columns_with_index.len());
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(requested_columns_with_index.len());

        for &(column_name, column_index) in requested_columns_with_index.iter() {
            let arrow_col: ArrayRef = match &self.columns[column_index] {
                Column::String(vals) => {
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
                Column::Tag(vals) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Utf8, true));
                    let mut builder = StringBuilder::with_capacity(vals.len(), vals.len() * 10);

                    for v in vals {
                        match v {
                            None => builder.append_null(),
                            Some(value_id) => {
                                let tag_value = partition.dictionary.lookup_id(*value_id).context(
                                    TagValueIdNotFoundInDictionary {
                                        value: *value_id,
                                        partition: &partition.key,
                                    },
                                )?;
                                builder.append_value(tag_value)
                            }
                        }
                        .context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                }
                Column::F64(vals) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Float64, true));
                    let mut builder = Float64Builder::new(vals.len());

                    for v in vals {
                        builder.append_option(*v).context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                }
                Column::I64(vals) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Int64, true));
                    let mut builder = Int64Builder::new(vals.len());

                    for v in vals {
                        builder.append_option(*v).context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                }
                Column::Bool(vals) => {
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

    /// returns true if this table should be included in a query that
    /// has an optional table_symbol_predicate. Returns true f the
    /// table_symbol_predicate is not preset, or the table's id
    pub fn matches_id_predicate(&self, table_symbol_predicate: &Option<u32>) -> bool {
        match table_symbol_predicate {
            None => true,
            Some(table_symbol) => self.id == *table_symbol,
        }
    }

    /// returns true if there are any timestamps in this table that
    /// fall within the timestamp range
    pub fn matches_timestamp_predicate(&self, pred: Option<&TimestampPredicate>) -> Result<bool> {
        match pred {
            None => Ok(true),
            Some(pred) => {
                let time_column = self.column(pred.time_column_id)?;
                time_column
                    .has_i64_range(pred.range.start, pred.range.end)
                    .context(ColumnPredicateEvaluation {
                        column: pred.time_column_id,
                    })
            }
        }
    }

    /// returns true if this table has all columns specified in
    /// predicate_column_symbols or predicate_column_symbols is empty
    pub fn has_all_predicate_columns(
        &self,
        predicate_table_columns: Option<&PredicateTableColumns>,
    ) -> bool {
        if let Some(predicate_table_columns) = predicate_table_columns {
            use PredicateTableColumns::*;

            match predicate_table_columns {
                NoColumns => true,
                AtLeastOneMissing => false,
                Present(predicate_column_symbols) => {
                    for symbol in predicate_column_symbols {
                        if !self.column_id_to_index.contains_key(symbol) {
                            return false;
                        }
                    }
                    true
                }
            }
        } else {
            true
        }
    }

    /// returns true if there are any rows in column that are non-null
    /// and within the timestamp range specified by pred
    pub fn column_matches_timestamp_predicate<T>(
        &self,
        column: &[Option<T>],
        pred: Option<&TimestampPredicate>,
    ) -> Result<bool> {
        match pred {
            None => Ok(true),
            Some(pred) => {
                let time_column = self.column(pred.time_column_id)?;
                time_column
                    .has_non_null_i64_range(column, pred.range.start, pred.range.end)
                    .context(ColumnPredicateEvaluation {
                        column: pred.time_column_id,
                    })
            }
        }
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
        Expr::Column(self.as_ref().clone())
    }
}

impl IntoExpr for str {
    fn into_expr(&self) -> Expr {
        Expr::Column(self.to_string())
    }
}

#[cfg(test)]
mod tests {
    use arrow::util::pretty::pretty_format_batches;
    use delorean_data_types::data::split_lines_into_write_entry_partitions;
    use delorean_line_parser::{parse_lines, ParsedLine};
    use delorean_storage::exec::Executor;
    use delorean_test_helpers::str_vec_to_arc_vec;

    use super::*;

    #[test]
    fn test_add_timestamp_predicate_expr() {
        // Test that the generated predicate is correct

        let expr = Expr::Column(String::from("foo"));
        let timestamp_predicate = TimestampPredicate {
            time_column_id: 0,
            range: TimestampRange::new(101, 202),
        };

        let ts_predicate_expr = Table::add_timestamp_predicate_expr(expr, &timestamp_predicate);
        let expected_string = "#foo And Int64(101) LtEq #time And #time Lt Int64(202)";
        let actual_string = format!("{:?}", ts_predicate_expr);

        assert_eq!(actual_string, expected_string);
    }

    #[test]
    fn test_has_all_predicate_columns() {
        // setup a test table
        let mut partition = Partition::new("dummy_partition_key");
        let dictionary = &mut partition.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
        ];

        write_lines_to_table(&mut table, dictionary, lp_lines);

        let state_symbol = dictionary.id("state").unwrap();
        let new_symbol = dictionary.lookup_value_or_insert("not_a_columns");

        assert!(table.has_all_predicate_columns(None));

        let pred = PredicateTableColumns::NoColumns;
        assert!(table.has_all_predicate_columns(Some(&pred)));

        let pred = PredicateTableColumns::AtLeastOneMissing;
        assert!(!table.has_all_predicate_columns(Some(&pred)));

        let set = BTreeSet::<u32>::new();
        let pred = PredicateTableColumns::Present(set);
        assert!(table.has_all_predicate_columns(Some(&pred)));

        let mut set = BTreeSet::new();
        set.insert(state_symbol);
        let pred = PredicateTableColumns::Present(set);
        assert!(table.has_all_predicate_columns(Some(&pred)));

        let mut set = BTreeSet::new();
        set.insert(new_symbol);
        let pred = PredicateTableColumns::Present(set);
        assert!(!table.has_all_predicate_columns(Some(&pred)));

        let mut set = BTreeSet::new();
        set.insert(state_symbol);
        set.insert(new_symbol);
        let pred = PredicateTableColumns::Present(set);
        assert!(!table.has_all_predicate_columns(Some(&pred)));
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_series_set_plan() {
        // setup a test table
        let mut partition = Partition::new("dummy_partition_key");
        let dictionary = &mut partition.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
            "h2o,state=CA,city=LA temp=90.0 200",
            "h2o,state=CA,city=LA temp=90.0 350",
        ];

        write_lines_to_table(&mut table, dictionary, lp_lines);

        let predicate = None;
        let timestamp_predicate = None;
        let series_set_plan = table
            .series_set_plan(predicate, timestamp_predicate, &partition)
            .expect("creating the series set plan");

        assert_eq!(series_set_plan.table_name.as_ref(), "table_name");
        assert_eq!(
            series_set_plan.tag_columns,
            *str_vec_to_arc_vec(&["city", "state"])
        );
        assert_eq!(
            series_set_plan.field_columns,
            *str_vec_to_arc_vec(&["temp"])
        );

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

    #[tokio::test(threaded_scheduler)]
    async fn test_series_set_plan_order() {
        // test that the columns and rows come out in the right order (tags then timestamp)

        // setup a test table
        let mut partition = Partition::new("dummy_partition_key");
        let dictionary = &mut partition.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            "h2o,zz_tag=A,state=MA,city=Kingston temp=70.1 800",
            "h2o,state=MA,city=Kingston,zz_tag=B temp=70.2 100",
            "h2o,state=CA,city=Boston temp=70.3 250",
            "h2o,state=MA,city=Boston,zz_tag=A temp=70.4 1000",
            "h2o,state=MA,city=Boston temp=70.5,other=5.0 250",
        ];

        write_lines_to_table(&mut table, dictionary, lp_lines);

        let predicate = None;
        let timestamp_predicate = None;
        let series_set_plan = table
            .series_set_plan(predicate, timestamp_predicate, &partition)
            .expect("creating the series set plan");

        assert_eq!(series_set_plan.table_name.as_ref(), "table_name");
        assert_eq!(
            series_set_plan.tag_columns,
            *str_vec_to_arc_vec(&["city", "state", "zz_tag"])
        );
        assert_eq!(
            series_set_plan.field_columns,
            *str_vec_to_arc_vec(&["other", "temp"])
        );

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

    #[tokio::test(threaded_scheduler)]
    async fn test_series_set_plan_filter() {
        // test that filters are applied reasonably

        // setup a test table
        let mut partition = Partition::new("dummy_partition_key");
        let dictionary = &mut partition.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
            "h2o,state=CA,city=LA temp=90.0 200",
            "h2o,state=CA,city=LA temp=90.0 350",
        ];

        write_lines_to_table(&mut table, dictionary, lp_lines);

        let expr = Expr::BinaryExpr {
            left: Box::new(Expr::Column("city".into())),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Utf8(Some("LA".into())))),
        };
        let predicate = Some(Predicate { expr });

        let range = Some(TimestampRange::new(190, 210));
        let timestamp_predicate = partition
            .make_timestamp_predicate(range)
            .expect("Made a timestamp predicate");

        let series_set_plan = table
            .series_set_plan(predicate.as_ref(), timestamp_predicate.as_ref(), &partition)
            .expect("creating the series set plan");

        assert_eq!(series_set_plan.table_name.as_ref(), "table_name");
        assert_eq!(
            series_set_plan.tag_columns,
            *str_vec_to_arc_vec(&["city", "state"])
        );
        assert_eq!(
            series_set_plan.field_columns,
            *str_vec_to_arc_vec(&["temp"])
        );

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

    #[tokio::test(threaded_scheduler)]
    async fn test_grouped_series_set_plan() {
        // test that filters are applied reasonably

        // setup a test table
        let mut partition = Partition::new("dummy_partition_key");
        let dictionary = &mut partition.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
            "h2o,state=CA,city=LA temp=90.0 200",
            "h2o,state=CA,city=LA temp=90.0 350",
        ];

        write_lines_to_table(&mut table, dictionary, lp_lines);

        let expr = Expr::BinaryExpr {
            left: Box::new(Expr::Column("city".into())),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Utf8(Some("LA".into())))),
        };
        let predicate = Some(Predicate { expr });

        let range = Some(TimestampRange::new(190, 210));
        let timestamp_predicate = partition
            .make_timestamp_predicate(range)
            .expect("Made a timestamp predicate");

        let group_columns = vec![String::from("state")];
        let grouped_series_set_plan = table
            .grouped_series_set_plan(
                predicate.as_ref(),
                timestamp_predicate.as_ref(),
                &group_columns,
                &partition,
            )
            .expect("creating the grouped_series set plan");

        assert_eq!(grouped_series_set_plan.num_prefix_tag_group_columns, 1);

        // run the created plan, ensuring the output is as expected
        let results = run_plan(grouped_series_set_plan.series_set_plan.plan).await;

        let expected = vec![
            "+-------+------+------+------+",
            "| state | city | temp | time |",
            "+-------+------+------+------+",
            "| CA    | LA   | 90   | 200  |",
            "+-------+------+------+------+",
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

    // returns the error string or panics if `reorder_prefix` doesn't return an error
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

        let data = split_lines_into_write_entry_partitions(partition_key_func, &lines);

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

    fn partition_key_func(_: &ParsedLine<'_>) -> String {
        String::from("the_partition_key")
    }
}
