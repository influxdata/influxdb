use delorean_generated_types::wal as wb;
use delorean_storage::{exec::make_schema_pivot, Predicate, TimestampRange};

use delorean_line_parser::FieldValue;
use std::{collections::HashMap, sync::Arc};

use crate::{
    column::{Column, ColumnValue, Value},
    dictionary::{Dictionary, Error as DictionaryError},
    partition::Partition,
    partition::TIME_COLUMN_NAME,
    wal::{type_description, WalEntryBuilder},
};
use snafu::{ensure, OptionExt, ResultExt, Snafu};

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

    #[snafu(display("Column {} not found", column))]
    ColumnNotFound { column: String },

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
        partition: u32,
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
        "Column name '{}' not found in dictionary of partition {}",
        column_name,
        partition
    ))]
    ColumnNameNotFoundInDictionary {
        column_name: String,
        partition: u32,
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
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone, Copy)]
pub struct TimestampPredicate {
    pub time_column_id: u32,
    pub range: TimestampRange,
}

#[derive(Debug)]
pub struct Table {
    id: u32,
    /// Maps column name (as a u32 in the partition dictionary) to an index in self.columns
    pub column_id_to_index: HashMap<u32, usize>,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            column_id_to_index: HashMap::new(),
            columns: Vec::new(),
        }
    }

    pub fn add_wal_row(
        &mut self,
        dictionary: &mut Dictionary,
        values: &flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<wb::Value<'_>>>,
    ) -> Result<()> {
        let row_count = self.row_count();

        // insert new columns and validate existing ones
        for value in values {
            let column_name = value.column().expect("WAL Value should have column");
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

    pub fn add_row(
        &mut self,
        values: &[ColumnValue<'_>],
        dictionary: &Dictionary,
        builder: &mut Option<WalEntryBuilder<'_>>,
    ) -> Result<()> {
        let row_count = self.row_count();

        // insert new columns and validate existing ones
        for col_val in values {
            let column = match self.column_id_to_index.get(&col_val.id) {
                Some(idx) => &mut self.columns[*idx],
                None => {
                    // Add the column and make all values for existing rows None
                    let index = self.columns.len();
                    self.column_id_to_index.insert(col_val.id, index);

                    let column_values = match col_val.value {
                        Value::TagValue(_, _) => {
                            let mut v = Vec::with_capacity(row_count + 1);
                            v.resize_with(row_count, || None);
                            Column::Tag(v)
                        }
                        Value::FieldValue(FieldValue::I64(_)) => {
                            let mut v = Vec::with_capacity(row_count + 1);
                            v.resize_with(row_count, || None);
                            Column::I64(v)
                        }
                        Value::FieldValue(FieldValue::F64(_)) => {
                            let mut v = Vec::with_capacity(row_count + 1);
                            v.resize_with(row_count, || None);
                            Column::F64(v)
                        }
                        Value::FieldValue(FieldValue::Boolean(_)) => {
                            let mut v = Vec::with_capacity(row_count + 1);
                            v.resize_with(row_count, || None);
                            Column::Bool(v)
                        }
                        Value::FieldValue(FieldValue::String(_)) => {
                            let mut v = Vec::with_capacity(row_count + 1);
                            v.resize_with(row_count, || None);
                            Column::String(v)
                        }
                    };

                    self.columns.push(column_values);

                    &mut self.columns[index]
                }
            };

            ensure!(
                column.matches_type(&col_val),
                SchemaMismatch {
                    column: col_val.id,
                    existing_column_type: column.type_description(),
                    inserted_value_type: col_val.value.type_description(),
                }
            );
        }

        // insert the actual values
        for col_val in values {
            let idx = self
                .column_id_to_index
                .get(&col_val.id)
                .expect("column id existed or was just inserted");

            let column = self
                .columns
                .get_mut(*idx)
                .expect("column existed or was just added");

            column
                .push(&col_val.value)
                .context(InternalSchemaMismatch { column: col_val.id })?;
        }

        // send the values to the WAL
        if let Some(builder) = builder {
            let table_name = dictionary
                .lookup_id(self.id)
                .expect("this table's name should exist in the partition dictionary");
            builder.add_row(table_name, values);
        }

        // make sure all columns are of the same length
        for col in &mut self.columns {
            col.push_none_if_len_equal(row_count);
        }

        Ok(())
    }

    /// Creates a DataFusion LogicalPlan that returns tag names as a
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
        // only retrieve the requested columns
        let requested_columns_with_index =
            requested_columns
                .iter()
                .map(|&column_name| {
                    let column_id = partition.dictionary.lookup_value(column_name).context(
                        ColumnNameNotFoundInDictionary {
                            column_name,
                            partition: partition.generation,
                        },
                    )?;

                    let column_index = *self.column_id_to_index.get(&column_id).context(
                        InternalNoColumnInIndex {
                            column_name,
                            column_id,
                        },
                    )?;

                    Ok((column_name, column_index))
                })
                .collect::<Result<Vec<_>>>()?;

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
                                        partition: partition.generation,
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

#[cfg(test)]
mod tests {
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
}
