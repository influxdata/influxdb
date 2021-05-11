use std::collections::BTreeMap;

use crate::{
    column,
    column::Column,
    dictionary::{Dictionary, DID},
};
use data_types::{
    partition_metadata::{ColumnSummary, InfluxDbType},
    server_id::ServerId,
};
use entry::{self, ClockValue};
use internal_types::{
    schema::{builder::SchemaBuilder, InfluxColumnType, Schema},
    selection::Selection,
};

use snafu::{ensure, OptionExt, ResultExt, Snafu};

use arrow::{array::Array, record_batch::RecordBatch};

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

    #[snafu(display("Internal error: unexpected aggregate request for None aggregate",))]
    InternalUnexpectedNoneAggregate {},

    #[snafu(display("Column name '{}' not found in dictionary of chunk", column_name,))]
    ColumnNameNotFoundInDictionary { column_name: String },

    #[snafu(display("Internal: Column id '{}' not found in dictionary", column_id,))]
    ColumnIdNotFoundInDictionary { column_id: DID },

    #[snafu(display("arrow conversion error: {}", source))]
    ArrowError { source: arrow::error::ArrowError },

    #[snafu(display("Internal error converting schema: {}", source))]
    InternalSchema {
        source: internal_types::schema::builder::Error,
    },

    #[snafu(display(
        "No index entry found for column {} with id {}",
        column_name,
        column_id
    ))]
    InternalNoColumnInIndex { column_name: String, column_id: DID },

    #[snafu(display("Error evaluating column predicate for column {}: {}", column, source))]
    ColumnPredicateEvaluation {
        column: DID,
        source: crate::column::Error,
    },

    #[snafu(display("Row insert to table {} missing column name", table))]
    ColumnNameNotInRow { table: DID },

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

    #[snafu(display("Column {} not found in table {}", id, table_id))]
    ColumnIdNotFound { id: DID, table_id: DID },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Table {
    /// Name of the table as a DID in the chunk dictionary
    pub id: DID,

    /// Map of column id from the chunk dictionary to the column
    pub columns: BTreeMap<DID, Column>,
}

impl Table {
    pub fn new(id: DID) -> Self {
        Self {
            id,
            columns: BTreeMap::new(),
        }
    }

    pub fn row_count(&self) -> usize {
        self.columns
            .values()
            .next()
            .map(|col| col.len())
            .unwrap_or(0)
    }

    /// The approximate memory size of the data in the table, in bytes. Note
    /// that the space taken for the tag string values is represented in the
    /// dictionary size in the chunk that holds the table.
    pub fn size(&self) -> usize {
        self.columns.values().map(|v| v.size()).sum()
    }

    /// Returns an iterator over (column_name, estimated_size) for each column
    pub fn column_sizes(&self) -> impl Iterator<Item = (&DID, usize)> + '_ {
        self.columns.iter().map(|(did, c)| (did, c.size()))
    }

    /// Returns a reference to the specified column
    pub(crate) fn column(&self, column_id: DID) -> Result<&Column> {
        self.columns.get(&column_id).context(ColumnIdNotFound {
            id: column_id,
            table_id: self.id,
        })
    }

    /// Validates the schema of the passed in columns, then adds their values to
    /// the associated columns in the table and updates summary statistics.
    pub fn write_columns(
        &mut self,
        dictionary: &mut Dictionary,
        _clock_value: ClockValue,
        _server_id: ServerId,
        columns: Vec<entry::Column<'_>>,
    ) -> Result<()> {
        let row_count_before_insert = self.row_count();
        let additional_rows = columns.first().map(|x| x.row_count).unwrap_or_default();
        let final_row_count = row_count_before_insert + additional_rows;

        // get the column ids and validate schema for those that already exist
        let column_ids = columns
            .iter()
            .map(|column| {
                ensure!(
                    column.row_count == additional_rows,
                    IncorrectRowCount {
                        column: column.name(),
                        expected: additional_rows,
                        actual: column.row_count,
                    }
                );

                let id = dictionary.lookup_value_or_insert(column.name());
                if let Some(c) = self.columns.get(&id) {
                    c.validate_schema(&column).context(ColumnError {
                        column: column.name(),
                    })?;
                }

                Ok(id)
            })
            .collect::<Result<Vec<_>, _>>()?;

        for (fb_column, column_id) in columns.into_iter().zip(column_ids.into_iter()) {
            let influx_type = fb_column.influx_type();

            let column = self
                .columns
                .entry(column_id)
                .or_insert_with(|| Column::new(row_count_before_insert, influx_type));

            column.append(&fb_column, dictionary).context(ColumnError {
                column: fb_column.name(),
            })?;

            assert_eq!(column.len(), final_row_count);
        }

        for c in self.columns.values_mut() {
            c.push_nulls_to_len(final_row_count);
        }

        Ok(())
    }

    /// Returns the column selection for all the columns in this table, orderd
    /// by table name
    fn all_columns_selection<'a>(
        &self,
        dictionary: &'a Dictionary,
    ) -> Result<TableColSelection<'a>> {
        let cols = self
            .columns
            .iter()
            .map(|(column_id, _)| {
                let column_name =
                    dictionary
                        .lookup_id(*column_id)
                        .context(ColumnIdNotFoundInDictionary {
                            column_id: *column_id,
                        })?;
                Ok(ColSelection {
                    column_name,
                    column_id: *column_id,
                })
            })
            .collect::<Result<_>>()?;

        let selection = TableColSelection { cols };

        // sort so the columns always come out in a predictable name
        Ok(selection.sort_by_name())
    }

    /// Returns a column selection for just the specified columns
    fn specific_columns_selection<'a>(
        &self,
        dictionary: &'a Dictionary,
        columns: &'a [&'a str],
    ) -> Result<TableColSelection<'a>> {
        let cols = columns
            .iter()
            .map(|&column_name| {
                let column_id = dictionary
                    .lookup_value(column_name)
                    .context(ColumnNameNotFoundInDictionary { column_name })?;

                Ok(ColSelection {
                    column_name,
                    column_id,
                })
            })
            .collect::<Result<_>>()?;

        Ok(TableColSelection { cols })
    }

    /// Converts this table to an arrow record batch.
    pub fn to_arrow(
        &self,
        dictionary: &Dictionary,
        selection: Selection<'_>,
    ) -> Result<RecordBatch> {
        // translate chunk selection into name/indexes:
        let selection = match selection {
            Selection::All => self.all_columns_selection(dictionary),
            Selection::Some(cols) => self.specific_columns_selection(dictionary, cols),
        }?;
        self.to_arrow_impl(dictionary, &selection)
    }

    pub fn schema(&self, dictionary: &Dictionary, selection: Selection<'_>) -> Result<Schema> {
        // translate chunk selection into name/indexes:
        let selection = match selection {
            Selection::All => self.all_columns_selection(dictionary),
            Selection::Some(cols) => self.specific_columns_selection(dictionary, cols),
        }?;
        self.schema_impl(&selection)
    }

    /// Returns the Schema of this table
    fn schema_impl(&self, selection: &TableColSelection<'_>) -> Result<Schema> {
        let mut schema_builder = SchemaBuilder::new();
        for col in &selection.cols {
            let column = self.column(col.column_id)?;
            schema_builder = schema_builder.influx_column(col.column_name, column.influx_type());
        }
        schema_builder.build().context(InternalSchema)
    }

    /// Converts this table to an arrow record batch,
    ///
    /// requested columns with index are tuples of column_name, column_index
    fn to_arrow_impl(
        &self,
        dictionary: &Dictionary,
        selection: &TableColSelection<'_>,
    ) -> Result<RecordBatch> {
        let encoded_dictionary = dictionary.values().to_arrow();
        let columns = selection
            .cols
            .iter()
            .map(|col| {
                let column = self.column(col.column_id)?;
                column
                    .to_arrow(encoded_dictionary.data())
                    .context(ColumnError {
                        column: col.column_name,
                    })
            })
            .collect::<Result<Vec<_>>>()?;

        let schema = self.schema_impl(selection)?.into();
        RecordBatch::try_new(schema, columns).context(ArrowError {})
    }

    pub fn stats(&self, dictionary: &Dictionary) -> Vec<ColumnSummary> {
        self.columns
            .iter()
            .map(|(column_id, c)| {
                let column_name = dictionary
                    .lookup_id(*column_id)
                    .expect("column name in dictionary");

                ColumnSummary {
                    name: column_name.to_string(),
                    stats: c.stats(),
                    influxdb_type: Some(match c.influx_type() {
                        InfluxColumnType::Tag => InfluxDbType::Tag,
                        InfluxColumnType::Field(_) => InfluxDbType::Field,
                        InfluxColumnType::Timestamp => InfluxDbType::Timestamp,
                    }),
                }
            })
            .collect()
    }
}

struct ColSelection<'a> {
    column_name: &'a str,
    column_id: DID,
}

/// Represets a set of column_name, column_index pairs
/// for a specific selection
struct TableColSelection<'a> {
    cols: Vec<ColSelection<'a>>,
}

impl<'a> TableColSelection<'a> {
    /// Sorts the columns by name
    fn sort_by_name(mut self) -> Self {
        self.cols.sort_by(|a, b| a.column_name.cmp(b.column_name));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType as ArrowDataType;
    use entry::test_helpers::lp_to_entry;
    use internal_types::schema::{InfluxColumnType, InfluxFieldType};
    use std::convert::TryFrom;

    #[test]
    fn table_size() {
        let mut dictionary = Dictionary::new();
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
        ];

        write_lines_to_table(&mut table, &mut dictionary, lp_lines.clone());
        let s1 = table.size();

        write_lines_to_table(&mut table, &mut dictionary, lp_lines.clone());
        let s2 = table.size();

        write_lines_to_table(&mut table, &mut dictionary, lp_lines);
        let s3 = table.size();

        // Should increase by a constant amount each time
        assert_eq!(s2 - s1, s3 - s2);
    }

    #[test]
    fn test_to_arrow_schema_all() {
        let mut dictionary = Dictionary::new();
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston float_field=70.4,int_field=8i,uint_field=42u,bool_field=t,string_field=\"foo\" 100",
        ];

        write_lines_to_table(&mut table, &mut dictionary, lp_lines);

        let selection = Selection::All;
        let actual_schema = table.schema(&dictionary, selection).unwrap();
        let expected_schema = SchemaBuilder::new()
            .field("bool_field", ArrowDataType::Boolean)
            .tag("city")
            .field("float_field", ArrowDataType::Float64)
            .field("int_field", ArrowDataType::Int64)
            .tag("state")
            .field("string_field", ArrowDataType::Utf8)
            .timestamp()
            .field("uint_field", ArrowDataType::UInt64)
            .build()
            .unwrap();

        assert_eq!(
            expected_schema, actual_schema,
            "Expected:\n{:#?}\nActual:\n{:#?}\n",
            expected_schema, actual_schema
        );
    }

    #[test]
    fn test_to_arrow_schema_subset() {
        let mut dictionary = Dictionary::new();
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec!["h2o,state=MA,city=Boston float_field=70.4 100"];

        write_lines_to_table(&mut table, &mut dictionary, lp_lines);

        let selection = Selection::Some(&["float_field"]);
        let actual_schema = table.schema(&dictionary, selection).unwrap();
        let expected_schema = SchemaBuilder::new()
            .field("float_field", ArrowDataType::Float64)
            .build()
            .unwrap();

        assert_eq!(
            expected_schema, actual_schema,
            "Expected:\n{:#?}\nActual:\n{:#?}\n",
            expected_schema, actual_schema
        );
    }

    #[test]
    fn write_columns_validates_schema() {
        let mut dictionary = Dictionary::new();
        let mut table = Table::new(dictionary.lookup_value_or_insert("foo"));
        let server_id = ServerId::try_from(1).unwrap();
        let clock_value = ClockValue::try_from(5).unwrap();

        let lp = "foo,t1=asdf iv=1i,uv=1u,fv=1.0,bv=true,sv=\"hi\" 1";
        let entry = lp_to_entry(&lp);
        table
            .write_columns(
                &mut dictionary,
                clock_value,
                server_id,
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
            )
            .unwrap();

        let lp = "foo t1=\"string\" 1";
        let entry = lp_to_entry(&lp);
        let response = table
            .write_columns(
                &mut dictionary,
                clock_value,
                server_id,
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
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
        let entry = lp_to_entry(&lp);
        let response = table
            .write_columns(
                &mut dictionary,
                clock_value,
                server_id,
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
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
        let entry = lp_to_entry(&lp);
        let response = table
            .write_columns(
                &mut dictionary,
                clock_value,
                server_id,
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
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
        let entry = lp_to_entry(&lp);
        let response = table
            .write_columns(
                &mut dictionary,
                clock_value,
                server_id,
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
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
        let entry = lp_to_entry(&lp);
        let response = table
            .write_columns(
                &mut dictionary,
                clock_value,
                server_id,
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
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
        let entry = lp_to_entry(&lp);
        let response = table
            .write_columns(
                &mut dictionary,
                clock_value,
                server_id,
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
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

    ///  Insert the line protocol lines in `lp_lines` into this table
    fn write_lines_to_table(table: &mut Table, dictionary: &mut Dictionary, lp_lines: Vec<&str>) {
        let lp_data = lp_lines.join("\n");
        let entry = lp_to_entry(&lp_data);

        for batch in entry
            .partition_writes()
            .unwrap()
            .first()
            .unwrap()
            .table_batches()
        {
            table
                .write_columns(
                    dictionary,
                    ClockValue::try_from(5).unwrap(),
                    ServerId::try_from(1).unwrap(),
                    batch.columns(),
                )
                .unwrap();
        }
    }
}
