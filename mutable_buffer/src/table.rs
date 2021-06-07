use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use hashbrown::HashMap;
use snafu::{ensure, OptionExt, ResultExt, Snafu};

use data_types::partition_metadata::{ColumnSummary, InfluxDbType};
use internal_types::{
    schema::{builder::SchemaBuilder, InfluxColumnType, Schema},
    selection::Selection,
};

use crate::column;
use crate::column::Column;

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
    InternalSchema {
        source: internal_types::schema::builder::Error,
    },

    #[snafu(display("Column not found: {}", column))]
    ColumnNotFound { column: String },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Table {
    /// Name of the table
    pub table_name: Arc<str>,

    /// Map of column id from the chunk dictionary to the column
    pub columns: HashMap<String, Column>,
}

impl Table {
    pub fn new(table_name: Arc<str>) -> Self {
        Self {
            table_name,
            columns: Default::default(),
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
        self.columns.iter().map(|(k, v)| k.len() + v.size()).sum()
    }

    /// Returns an iterator over (column_name, estimated_size) for each column
    pub fn column_sizes(&self) -> impl Iterator<Item = (&str, usize)> + '_ {
        self.columns
            .iter()
            .map(|(column_name, c)| (column_name.as_str(), c.size()))
    }

    /// Returns a reference to the specified column
    pub(crate) fn column(&self, column: &str) -> Result<&Column> {
        self.columns.get(column).context(ColumnNotFound { column })
    }

    /// Validates the schema of the passed in columns, then adds their values to
    /// the associated columns in the table and updates summary statistics.
    pub fn write_columns(
        &mut self,
        _sequencer_id: u32,
        _sequence_number: u64,
        columns: Vec<entry::Column<'_>>,
    ) -> Result<()> {
        let row_count_before_insert = self.row_count();
        let additional_rows = columns.first().map(|x| x.row_count).unwrap_or_default();
        let final_row_count = row_count_before_insert + additional_rows;

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

            if let Some(c) = self.columns.get(column.name()) {
                c.validate_schema(&column).context(ColumnError {
                    column: column.name(),
                })?;
            }

            Ok(())
        })?;

        for fb_column in columns {
            let influx_type = fb_column.influx_type();

            let column = self
                .columns
                .raw_entry_mut()
                .from_key(fb_column.name())
                .or_insert_with(|| {
                    (
                        fb_column.name().to_string(),
                        Column::new(row_count_before_insert, influx_type),
                    )
                })
                .1;

            column.append(&fb_column).context(ColumnError {
                column: fb_column.name(),
            })?;

            assert_eq!(column.len(), final_row_count);
        }

        for c in self.columns.values_mut() {
            c.push_nulls_to_len(final_row_count);
        }

        Ok(())
    }

    /// Converts this table to an arrow record batch
    ///
    /// If Selection::All the returned columns are sorted by name
    pub fn to_arrow(&self, selection: Selection<'_>) -> Result<RecordBatch> {
        let schema = self.schema(selection)?;
        let columns = schema
            .iter()
            .map(|(_, field)| {
                let column = self
                    .columns
                    .get(field.name())
                    .expect("schema contains non-existent column");

                column.to_arrow().context(ColumnError {
                    column: field.name(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        RecordBatch::try_new(schema.into(), columns).context(ArrowError {})
    }

    /// Returns the schema for a given selection
    ///
    /// If Selection::All the returned columns are sorted by name
    pub fn schema(&self, selection: Selection<'_>) -> Result<Schema> {
        let mut schema_builder = SchemaBuilder::new();
        let schema = match selection {
            Selection::All => {
                for (column_name, column) in self.columns.iter() {
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

    /// Returns a summary of all the columns, sorted by column name
    pub fn stats(&self) -> Vec<ColumnSummary> {
        let mut columns: Vec<_> = self
            .columns
            .iter()
            .map(|(column_name, c)| ColumnSummary {
                name: column_name.to_string(),
                stats: c.stats(),
                influxdb_type: Some(match c.influx_type() {
                    InfluxColumnType::Tag => InfluxDbType::Tag,
                    InfluxColumnType::Field(_) => InfluxDbType::Field,
                    InfluxColumnType::Timestamp => InfluxDbType::Timestamp,
                }),
            })
            .collect();

        columns.sort_by(|a, b| a.name.cmp(&b.name));
        columns
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType as ArrowDataType;

    use entry::test_helpers::lp_to_entry;
    use internal_types::schema::{InfluxColumnType, InfluxFieldType};

    use super::*;

    #[test]
    fn table_size() {
        let mut table = Table::new(Arc::from("table_name"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
        ];

        write_lines_to_table(&mut table, lp_lines.clone());
        let s1 = table.size();

        write_lines_to_table(&mut table, lp_lines.clone());
        let s2 = table.size();

        write_lines_to_table(&mut table, lp_lines);
        let s3 = table.size();

        // Should increase by a constant amount each time
        assert_eq!(s2 - s1, s3 - s2);
    }

    #[test]
    fn test_to_arrow_schema_all() {
        let mut table = Table::new(Arc::from("table_name"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston float_field=70.4,int_field=8i,uint_field=42u,bool_field=t,string_field=\"foo\" 100",
        ];

        write_lines_to_table(&mut table, lp_lines);

        let selection = Selection::All;
        let actual_schema = table.schema(selection).unwrap();
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
        let mut table = Table::new(Arc::from("table_name"));

        let lp_lines = vec!["h2o,state=MA,city=Boston float_field=70.4 100"];

        write_lines_to_table(&mut table, lp_lines);

        let selection = Selection::Some(&["float_field"]);
        let actual_schema = table.schema(selection).unwrap();
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
        let mut table = Table::new(Arc::from("foo"));
        let sequencer_id = 1;
        let sequence_number = 5;

        let lp = "foo,t1=asdf iv=1i,uv=1u,fv=1.0,bv=true,sv=\"hi\" 1";
        let entry = lp_to_entry(&lp);
        table
            .write_columns(
                sequencer_id,
                sequence_number,
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
                sequencer_id,
                sequence_number,
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
                sequencer_id,
                sequence_number,
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
                sequencer_id,
                sequence_number,
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
                sequencer_id,
                sequence_number,
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
                sequencer_id,
                sequence_number,
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
                sequencer_id,
                sequence_number,
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
    fn write_lines_to_table(table: &mut Table, lp_lines: Vec<&str>) {
        let lp_data = lp_lines.join("\n");
        let entry = lp_to_entry(&lp_data);

        for batch in entry
            .partition_writes()
            .unwrap()
            .first()
            .unwrap()
            .table_batches()
        {
            table.write_columns(1, 5, batch.columns()).unwrap();
        }
    }
}
