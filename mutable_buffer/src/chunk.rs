use crate::{
    chunk::snapshot::ChunkSnapshot,
    column::{self, Column},
};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use data_types::partition_metadata::{ColumnSummary, InfluxDbType, TableSummaryAndTimes};
use entry::{Sequence, TableBatch};
use hashbrown::HashMap;
use internal_types::{
    schema::{builder::SchemaBuilder, InfluxColumnType, Schema},
    selection::Selection,
};
use parking_lot::Mutex;
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::{collections::BTreeSet, sync::Arc};

pub mod snapshot;

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
#[allow(missing_copy_implementations)]
pub struct ChunkMetrics {
    // Placeholder
}

impl ChunkMetrics {
    /// Creates an instance of ChunkMetrics that isn't registered with a central
    /// metrics registry. Observations made to instruments on this ChunkMetrics instance
    /// will therefore not be visible to other ChunkMetrics instances or metric instruments
    /// created on a metrics domain, and vice versa
    pub fn new_unregistered() -> Self {
        Self {}
    }

    pub fn new(_metrics: &metrics::Domain) -> Self {
        Self {}
    }
}

/// Represents a Chunk of data (a horizontal subset of a table) in
/// the mutable store.
#[derive(Debug)]
pub struct MBChunk {
    /// The name of this table
    table_name: Arc<str>,

    /// Metrics tracked by this chunk
    metrics: ChunkMetrics,

    /// Map of column id from the chunk dictionary to the column
    columns: HashMap<String, Column>,

    /// Cached chunk snapshot
    ///
    /// Note: This is a mutex to allow mutation within
    /// `Chunk::snapshot()` which only takes an immutable borrow
    snapshot: Mutex<Option<Arc<ChunkSnapshot>>>,

    /// Time at which the first data was written into this chunk. Note
    /// this is not the same as the timestamps on the data itself
    time_of_first_write: DateTime<Utc>,

    /// Most recent time at which data write was initiated into this
    /// chunk. Note this is not the same as the timestamps on the data
    /// itself
    time_of_last_write: DateTime<Utc>,
}

impl MBChunk {
    /// Create a new batch and write the contents of the [`TableBatch`] into it. Chunks
    /// shouldn't exist without some data.
    pub fn new(
        metrics: ChunkMetrics,
        sequence: Option<&Sequence>,
        batch: TableBatch<'_>,
    ) -> Result<Self> {
        let table_name = Arc::from(batch.name());

        let now = Utc::now();

        let mut chunk = Self {
            table_name,
            columns: Default::default(),
            metrics,
            snapshot: Mutex::new(None),
            time_of_first_write: now,
            time_of_last_write: now,
        };

        let columns = batch.columns();
        chunk.write_columns(sequence, columns)?;

        Ok(chunk)
    }

    /// Write the contents of a [`TableBatch`] into this Chunk.
    ///
    /// Panics if the batch specifies a different name for the table in this Chunk
    pub fn write_table_batch(
        &mut self,
        sequence: Option<&Sequence>,
        batch: TableBatch<'_>,
    ) -> Result<()> {
        let table_name = batch.name();
        assert_eq!(
            table_name,
            self.table_name.as_ref(),
            "can only insert table batch for a single table to chunk"
        );

        let columns = batch.columns();
        self.write_columns(sequence, columns)?;

        // Invalidate chunk snapshot
        *self
            .snapshot
            .try_lock()
            .expect("concurrent readers/writers to MBChunk") = None;

        self.time_of_last_write = Utc::now();

        Ok(())
    }

    /// Returns a queryable snapshot of this chunk
    #[cfg(not(feature = "nocache"))]
    pub fn snapshot(&self) -> Arc<ChunkSnapshot> {
        let mut guard = self.snapshot.lock();
        if let Some(snapshot) = &*guard {
            return Arc::clone(snapshot);
        }

        let snapshot = Arc::new(ChunkSnapshot::new(self));
        *guard = Some(Arc::clone(&snapshot));
        snapshot
    }

    /// Returns a queryable snapshot of this chunk
    #[cfg(feature = "nocache")]
    pub fn snapshot(&self) -> Arc<ChunkSnapshot> {
        Arc::new(ChunkSnapshot::new(
            self,
            self.metrics.memory_bytes.clone_empty(),
        ))
    }

    /// Return the name of the table in this chunk
    pub fn table_name(&self) -> &Arc<str> {
        &self.table_name
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

    /// Convert the table specified in this chunk into some number of
    /// record batches, appended to dst
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

    /// Returns a table summary for this chunk
    pub fn table_summary(&self) -> TableSummaryAndTimes {
        let mut columns: Vec<_> = self
            .columns
            .iter()
            .map(|(column_name, c)| ColumnSummary {
                name: column_name.to_string(),
                stats: c.stats(),
                influxdb_type: Some(match c.influx_type() {
                    InfluxColumnType::IOx(_) => InfluxDbType::IOx,
                    InfluxColumnType::Tag => InfluxDbType::Tag,
                    InfluxColumnType::Field(_) => InfluxDbType::Field,
                    InfluxColumnType::Timestamp => InfluxDbType::Timestamp,
                }),
            })
            .collect();

        columns.sort_by(|a, b| a.name.cmp(&b.name));

        TableSummaryAndTimes {
            name: self.table_name.to_string(),
            columns,
            time_of_first_write: self.time_of_first_write,
            time_of_last_write: self.time_of_last_write,
        }
    }

    /// Return the approximate memory size of the chunk, in bytes including the
    /// dictionary, tables, and their rows.
    ///
    /// Note: This does not include the size of any cached ChunkSnapshot
    pub fn size(&self) -> usize {
        // TODO: Better accounting of non-column data (#1565)
        self.columns
            .iter()
            .map(|(k, v)| k.len() + v.size())
            .sum::<usize>()
            + self.table_name.len()
    }

    /// Returns an iterator over (column_name, estimated_size) for all
    /// columns in this chunk.
    pub fn column_sizes(&self) -> impl Iterator<Item = (&str, usize)> + '_ {
        self.columns
            .iter()
            .map(|(column_name, c)| (column_name.as_str(), c.size()))
    }

    /// Return the number of rows in this chunk
    pub fn rows(&self) -> usize {
        self.columns
            .values()
            .next()
            .map(|col| col.len())
            .unwrap_or(0)
    }

    /// Returns a reference to the specified column
    pub(crate) fn column(&self, column: &str) -> Result<&Column> {
        self.columns.get(column).context(ColumnNotFound { column })
    }

    /// Validates the schema of the passed in columns, then adds their values to
    /// the associated columns in the table and updates summary statistics.
    fn write_columns(
        &mut self,
        _sequence: Option<&Sequence>,
        columns: Vec<entry::Column<'_>>,
    ) -> Result<()> {
        let row_count_before_insert = self.rows();
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
}

pub mod test_helpers {
    use entry::test_helpers::lp_to_entry;

    use super::*;

    /// A helper that will write line protocol string to the passed in Chunk.
    /// All data will be under a single partition with a clock value and
    /// server id of 1.
    pub fn write_lp_to_chunk(lp: &str, chunk: &mut MBChunk) -> Result<()> {
        let entry = lp_to_entry(lp);

        for w in entry.partition_writes().unwrap() {
            let table_batches = w.table_batches();
            // ensure they are all to the same table
            let table_names: BTreeSet<String> =
                table_batches.iter().map(|b| b.name().to_string()).collect();

            assert!(
                table_names.len() <= 1,
                "Can only write 0 or one tables to chunk. Found {:?}",
                table_names
            );

            for batch in table_batches {
                let seq = Some(Sequence::new(1, 5));
                chunk.write_table_batch(seq.as_ref(), batch)?;
            }
        }

        Ok(())
    }

    pub fn write_lp_to_new_chunk(lp: &str) -> Result<MBChunk> {
        let entry = lp_to_entry(lp);
        let mut chunk: Option<MBChunk> = None;

        for w in entry.partition_writes().unwrap() {
            let table_batches = w.table_batches();
            // ensure they are all to the same table
            let table_names: BTreeSet<String> =
                table_batches.iter().map(|b| b.name().to_string()).collect();

            assert!(
                table_names.len() <= 1,
                "Can only write 0 or one tables to chunk. Found {:?}",
                table_names
            );

            for batch in table_batches {
                let seq = Some(Sequence::new(1, 5));

                match chunk {
                    Some(ref mut c) => c.write_table_batch(seq.as_ref(), batch)?,
                    None => {
                        chunk = Some(MBChunk::new(
                            ChunkMetrics::new_unregistered(),
                            seq.as_ref(),
                            batch,
                        )?);
                    }
                }
            }
        }

        Ok(chunk.expect("Must write at least one table batch to create a chunk"))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        test_helpers::{write_lp_to_chunk, write_lp_to_new_chunk},
        *,
    };
    use arrow::datatypes::DataType as ArrowDataType;
    use arrow_util::assert_batches_eq;
    use data_types::partition_metadata::{ColumnSummary, InfluxDbType, StatValues, Statistics};
    use entry::test_helpers::lp_to_entry;
    use internal_types::schema::{InfluxColumnType, InfluxFieldType};
    use std::num::NonZeroU64;

    #[test]
    fn writes_table_batches() {
        let lp = vec!["cpu,host=a val=23 1", "cpu,host=b val=2 1"].join("\n");
        let chunk = write_lp_to_new_chunk(&lp).unwrap();

        assert_batches_eq!(
            vec![
                "+------+-------------------------------+-----+",
                "| host | time                          | val |",
                "+------+-------------------------------+-----+",
                "| a    | 1970-01-01 00:00:00.000000001 | 23  |",
                "| b    | 1970-01-01 00:00:00.000000001 | 2   |",
                "+------+-------------------------------+-----+",
            ],
            &chunk_to_batches(&chunk)
        );
    }

    #[test]
    fn writes_table_3_batches() {
        let before_creation = Utc::now();
        let lp = vec!["cpu,host=a val=23 1", "cpu,host=b val=2 1"].join("\n");
        let mut chunk = write_lp_to_new_chunk(&lp).unwrap();
        let after_creation = Utc::now();

        // There was only one write so far, so first and last write times should be equal
        let first_write = chunk.time_of_first_write;
        assert_eq!(first_write, chunk.time_of_last_write);

        assert!(before_creation < first_write);
        assert!(first_write < after_creation);

        let lp = vec!["cpu,host=c val=11 1"].join("\n");
        write_lp_to_chunk(&lp, &mut chunk).unwrap();
        let after_write = Utc::now();

        // Now the first and last times should be different
        assert_ne!(chunk.time_of_first_write, chunk.time_of_last_write);
        // The first write time should not have been updated
        assert_eq!(chunk.time_of_first_write, first_write);
        // The last write time should have been updated
        assert!(after_creation < chunk.time_of_last_write);
        assert!(chunk.time_of_last_write < after_write);

        let lp = vec!["cpu,host=a val=14 2"].join("\n");
        write_lp_to_chunk(&lp, &mut chunk).unwrap();

        assert_batches_eq!(
            vec![
                "+------+-------------------------------+-----+",
                "| host | time                          | val |",
                "+------+-------------------------------+-----+",
                "| a    | 1970-01-01 00:00:00.000000001 | 23  |",
                "| b    | 1970-01-01 00:00:00.000000001 | 2   |",
                "| c    | 1970-01-01 00:00:00.000000001 | 11  |",
                "| a    | 1970-01-01 00:00:00.000000002 | 14  |",
                "+------+-------------------------------+-----+",
            ],
            &chunk_to_batches(&chunk)
        );
    }

    #[test]
    fn test_summary() {
        let before_write = Utc::now();

        let lp = r#"
            cpu,host=a val=23 1
            cpu,host=b,env=prod val=2 1
            cpu,host=c,env=stage val=11 1
            cpu,host=a,env=prod val=14 2
        "#;
        let chunk = write_lp_to_new_chunk(&lp).unwrap();

        let after_write = Utc::now();

        // There was only one write, so first and last write times should be equal
        assert_eq!(chunk.time_of_first_write, chunk.time_of_last_write);

        assert!(before_write < chunk.time_of_first_write);
        assert!(chunk.time_of_first_write < after_write);

        let summary = chunk.table_summary();
        assert_eq!(summary.name, "cpu");
        let expected_column_summaries = vec![
            ColumnSummary {
                name: "env".to_string(),
                influxdb_type: Some(InfluxDbType::Tag),
                stats: Statistics::String(StatValues {
                    min: Some("prod".to_string()),
                    max: Some("stage".to_string()),
                    count: 3,
                    distinct_count: Some(NonZeroU64::new(3).unwrap()),
                }),
            },
            ColumnSummary {
                name: "host".to_string(),
                influxdb_type: Some(InfluxDbType::Tag),
                stats: Statistics::String(StatValues {
                    min: Some("a".to_string()),
                    max: Some("c".to_string()),
                    count: 4,
                    distinct_count: Some(NonZeroU64::new(3).unwrap()),
                }),
            },
            ColumnSummary {
                name: "time".to_string(),
                influxdb_type: Some(InfluxDbType::Timestamp),
                stats: Statistics::I64(StatValues {
                    min: Some(1),
                    max: Some(2),
                    count: 4,
                    distinct_count: None,
                }),
            },
            ColumnSummary {
                name: "val".to_string(),
                influxdb_type: Some(InfluxDbType::Field),
                stats: Statistics::F64(StatValues {
                    min: Some(2.),
                    max: Some(23.),
                    count: 4,
                    distinct_count: None,
                }),
            },
        ];
        assert_eq!(summary.columns, expected_column_summaries);
    }

    #[test]
    #[cfg(not(feature = "nocache"))]
    fn test_snapshot() {
        let lp = vec!["cpu,host=a val=23 1", "cpu,host=b val=2 1"].join("\n");
        let mut chunk = write_lp_to_new_chunk(&lp).unwrap();

        let s1 = chunk.snapshot();
        let s2 = chunk.snapshot();

        write_lp_to_chunk(&lp, &mut chunk).unwrap();
        let s3 = chunk.snapshot();
        let s4 = chunk.snapshot();

        assert_eq!(Arc::as_ptr(&s1), Arc::as_ptr(&s2));
        assert_ne!(Arc::as_ptr(&s1), Arc::as_ptr(&s3));
        assert_eq!(Arc::as_ptr(&s3), Arc::as_ptr(&s4));
    }

    fn chunk_to_batches(chunk: &MBChunk) -> Vec<RecordBatch> {
        vec![chunk.to_arrow(Selection::All).unwrap()]
    }

    #[test]
    fn table_size() {
        let lp = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
        ]
        .join("\n");
        let mut chunk = write_lp_to_new_chunk(&lp).unwrap();
        let s1 = chunk.size();

        write_lp_to_chunk(&lp, &mut chunk).unwrap();
        let s2 = chunk.size();

        write_lp_to_chunk(&lp, &mut chunk).unwrap();
        let s3 = chunk.size();

        // Should increase by a constant amount each time
        assert_eq!(s2 - s1, s3 - s2);
    }

    #[test]
    fn test_to_arrow_schema_all() {
        let lp = "h2o,state=MA,city=Boston float_field=70.4,int_field=8i,uint_field=42u,bool_field=t,string_field=\"foo\" 100";
        let chunk = write_lp_to_new_chunk(lp).unwrap();

        let selection = Selection::All;
        let actual_schema = chunk.schema(selection).unwrap();
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
        let lp = "h2o,state=MA,city=Boston float_field=70.4 100";
        let chunk = write_lp_to_new_chunk(lp).unwrap();

        let selection = Selection::Some(&["float_field"]);
        let actual_schema = chunk.schema(selection).unwrap();
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
        let sequencer_id = 1;
        let sequence_number = 5;
        let sequence = Some(Sequence::new(sequencer_id, sequence_number));

        let lp = "foo,t1=asdf iv=1i,uv=1u,fv=1.0,bv=true,sv=\"hi\" 1";
        let mut table = write_lp_to_new_chunk(lp).unwrap();

        let lp = "foo t1=\"string\" 1";
        let entry = lp_to_entry(&lp);
        let response = table
            .write_columns(
                sequence.as_ref(),
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
                sequence.as_ref(),
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
                sequence.as_ref(),
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
                sequence.as_ref(),
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
                sequence.as_ref(),
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
                sequence.as_ref(),
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
}
