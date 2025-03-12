use std::sync::Arc;

use crate::{Precision, WriteLineError, write_buffer::Result};
use data_types::{NamespaceName, Timestamp};
use indexmap::IndexMap;
use influxdb3_catalog::catalog::{
    Catalog, CatalogSequenceNumber, DatabaseCatalogTransaction, Prompt,
};

use influxdb_line_protocol::{ParsedLine, parse_lines};
use influxdb3_id::{DbId, TableId};
use influxdb3_types::http::FieldDataType;
use influxdb3_wal::{Field, FieldData, Gen1Duration, Row, TableChunks, WriteBatch};
use iox_time::Time;
use observability_deps::tracing::trace;
use schema::TIME_COLUMN_NAME;

use super::Error;

/// Type state for the [`WriteValidator`] after it has been initialized
/// with the catalog.
#[derive(Debug)]
pub struct Initialized {
    catalog: Arc<Catalog>,
    txn: DatabaseCatalogTransaction,
}

/// Type state for the [`WriteValidator`] after it has parsed v1 or v3
/// line protocol.
#[derive(Debug)]
pub struct LinesParsed {
    catalog: Arc<Catalog>,
    txn: DatabaseCatalogTransaction,
    lines: Vec<QualifiedLine>,
    bytes: u64,
    errors: Vec<WriteLineError>,
}

impl LinesParsed {
    /// Convert this set of parsed and qualified lines into a set of rows
    ///
    /// This is useful for testing when you need to use the write validator to parse line protocol
    /// and get the raw row data for the WAL.
    pub fn to_rows(self) -> Vec<Row> {
        self.lines.into_iter().map(|line| line.row).collect()
    }

    pub fn txn(&self) -> &DatabaseCatalogTransaction {
        &self.txn
    }
}

/// Type state for [`WriteValidator`] after any catalog changes have been committed successfully
/// to the object store.
#[derive(Debug)]
pub struct CatalogChangesCommitted {
    catalog_sequence: CatalogSequenceNumber,
    db_id: DbId,
    db_name: Arc<str>,
    lines: Vec<QualifiedLine>,
    bytes: u64,
    errors: Vec<WriteLineError>,
}

impl CatalogChangesCommitted {
    /// Convert this set of parsed and qualified lines into a set of rows
    ///
    /// This is useful for testing when you need to use the write validator to parse line protocol
    /// and get the raw row data for the WAL.
    pub fn to_rows(self) -> Vec<Row> {
        self.lines.into_iter().map(|line| line.row).collect()
    }
}

/// A state machine for validating v1 or v3 line protocol and updating
/// the [`Catalog`] with new tables or schema changes.
#[derive(Debug)]
pub struct WriteValidator<State> {
    state: State,
}

impl<T> WriteValidator<T> {
    /// Convert this into the inner [`LinesParsed`]
    ///
    /// This is mainly used for testing
    pub fn into_inner(self) -> T {
        self.state
    }

    pub fn inner(&self) -> &T {
        &self.state
    }
}

impl WriteValidator<Initialized> {
    /// Initialize the [`WriteValidator`] by starting a catalog transaction on the given database
    /// with name `db_name`. This initializes the database if it does not already exist.
    pub fn initialize(db_name: NamespaceName<'static>, catalog: Arc<Catalog>) -> Result<Self> {
        let txn = catalog.begin(db_name.as_str())?;
        trace!(transaction = ?txn, "initialize write validator");
        Ok(WriteValidator {
            state: Initialized { catalog, txn },
        })
    }

    /// Parse the incoming lines of line protocol using the v1 parser and update the transaction
    /// to the catalog if:
    ///
    /// * A new table is being added
    /// * New fields or tags are being added to an existing table
    ///
    /// # Implementation Note
    ///
    /// This does not apply the changes to the catalog, it only modifies the database copy that
    /// is held on the catalog transaction.
    pub fn v1_parse_lines_and_catalog_updates(
        mut self,
        lp: &str,
        accept_partial: bool,
        ingest_time: Time,
        precision: Precision,
    ) -> Result<WriteValidator<LinesParsed>> {
        let mut errors = vec![];
        let mut lp_lines = lp.lines();
        let mut lines = vec![];
        let mut bytes = 0;

        for (line_idx, maybe_line) in parse_lines(lp).enumerate() {
            let qualified_line = match maybe_line
                .map_err(|e| WriteLineError {
                    // This unwrap is fine because we're moving line by line
                    // alongside the output from parse_lines
                    original_line: lp_lines.next().unwrap().to_string(),
                    line_number: line_idx + 1,
                    error_message: e.to_string(),
                })
                .and_then(|l| {
                    let raw_line = lp_lines.next().unwrap();
                    validate_and_qualify_v1_line(
                        &mut self.state.txn,
                        line_idx,
                        l,
                        ingest_time,
                        precision,
                    )
                    .inspect(|_| bytes += raw_line.len() as u64)
                }) {
                Ok(qualified_line) => qualified_line,
                Err(e) => {
                    if !accept_partial {
                        return Err(Error::ParseError(e));
                    } else {
                        errors.push(e);
                    }
                    continue;
                }
            };
            // This unwrap is fine because we're moving line by line
            // alongside the output from parse_lines
            lines.push(qualified_line);
        }

        Ok(WriteValidator {
            state: LinesParsed {
                catalog: self.state.catalog,
                txn: self.state.txn,
                lines,
                errors,
                bytes,
            },
        })
    }
}

/// Validate a line of line protocol against the given schema definition
///
/// This is for scenarios where a write comes in for a table that exists, but may have
/// invalid field types, based on the pre-existing schema.
///
/// An error will also be produced if the write, which is for the v1 data model, is targetting
/// a v3 table.
fn validate_and_qualify_v1_line(
    txn: &mut DatabaseCatalogTransaction,
    line_number: usize,
    line: ParsedLine<'_>,
    ingest_time: Time,
    precision: Precision,
) -> Result<QualifiedLine, WriteLineError> {
    let table_name = line.series.measurement.as_str();
    let mut fields = Vec::with_capacity(line.column_count());
    let mut index_count = 0;
    let mut field_count = 0;
    let table_def = txn
        .table_or_create(table_name)
        .map_err(|error| WriteLineError {
            original_line: line.to_string(),
            line_number: line_number + 1,
            error_message: error.to_string(),
        })?;

    if let Some(tag_set) = &line.series.tag_set {
        for (tag_key, tag_val) in tag_set {
            let col_id = txn
                .column_or_create(table_name, tag_key.as_str(), FieldDataType::Tag)
                .map_err(|error| WriteLineError {
                    original_line: line.to_string(),
                    line_number: line_number + 1,
                    error_message: error.to_string(),
                })?;
            fields.push(Field::new(col_id, FieldData::Tag(tag_val.to_string())));
            index_count += 1;
        }
    }

    for (field_name, field_val) in line.field_set.iter() {
        let col_id = txn
            .column_or_create(table_name, field_name, field_val.into())
            .map_err(|error| WriteLineError {
                original_line: line.to_string(),
                line_number: line_number + 1,
                error_message: error.to_string(),
            })?;
        fields.push(Field::new(col_id, field_val));
        field_count += 1;
    }

    let time_col_id = txn
        .column_or_create(table_name, TIME_COLUMN_NAME, FieldDataType::Timestamp)
        .map_err(|error| WriteLineError {
            original_line: line.to_string(),
            line_number: line_number + 1,
            error_message: error.to_string(),
        })?;
    let timestamp_ns = line
        .timestamp
        .map(|ts| apply_precision_to_timestamp(precision, ts))
        .unwrap_or(ingest_time.timestamp_nanos());
    fields.push(Field::new(time_col_id, FieldData::Timestamp(timestamp_ns)));

    Ok(QualifiedLine {
        table_id: table_def.table_id,
        row: Row {
            time: timestamp_ns,
            fields,
        },
        index_count,
        field_count,
    })
}

impl WriteValidator<LinesParsed> {
    pub async fn commit_catalog_changes(
        self,
    ) -> Result<Prompt<WriteValidator<CatalogChangesCommitted>>> {
        let db_schema = self.state.txn.db_schema();
        let db_id = db_schema.id;
        let db_name = Arc::clone(&db_schema.name);
        match self.state.catalog.commit(self.state.txn).await? {
            Prompt::Success(catalog_sequence) => Ok(Prompt::Success(WriteValidator {
                state: CatalogChangesCommitted {
                    catalog_sequence,
                    db_id,
                    db_name,
                    lines: self.state.lines,
                    bytes: self.state.bytes,
                    errors: self.state.errors,
                },
            })),
            Prompt::Retry(_) => Ok(Prompt::Retry(())),
        }
    }

    pub fn ignore_catalog_changes_and_convert_lines_to_buffer(
        self,
        gen1_duration: Gen1Duration,
    ) -> ValidatedLines {
        let db_schema = self.state.txn.db_schema();
        let ignored = WriteValidator {
            state: CatalogChangesCommitted {
                catalog_sequence: self.state.txn.sequence_number(),
                db_id: db_schema.id,
                db_name: Arc::clone(&db_schema.name),
                lines: self.state.lines,
                bytes: self.state.bytes,
                errors: self.state.errors,
            },
        };
        ignored.convert_lines_to_buffer(gen1_duration)
    }
}

/// Result of conversion from line protocol to valid chunked data
/// for the buffer.
#[derive(Debug)]
pub struct ValidatedLines {
    /// Number of lines passed in
    pub(crate) line_count: usize,
    /// Number of bytes of all valid lines written
    pub(crate) valid_bytes_count: u64,
    /// Number of fields passed in
    pub(crate) field_count: usize,
    /// Number of index columns passed in, whether tags (v1) or series keys (v3)
    pub(crate) index_count: usize,
    /// Any errors that occurred while parsing the lines
    pub errors: Vec<WriteLineError>,
    /// Only valid lines will be converted into a WriteBatch
    pub valid_data: WriteBatch,
}

impl From<ValidatedLines> for WriteBatch {
    fn from(value: ValidatedLines) -> Self {
        value.valid_data
    }
}

impl WriteValidator<CatalogChangesCommitted> {
    /// Convert a set of valid parsed `v3` lines to a [`ValidatedLines`] which will
    /// be buffered and written to the WAL, if configured.
    ///
    /// This involves splitting out the writes into different batches for each chunk, which will
    /// map to the `Gen1Duration`. This function should be infallible, because
    /// the schema for incoming writes has been fully validated.
    pub fn convert_lines_to_buffer(self, gen1_duration: Gen1Duration) -> ValidatedLines {
        let mut table_chunks = IndexMap::new();
        let line_count = self.state.lines.len();
        let mut field_count = 0;
        let mut index_count = 0;

        for line in self.state.lines.into_iter() {
            field_count += line.field_count;
            index_count += line.index_count;

            convert_qualified_line(line, &mut table_chunks, gen1_duration);
        }

        let write_batch = WriteBatch::new(
            self.state.catalog_sequence.get(),
            self.state.db_id,
            self.state.db_name,
            table_chunks,
        );

        ValidatedLines {
            line_count,
            valid_bytes_count: self.state.bytes,
            field_count,
            index_count,
            errors: self.state.errors,
            valid_data: write_batch,
        }
    }
}

fn convert_qualified_line(
    line: QualifiedLine,
    table_chunk_map: &mut IndexMap<TableId, TableChunks>,
    gen1_duration: Gen1Duration,
) {
    // Add the row into the correct chunk in the table
    let chunk_time = gen1_duration.chunk_time_for_timestamp(Timestamp::new(line.row.time));
    let table_chunks = table_chunk_map.entry(line.table_id).or_default();
    table_chunks.push_row(chunk_time, line.row);
}

#[derive(Debug)]
struct QualifiedLine {
    table_id: TableId,
    row: Row,
    index_count: usize,
    field_count: usize,
}

fn apply_precision_to_timestamp(precision: Precision, ts: i64) -> i64 {
    let multiplier = match precision {
        Precision::Auto => match crate::guess_precision(ts) {
            Precision::Second => 1_000_000_000,
            Precision::Millisecond => 1_000_000,
            Precision::Microsecond => 1_000,
            Precision::Nanosecond => 1,

            Precision::Auto => unreachable!(),
        },
        Precision::Second => 1_000_000_000,
        Precision::Millisecond => 1_000_000,
        Precision::Microsecond => 1_000,
        Precision::Nanosecond => 1,
    };

    ts * multiplier
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::WriteValidator;
    use crate::{Precision, write_buffer::Error};

    use data_types::NamespaceName;
    use influxdb3_catalog::catalog::Catalog;
    use influxdb3_id::TableId;
    use influxdb3_wal::Gen1Duration;
    use iox_time::{MockProvider, Time};
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn write_validator_v1() -> Result<(), Error> {
        let node_id = Arc::from("sample-host-id");
        let obj_store = Arc::new(InMemory::new());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let namespace = NamespaceName::new("test").unwrap();
        let catalog = Arc::new(
            Catalog::new(node_id, obj_store, time_provider)
                .await
                .unwrap(),
        );
        let expected_sequence = catalog.sequence_number().next();
        let result = WriteValidator::initialize(namespace.clone(), Arc::clone(&catalog))?
            .v1_parse_lines_and_catalog_updates(
                "cpu,tag1=foo val1=\"bar\" 1234",
                false,
                Time::from_timestamp_nanos(0),
                Precision::Auto,
            )?
            .commit_catalog_changes()
            .await?
            .unwrap_success()
            .convert_lines_to_buffer(Gen1Duration::new_5m());

        println!("result: {result:?}");
        assert_eq!(result.line_count, 1);
        assert_eq!(result.field_count, 1);
        assert_eq!(result.index_count, 1);
        assert!(result.errors.is_empty());
        assert_eq!(expected_sequence, catalog.sequence_number());
        assert_eq!(result.valid_data.database_name.as_ref(), namespace.as_str());
        // cpu table
        let batch = result
            .valid_data
            .table_chunks
            .get(&TableId::from(0))
            .unwrap();
        assert_eq!(batch.row_count(), 1);

        // Validate another write, the result should be very similar, but now the catalog
        // has the table/columns added, so it will excercise a different code path:
        let expected_sequence = catalog.sequence_number();
        let result = WriteValidator::initialize(namespace.clone(), Arc::clone(&catalog))?
            .v1_parse_lines_and_catalog_updates(
                "cpu,tag1=foo val1=\"bar\" 1235",
                false,
                Time::from_timestamp_nanos(0),
                Precision::Auto,
            )?
            .commit_catalog_changes()
            .await?
            .unwrap_success()
            .convert_lines_to_buffer(Gen1Duration::new_5m());

        println!("result: {result:?}");
        assert_eq!(result.line_count, 1);
        assert_eq!(result.field_count, 1);
        assert_eq!(result.index_count, 1);
        assert_eq!(expected_sequence, catalog.sequence_number());
        assert!(result.errors.is_empty());

        // Validate another write, this time adding a new field:
        let expected_sequence = catalog.sequence_number().next();
        let result = WriteValidator::initialize(namespace.clone(), Arc::clone(&catalog))?
            .v1_parse_lines_and_catalog_updates(
                "cpu,tag1=foo val1=\"bar\",val2=false 1236",
                false,
                Time::from_timestamp_nanos(0),
                Precision::Auto,
            )?
            .commit_catalog_changes()
            .await?
            .unwrap_success()
            .convert_lines_to_buffer(Gen1Duration::new_5m());

        println!("result: {result:?}");
        assert_eq!(result.line_count, 1);
        assert_eq!(result.field_count, 2);
        assert_eq!(result.index_count, 1);
        assert!(result.errors.is_empty());
        assert_eq!(expected_sequence, catalog.sequence_number());

        Ok(())
    }
}
