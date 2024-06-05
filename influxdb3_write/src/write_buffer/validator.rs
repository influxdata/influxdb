use std::{borrow::Cow, collections::HashMap, sync::Arc};

use data_types::NamespaceName;
use influxdb_line_protocol::{parse_lines, FieldValue, ParsedLine};
use iox_time::Time;
use schema::{InfluxColumnType, TIME_COLUMN_NAME};

use crate::{
    catalog::{influx_column_type_from_field_value, Catalog, DatabaseSchema, TableDefinition},
    write_buffer::Result,
    LpWriteOp, Precision, SegmentDuration, SequenceNumber, WalOp, WriteLineError,
};

use super::{Error, Field, FieldData, Row, TableBatchMap, ValidSegmentedData};

pub(crate) struct WithCatalog {
    db_name: NamespaceName<'static>,
    catalog: Arc<Catalog>,
    sequence: SequenceNumber,
    db_schema: Arc<DatabaseSchema>,
}

pub(crate) struct LinesParsed<'raw, PL> {
    catalog: WithCatalog,
    lines: Vec<(PL, &'raw str)>,
    errors: Vec<WriteLineError>,
}

pub(crate) struct WriteValidator<State> {
    state: State,
}

impl WriteValidator<WithCatalog> {
    /// Initialize the [`WriteValidator`] by getting a handle to, or creating
    /// a handle to the [`DatabaseSchema`] for the given namespace name `db_name`.
    pub(crate) fn initialize(
        db_name: NamespaceName<'static>,
        catalog: Arc<Catalog>,
    ) -> Result<WriteValidator<WithCatalog>> {
        let (sequence, db_schema) = catalog.db_or_create(db_name.as_str())?;
        Ok(WriteValidator {
            state: WithCatalog {
                db_name,
                catalog,
                sequence,
                db_schema,
            },
        })
    }

    /// Parse the incoming lines of line protocol using the v1 parser and update
    /// the [`DatabaseSchema`] if:
    ///
    /// * A new table is being added
    /// * New fields, or tags are being added to an existing table
    ///
    /// # Implementation Note
    ///
    /// If this function succeeds, then the catalog will receive an update, so
    /// steps following this should be infallible.
    pub(crate) fn v1_parse_lines_and_update_schema(
        self,
        lp: &str,
        accept_partial: bool,
    ) -> Result<WriteValidator<LinesParsed<'_, ParsedLine<'_>>>> {
        let mut errors = vec![];
        let mut lp_lines = lp.lines();
        let mut lines = vec![];
        let mut schema = Cow::Borrowed(self.state.db_schema.as_ref());

        for (line_idx, maybe_line) in parse_lines(lp).enumerate() {
            let line = match maybe_line
                .map_err(|e| WriteLineError {
                    // This unwrap is fine because we're moving line by line
                    // alongside the output from parse_lines
                    original_line: lp_lines.next().unwrap().to_string(),
                    line_number: line_idx + 1,
                    error_message: e.to_string(),
                })
                .and_then(|l| validate_v1_line(&mut schema, line_idx, l))
            {
                Ok(line) => line,
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
            lines.push((line, lp_lines.next().unwrap()));
        }

        // All lines are parsed and validated, so all steps after this
        // are infallible, therefore, update the catalog if changes were
        // made to the schema:
        if let Cow::Owned(schema) = schema {
            self.state
                .catalog
                .replace_database(self.state.sequence, Arc::new(schema))?;
        }

        Ok(WriteValidator {
            state: LinesParsed {
                catalog: self.state,
                lines,
                errors,
            },
        })
    }
}

/// Validate a line of line protocol against the given schema definition
///
/// This is for scenarios where a write comes in for a table that exists, but may have
/// invalid field types, based on the pre-existing schema.
fn validate_v1_line<'a>(
    db_schema: &mut Cow<'_, DatabaseSchema>,
    line_number: usize,
    line: ParsedLine<'a>,
) -> Result<ParsedLine<'a>, WriteLineError> {
    let table_name = line.series.measurement.as_str();
    if let Some(table_def) = db_schema.get_table(table_name) {
        // This table already exists, so update with any new columns if present:
        let mut columns = Vec::with_capacity(line.column_count() + 1);
        if let Some(tag_set) = &line.series.tag_set {
            for (tag_key, _) in tag_set {
                if !table_def.column_exists(tag_key) {
                    columns.push((tag_key.to_string(), InfluxColumnType::Tag));
                }
            }
        }
        for (field_name, field_val) in line.field_set.iter() {
            // This field already exists, so check the incoming type matches existing type:
            if let Some(schema_col_type) = table_def.field_type_by_name(field_name) {
                let field_col_type = influx_column_type_from_field_value(field_val);
                if field_col_type != schema_col_type {
                    let field_name = field_name.to_string();
                    return Err(WriteLineError {
                        original_line: line.to_string(),
                        line_number: line_number + 1,
                        error_message: format!(
                        "invalid field value in line protocol for field '{field_name}' on line \
                        {line_number}: expected type {expected}, but got {got}",
                        expected = schema_col_type,
                        got = field_col_type,
                    ),
                    });
                }
            } else {
                columns.push((
                    field_name.to_string(),
                    influx_column_type_from_field_value(field_val),
                ));
            }
        }
        if !columns.is_empty() {
            // unwrap is safe due to the surrounding if let condition:
            let t = db_schema.to_mut().tables.get_mut(table_name).unwrap();
            t.add_columns(columns);
        }
    } else {
        // This is a new table, so build up its columns:
        let mut columns = Vec::new();
        if let Some(tag_set) = &line.series.tag_set {
            for (tag_key, _) in tag_set {
                columns.push((tag_key.to_string(), InfluxColumnType::Tag));
            }
        }
        for (field_name, field_val) in &line.field_set {
            columns.push((
                field_name.to_string(),
                influx_column_type_from_field_value(field_val),
            ));
        }
        // Always add time last on new table:
        columns.push((TIME_COLUMN_NAME.to_string(), InfluxColumnType::Timestamp));
        let table = TableDefinition::new(table_name, columns);

        assert!(
            db_schema
                .to_mut()
                .tables
                .insert(table_name.to_string(), table)
                .is_none(),
            "attempted to overwrite existing table"
        );
    }

    Ok(line)
}

/// Result of conversion from line protocol to valid segmented data
/// for the buffer.
#[derive(Debug, Default)]
pub(crate) struct ValidatedLines {
    /// Number of lines passed in
    pub(crate) line_count: usize,
    /// Number of fields passed in
    pub(crate) field_count: usize,
    /// Number of tags passed in
    pub(crate) tag_count: usize,
    /// Any errors that occurred while parsing the lines
    pub(crate) errors: Vec<WriteLineError>,
    /// Only valid lines from what was passed in to validate, segmented based on the
    /// timestamps of the data.
    pub(crate) valid_segmented_data: Vec<ValidSegmentedData>,
}

impl<'raw> WriteValidator<LinesParsed<'raw, ParsedLine<'raw>>> {
    /// Convert a set of valid parsed lines to a [`ValidatedLines`] which will
    /// be buffered and written to the WAL, if configured.
    ///
    /// This involves splitting out the writes into different batches for any
    /// segment affected by the write.
    pub(crate) fn convert_lines_to_buffer(
        self,
        ingest_time: Time,
        segment_duration: SegmentDuration,
        precision: Precision,
    ) -> ValidatedLines {
        let mut segment_table_batches = HashMap::new();
        let line_count = self.state.lines.len();
        let mut field_count = 0;
        let mut tag_count = 0;

        for (line, raw_line) in self.state.lines.into_iter() {
            field_count += line.field_set.len();
            tag_count += line.series.tag_set.as_ref().map(|t| t.len()).unwrap_or(0);

            convert_parsed_line(
                line,
                raw_line,
                &mut segment_table_batches,
                ingest_time,
                segment_duration,
                precision,
            );
        }

        let valid_segmented_data = segment_table_batches
            .into_iter()
            .map(|(segment_start, table_batches)| ValidSegmentedData {
                database_name: self.state.catalog.db_name.clone(),
                segment_start,
                table_batches: table_batches.table_batches,
                wal_op: WalOp::LpWrite(LpWriteOp {
                    db_name: self.state.catalog.db_name.to_string(),
                    lp: table_batches.lines.join("\n"),
                    default_time: ingest_time.timestamp_nanos(),
                    precision,
                }),
                starting_catalog_sequence_number: self.state.catalog.sequence,
            })
            .collect();

        ValidatedLines {
            line_count,
            field_count,
            tag_count,
            errors: self.state.errors,
            valid_segmented_data,
        }
    }
}

fn convert_parsed_line<'a>(
    line: ParsedLine<'_>,
    raw_line: &'a str,
    segment_table_batches: &mut HashMap<Time, TableBatchMap<'a>>,
    ingest_time: Time,
    segment_duration: SegmentDuration,
    precision: Precision,
) {
    // now that we've ensured all columns exist in the schema, construct the actual row and values
    // while validating the column types match.
    let mut values = Vec::with_capacity(line.column_count() + 1);

    // validate tags, collecting any new ones that must be inserted, or adding the values
    if let Some(tag_set) = line.series.tag_set {
        for (tag_key, value) in tag_set {
            let value = Field {
                name: tag_key.to_string(),
                value: FieldData::Tag(value.to_string()),
            };
            values.push(value);
        }
    }

    // validate fields, collecting any new ones that must be inserted, or adding values
    for (field_name, value) in line.field_set {
        let field_data = match value {
            FieldValue::I64(v) => FieldData::Integer(v),
            FieldValue::F64(v) => FieldData::Float(v),
            FieldValue::U64(v) => FieldData::UInteger(v),
            FieldValue::Boolean(v) => FieldData::Boolean(v),
            FieldValue::String(v) => FieldData::String(v.to_string()),
        };
        let value = Field {
            name: field_name.to_string(),
            value: field_data,
        };
        values.push(value);
    }

    // set the time value
    let time_value_nanos = line
        .timestamp
        .map(|ts| {
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
        })
        .unwrap_or(ingest_time.timestamp_nanos());

    let segment_start = segment_duration.start_time(time_value_nanos / 1_000_000_000);

    values.push(Field {
        name: TIME_COLUMN_NAME.to_string(),
        value: FieldData::Timestamp(time_value_nanos),
    });

    let table_batch_map = segment_table_batches.entry(segment_start).or_default();

    let table_batch = table_batch_map
        .table_batches
        .entry(line.series.measurement.to_string())
        .or_default();
    table_batch.rows.push(Row {
        time: time_value_nanos,
        fields: values,
    });

    table_batch_map.lines.push(raw_line);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use data_types::NamespaceName;
    use iox_time::Time;

    use crate::{catalog::Catalog, write_buffer::Error, Precision, SegmentDuration};

    use super::WriteValidator;

    #[test]
    fn write_validator_v1() -> Result<(), Error> {
        let namespace = NamespaceName::new("test").unwrap();
        let catalog = Arc::new(Catalog::new());
        let result = WriteValidator::initialize(namespace.clone(), catalog)?
            .v1_parse_lines_and_update_schema("cpu,tag1=foo val1=\"bar\" 1234", false)?
            .convert_lines_to_buffer(
                Time::from_timestamp_nanos(0),
                SegmentDuration::new_5m(),
                Precision::Auto,
            );

        assert_eq!(result.line_count, 1);
        assert_eq!(result.field_count, 1);
        assert_eq!(result.tag_count, 1);
        assert!(result.errors.is_empty());

        let data = &result.valid_segmented_data[0];
        assert_eq!(data.database_name, namespace);
        let batch = data.table_batches.get("cpu").unwrap();
        assert_eq!(batch.rows.len(), 1);

        println!("{result:#?}");

        Ok(())
    }
}
