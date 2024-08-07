use std::{borrow::Cow, sync::Arc};

use crate::{write_buffer::Result, Precision, WriteLineError};
use data_types::{NamespaceName, Timestamp};
use hashbrown::HashMap;
use influxdb3_catalog::catalog::{
    influx_column_type_from_field_value, Catalog, DatabaseSchema, TableDefinition,
};

use influxdb3_wal::{
    CatalogBatch, CatalogOp, Field, FieldAdditions, FieldData, FieldDataType, FieldDefinition,
    Level0Duration, Row, TableChunks, WriteBatch,
};
use influxdb_line_protocol::{parse_lines, v3, FieldValue, ParsedLine};
use iox_time::Time;
use schema::{InfluxColumnType, TIME_COLUMN_NAME};

use super::Error;

/// Type state for the [`WriteValidator`] after it has been initialized
/// with the catalog.
pub(crate) struct WithCatalog {
    catalog: Arc<Catalog>,
    db_schema: Arc<DatabaseSchema>,
}

/// Type state for the [`WriteValidator`] after it has parsed v1 or v3
/// line protocol.
pub(crate) struct LinesParsed<'raw, PL> {
    catalog: WithCatalog,
    lines: Vec<(PL, &'raw str)>,
    catalog_batch: Option<CatalogBatch>,
    errors: Vec<WriteLineError>,
}

/// A state machine for validating v1 or v3 line protocol and updating
/// the [`Catalog`] with new tables or schema changes.
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
        let db_schema = catalog.db_or_create(db_name.as_str())?;
        Ok(WriteValidator {
            state: WithCatalog { catalog, db_schema },
        })
    }

    /// Parse the incoming lines of line protocol using the v3 parser and update
    /// the [`DatabaseSchema`] if:
    ///
    /// * A new table is being added
    /// * New fields, or tags are being added to an existing table
    ///
    /// # Implementation Note
    ///
    /// If this function succeeds, then the catalog will receive an update, so
    /// steps following this should be infallible.
    pub(crate) fn v3_parse_lines_and_update_schema(
        self,
        lp: &str,
        accept_partial: bool,
    ) -> Result<WriteValidator<LinesParsed<'_, v3::ParsedLine<'_>>>> {
        let mut errors = vec![];
        let mut lp_lines = lp.lines().peekable();
        let mut lines = vec![];
        let mut catalog_updates = vec![];
        let mut schema = Cow::Borrowed(self.state.db_schema.as_ref());

        for (line_idx, maybe_line) in v3::parse_lines(lp).enumerate() {
            let (line, catalog_op) = match maybe_line
                .map_err(|e| WriteLineError {
                    original_line: lp_lines.next().unwrap().to_string(),
                    line_number: line_idx + 1,
                    error_message: e.to_string(),
                })
                .and_then(|l| validate_v3_line(&mut schema, line_idx, l, lp_lines.peek().unwrap()))
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

            if let Some(op) = catalog_op {
                catalog_updates.push(op);
            }

            lines.push((line, lp_lines.next().unwrap()));
        }

        let catalog_batch = if catalog_updates.is_empty() {
            None
        } else {
            let catalog_batch = CatalogBatch {
                database_name: Arc::clone(&self.state.db_schema.name),
                ops: catalog_updates,
            };
            self.state.catalog.apply_catalog_batch(&catalog_batch)?;
            Some(catalog_batch)
        };

        Ok(WriteValidator {
            state: LinesParsed {
                catalog: self.state,
                lines,
                catalog_batch,
                errors,
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
        let mut catalog_updates = vec![];
        let mut schema = Cow::Borrowed(self.state.db_schema.as_ref());

        for (line_idx, maybe_line) in parse_lines(lp).enumerate() {
            let (line, catalog_op) = match maybe_line
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
            if let Some(op) = catalog_op {
                catalog_updates.push(op);
            }
            // This unwrap is fine because we're moving line by line
            // alongside the output from parse_lines
            lines.push((line, lp_lines.next().unwrap()));
        }

        // All lines are parsed and validated, so all steps after this
        // are infallible, therefore, update the catalog if changes were
        // made to the schema:
        let catalog_batch = if catalog_updates.is_empty() {
            None
        } else {
            let catalog_batch = CatalogBatch {
                database_name: Arc::clone(&self.state.db_schema.name),
                ops: catalog_updates,
            };
            self.state.catalog.apply_catalog_batch(&catalog_batch)?;
            Some(catalog_batch)
        };

        Ok(WriteValidator {
            state: LinesParsed {
                catalog: self.state,
                lines,
                errors,
                catalog_batch,
            },
        })
    }
}

/// Validate an individual line of v3 line protocol and update the database
/// schema
///
/// The [`DatabaseSchema`] will be updated if the line is being written to a new table, or if
/// the line contains new fields. Note that for v3, the series key members must be consistent,
/// and therefore new tag columns will never be added after the first write.
///
/// This errors if the write is being performed against a v1 table, i.e., one that does not have
/// a series key.
fn validate_v3_line<'a>(
    db_schema: &mut Cow<'_, DatabaseSchema>,
    line_number: usize,
    line: v3::ParsedLine<'a>,
    raw_line: &str,
) -> Result<(v3::ParsedLine<'a>, Option<CatalogOp>), WriteLineError> {
    let mut catalog_op = None;
    let table_name = line.series.measurement.as_str();
    if let Some(table_def) = db_schema.get_table(table_name) {
        if !table_def.is_v3() {
            return Err(WriteLineError {
                original_line: raw_line.to_string(),
                line_number,
                error_message: "received v3 write protocol for a table that uses the v1 data model"
                    .to_string(),
            });
        }
        let mut columns = Vec::with_capacity(line.column_count() + 1);
        match (table_def.schema().series_key(), &line.series.series_key) {
            (Some(s), Some(l)) => {
                let l = l.iter().map(|sk| sk.0.as_str()).collect::<Vec<&str>>();
                if s != l {
                    return Err(WriteLineError {
                        original_line: raw_line.to_string(),
                        line_number,
                        error_message: format!(
                            "write to table {table_name} had the incorrect series key, \
                            expected: [{expected}], received: [{received}]",
                            table_name = table_def.name,
                            expected = s.join(", "),
                            received = l.join(", "),
                        ),
                    });
                }
            }
            (Some(s), None) => {
                if !s.is_empty() {
                    return Err(WriteLineError {
                        original_line: raw_line.to_string(),
                        line_number,
                        error_message: format!(
                            "write to table {table_name} was missing a series key, the series key \
                            contains [{key_members}]",
                            table_name = table_def.name,
                            key_members = s.join(", "),
                        ),
                    });
                }
            }
            (None, _) => unreachable!(),
        }
        if let Some(series_key) = &line.series.series_key {
            for (sk, _) in series_key.iter() {
                if !table_def.column_exists(sk) {
                    columns.push((sk.to_string(), InfluxColumnType::Tag));
                }
            }
        }
        for (field_name, field_val) in line.field_set.iter() {
            if let Some(schema_col_type) = table_def.field_type_by_name(field_name) {
                let field_col_type = influx_column_type_from_field_value(field_val);
                if field_col_type != schema_col_type {
                    let field_name = field_name.to_string();
                    return Err(WriteLineError {
                        original_line: raw_line.to_string(),
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

        // if we have new columns defined, add them to the db_schema table so that subsequent lines
        // won't try to add the same definitions. Collect these additions into a catalog op, which
        // will be applied to the catalog with any other ops after all lines in the write request
        // have been parsed and validated.
        if !columns.is_empty() {
            let database_name = Arc::clone(&db_schema.name);
            let t = db_schema.to_mut().tables.get_mut(table_name).unwrap();

            let mut fields = Vec::with_capacity(columns.len());
            for (name, influx_type) in &columns {
                fields.push(FieldDefinition {
                    name: name.as_str().into(),
                    data_type: FieldDataType::from(influx_type),
                });
            }
            catalog_op = Some(CatalogOp::AddFields(FieldAdditions {
                database_name,
                table_name: Arc::clone(&t.name),
                field_definitions: fields,
            }));

            t.add_columns(columns).map_err(|e| WriteLineError {
                original_line: raw_line.to_string(),
                line_number: line_number + 1,
                error_message: e.to_string(),
            })?;
        }
    } else {
        let mut columns = Vec::new();
        let mut key = Vec::new();
        if let Some(series_key) = &line.series.series_key {
            for (sk, _) in series_key.iter() {
                key.push(sk.to_string());
                columns.push((sk.to_string(), InfluxColumnType::Tag));
            }
        }
        for (field_name, field_val) in line.field_set.iter() {
            columns.push((
                field_name.to_string(),
                influx_column_type_from_field_value(field_val),
            ));
        }
        // Always add time last on new table:
        columns.push((TIME_COLUMN_NAME.to_string(), InfluxColumnType::Timestamp));

        let table_name = table_name.into();

        let mut fields = Vec::with_capacity(columns.len());
        for (name, influx_type) in &columns {
            fields.push(FieldDefinition {
                name: name.as_str().into(),
                data_type: FieldDataType::from(influx_type),
            });
        }

        let table = TableDefinition::new(Arc::clone(&table_name), columns, Some(key.clone()))
            .map_err(|e| WriteLineError {
                original_line: raw_line.to_string(),
                line_number: line_number + 1,
                error_message: e.to_string(),
            })?;

        let table_definition_op = CatalogOp::CreateTable(influxdb3_wal::TableDefinition {
            database_name: Arc::clone(&db_schema.name),
            table_name: Arc::clone(&table_name),
            field_definitions: fields,
            key: Some(key),
        });
        catalog_op = Some(table_definition_op);

        assert!(
            db_schema
                .to_mut()
                .tables
                .insert(table_name, table)
                .is_none(),
            "attempted to overwrite existing table"
        )
    }

    Ok((line, catalog_op))
}

/// Validate a line of line protocol against the given schema definition
///
/// This is for scenarios where a write comes in for a table that exists, but may have
/// invalid field types, based on the pre-existing schema.
///
/// An error will also be produced if the write, which is for the v1 data model, is targetting
/// a v3 table.
fn validate_v1_line<'a>(
    db_schema: &mut Cow<'_, DatabaseSchema>,
    line_number: usize,
    line: ParsedLine<'a>,
) -> Result<(ParsedLine<'a>, Option<CatalogOp>), WriteLineError> {
    let mut catalog_op = None;
    let table_name = line.series.measurement.as_str();
    if let Some(table_def) = db_schema.get_table(table_name) {
        if table_def.is_v3() {
            return Err(WriteLineError {
                original_line: line.to_string(),
                line_number,
                error_message: "received v1 write protocol for a table that uses the v3 data model"
                    .to_string(),
            });
        }
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

        // if we have new columns defined, add them to the db_schema table so that subsequent lines
        // won't try to add the same definitions. Collect these additions into a catalog op, which
        // will be applied to the catalog with any other ops after all lines in the write request
        // have been parsed and validated.
        if !columns.is_empty() {
            let database_name = Arc::clone(&db_schema.name);
            let table_name = Arc::clone(&table_def.name);

            let mut fields = Vec::with_capacity(columns.len());
            for (name, influx_type) in &columns {
                fields.push(FieldDefinition {
                    name: name.as_str().into(),
                    data_type: FieldDataType::from(influx_type),
                });
            }

            // unwrap is safe due to the surrounding if let condition:
            let t = db_schema
                .to_mut()
                .tables
                .get_mut(table_name.as_ref())
                .unwrap();
            t.add_columns(columns).map_err(|e| WriteLineError {
                original_line: line.to_string(),
                line_number: line_number + 1,
                error_message: e.to_string(),
            })?;

            catalog_op = Some(CatalogOp::AddFields(FieldAdditions {
                database_name,
                table_name,
                field_definitions: fields,
            }));
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

        let table_name = table_name.into();
        let mut fields = Vec::with_capacity(columns.len());

        for (name, influx_type) in &columns {
            fields.push(FieldDefinition {
                name: name.as_str().into(),
                data_type: FieldDataType::from(influx_type),
            });
        }
        catalog_op = Some(CatalogOp::CreateTable(influxdb3_wal::TableDefinition {
            database_name: Arc::clone(&db_schema.name),
            table_name: Arc::clone(&table_name),
            field_definitions: fields,
            key: None,
        }));

        let table = TableDefinition::new(
            Arc::clone(&table_name),
            columns,
            Option::<Vec<String>>::None,
        )
        .unwrap();

        assert!(
            db_schema
                .to_mut()
                .tables
                .insert(table_name, table)
                .is_none(),
            "attempted to overwrite existing table"
        );
    }

    Ok((line, catalog_op))
}

/// Result of conversion from line protocol to valid chunked data
/// for the buffer.
#[derive(Debug)]
pub(crate) struct ValidatedLines {
    /// Number of lines passed in
    pub(crate) line_count: usize,
    /// Number of fields passed in
    pub(crate) field_count: usize,
    /// Number of index columns passed in, whether tags (v1) or series keys (v3)
    pub(crate) index_count: usize,
    /// Any errors that occurred while parsing the lines
    pub(crate) errors: Vec<WriteLineError>,
    /// Only valid lines will be converted into a WriteBatch
    pub(crate) valid_data: WriteBatch,
    /// If any catalog updates were made, they will be included here
    pub(crate) catalog_updates: Option<CatalogBatch>,
}

impl<'lp> WriteValidator<LinesParsed<'lp, v3::ParsedLine<'lp>>> {
    /// Convert a set of valid parsed `v3` lines to a [`ValidatedLines`] which will
    /// be buffered and written to the WAL, if configured.
    ///
    /// This involves splitting out the writes into different batches for each chunk, which will
    /// map to the `Level0Duration`. This function should be infallible, because
    /// the schema for incoming writes has been fully validated.
    pub(crate) fn convert_lines_to_buffer(
        self,
        ingest_time: Time,
        level_0_duration: Level0Duration,
        precision: Precision,
    ) -> ValidatedLines {
        let mut table_chunks = HashMap::new();
        let line_count = self.state.lines.len();
        let mut field_count = 0;
        let mut series_key_count = 0;

        for (line, _raw_line) in self.state.lines.into_iter() {
            field_count += line.field_set.len();
            series_key_count += line
                .series
                .series_key
                .as_ref()
                .map(|sk| sk.len())
                .unwrap_or(0);

            convert_v3_parsed_line(
                line,
                &mut table_chunks,
                ingest_time,
                level_0_duration,
                precision,
            );
        }

        let write_batch =
            WriteBatch::new(Arc::clone(&self.state.catalog.db_schema.name), table_chunks);

        ValidatedLines {
            line_count,
            field_count,
            index_count: series_key_count,
            errors: self.state.errors,
            valid_data: write_batch,
            catalog_updates: self.state.catalog_batch,
        }
    }
}

fn convert_v3_parsed_line(
    line: v3::ParsedLine<'_>,
    table_chunk_map: &mut HashMap<Arc<str>, TableChunks>,
    ingest_time: Time,
    level_0_duration: Level0Duration,
    precision: Precision,
) {
    // Set up row values:
    let mut fields = Vec::with_capacity(line.column_count() + 1);

    // Add series key columns:
    if let Some(series_key) = line.series.series_key {
        for (sk, sv) in series_key.iter() {
            fields.push(Field {
                name: sk.to_string().into(),
                value: sv.into(),
            });
        }
    }

    // Add fields columns:
    for (name, val) in line.field_set {
        fields.push(Field {
            name: name.to_string().into(),
            value: val.into(),
        });
    }

    // Add time column:
    // TODO: change the default time resolution to microseconds in v3
    let time_value_nanos = line
        .timestamp
        .map(|ts| apply_precision_to_timestamp(precision, ts))
        .unwrap_or(ingest_time.timestamp_nanos());
    fields.push(Field {
        name: TIME_COLUMN_NAME.to_string().into(),
        value: FieldData::Timestamp(time_value_nanos),
    });

    // Add the row into the correct chunk in the table
    let chunk_time = level_0_duration.chunk_time_for_timestamp(Timestamp::new(time_value_nanos));
    let table_name: Arc<str> = line.series.measurement.to_string().into();
    let table_chunks = table_chunk_map.entry(Arc::clone(&table_name)).or_default();
    table_chunks.push_row(
        chunk_time,
        Row {
            time: time_value_nanos,
            fields,
        },
    );
}

impl<'lp> WriteValidator<LinesParsed<'lp, ParsedLine<'lp>>> {
    /// Convert a set of valid parsed lines to a [`ValidatedLines`] which will
    /// be buffered and written to the WAL, if configured.
    ///
    /// This involves splitting out the writes into different batches for each chunk, which will
    /// map to the `Level0Duration`. This function should be infallible, because
    /// the schema for incoming writes has been fully validated.
    pub(crate) fn convert_lines_to_buffer(
        self,
        ingest_time: Time,
        level_0_duration: Level0Duration,
        precision: Precision,
    ) -> ValidatedLines {
        let mut table_chunks = HashMap::new();
        let line_count = self.state.lines.len();
        let mut field_count = 0;
        let mut tag_count = 0;

        for (line, _raw_line) in self.state.lines.into_iter() {
            field_count += line.field_set.len();
            tag_count += line.series.tag_set.as_ref().map(|t| t.len()).unwrap_or(0);

            convert_v1_parsed_line(
                line,
                &mut table_chunks,
                ingest_time,
                level_0_duration,
                precision,
            );
        }

        let write_batch =
            WriteBatch::new(Arc::clone(&self.state.catalog.db_schema.name), table_chunks);

        ValidatedLines {
            line_count,
            field_count,
            index_count: tag_count,
            errors: self.state.errors,
            valid_data: write_batch,
            catalog_updates: self.state.catalog_batch,
        }
    }
}

fn convert_v1_parsed_line(
    line: ParsedLine<'_>,
    table_chunk_map: &mut HashMap<Arc<str>, TableChunks>,
    ingest_time: Time,
    level_0_duration: Level0Duration,
    precision: Precision,
) {
    // now that we've ensured all columns exist in the schema, construct the actual row and values
    // while validating the column types match.
    let mut values = Vec::with_capacity(line.column_count() + 1);

    // validate tags, collecting any new ones that must be inserted, or adding the values
    if let Some(tag_set) = line.series.tag_set {
        for (tag_key, value) in tag_set {
            let value = Field {
                name: tag_key.to_string().into(),
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
            name: field_name.to_string().into(),
            value: field_data,
        };
        values.push(value);
    }

    // set the time value
    let time_value_nanos = line
        .timestamp
        .map(|ts| apply_precision_to_timestamp(precision, ts))
        .unwrap_or(ingest_time.timestamp_nanos());

    let chunk_time = level_0_duration.chunk_time_for_timestamp(Timestamp::new(time_value_nanos));

    values.push(Field {
        name: TIME_COLUMN_NAME.to_string().into(),
        value: FieldData::Timestamp(time_value_nanos),
    });

    let table_name: Arc<str> = line.series.measurement.to_string().into();
    let table_chunks = table_chunk_map.entry(table_name).or_default();
    table_chunks.push_row(
        chunk_time,
        Row {
            time: time_value_nanos,
            fields: values,
        },
    );
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

    use crate::{catalog::Catalog, write_buffer::Error, Precision};
    use data_types::NamespaceName;
    use influxdb3_wal::Level0Duration;
    use iox_time::Time;

    use super::WriteValidator;

    #[test]
    fn write_validator_v1() -> Result<(), Error> {
        let namespace = NamespaceName::new("test").unwrap();
        let catalog = Arc::new(Catalog::new());
        let result = WriteValidator::initialize(namespace.clone(), catalog)?
            .v1_parse_lines_and_update_schema("cpu,tag1=foo val1=\"bar\" 1234", false)?
            .convert_lines_to_buffer(
                Time::from_timestamp_nanos(0),
                Level0Duration::new_5m(),
                Precision::Auto,
            );

        assert_eq!(result.line_count, 1);
        assert_eq!(result.field_count, 1);
        assert_eq!(result.index_count, 1);
        assert!(result.errors.is_empty());

        assert_eq!(result.valid_data.database_name.as_ref(), namespace.as_str());
        let batch = result.valid_data.table_chunks.get("cpu").unwrap();
        assert_eq!(batch.row_count(), 1);

        Ok(())
    }
}
