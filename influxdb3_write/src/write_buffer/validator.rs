use std::{borrow::Cow, sync::Arc};

use crate::{write_buffer::Result, Precision, WriteLineError};
use data_types::{NamespaceName, Timestamp};
use indexmap::IndexMap;
use influxdb3_catalog::catalog::{
    influx_column_type_from_field_value, Catalog, DatabaseSchema, TableDefinition,
};

use influxdb3_id::{ColumnId, TableId};
use influxdb3_wal::{
    CatalogBatch, CatalogOp, Field, FieldAdditions, FieldData, FieldDefinition, Gen1Duration, Row,
    TableChunks, WriteBatch,
};
use influxdb_line_protocol::{parse_lines, v3, ParsedLine};
use iox_time::Time;
use schema::{InfluxColumnType, TIME_COLUMN_NAME};

use super::Error;

/// Type state for the [`WriteValidator`] after it has been initialized
/// with the catalog.
pub(crate) struct WithCatalog {
    catalog: Arc<Catalog>,
    db_schema: Arc<DatabaseSchema>,
    time_now_ns: i64,
}

/// Type state for the [`WriteValidator`] after it has parsed v1 or v3
/// line protocol.
pub(crate) struct LinesParsed {
    catalog: WithCatalog,
    lines: Vec<QualifiedLine>,
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
        time_now_ns: i64,
    ) -> Result<WriteValidator<WithCatalog>> {
        let db_schema = catalog.db_or_create(db_name.as_str())?;
        Ok(WriteValidator {
            state: WithCatalog {
                catalog,
                db_schema,
                time_now_ns,
            },
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
    ) -> Result<WriteValidator<LinesParsed>> {
        let mut errors = vec![];
        let mut lp_lines = lp.lines();
        let mut lines = vec![];
        let mut catalog_updates = vec![];
        let mut schema = Cow::Borrowed(self.state.db_schema.as_ref());

        for (line_idx, maybe_line) in v3::parse_lines(lp).enumerate() {
            let (qualified_line, catalog_op) = match maybe_line
                .map_err(|e| WriteLineError {
                    original_line: lp_lines.next().unwrap().to_string(),
                    line_number: line_idx + 1,
                    error_message: e.to_string(),
                })
                .and_then(|line| {
                    validate_and_qualify_v3_line(
                        &mut schema,
                        line_idx,
                        line,
                        lp_lines.next().unwrap(),
                    )
                }) {
                Ok((qualified_line, catalog_ops)) => (qualified_line, catalog_ops),
                Err(error) => {
                    if accept_partial {
                        errors.push(error);
                    } else {
                        return Err(Error::ParseError(error));
                    }
                    continue;
                }
            };

            if let Some(op) = catalog_op {
                catalog_updates.push(op);
            }

            lines.push(qualified_line);
        }

        let catalog_batch = if catalog_updates.is_empty() {
            None
        } else {
            let catalog_batch = CatalogBatch {
                database_id: self.state.db_schema.id,
                database_name: Arc::clone(&self.state.db_schema.name),
                time_ns: self.state.time_now_ns,
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
    ) -> Result<WriteValidator<LinesParsed>> {
        let mut errors = vec![];
        let mut lp_lines = lp.lines();
        let mut lines = vec![];
        let mut catalog_updates = vec![];
        let mut schema = Cow::Borrowed(self.state.db_schema.as_ref());

        for (line_idx, maybe_line) in parse_lines(lp).enumerate() {
            let (qualified_line, catalog_op) = match maybe_line
                .map_err(|e| WriteLineError {
                    // This unwrap is fine because we're moving line by line
                    // alongside the output from parse_lines
                    original_line: lp_lines.next().unwrap().to_string(),
                    line_number: line_idx + 1,
                    error_message: e.to_string(),
                })
                .and_then(|l| {
                    validate_and_qualify_v1_line(&mut schema, line_idx, l, lp_lines.next().unwrap())
                }) {
                Ok((qualified_line, catalog_op)) => (qualified_line, catalog_op),
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
            lines.push(qualified_line);
        }

        // All lines are parsed and validated, so all steps after this
        // are infallible, therefore, update the catalog if changes were
        // made to the schema:
        let catalog_batch = if catalog_updates.is_empty() {
            None
        } else {
            let catalog_batch = CatalogBatch {
                database_id: self.state.db_schema.id,
                time_ns: self.state.time_now_ns,
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

/// Type alias for storing new columns added by a write
type ColumnTracker = Vec<(ColumnId, Arc<str>, InfluxColumnType)>;

/// Validate an individual line of v3 line protocol and update the database
/// schema
///
/// The [`DatabaseSchema`] will be updated if the line is being written to a new table, or if
/// the line contains new fields. Note that for v3, the series key members must be consistent,
/// and therefore new tag columns will never be added after the first write.
///
/// This errors if the write is being performed against a v1 table, i.e., one that does not have
/// a series key.
fn validate_and_qualify_v3_line(
    db_schema: &mut Cow<'_, DatabaseSchema>,
    line_number: usize,
    line: v3::ParsedLine,
    raw_line: &str,
) -> Result<(QualifiedLine, Option<CatalogOp>), WriteLineError> {
    let mut catalog_op = None;
    let table_name = line.series.measurement.as_str();
    let qualified = if let Some(table_def) = db_schema.table_definition(table_name) {
        let table_id = table_def.table_id;
        if !table_def.is_v3() {
            return Err(WriteLineError {
                original_line: raw_line.to_string(),
                line_number,
                error_message: "received v3 write protocol for a table that uses the v1 data model"
                    .to_string(),
            });
        }
        // TODO: may be faster to compare using table def/column IDs than comparing with schema:
        match (
            table_def.influx_schema().series_key(),
            &line.series.series_key,
        ) {
            (Some(s), Some(l)) => {
                let l = l.iter().map(|sk| sk.0.as_str()).collect::<Vec<&str>>();
                if s != l {
                    return Err(WriteLineError {
                        original_line: raw_line.to_string(),
                        line_number,
                        error_message: format!(
                            "write to table {table_name} had the incorrect series key, \
                            expected: [{expected}], received: [{received}]",
                            table_name = table_def.table_name,
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
                            table_name = table_def.table_name,
                            key_members = s.join(", "),
                        ),
                    });
                }
            }
            (None, _) => unreachable!(),
        }

        let mut columns = ColumnTracker::with_capacity(line.column_count() + 1);

        // qualify the series key members:
        let tag_set = if let Some(sk) = &line.series.series_key {
            let mut ts = Vec::with_capacity(sk.len());
            for (key, val) in sk.iter() {
                let col_id =
                    table_def
                        .column_name_to_id(key.as_str())
                        .ok_or_else(|| WriteLineError {
                            original_line: raw_line.to_string(),
                            line_number,
                            error_message: format!(
                                "write contained invalid series key column ({key})\
                            that does not exist in the catalog table definition"
                            ),
                        })?;
                ts.push(Field::new(col_id, val));
            }
            Some(ts)
        } else {
            None
        };

        // qualify the fields:
        let mut field_set = Vec::with_capacity(line.field_set.len());
        for (field_name, field_val) in line.field_set.iter() {
            if let Some((col_id, col_def)) = table_def.column_def_and_id(field_name.as_str()) {
                let field_col_type = influx_column_type_from_field_value(field_val);
                let existing_col_type = col_def.data_type;
                if field_col_type != existing_col_type {
                    let field_name = field_name.to_string();
                    return Err(WriteLineError {
                        original_line: raw_line.to_string(),
                        line_number: line_number + 1,
                        error_message: format!(
                        "invalid field value in line protocol for field '{field_name}' on line \
                        {line_number}: expected type {expected}, but got {got}",
                        expected = existing_col_type,
                        got = field_col_type,
                    ),
                    });
                }
                field_set.push(Field::new(col_id, field_val));
            } else {
                let col_id = ColumnId::new();
                columns.push((
                    col_id,
                    Arc::from(field_name.as_str()),
                    influx_column_type_from_field_value(field_val),
                ));
                field_set.push(Field::new(col_id, field_val));
            }
        }

        // qualify the timestamp:
        let time_col_id = table_def
            .column_name_to_id(TIME_COLUMN_NAME)
            .unwrap_or_else(ColumnId::new);
        let timestamp = (time_col_id, line.timestamp);

        // if we have new columns defined, add them to the db_schema table so that subsequent lines
        // won't try to add the same definitions. Collect these additions into a catalog op, which
        // will be applied to the catalog with any other ops after all lines in the write request
        // have been parsed and validated.
        if !columns.is_empty() {
            let database_name = Arc::clone(&db_schema.name);
            let database_id = db_schema.id;
            let db_schema = db_schema.to_mut();
            let mut new_table_def = db_schema
                .tables
                .get_mut(&table_id)
                .unwrap()
                .as_ref()
                .clone();

            let mut fields = Vec::with_capacity(columns.len());
            for (id, name, influx_type) in columns.iter() {
                fields.push(FieldDefinition::new(*id, Arc::clone(name), influx_type));
            }
            catalog_op = Some(CatalogOp::AddFields(FieldAdditions {
                database_id,
                database_name,
                table_id: new_table_def.table_id,
                table_name: Arc::clone(&new_table_def.table_name),
                field_definitions: fields,
            }));

            new_table_def
                .add_columns(columns)
                .map_err(|e| WriteLineError {
                    original_line: raw_line.to_string(),
                    line_number: line_number + 1,
                    error_message: e.to_string(),
                })?;
            db_schema.insert_table(table_id, Arc::new(new_table_def));
        }
        QualifiedLine {
            table_id,
            tag_set,
            field_set,
            timestamp,
        }
    } else {
        let table_id = TableId::new();
        let mut columns = Vec::new();
        let mut key = Vec::new();
        let tag_set = if let Some(series_key) = &line.series.series_key {
            let mut ts = Vec::with_capacity(series_key.len());
            for (sk, sv) in series_key.iter() {
                let col_id = ColumnId::new();
                key.push(col_id);
                columns.push((col_id, Arc::from(sk.as_str()), InfluxColumnType::Tag));
                ts.push(Field::new(col_id, sv));
            }
            Some(ts)
        } else {
            None
        };
        let mut field_set = Vec::with_capacity(line.field_set.len());
        for (field_name, field_val) in line.field_set.iter() {
            let col_id = ColumnId::new();
            columns.push((
                col_id,
                Arc::from(field_name.as_str()),
                influx_column_type_from_field_value(field_val),
            ));
            field_set.push(Field::new(col_id, field_val));
        }
        // Always add time last on new table:
        let time_col_id = ColumnId::new();
        columns.push((
            time_col_id,
            Arc::from(TIME_COLUMN_NAME),
            InfluxColumnType::Timestamp,
        ));
        let timestamp = (time_col_id, line.timestamp);

        let table_name = table_name.into();

        let mut fields = Vec::with_capacity(columns.len());
        for (id, name, influx_type) in &columns {
            fields.push(FieldDefinition::new(*id, Arc::clone(name), influx_type));
        }

        let table = TableDefinition::new(
            table_id,
            Arc::clone(&table_name),
            columns,
            Some(key.clone()),
        )
        .map_err(|e| WriteLineError {
            original_line: raw_line.to_string(),
            line_number: line_number + 1,
            error_message: e.to_string(),
        })?;

        let table_definition_op = CatalogOp::CreateTable(influxdb3_wal::TableDefinition {
            table_id,
            database_id: db_schema.id,
            database_name: Arc::clone(&db_schema.name),
            table_name: Arc::clone(&table_name),
            field_definitions: fields,
            key: Some(key),
        });
        catalog_op = Some(table_definition_op);

        let db_schema = db_schema.to_mut();
        assert!(
            db_schema.insert_table(table_id, Arc::new(table)).is_none(),
            "attempted to overwrite existing table"
        );
        QualifiedLine {
            table_id,
            tag_set,
            field_set,
            timestamp,
        }
    };

    Ok((qualified, catalog_op))
}

/// Validate a line of line protocol against the given schema definition
///
/// This is for scenarios where a write comes in for a table that exists, but may have
/// invalid field types, based on the pre-existing schema.
///
/// An error will also be produced if the write, which is for the v1 data model, is targetting
/// a v3 table.
fn validate_and_qualify_v1_line(
    db_schema: &mut Cow<'_, DatabaseSchema>,
    line_number: usize,
    line: ParsedLine,
    _raw_line: &str,
) -> Result<(QualifiedLine, Option<CatalogOp>), WriteLineError> {
    let mut catalog_op = None;
    let table_name = line.series.measurement.as_str();
    let qualified = if let Some(table_def) = db_schema.table_definition(table_name) {
        if table_def.is_v3() {
            return Err(WriteLineError {
                original_line: line.to_string(),
                line_number,
                error_message: "received v1 write protocol for a table that uses the v3 data model"
                    .to_string(),
            });
        }
        // This table already exists, so update with any new columns if present:
        let mut columns = ColumnTracker::with_capacity(line.column_count() + 1);
        let tag_set = if let Some(tag_set) = &line.series.tag_set {
            let mut ts = Vec::with_capacity(tag_set.len());
            for (tag_key, tag_val) in tag_set {
                if let Some(col_id) = table_def.column_name_to_id(tag_key.as_str()) {
                    ts.push(Field::new(col_id, FieldData::Tag(tag_val.to_string())));
                } else {
                    let col_id = ColumnId::new();
                    columns.push((col_id, Arc::from(tag_key.as_str()), InfluxColumnType::Tag));
                    ts.push(Field::new(col_id, FieldData::Tag(tag_val.to_string())));
                }
            }
            Some(ts)
        } else {
            None
        };
        let mut field_set = Vec::with_capacity(line.field_set.len());
        for (field_name, field_val) in line.field_set.iter() {
            // This field already exists, so check the incoming type matches existing type:
            if let Some((col_id, col_def)) = table_def.column_def_and_id(field_name.as_str()) {
                let field_col_type = influx_column_type_from_field_value(field_val);
                let existing_col_type = col_def.data_type;
                if field_col_type != existing_col_type {
                    let field_name = field_name.to_string();
                    return Err(WriteLineError {
                        original_line: line.to_string(),
                        line_number: line_number + 1,
                        error_message: format!(
                            "invalid field value in line protocol for field '{field_name}' on line \
                            {line_number}: expected type {expected}, but got {got}",
                            expected = existing_col_type,
                            got = field_col_type,
                        ),
                    });
                }
                field_set.push(Field::new(col_id, field_val));
            } else {
                let col_id = ColumnId::new();
                columns.push((
                    col_id,
                    Arc::from(field_name.as_str()),
                    influx_column_type_from_field_value(field_val),
                ));
                field_set.push(Field::new(col_id, field_val));
            }
        }

        let time_col_id = table_def
            .column_name_to_id(TIME_COLUMN_NAME)
            .unwrap_or_default();
        let timestamp = (time_col_id, line.timestamp);

        // if we have new columns defined, add them to the db_schema table so that subsequent lines
        // won't try to add the same definitions. Collect these additions into a catalog op, which
        // will be applied to the catalog with any other ops after all lines in the write request
        // have been parsed and validated.
        if !columns.is_empty() {
            let database_name = Arc::clone(&db_schema.name);
            let database_id = db_schema.id;
            let table_name: Arc<str> = Arc::clone(&table_def.table_name);
            let table_id = table_def.table_id;

            let mut fields = Vec::with_capacity(columns.len());
            for (id, name, influx_type) in &columns {
                fields.push(FieldDefinition::new(*id, Arc::clone(name), influx_type));
            }

            let db_schema = db_schema.to_mut();
            let mut new_table_def = db_schema
                .tables
                .get_mut(&table_id)
                // unwrap is safe due to the surrounding if let condition:
                .unwrap()
                .as_ref()
                .clone();
            new_table_def
                .add_columns(columns)
                .map_err(|e| WriteLineError {
                    original_line: line.to_string(),
                    line_number: line_number + 1,
                    error_message: e.to_string(),
                })?;
            db_schema.insert_table(table_id, Arc::new(new_table_def));

            catalog_op = Some(CatalogOp::AddFields(FieldAdditions {
                database_name,
                database_id,
                table_id,
                table_name,
                field_definitions: fields,
            }));
        }
        QualifiedLine {
            table_id: table_def.table_id,
            tag_set,
            field_set,
            timestamp,
        }
    } else {
        let table_id = TableId::new();
        // This is a new table, so build up its columns:
        let mut columns = Vec::new();
        let tag_set = if let Some(tag_set) = &line.series.tag_set {
            let mut ts = Vec::with_capacity(tag_set.len());
            for (tag_key, tag_val) in tag_set {
                let col_id = ColumnId::new();
                ts.push(Field::new(col_id, FieldData::Tag(tag_val.to_string())));
                columns.push((col_id, Arc::from(tag_key.as_str()), InfluxColumnType::Tag));
            }
            Some(ts)
        } else {
            None
        };
        let mut field_set = Vec::with_capacity(line.field_set.len());
        for (field_name, field_val) in &line.field_set {
            let col_id = ColumnId::new();
            columns.push((
                col_id,
                Arc::from(field_name.as_str()),
                influx_column_type_from_field_value(field_val),
            ));
            field_set.push(Field::new(col_id, field_val));
        }
        // Always add time last on new table:
        let time_col_id = ColumnId::new();
        columns.push((
            time_col_id,
            Arc::from(TIME_COLUMN_NAME),
            InfluxColumnType::Timestamp,
        ));
        let timestamp = (time_col_id, line.timestamp);

        let table_name = table_name.into();
        let mut fields = Vec::with_capacity(columns.len());

        for (id, name, influx_type) in &columns {
            fields.push(FieldDefinition::new(*id, Arc::clone(name), influx_type));
        }
        catalog_op = Some(CatalogOp::CreateTable(influxdb3_wal::TableDefinition {
            table_id,
            database_id: db_schema.id,
            database_name: Arc::clone(&db_schema.name),
            table_name: Arc::clone(&table_name),
            field_definitions: fields,
            key: None,
        }));

        let table = TableDefinition::new(table_id, Arc::clone(&table_name), columns, None).unwrap();

        let db_schema = db_schema.to_mut();
        assert!(
            db_schema.insert_table(table_id, Arc::new(table)).is_none(),
            "attempted to overwrite existing table"
        );
        QualifiedLine {
            table_id,
            tag_set,
            field_set,
            timestamp,
        }
    };

    Ok((qualified, catalog_op))
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

impl WriteValidator<LinesParsed> {
    /// Convert a set of valid parsed `v3` lines to a [`ValidatedLines`] which will
    /// be buffered and written to the WAL, if configured.
    ///
    /// This involves splitting out the writes into different batches for each chunk, which will
    /// map to the `Gen1Duration`. This function should be infallible, because
    /// the schema for incoming writes has been fully validated.
    pub(crate) fn convert_lines_to_buffer(
        self,
        ingest_time: Time,
        gen1_duration: Gen1Duration,
        precision: Precision,
    ) -> ValidatedLines {
        let mut table_chunks = IndexMap::new();
        let line_count = self.state.lines.len();
        let mut field_count = 0;
        let mut index_count = 0;

        for line in self.state.lines.into_iter() {
            field_count += line.field_set.len();
            index_count += line.tag_set.as_ref().map(|tags| tags.len()).unwrap_or(0);

            convert_qualified_line(
                line,
                &mut table_chunks,
                ingest_time,
                gen1_duration,
                precision,
            );
        }

        let write_batch = WriteBatch::new(
            self.state.catalog.db_schema.id,
            Arc::clone(&self.state.catalog.db_schema.name),
            table_chunks,
        );

        ValidatedLines {
            line_count,
            field_count,
            index_count,
            errors: self.state.errors,
            valid_data: write_batch,
            catalog_updates: self.state.catalog_batch,
        }
    }
}

fn convert_qualified_line(
    line: QualifiedLine,
    table_chunk_map: &mut IndexMap<TableId, TableChunks>,
    ingest_time: Time,
    gen1_duration: Gen1Duration,
    precision: Precision,
) {
    // Set up row values:
    let mut fields = Vec::with_capacity(line.column_count() + 1);

    // Add series key columns:
    if let Some(tag_set) = line.tag_set {
        fields.extend(tag_set);
    }

    // Add fields columns:
    fields.extend(line.field_set);

    // Add time column:
    let time_value_nanos = line
        .timestamp
        .1
        .map(|ts| apply_precision_to_timestamp(precision, ts))
        .unwrap_or(ingest_time.timestamp_nanos());
    fields.push(Field::new(
        line.timestamp.0,
        FieldData::Timestamp(time_value_nanos),
    ));

    // Add the row into the correct chunk in the table
    let chunk_time = gen1_duration.chunk_time_for_timestamp(Timestamp::new(time_value_nanos));
    let table_chunks = table_chunk_map.entry(line.table_id).or_default();
    table_chunks.push_row(
        chunk_time,
        Row {
            time: time_value_nanos,
            fields,
        },
    );
}

struct QualifiedLine {
    table_id: TableId,
    tag_set: Option<Vec<Field>>,
    field_set: Vec<Field>,
    timestamp: (ColumnId, Option<i64>),
}

impl QualifiedLine {
    fn column_count(&self) -> usize {
        self.tag_set.as_ref().map(|ts| ts.len()).unwrap_or(0) + self.field_set.len() + 1
    }
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
    use influxdb3_id::TableId;
    use influxdb3_wal::Gen1Duration;
    use iox_time::Time;

    use super::WriteValidator;

    #[test]
    fn write_validator_v1() -> Result<(), Error> {
        let host_id = Arc::from("sample-host-id");
        let instance_id = Arc::from("sample-instance-id");
        let namespace = NamespaceName::new("test").unwrap();
        let catalog = Arc::new(Catalog::new(host_id, instance_id));
        let result = WriteValidator::initialize(namespace.clone(), catalog, 0)?
            .v1_parse_lines_and_update_schema("cpu,tag1=foo val1=\"bar\" 1234", false)?
            .convert_lines_to_buffer(
                Time::from_timestamp_nanos(0),
                Gen1Duration::new_5m(),
                Precision::Auto,
            );

        assert_eq!(result.line_count, 1);
        assert_eq!(result.field_count, 1);
        assert_eq!(result.index_count, 1);
        assert!(result.errors.is_empty());

        assert_eq!(result.valid_data.database_name.as_ref(), namespace.as_str());
        // cpu table
        let batch = result
            .valid_data
            .table_chunks
            .get(&TableId::from(0))
            .unwrap();
        assert_eq!(batch.row_count(), 1);

        Ok(())
    }
}
