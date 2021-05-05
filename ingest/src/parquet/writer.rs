//! This module contains the code to write table data to parquet
use internal_types::schema::{InfluxColumnType, InfluxFieldType, Schema};
use observability_deps::tracing::{debug, log::warn};
use parquet::file::writer::ParquetWriter;
use parquet::{
    self,
    basic::{
        Compression, Encoding, IntType, LogicalType, Repetition, TimeUnit, TimestampType,
        Type as PhysicalType,
    },
    errors::ParquetError,
    file::{
        properties::{WriterProperties, WriterPropertiesBuilder},
        writer::{FileWriter, SerializedFileWriter, TryClone},
    },
    schema::types::{ColumnPath, Type},
};
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    fmt,
    io::{Seek, Write},
    str::FromStr,
    sync::Arc,
};

use super::metadata::parquet_schema_as_string;
use packers::{Error as TableError, IOxTableWriter, Packers};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(r#"{}, underlying parqet error {}"#, message, source))]
    ParquetLibraryError {
        message: String,
        source: ParquetError,
    },

    #[snafu(display(r#"Could not get packer for column {}"#, column_number))]
    MismatchedColumns { column_number: usize },

    #[snafu(display(
        r#"Unknown compression level '{}'. Valid options 'max' or 'compatibility'"#,
        compression_level
    ))]
    UnknownCompressionLevel { compression_level: String },

    #[snafu(display(r#"Unsupported datatype for parquet writing: {:?}"#, data_type,))]
    UnsupportedDataType { data_type: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for TableError {
    fn from(other: Error) -> Self {
        Self::Data {
            source: Box::new(other),
        }
    }
}

/// Specify the desired compression level when writing parquet files
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompressionLevel {
    /// Minimize the size of the written parquet file
    Maximum,

    // Attempt to maximize interoperability with other ecosystem tools.
    //
    // See https://github.com/influxdata/influxdb_iox/issues/184
    Compatibility,
}

impl FromStr for CompressionLevel {
    type Err = Error;

    fn from_str(compression_level: &str) -> Result<Self, Self::Err> {
        match compression_level {
            "max" => Ok(Self::Maximum),
            "compatibility" => Ok(Self::Compatibility),
            _ => UnknownCompressionLevel { compression_level }.fail(),
        }
    }
}

/// A `IOxParquetTableWriter` is used for writing batches of rows
/// parquet files.
pub struct IOxParquetTableWriter<W>
where
    W: ParquetWriter,
{
    parquet_schema: Arc<parquet::schema::types::Type>,
    file_writer: SerializedFileWriter<W>,
}

impl<W: 'static> IOxParquetTableWriter<W>
where
    W: Write + Seek + TryClone,
{
    /// Create a new TableWriter that writes its rows to something
    /// that implements the trait (e.g. std::File). For example:
    ///
    /// ```
    /// # use std::fs;
    /// # use internal_types::schema::{builder::SchemaBuilder, InfluxFieldType};
    /// # use packers::IOxTableWriter;
    /// # use packers::{Packer, Packers};
    /// # use ingest::parquet::writer::{IOxParquetTableWriter, CompressionLevel};
    /// # use parquet::data_type::ByteArray;
    ///
    /// let schema = SchemaBuilder::new()
    ///      .measurement("measurement_name")
    ///      .tag("tag1")
    ///      .influx_field("field1", InfluxFieldType::Integer)
    ///      .timestamp()
    ///      .build()
    ///      .unwrap();
    ///
    /// let mut packers: Vec<Packers> = vec![
    ///     Packers::Bytes(Packer::new()),  // 0: tag1
    ///     Packers::Integer(Packer::new()), // 1: field1
    ///     Packers::Integer(Packer::new()), // 2: timestamp
    /// ];
    ///
    /// packers[0].bytes_packer_mut().push(ByteArray::from("tag1")); // tag1 val
    /// packers[1].i64_packer_mut().push(100);                       // field1 val
    /// packers[2].push_none();                                      // no timestamp
    ///
    /// // Write to '/tmp/example.parquet'
    /// let mut output_file_name = std::env::temp_dir();
    /// output_file_name.push("example.parquet");
    /// let output_file = fs::File::create(output_file_name.as_path()).unwrap();
    ///
    /// let compression_level = CompressionLevel::Compatibility;
    ///
    /// let mut parquet_writer = IOxParquetTableWriter::new(
    ///     &schema, compression_level, output_file)
    ///     .unwrap();
    ///
    /// // write the actual data to parquet
    /// parquet_writer.write_batch(&packers).unwrap();
    ///
    /// // Closing the writer closes the data and the file
    /// parquet_writer.close().unwrap();
    ///
    /// # std::fs::remove_file(output_file_name);
    /// ```
    pub fn new(
        schema: &Schema,
        compression_level: CompressionLevel,
        writer: W,
    ) -> Result<Self, Error> {
        let writer_props = create_writer_props(&schema, compression_level);
        let parquet_schema = convert_to_parquet_schema(&schema)?;

        let file_writer =
            SerializedFileWriter::new(writer, Arc::clone(&parquet_schema), writer_props).context(
                ParquetLibraryError {
                    message: String::from("Error trying to create a SerializedFileWriter"),
                },
            )?;

        let parquet_writer = Self {
            parquet_schema,
            file_writer,
        };
        debug!(
            "ParqutWriter created for schema: {}",
            parquet_schema_as_string(&parquet_writer.parquet_schema)
        );
        Ok(parquet_writer)
    }
}
impl<W: 'static> IOxTableWriter for IOxParquetTableWriter<W>
where
    W: Write + Seek + TryClone,
{
    /// Writes a batch of packed data to the output file in a single
    /// column chunk
    ///
    /// TODO: better control of column chunks
    fn write_batch(&mut self, packers: &[Packers]) -> Result<(), TableError> {
        // now write out the data
        let mut row_group_writer =
            self.file_writer
                .next_row_group()
                .context(ParquetLibraryError {
                    message: String::from("Error creating next row group writer"),
                })?;

        use parquet::column::writer::ColumnWriter::*;
        let mut column_number = 0;
        while let Some(mut col_writer) =
            row_group_writer
                .next_column()
                .context(ParquetLibraryError {
                    message: String::from("Can't create the next row_group_writer"),
                })?
        {
            let packer = packers
                .get(column_number)
                .context(MismatchedColumns { column_number })
                .map_err(TableError::from_other)?;

            // TODO(edd) This seems super awkward and not the right way to do it...
            // We know we have a direct mapping between a col_writer (ColumnWriter)
            // type and a Packers variant. We also know that we do exactly the same
            // work for each variant (we just dispatch to the writ_batch method)
            // on the column write.
            //
            // I think this match could be so much shorter but not sure how yet.
            match col_writer {
                BoolColumnWriter(ref mut w) => {
                    let p = packer.bool_packer();
                    let n = w
                        .write_batch(&p.some_values(), Some(&p.def_levels()), None)
                        .context(ParquetLibraryError {
                            message: String::from("Can't write_batch with bool values"),
                        })?;
                    debug!("Wrote {} rows of bool data", n);
                }
                Int32ColumnWriter(_) => unreachable!("ParquetWriter does not support INT32 data"),
                Int64ColumnWriter(ref mut w) => {
                    let p = packer.i64_packer();
                    let n = w
                        .write_batch(&p.some_values(), Some(&p.def_levels()), None)
                        .context(ParquetLibraryError {
                            message: String::from("Can't write_batch with int64 values"),
                        })?;
                    debug!("Wrote {} rows of int64 data", n);
                }
                Int96ColumnWriter(_) => unreachable!("ParquetWriter does not support INT96 data"),
                FloatColumnWriter(_) => {
                    unreachable!("ParquetWriter does not support FLOAT (32-bit float) data")
                }
                DoubleColumnWriter(ref mut w) => {
                    let p = packer.f64_packer();
                    let n = w
                        .write_batch(&p.some_values(), Some(&p.def_levels()), None)
                        .context(ParquetLibraryError {
                            message: String::from("Can't write_batch with f64 values"),
                        })?;
                    debug!("Wrote {} rows of f64 data", n);
                }
                ByteArrayColumnWriter(ref mut w) => {
                    let p = packer.bytes_packer();
                    let n = w
                        .write_batch(&p.some_values(), Some(&p.def_levels()), None)
                        .context(ParquetLibraryError {
                            message: String::from("Can't write_batch with byte array values"),
                        })?;
                    debug!("Wrote {} rows of byte data", n);
                }
                FixedLenByteArrayColumnWriter(_) => {
                    unreachable!("ParquetWriter does not support FIXED_LEN_BYTE_ARRAY data");
                }
            };
            debug!("Closing column writer for {}", column_number);
            row_group_writer
                .close_column(col_writer)
                .context(ParquetLibraryError {
                    message: String::from("Can't close column writer"),
                })?;
            column_number += 1;
        }
        self.file_writer
            .close_row_group(row_group_writer)
            .context(ParquetLibraryError {
                message: String::from("Can't close row group writer"),
            })?;
        Ok(())
    }

    /// Closes this writer, and finalizes the underlying parquet file
    fn close(&mut self) -> Result<(), TableError> {
        self.file_writer.close().context(ParquetLibraryError {
            message: String::from("Can't close file writer"),
        })?;
        Ok(())
    }
}

impl<W> fmt::Debug for IOxParquetTableWriter<W>
where
    W: Write + Seek + TryClone,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IOxParquetTableWriter")
            .field("parquet_schema", &self.parquet_schema)
            .field("file_writer", &"SerializedFileWriter")
            .finish()
    }
}

// Converts from line protocol `Schema` to the equivalent parquet schema `Type`.
fn convert_to_parquet_schema(schema: &Schema) -> Result<Arc<parquet::schema::types::Type>, Error> {
    let mut parquet_columns = Vec::new();

    for (i, (influxdb_column_type, field)) in schema.iter().enumerate() {
        debug!(
            "Determining parquet schema for column[{}] {:?} -> {:?}",
            i, influxdb_column_type, field
        );
        let (physical_type, logical_type) = match influxdb_column_type {
            Some(InfluxColumnType::Tag) => (
                PhysicalType::BYTE_ARRAY,
                Some(LogicalType::STRING(Default::default())),
            ),
            Some(InfluxColumnType::Field(InfluxFieldType::Boolean)) => {
                (PhysicalType::BOOLEAN, None)
            }
            Some(InfluxColumnType::Field(InfluxFieldType::Float)) => (PhysicalType::DOUBLE, None),
            Some(InfluxColumnType::Field(InfluxFieldType::Integer)) => (PhysicalType::INT64, None),
            Some(InfluxColumnType::Field(InfluxFieldType::UInteger)) => (
                PhysicalType::INT64,
                Some(LogicalType::INTEGER(IntType {
                    bit_width: 64,
                    is_signed: false,
                })),
            ),
            Some(InfluxColumnType::Field(InfluxFieldType::String)) => (
                PhysicalType::BYTE_ARRAY,
                Some(LogicalType::STRING(Default::default())),
            ),
            Some(InfluxColumnType::Timestamp) => {
                (
                    PhysicalType::INT64,
                    Some(LogicalType::TIMESTAMP(TimestampType {
                        unit: TimeUnit::NANOS(Default::default()),
                        // Indicates that the timestamp is stored as UTC values
                        is_adjusted_to_u_t_c: true,
                    })),
                )
            }
            None => {
                return UnsupportedDataType {
                    data_type: format!("Arrow type: {:?}", field.data_type()),
                }
                .fail();
            }
        };

        // All fields are optional
        let parquet_column_builder = Type::primitive_type_builder(field.name(), physical_type)
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(logical_type);

        let parquet_column_type = parquet_column_builder
            .build()
            .context(ParquetLibraryError {
                message: String::from("Can't create parquet column type"),
            })?;
        debug!(
            "Using parquet type {} for column {:?}",
            parquet_schema_as_string(&parquet_column_type),
            field.name()
        );

        parquet_columns.push(Arc::new(parquet_column_type));
    }

    let measurement = schema.measurement().unwrap();
    let parquet_schema = Type::group_type_builder(measurement)
        .with_fields(&mut parquet_columns)
        .build()
        .context(ParquetLibraryError {
            message: String::from("Can't create top level parquet schema"),
        })?;

    Ok(Arc::new(parquet_schema))
}

fn set_integer_encoding(
    influxdb_column_type: InfluxColumnType,
    compression_level: CompressionLevel,
    col_path: ColumnPath,
    builder: WriterPropertiesBuilder,
) -> WriterPropertiesBuilder {
    match compression_level {
        CompressionLevel::Maximum => {
            debug!(
                "Setting encoding of {:?} col {} to DELTA_BINARY_PACKED (Maximum)",
                influxdb_column_type, col_path
            );
            builder
                .set_column_encoding(col_path.clone(), Encoding::DELTA_BINARY_PACKED)
                .set_column_dictionary_enabled(col_path, false)
        }
        CompressionLevel::Compatibility => {
            debug!(
                "Setting encoding of {:?} col {} to PLAIN/RLE (Compatibility)",
                influxdb_column_type, col_path
            );
            builder
                .set_column_encoding(col_path.clone(), Encoding::PLAIN)
                .set_column_dictionary_enabled(col_path, true)
        }
    }
}

/// Create the parquet writer properties (which defines the encoding
/// and compression for each column) for a given schema.
fn create_writer_props(
    schema: &Schema,
    compression_level: CompressionLevel,
) -> Arc<WriterProperties> {
    let mut builder = WriterProperties::builder();

    // TODO: Maybe tweak more of these settings for maximum performance.

    // start off with GZIP for maximum compression ratio (at expense of CPU
    // performance...)
    builder = builder.set_compression(Compression::GZIP);

    // Setup encoding as defined in
    // https://github.com/influxdata/influxdb_iox/blob/alamb/encoding_thoughts/docs/encoding_thoughts.md
    //
    // Note: the property writer builder's default is to encode
    // everything with dictionary encoding, and it turns out that
    // dictionary encoding overrides all other encodings. Thus, we
    // must explicitly disable dictionary encoding when another
    // encoding is desired.
    for (i, (influxdb_column_type, field)) in schema.iter().enumerate() {
        let column_name = field.name().clone();
        let col_path: ColumnPath = column_name.into();

        match influxdb_column_type {
            Some(InfluxColumnType::Field(InfluxFieldType::Boolean)) => {
                debug!(
                    "Setting encoding of {:?} col {} to RLE",
                    influxdb_column_type, i
                );
                builder = builder
                    .set_column_encoding(col_path.clone(), Encoding::RLE)
                    .set_column_dictionary_enabled(col_path, false);
            }
            Some(InfluxColumnType::Field(InfluxFieldType::Integer)) => {
                builder = set_integer_encoding(
                    influxdb_column_type.unwrap(),
                    compression_level,
                    col_path,
                    builder,
                )
            }
            Some(InfluxColumnType::Field(InfluxFieldType::UInteger)) => {
                builder = set_integer_encoding(
                    influxdb_column_type.unwrap(),
                    compression_level,
                    col_path,
                    builder,
                )
            }
            Some(InfluxColumnType::Field(InfluxFieldType::Float)) => {
                debug!(
                    "Setting encoding of {:?} col {} to PLAIN",
                    influxdb_column_type, col_path
                );
                builder = builder
                    .set_column_encoding(col_path.clone(), Encoding::PLAIN)
                    .set_column_dictionary_enabled(col_path, false);
            }
            Some(InfluxColumnType::Field(InfluxFieldType::String)) => {
                debug!(
                    "Setting encoding of non-tag val {:?} col {} to DELTA_LENGTH_BYTE_ARRAY",
                    influxdb_column_type, col_path
                );
                builder = builder
                    .set_column_encoding(col_path.clone(), Encoding::DELTA_LENGTH_BYTE_ARRAY)
                    .set_column_dictionary_enabled(col_path, false);
            }
            // tag values are often very much repeated
            Some(InfluxColumnType::Tag) => {
                debug!(
                    "Setting encoding of tag val {:?} col {} to dictionary",
                    influxdb_column_type, col_path
                );
                builder = builder.set_column_dictionary_enabled(col_path, true);
            }
            Some(InfluxColumnType::Timestamp) => {
                builder = set_integer_encoding(
                    influxdb_column_type.unwrap(),
                    compression_level,
                    col_path,
                    builder,
                )
            }
            None => {
                warn!(
                    "Using default parquet encoding for column {} which has no LP annotations",
                    field.name()
                )
            }
        };
    }

    // Even though the 'set_statistics_enabled()' method is called here, the
    // resulting parquet file does not appear to have statistics enabled.
    //
    // This is due to the fact that the underlying rust parquet
    // library does not support statistics generation at this time.
    let props = builder
        .set_statistics_enabled(true)
        .set_created_by("InfluxDB IOx".to_string())
        .build();
    Arc::new(props)
}

#[cfg(test)]
mod tests {
    use internal_types::schema::builder::SchemaBuilder;

    use super::*;

    // Collapses multiple spaces into a single space, and removes trailing
    // whitespace
    fn normalize_spaces(s: &str) -> String {
        // previous non space, if any
        let mut prev: Option<char> = None;
        let no_double_spaces: String = s
            .chars()
            .filter(|c| {
                if let Some(prev_c) = prev {
                    if prev_c == ' ' && *c == ' ' {
                        return false;
                    }
                }
                prev = Some(*c);
                true
            })
            .collect();
        no_double_spaces
            .trim_end_matches(|c| c == ' ' || c == '\n')
            .to_string()
    }

    #[test]
    fn test_convert_to_parquet_schema() {
        let schema = SchemaBuilder::new()
            .measurement("measurement_name")
            .tag("tag1")
            .influx_field("string_field", InfluxFieldType::String)
            .influx_field("float_field", InfluxFieldType::Float)
            .influx_field("int_field", InfluxFieldType::Integer)
            .influx_field("uint_field", InfluxFieldType::UInteger)
            .influx_field("bool_field", InfluxFieldType::Boolean)
            .timestamp()
            .build()
            .unwrap();

        let parquet_schema = convert_to_parquet_schema(&schema).expect("conversion successful");
        let parquet_schema_string = normalize_spaces(&parquet_schema_as_string(&parquet_schema));
        let expected_schema_string = normalize_spaces(
            r#"message measurement_name {
            OPTIONAL BYTE_ARRAY tag1 (STRING);
            OPTIONAL BYTE_ARRAY string_field (STRING);
            OPTIONAL DOUBLE float_field;
            OPTIONAL INT64 int_field;
            OPTIONAL INT64 uint_field (INTEGER(64,false));
            OPTIONAL BOOLEAN bool_field;
            OPTIONAL INT64 time (TIMESTAMP(NANOS,true));
}"#,
        );

        assert_eq!(parquet_schema_string, expected_schema_string);
    }

    fn make_test_schema() -> Schema {
        SchemaBuilder::new()
            .measurement("measurement_name")
            .tag("tag1")
            .influx_field("string_field", InfluxFieldType::String)
            .influx_field("float_field", InfluxFieldType::Float)
            .influx_field("int_field", InfluxFieldType::Integer)
            .influx_field("bool_field", InfluxFieldType::Boolean)
            .timestamp()
            .build()
            .unwrap()
    }

    #[test]
    fn test_create_writer_props_maximum() {
        do_test_create_writer_props(CompressionLevel::Maximum);
    }

    #[test]
    fn test_create_writer_props_compatibility() {
        do_test_create_writer_props(CompressionLevel::Compatibility);
    }

    fn do_test_create_writer_props(compression_level: CompressionLevel) {
        let schema = make_test_schema();
        let writer_props = create_writer_props(&schema, compression_level);

        let tag1_colpath = ColumnPath::from("tag1");
        assert_eq!(writer_props.encoding(&tag1_colpath), None);
        assert_eq!(writer_props.compression(&tag1_colpath), Compression::GZIP);
        assert_eq!(writer_props.dictionary_enabled(&tag1_colpath), true);
        assert_eq!(writer_props.statistics_enabled(&tag1_colpath), true);

        let string_field_colpath = ColumnPath::from("string_field");
        assert_eq!(
            writer_props.encoding(&string_field_colpath),
            Some(Encoding::DELTA_LENGTH_BYTE_ARRAY)
        );
        assert_eq!(
            writer_props.compression(&string_field_colpath),
            Compression::GZIP
        );
        assert_eq!(
            writer_props.dictionary_enabled(&string_field_colpath),
            false
        );
        assert_eq!(writer_props.statistics_enabled(&string_field_colpath), true);

        let float_field_colpath = ColumnPath::from("float_field");
        assert_eq!(
            writer_props.encoding(&float_field_colpath),
            Some(Encoding::PLAIN)
        );
        assert_eq!(
            writer_props.compression(&float_field_colpath),
            Compression::GZIP
        );
        assert_eq!(writer_props.dictionary_enabled(&float_field_colpath), false);
        assert_eq!(writer_props.statistics_enabled(&float_field_colpath), true);

        let int_field_colpath = ColumnPath::from("int_field");
        match compression_level {
            CompressionLevel::Maximum => {
                assert_eq!(
                    writer_props.encoding(&int_field_colpath),
                    Some(Encoding::DELTA_BINARY_PACKED)
                );
                assert_eq!(writer_props.dictionary_enabled(&int_field_colpath), false);
            }
            CompressionLevel::Compatibility => {
                assert_eq!(
                    writer_props.encoding(&int_field_colpath),
                    Some(Encoding::PLAIN)
                );
                assert_eq!(writer_props.dictionary_enabled(&int_field_colpath), true);
            }
        }
        assert_eq!(
            writer_props.compression(&int_field_colpath),
            Compression::GZIP
        );
        assert_eq!(writer_props.statistics_enabled(&int_field_colpath), true);

        let bool_field_colpath = ColumnPath::from("bool_field");
        assert_eq!(
            writer_props.encoding(&bool_field_colpath),
            Some(Encoding::RLE)
        );
        assert_eq!(
            writer_props.compression(&bool_field_colpath),
            Compression::GZIP
        );
        assert_eq!(writer_props.dictionary_enabled(&bool_field_colpath), false);
        assert_eq!(writer_props.statistics_enabled(&bool_field_colpath), true);

        let timestamp_field_colpath = ColumnPath::from("time");
        match compression_level {
            CompressionLevel::Maximum => {
                assert_eq!(
                    writer_props.encoding(&timestamp_field_colpath),
                    Some(Encoding::DELTA_BINARY_PACKED)
                );
                assert_eq!(
                    writer_props.dictionary_enabled(&timestamp_field_colpath),
                    false
                );
            }
            CompressionLevel::Compatibility => {
                assert_eq!(
                    writer_props.encoding(&timestamp_field_colpath),
                    Some(Encoding::PLAIN)
                );
                assert_eq!(
                    writer_props.dictionary_enabled(&timestamp_field_colpath),
                    true
                );
            }
        }

        assert_eq!(
            writer_props.compression(&timestamp_field_colpath),
            Compression::GZIP
        );

        assert_eq!(
            writer_props.statistics_enabled(&timestamp_field_colpath),
            true
        );
    }

    #[test]
    fn compression_level() {
        assert_eq!(
            CompressionLevel::from_str("max").ok().unwrap(),
            CompressionLevel::Maximum
        );
        assert_eq!(
            CompressionLevel::from_str("compatibility").ok().unwrap(),
            CompressionLevel::Compatibility
        );

        let bad = CompressionLevel::from_str("madxxxx");
        assert!(bad.is_err());
    }
}
