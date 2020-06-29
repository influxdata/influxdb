//! This module contains the code to write delorean table data to parquet
use log::debug;
use parquet::{
    basic::{Compression, Encoding, LogicalType, Repetition, Type as PhysicalType},
    errors::ParquetError,
    file::{
        properties::{WriterProperties, WriterPropertiesBuilder},
        reader::TryClone,
        writer::{FileWriter, SerializedFileWriter},
    },
    schema::types::{ColumnPath, Type},
};
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    fmt,
    io::{Seek, Write},
    rc::Rc,
    str::FromStr,
};

use crate::metadata::parquet_schema_as_string;
use delorean_table::{DeloreanTableWriter, Error as TableError, Packers};

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
    MAXIMUM,

    // Attempt to maximize interoperability with other ecosystem tools.
    //
    // See https://github.com/influxdata/delorean/issues/184
    COMPATIBILITY,
}

impl FromStr for CompressionLevel {
    type Err = Error;

    fn from_str(compression_level: &str) -> Result<Self, Self::Err> {
        match compression_level {
            "max" => Ok(Self::MAXIMUM),
            "compatibility" => Ok(Self::COMPATIBILITY),
            _ => UnknownCompressionLevel { compression_level }.fail(),
        }
    }
}

/// A `DeloreanParquetTableWriter` is used for writing batches of rows
/// represented using the structures in `delorean_table` to parquet files.
pub struct DeloreanParquetTableWriter<W>
where
    W: Write + Seek + TryClone,
{
    parquet_schema: Rc<parquet::schema::types::Type>,
    file_writer: SerializedFileWriter<W>,
}

impl<W: 'static> DeloreanParquetTableWriter<W>
where
    W: Write + Seek + TryClone,
{
    /// Create a new TableWriter that writes its rows to something
    /// that implements the trait (e.g. std::File). For example:
    ///
    /// ```
    /// # use std::fs;
    /// # use delorean_table_schema;
    /// # use delorean_table_schema::DataType;
    /// # use delorean_table::DeloreanTableWriter;
    /// # use delorean_table::packers::{Packer, Packers};
    /// # use delorean_parquet::writer::{DeloreanParquetTableWriter, CompressionLevel};
    /// # use parquet::data_type::ByteArray;
    ///
    /// let schema = delorean_table_schema::SchemaBuilder::new("measurement_name")
    ///      .tag("tag1")
    ///      .field("field1", delorean_table_schema::DataType::Integer)
    ///      .build();
    ///
    /// let mut packers: Vec<Packers> = vec![
    ///     Packers::String(Packer::new()),  // 0: tag1
    ///     Packers::Integer(Packer::new()), // 1: field1
    ///     Packers::Integer(Packer::new()), // 2: timestamp
    /// ];
    ///
    /// packers[0].str_packer_mut().push(ByteArray::from("tag1")); // tag1 val
    /// packers[1].i64_packer_mut().push(100);                     // field1 val
    /// packers[2].push_none();                                    // no timestamp
    ///
    /// // Write to '/tmp/example.parquet'
    /// let mut output_file_name = std::env::temp_dir();
    /// output_file_name.push("example.parquet");
    /// let output_file = fs::File::create(output_file_name.as_path()).unwrap();
    ///
    /// let compression_level = CompressionLevel::COMPATIBILITY;
    ///
    /// let mut parquet_writer = DeloreanParquetTableWriter::new(
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
        schema: &delorean_table_schema::Schema,
        compression_level: CompressionLevel,
        writer: W,
    ) -> Result<Self, Error> {
        let writer_props = create_writer_props(&schema, compression_level);
        let parquet_schema = convert_to_parquet_schema(&schema)?;

        let file_writer = SerializedFileWriter::new(writer, parquet_schema.clone(), writer_props)
            .context(ParquetLibraryError {
            message: String::from("Error trying to create a SerializedFileWriter"),
        })?;

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
impl<W: 'static> DeloreanTableWriter for DeloreanParquetTableWriter<W>
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
                        .write_batch(p.values(), Some(p.def_levels()), Some(p.rep_levels()))
                        .context(ParquetLibraryError {
                            message: String::from("Can't write_batch with bool values"),
                        })?;
                    debug!("Wrote {} rows of bool data", n);
                }
                Int32ColumnWriter(_) => unreachable!("ParquetWriter does not support INT32 data"),
                Int64ColumnWriter(ref mut w) => {
                    let p = packer.i64_packer();
                    let n = w
                        .write_batch(p.values(), Some(p.def_levels()), Some(p.rep_levels()))
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
                        .write_batch(p.values(), Some(p.def_levels()), Some(p.rep_levels()))
                        .context(ParquetLibraryError {
                            message: String::from("Can't write_batch with f64 values"),
                        })?;
                    debug!("Wrote {} rows of f64 data", n);
                }
                ByteArrayColumnWriter(ref mut w) => {
                    let p = packer.str_packer();
                    let n = w
                        .write_batch(p.values(), Some(p.def_levels()), Some(p.rep_levels()))
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

impl<W> fmt::Debug for DeloreanParquetTableWriter<W>
where
    W: Write + Seek + TryClone,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeloreanParquetTableWriter")
            .field("parquet_schema", &self.parquet_schema)
            .field("file_writer", &"SerializedFileWriter")
            .finish()
    }
}

// Converts from line protocol `Schema` to the equivalent parquet schema `Type`.
fn convert_to_parquet_schema(
    schema: &delorean_table_schema::Schema,
) -> Result<Rc<parquet::schema::types::Type>, Error> {
    let mut parquet_columns = Vec::new();

    let col_defs = schema.get_col_defs();
    for col_def in col_defs {
        debug!("Determining parquet schema for column {:?}", col_def);
        let (physical_type, logical_type) = match col_def.data_type {
            delorean_table_schema::DataType::Boolean => (PhysicalType::BOOLEAN, None),
            delorean_table_schema::DataType::Float => (PhysicalType::DOUBLE, None),
            delorean_table_schema::DataType::Integer => {
                (PhysicalType::INT64, Some(LogicalType::UINT_64))
            }
            delorean_table_schema::DataType::String => {
                (PhysicalType::BYTE_ARRAY, Some(LogicalType::UTF8))
            }
            delorean_table_schema::DataType::Timestamp => {
                // At the time of writing, the underlying rust parquet
                // library doesn't support nanosecond timestamp
                // precisions yet
                //
                // Timestamp handling (including nanosecond support)
                // was changed as part of Parquet version 2.6 according
                // to
                // https://github.com/apache/parquet-format/blob/master/CHANGES.md#version-260
                //
                // The rust implementation claims to only support parquet-version 2.4
                // https://github.com/apache/arrow/tree/master/rust/parquet#supported-parquet-version
                //
                // Thus store timestampts using microsecond precision instead of nanosecond
                (PhysicalType::INT64, Some(LogicalType::TIMESTAMP_MICROS))
            }
        };

        // All fields are optional
        let mut parquet_column_builder = Type::primitive_type_builder(&col_def.name, physical_type)
            .with_repetition(Repetition::OPTIONAL);

        if let Some(t) = logical_type {
            parquet_column_builder = parquet_column_builder.with_logical_type(t);
        }

        let parquet_column_type = parquet_column_builder
            .build()
            .context(ParquetLibraryError {
                message: String::from("Can't create parquet column type"),
            })?;
        debug!(
            "Using parquet type {} for column {:?}",
            parquet_schema_as_string(&parquet_column_type),
            col_def
        );

        parquet_columns.push(Rc::new(parquet_column_type));
    }

    let parquet_schema = Type::group_type_builder(&schema.measurement())
        .with_fields(&mut parquet_columns)
        .build()
        .context(ParquetLibraryError {
            message: String::from("Can't create top level parquet schema"),
        })?;

    Ok(Rc::new(parquet_schema))
}

fn set_integer_encoding(
    data_type: delorean_table_schema::DataType,
    compression_level: CompressionLevel,
    col_path: ColumnPath,
    builder: WriterPropertiesBuilder,
) -> WriterPropertiesBuilder {
    match compression_level {
        CompressionLevel::MAXIMUM => {
            debug!(
                "Setting encoding of {:?} col {} to DELTA_BINARY_PACKED (MAXIMUM)",
                data_type, col_path
            );
            builder
                .set_column_encoding(col_path.clone(), Encoding::DELTA_BINARY_PACKED)
                .set_column_dictionary_enabled(col_path, false)
        }
        CompressionLevel::COMPATIBILITY => {
            debug!(
                "Setting encoding of {:?} col {} to PLAIN/RLE (COMPATIBILITY)",
                data_type, col_path
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
    schema: &delorean_table_schema::Schema,
    compression_level: CompressionLevel,
) -> Rc<WriterProperties> {
    let mut builder = WriterProperties::builder();

    // TODO: Maybe tweak more of these settings for maximum performance.

    // start off with GZIP for maximum compression ratio (at expense of CPU performance...)
    builder = builder.set_compression(Compression::GZIP);

    // Setup encoding as defined in
    // https://github.com/influxdata/delorean/blob/alamb/encoding_thoughts/docs/encoding_thoughts.md
    //
    // Note: the property writer builder's default is to encode
    // everything with dictionary encoding, and it turns out that
    // dictionary encoding overrides all other encodings. Thus, we
    // must explicitly disable dictionary encoding when another
    // encoding is desired.
    let col_defs = schema.get_col_defs();
    for col_def in col_defs {
        // locates the column definition in the schema
        let col_path = ColumnPath::from(col_def.name.clone());

        match col_def.data_type {
            data_type @ delorean_table_schema::DataType::Boolean => {
                debug!(
                    "Setting encoding of {:?} col {} to RLE",
                    data_type, col_path
                );
                builder = builder
                    .set_column_encoding(col_path.clone(), Encoding::RLE)
                    .set_column_dictionary_enabled(col_path, false);
            }
            data_type @ delorean_table_schema::DataType::Integer => {
                builder = set_integer_encoding(data_type, compression_level, col_path, builder)
            }
            data_type @ delorean_table_schema::DataType::Float => {
                debug!(
                    "Setting encoding of {:?} col {} to PLAIN",
                    data_type, col_path
                );
                builder = builder
                    .set_column_encoding(col_path.clone(), Encoding::PLAIN)
                    .set_column_dictionary_enabled(col_path, false);
            }
            // tag values are often very much repeated
            data_type @ delorean_table_schema::DataType::String if schema.is_tag(&col_def) => {
                debug!(
                    "Setting encoding of tag val {:?} col {} to dictionary",
                    data_type, col_path
                );
                builder = builder.set_column_dictionary_enabled(col_path, true);
            }
            data_type @ delorean_table_schema::DataType::String => {
                debug!(
                    "Setting encoding of non-tag val {:?} col {} to DELTA_LENGTH_BYTE_ARRAY",
                    data_type, col_path
                );
                builder = builder
                    .set_column_encoding(col_path.clone(), Encoding::DELTA_LENGTH_BYTE_ARRAY)
                    .set_column_dictionary_enabled(col_path, false);
            }
            data_type @ delorean_table_schema::DataType::Timestamp => {
                builder = set_integer_encoding(data_type, compression_level, col_path, builder)
            }
        };
    }

    // Even though the 'set_statistics_enabled()' method is called here, the resulting
    // parquet file does not appear to have statistics enabled.
    //
    // This is due to the fact that the underlying rust parquet
    // library does not support statistics generation at this time.
    let props = builder
        .set_statistics_enabled(true)
        .set_created_by("Delorean".to_string())
        .build();
    Rc::new(props)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Collapses multiple spaces into a single space, and removes trailing whitespace
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
        let schema = delorean_table_schema::SchemaBuilder::new("measurement_name")
            .tag("tag1")
            .field("string_field", delorean_table_schema::DataType::String)
            .field("float_field", delorean_table_schema::DataType::Float)
            .field("int_field", delorean_table_schema::DataType::Integer)
            .field("bool_field", delorean_table_schema::DataType::Boolean)
            .build();

        let parquet_schema = convert_to_parquet_schema(&schema).expect("conversion successful");
        let parquet_schema_string = normalize_spaces(&parquet_schema_as_string(&parquet_schema));
        let expected_schema_string = normalize_spaces(
            r#"message measurement_name {
            OPTIONAL BYTE_ARRAY tag1 (UTF8);
            OPTIONAL BYTE_ARRAY string_field (UTF8);
            OPTIONAL DOUBLE float_field;
            OPTIONAL INT64 int_field (UINT_64);
            OPTIONAL BOOLEAN bool_field;
            OPTIONAL INT64 time (TIMESTAMP_MICROS);
}"#,
        );

        assert_eq!(parquet_schema_string, expected_schema_string);
    }

    fn make_test_schema() -> delorean_table_schema::Schema {
        delorean_table_schema::SchemaBuilder::new("measurement_name")
            .tag("tag1")
            .field("string_field", delorean_table_schema::DataType::String)
            .field("float_field", delorean_table_schema::DataType::Float)
            .field("int_field", delorean_table_schema::DataType::Integer)
            .field("bool_field", delorean_table_schema::DataType::Boolean)
            .build()
    }

    #[test]
    fn test_create_writer_props_maximum() {
        do_test_create_writer_props(CompressionLevel::MAXIMUM);
    }

    #[test]
    fn test_create_writer_props_compatibility() {
        do_test_create_writer_props(CompressionLevel::COMPATIBILITY);
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
            CompressionLevel::MAXIMUM => {
                assert_eq!(
                    writer_props.encoding(&int_field_colpath),
                    Some(Encoding::DELTA_BINARY_PACKED)
                );
                assert_eq!(writer_props.dictionary_enabled(&int_field_colpath), false);
            }
            CompressionLevel::COMPATIBILITY => {
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
            CompressionLevel::MAXIMUM => {
                assert_eq!(
                    writer_props.encoding(&timestamp_field_colpath),
                    Some(Encoding::DELTA_BINARY_PACKED)
                );
                assert_eq!(
                    writer_props.dictionary_enabled(&timestamp_field_colpath),
                    false
                );
            }
            CompressionLevel::COMPATIBILITY => {
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
            CompressionLevel::from("max").ok().unwrap(),
            CompressionLevel::MAXIMUM
        );
        assert_eq!(
            CompressionLevel::from("compatibility").ok().unwrap(),
            CompressionLevel::COMPATIBILITY
        );

        let bad = CompressionLevel::from("madxxxx");
        assert!(bad.is_err());
    }
}
