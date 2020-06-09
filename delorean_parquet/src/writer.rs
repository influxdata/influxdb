//! This module contains the code to write delorean table data to parquet
use std::rc::Rc;

use log::debug;
use snafu::{ResultExt, Snafu};

use parquet::{
    basic::{Compression, Encoding, LogicalType, Repetition, Type as PhysicalType},
    errors::ParquetError,
    file::{
        properties::WriterProperties,
        reader::TryClone,
        writer::{FileWriter, SerializedFileWriter},
    },
    schema::{
        printer,
        types::{ColumnPath, Type},
    },
};
use std::io::{Seek, Write};

use delorean_table::packers::Packer;
use delorean_table::{DeloreanTableWriter, Error as TableError};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(r#"{}, underlying parqet error {}"#, message, source))]
    ParquetLibraryError {
        message: String,
        source: ParquetError,
    },
    #[snafu(display(r#"{}"#, message))]
    MismatchedColumns { message: String },
}

impl From<Error> for TableError {
    fn from(other: Error) -> Self {
        TableError::Data {
            source: Box::new(other),
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
    /// # use delorean_table::packers::Packer;
    /// # use delorean_parquet::writer::DeloreanParquetTableWriter;
    ///
    /// let schema = delorean_table_schema::SchemaBuilder::new("measurement_name")
    ///      .tag("tag1")
    ///      .field("field1", delorean_table_schema::DataType::Integer)
    ///      .build();
    ///
    /// let mut packers = vec![
    ///     Packer::new(DataType::String),  // 0: tag1
    ///     Packer::new(DataType::Integer), // 1: field1
    ///     Packer::new(DataType::Integer), // 2: timestamp
    /// ];
    ///
    /// packers[0].pack_str(Some("tag1")); // tag1 val
    /// packers[1].pack_i64(Some(100));    // field1 val
    /// packers[2].pack_none();            // no timestamp
    ///
    /// // Write to '/tmp/example.parquet'
    /// let mut output_file_name = std::env::temp_dir();
    /// output_file_name.push("example.parquet");
    /// let output_file = fs::File::create(output_file_name.as_path()).unwrap();
    ///
    /// let mut parquet_writer = DeloreanParquetTableWriter::new(&schema, output_file).unwrap();
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
        writer: W,
    ) -> Result<DeloreanParquetTableWriter<W>, Error> {
        let writer_props = create_writer_props(&schema);
        let parquet_schema = convert_to_parquet_schema(&schema)?;

        let file_writer = SerializedFileWriter::new(writer, parquet_schema.clone(), writer_props)
            .context(ParquetLibraryError {
            message: String::from("Error trying to create a SerializedFileWriter"),
        })?;

        let parquet_writer = DeloreanParquetTableWriter {
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
    fn write_batch(&mut self, packers: &[Packer]) -> Result<(), TableError> {
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
            let packer = match packers.get(column_number) {
                Some(packer) => packer,
                None => {
                    return Err(TableError::Other {
                        source: Box::new(Error::MismatchedColumns {
                            message: format!("Could not get packer for column {}", column_number),
                        }),
                    });
                }
            };
            match col_writer {
                BoolColumnWriter(ref mut w) => {
                    let bool_packer = packer.as_bool_packer();
                    let n = w
                        .write_batch(
                            &bool_packer.values,
                            Some(&bool_packer.def_levels),
                            Some(&bool_packer.rep_levels),
                        )
                        .context(ParquetLibraryError {
                            message: String::from("Can't write_batch with bool values"),
                        })?;
                    debug!("Wrote {} rows of bool data", n);
                }
                Int32ColumnWriter(_) => unreachable!("ParquetWriter does not support INT32 data"),
                Int64ColumnWriter(ref mut w) => {
                    let int_packer = packer.as_int_packer();
                    let n = w
                        .write_batch(
                            &int_packer.values,
                            Some(&int_packer.def_levels),
                            Some(&int_packer.rep_levels),
                        )
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
                    let float_packer = packer.as_float_packer();
                    let n = w
                        .write_batch(
                            &float_packer.values,
                            Some(&float_packer.def_levels),
                            Some(&float_packer.rep_levels),
                        )
                        .context(ParquetLibraryError {
                            message: String::from("Can't write_batch with f64 values"),
                        })?;
                    debug!("Wrote {} rows of f64 data", n);
                }
                ByteArrayColumnWriter(ref mut w) => {
                    let string_packer = packer.as_string_packer();
                    let n = w
                        .write_batch(
                            &string_packer.values,
                            Some(&string_packer.def_levels),
                            Some(&string_packer.rep_levels),
                        )
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

fn parquet_schema_as_string(parquet_schema: &parquet::schema::types::Type) -> String {
    let mut parquet_schema_string = Vec::new();
    printer::print_schema(&mut parquet_schema_string, parquet_schema);
    String::from_utf8_lossy(&parquet_schema_string).to_string()
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
                // The underlying parquet library doesn't seem to have
                // support for TIMESTAMP_NANOs yet. FIXME we need to
                // fix this as otherwise any other application that
                // uses the created parquet files will see the
                // incorrect timestamps;
                //
                // TODO: file a clear bug in the parquet JIRA project (and perhaps fix it)
                eprintln!("WARNING WARNING: writing parquet using MICROS not NANOS (no support for NANOs..)");
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

/// Create the parquet writer properties (which defines the encoding
/// and compression for each column) for a given schema.
fn create_writer_props(schema: &delorean_table_schema::Schema) -> Rc<WriterProperties> {
    let mut builder = WriterProperties::builder();

    // TODO: Maybe tweak more of these settings for maximum performance.

    // start off with GZIP for maximum compression ratio (at expense of CPU performance...)
    builder = builder.set_compression(Compression::GZIP);

    // Setup encoding as defined in
    // https://github.com/influxdata/delorean/blob/alamb/encoding_thoughts/docs/encoding_thoughts.md
    let col_defs = schema.get_col_defs();
    for col_def in col_defs {
        // locates the column definition in the schema
        let col_path = ColumnPath::from(col_def.name.clone());

        match col_def.data_type {
            data_type @ delorean_table_schema::DataType::Boolean
            | data_type @ delorean_table_schema::DataType::Float
            | data_type @ delorean_table_schema::DataType::Integer => {
                debug!(
                    "Setting encoding of {:?} col {} to RLE",
                    data_type, col_path
                );
                builder = builder.set_column_encoding(col_path, Encoding::RLE);
            }
            // tag values are often very much repeated
            delorean_table_schema::DataType::String if schema.is_tag(&col_def) => {
                debug!(
                    "Setting encoding of tag val DataType::String col {} to dictionary",
                    col_path
                );
                builder = builder.set_column_dictionary_enabled(col_path, true);
            }
            delorean_table_schema::DataType::String => {
                debug!("Setting encoding of non-tag val DataType::String col {} to DELTA_LENGTH_BYTE_ARRAY", col_path);
                builder = builder.set_column_encoding(col_path, Encoding::DELTA_LENGTH_BYTE_ARRAY);
            }
            delorean_table_schema::DataType::Timestamp => {
                debug!(
                    "Setting encoding of LPTimestamp col {} to DELTA_BINARY_PACKED",
                    col_path
                );
                builder = builder.set_column_encoding(col_path, Encoding::DELTA_BINARY_PACKED);
            }
        };
    }

    // Even though the 'set_statistics_enabled()' method is called here, the resulting
    // parquet file does not appear to have statistics enabled.
    // TODO: file a clear bug in the parquet JIRA project
    eprintln!("WARNING WARNING -- statistics generation does not appear to be working");
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
            OPTIONAL INT64 timestamp (TIMESTAMP_MICROS);
}"#,
        );

        assert_eq!(parquet_schema_string, expected_schema_string);
    }

    #[test]
    fn test_create_writer_props() {
        let schema = delorean_table_schema::SchemaBuilder::new("measurement_name")
            .tag("tag1")
            .field("string_field", delorean_table_schema::DataType::String)
            .field("float_field", delorean_table_schema::DataType::Float)
            .field("int_field", delorean_table_schema::DataType::Integer)
            .field("bool_field", delorean_table_schema::DataType::Boolean)
            .build();

        let writer_props = create_writer_props(&schema);

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
        assert_eq!(writer_props.dictionary_enabled(&string_field_colpath), true);
        assert_eq!(writer_props.statistics_enabled(&string_field_colpath), true);

        let float_field_colpath = ColumnPath::from("float_field");
        assert_eq!(
            writer_props.encoding(&float_field_colpath),
            Some(Encoding::RLE)
        );
        assert_eq!(
            writer_props.compression(&float_field_colpath),
            Compression::GZIP
        );
        assert_eq!(writer_props.dictionary_enabled(&float_field_colpath), true);
        assert_eq!(writer_props.statistics_enabled(&float_field_colpath), true);

        let int_field_colpath = ColumnPath::from("int_field");
        assert_eq!(
            writer_props.encoding(&int_field_colpath),
            Some(Encoding::RLE)
        );
        assert_eq!(
            writer_props.compression(&int_field_colpath),
            Compression::GZIP
        );
        assert_eq!(writer_props.dictionary_enabled(&int_field_colpath), true);
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
        assert_eq!(writer_props.dictionary_enabled(&bool_field_colpath), true);
        assert_eq!(writer_props.statistics_enabled(&bool_field_colpath), true);

        let timestamp_field_colpath = ColumnPath::from("timestamp");
        assert_eq!(
            writer_props.encoding(&timestamp_field_colpath),
            Some(Encoding::DELTA_BINARY_PACKED)
        );
        assert_eq!(
            writer_props.compression(&timestamp_field_colpath),
            Compression::GZIP
        );
        assert_eq!(
            writer_props.dictionary_enabled(&timestamp_field_colpath),
            true
        );
        assert_eq!(
            writer_props.statistics_enabled(&timestamp_field_colpath),
            true
        );
    }
}
