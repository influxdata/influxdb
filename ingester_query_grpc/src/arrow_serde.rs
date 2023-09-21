//! (De-)Serialization of Apache Arrow [`Schema`] and [`RecordBatch`] data.
//!
//! **⚠️ These routines are IOx-specific and MUST NOT be used as a public interface!**
//!
//! Specifically this is a custom protocol, similar to but not derived from Arrow Flight.
//! See <https://github.com/influxdata/influxdb_iox/issues/8169>.
use std::{collections::HashMap, sync::Arc};

use arrow::{
    buffer::Buffer,
    datatypes::{DataType, Field, FieldRef, Schema, SchemaRef},
    error::ArrowError,
    ipc::{
        convert::fb_to_schema,
        reader::{read_dictionary, read_record_batch},
        root_as_message,
        writer::{DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteOptions},
    },
    record_batch::{RecordBatch, RecordBatchOptions},
};
use bytes::Bytes;
use flatbuffers::InvalidFlatbuffer;
use snafu::{ensure, OptionExt, ResultExt, Snafu};

use crate::influxdata::iox::ingester::v2 as proto2;

/// Serialize [`Schema`] to [`Bytes`].
pub fn schema_to_bytes(schema: &Schema) -> Bytes {
    let EncodedData {
        ipc_message,
        arrow_data,
    } = IpcDataGenerator::default().schema_to_bytes(schema, &write_options());
    assert!(
        arrow_data.is_empty(),
        "arrow_data should always be empty for schema messages"
    );
    ipc_message.into()
}

#[derive(Debug, Snafu)]
#[snafu(module)]
pub enum BytesToSchemaError {
    #[snafu(display("Unable to get root as message: {source}"))]
    RootAsMessage { source: InvalidFlatbuffer },

    #[snafu(display("Unable to read IPC message as schema, is: {variant}"))]
    WrongMessageType { variant: &'static str },
}

/// Read [`Schema`] from bytes.
pub fn bytes_to_schema(data: &[u8]) -> Result<Schema, BytesToSchemaError> {
    let message = root_as_message(data).context(bytes_to_schema_error::RootAsMessageSnafu)?;
    let ipc_schema =
        message
            .header_as_schema()
            .context(bytes_to_schema_error::WrongMessageTypeSnafu {
                variant: message.header_type().variant_name().unwrap_or("<UNKNOWN>"),
            })?;
    let schema = fb_to_schema(ipc_schema);
    Ok(schema)
}

/// Encoder to read/write Arrow [`RecordBatch`]es from/to [`proto2::RecordBatch`].
#[derive(Debug)]
pub struct BatchEncoder {
    /// The original batch schema.
    batch_schema: SchemaRef,

    /// Schema with unique dictionary IDs.
    dict_schema: SchemaRef,
}

#[derive(Debug, Snafu)]
#[snafu(module)]
pub enum ProjectError {
    #[snafu(display("Cannot project: {source}"))]
    CannotProject { source: ArrowError },
}

#[derive(Debug, Snafu)]
#[snafu(module)]
pub enum WriteError {
    #[snafu(display("Invalid batch schema\n\nActual:\n{actual}\n\nExpected:\n{expected}"))]
    InvalidSchema {
        actual: SchemaRef,
        expected: SchemaRef,
    },
}

#[derive(Debug, Snafu)]
#[snafu(module)]
pub enum ReadError {
    #[snafu(display("Unable to get root as dictionary message #{idx} (0-based): {source}"))]
    DictionaryRootAsMessage {
        source: InvalidFlatbuffer,
        idx: usize,
    },

    #[snafu(display("Unable to read IPC message #{idx} (0-based) as dictionary, is: {variant}"))]
    DictionaryWrongMessageType { variant: &'static str, idx: usize },

    #[snafu(display("Cannot read dictionary: {source}"))]
    ReadDictionary { source: ArrowError },

    #[snafu(display("Record batch is required but missing"))]
    RecordBatchRequired,

    #[snafu(display("Unable to get root as record batch message: {source}"))]
    RecordBatchRootAsMessage { source: InvalidFlatbuffer },

    #[snafu(display("Unable to read IPC message as record batch, is: {variant}"))]
    RecordBatchWrongMessageType { variant: &'static str },

    #[snafu(display("Cannot read record batch: {source}"))]
    ReadRecordBatch { source: ArrowError },
}

impl BatchEncoder {
    /// Create new encoder.
    ///
    /// For schemas that contain dictionaries, this involves copying data and may be rather costly. If you can, try to
    /// only do this once and use [`project`](Self::project) to select the right columns for the appropriate batch.
    pub fn new(batch_schema: SchemaRef) -> Self {
        let mut dict_id_counter = 0;
        let dict_schema = Arc::new(Schema::new_with_metadata(
            batch_schema
                .fields()
                .iter()
                .map(|f| assign_dict_ids(f, &mut dict_id_counter))
                .collect::<Vec<_>>(),
            batch_schema.metadata().clone(),
        ));
        Self {
            batch_schema,
            dict_schema,
        }
    }

    /// Project schema stored within this encoder.
    pub fn project(&self, indices: &[usize]) -> Result<Self, ProjectError> {
        Ok(Self {
            batch_schema: Arc::new(
                self.batch_schema
                    .project(indices)
                    .context(project_error::CannotProjectSnafu)?,
            ),
            dict_schema: Arc::new(
                self.dict_schema
                    .project(indices)
                    .context(project_error::CannotProjectSnafu)?,
            ),
        })
    }

    /// Serialize batch.
    pub fn write(&self, batch: &RecordBatch) -> Result<proto2::RecordBatch, WriteError> {
        ensure!(
            batch.schema() == self.batch_schema,
            write_error::InvalidSchemaSnafu {
                actual: batch.schema(),
                expected: Arc::clone(&self.batch_schema),
            }
        );

        let batch = reassign_schema(batch, Arc::clone(&self.dict_schema));

        let mut dictionary_tracker = DictionaryTracker::new(true);
        let (dictionaries, batch) = IpcDataGenerator::default()
            .encoded_batch(&batch, &mut dictionary_tracker, &write_options())
            .expect("serialization w/o compression should NEVER fail");

        Ok(proto2::RecordBatch {
            dictionaries: dictionaries.into_iter().map(|enc| enc.into()).collect(),
            batch: Some(batch.into()),
        })
    }

    /// Deserialize batch.
    pub fn read(&self, batch: proto2::RecordBatch) -> Result<RecordBatch, ReadError> {
        let proto2::RecordBatch {
            dictionaries,
            batch,
        } = batch;

        let mut dictionaries_by_field = HashMap::with_capacity(dictionaries.len());
        for (idx, enc) in dictionaries.into_iter().enumerate() {
            let proto2::EncodedData {
                ipc_message,
                arrow_data,
            } = enc;

            let message = root_as_message(&ipc_message)
                .context(read_error::DictionaryRootAsMessageSnafu { idx })?;
            let dictionary_batch = message.header_as_dictionary_batch().context(
                read_error::DictionaryWrongMessageTypeSnafu {
                    variant: message.header_type().variant_name().unwrap_or("<UNKNOWN>"),
                    idx,
                },
            )?;

            read_dictionary(
                // copy & align
                &Buffer::from(&arrow_data),
                dictionary_batch,
                &self.dict_schema,
                &mut dictionaries_by_field,
                &message.version(),
            )
            .context(read_error::ReadDictionarySnafu)?;
        }

        let proto2::EncodedData {
            ipc_message,
            arrow_data,
        } = batch.context(read_error::RecordBatchRequiredSnafu)?;
        let message =
            root_as_message(&ipc_message).context(read_error::RecordBatchRootAsMessageSnafu)?;
        let record_batch = message.header_as_record_batch().context(
            read_error::RecordBatchWrongMessageTypeSnafu {
                variant: message.header_type().variant_name().unwrap_or("<UNKNOWN>"),
            },
        )?;

        let batch = read_record_batch(
            // copy & align
            &Buffer::from(&arrow_data),
            record_batch,
            Arc::clone(&self.dict_schema),
            &dictionaries_by_field,
            None,
            &message.version(),
        )
        .context(read_error::ReadRecordBatchSnafu)?;

        Ok(reassign_schema(&batch, Arc::clone(&self.batch_schema)))
    }
}

/// Recursively assign unique dictionary IDs.
fn assign_dict_ids(field: &FieldRef, counter: &mut i64) -> FieldRef {
    match field.data_type() {
        DataType::Dictionary(_, _) => {
            let dict_id = *counter;
            *counter += 1;
            Arc::new(
                Field::new_dict(
                    field.name(),
                    field.data_type().clone(),
                    field.is_nullable(),
                    dict_id,
                    field.dict_is_ordered().expect("is dict type"),
                )
                .with_metadata(field.metadata().clone()),
            )
        }
        DataType::Struct(fields) => {
            let data_type =
                DataType::Struct(fields.iter().map(|f| assign_dict_ids(f, counter)).collect());
            Arc::new(field.as_ref().clone().with_data_type(data_type))
        }
        DataType::Union(fields, mode) => {
            let data_type = DataType::Union(
                fields
                    .iter()
                    .map(|(id, f)| (id, assign_dict_ids(f, counter)))
                    .collect(),
                *mode,
            );
            Arc::new(field.as_ref().clone().with_data_type(data_type))
        }
        DataType::List(field) => {
            let data_type = DataType::List(assign_dict_ids(field, counter));
            Arc::new(field.as_ref().clone().with_data_type(data_type))
        }
        DataType::LargeList(field) => {
            let data_type = DataType::LargeList(assign_dict_ids(field, counter));
            Arc::new(field.as_ref().clone().with_data_type(data_type))
        }
        DataType::FixedSizeList(field, s) => {
            let data_type = DataType::FixedSizeList(assign_dict_ids(field, counter), *s);
            Arc::new(field.as_ref().clone().with_data_type(data_type))
        }
        DataType::Map(field, sorted) => {
            let data_type = DataType::Map(assign_dict_ids(field, counter), *sorted);
            Arc::new(field.as_ref().clone().with_data_type(data_type))
        }
        _ => Arc::clone(field),
    }
}

/// Re-assign schema to given batch.
///
/// This is required to overwrite dictionary IDs.
fn reassign_schema(batch: &RecordBatch, schema: SchemaRef) -> RecordBatch {
    RecordBatch::try_new_with_options(
        schema,
        batch.columns().to_vec(),
        &RecordBatchOptions::default().with_row_count(Some(batch.num_rows())),
    )
    .expect("re-assigning schema should always work")
}

impl From<EncodedData> for proto2::EncodedData {
    fn from(enc: EncodedData) -> Self {
        let EncodedData {
            ipc_message,
            arrow_data,
        } = enc;

        Self {
            ipc_message: ipc_message.into(),
            arrow_data: arrow_data.into(),
        }
    }
}

/// Write options that are used for all relevant methods in this module.
fn write_options() -> IpcWriteOptions {
    IpcWriteOptions::default()
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::{
        array::{ArrayRef, Int64Array, StringDictionaryBuilder},
        datatypes::Int32Type,
    };
    use datafusion::{
        arrow::datatypes::{DataType, Field},
        common::assert_contains,
    };
    use prost::Message;

    use super::*;

    #[test]
    fn test_schema_roundtrip() {
        let schema = schema();
        let bytes = schema_to_bytes(&schema);

        // ensure that the deserialization is NOT sensitive to alignment
        const MAX_OFFSET: usize = 8;

        for offset in 0..MAX_OFFSET {
            let buffer = unalign_buffer(&bytes, MAX_OFFSET, offset);
            let schema2 = bytes_to_schema(&buffer).unwrap();

            assert_eq!(schema, schema2);
        }
    }

    #[test]
    fn test_record_batch_roundtrip() {
        let schema = Arc::new(schema());
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![int64array(), dictarray1(), dictarray2(), dictarray1()],
        )
        .unwrap();

        let encoder = BatchEncoder::new(schema);
        let encoded = encoder.write(&batch).unwrap();

        // check that we actually use dictionaries and don't hydrate them
        assert_eq!(encoded.dictionaries.len(), 3);

        let encoded = encoded.encode_to_vec();

        // ensure that the deserialization is NOT sensitive to alignment
        const MAX_OFFSET: usize = 128;

        for offset in 0..MAX_OFFSET {
            let view = unalign_buffer(&encoded, MAX_OFFSET, offset);
            let encoded = proto2::RecordBatch::decode(view).unwrap();

            let batch2 = encoder.read(encoded).unwrap();
            assert_eq!(batch, batch2);
        }
    }

    #[test]
    fn test_write_checks_schema() {
        let schema = Arc::new(schema());
        let batch =
            RecordBatch::try_new(Arc::new(schema.project(&[0]).unwrap()), vec![int64array()])
                .unwrap();

        let encoder = BatchEncoder::new(schema);
        let err = encoder.write(&batch).unwrap_err();

        assert_contains!(err.to_string(), "Invalid batch schema");
    }

    #[test]
    fn test_project() {
        let schema = Arc::new(schema());
        let batch = RecordBatch::try_new(
            Arc::new(schema.project(&[0, 3, 2]).unwrap()),
            vec![int64array(), dictarray1(), dictarray1()],
        )
        .unwrap();

        let encoder = BatchEncoder::new(schema).project(&[0, 3, 2]).unwrap();
        let encoded = encoder.write(&batch).unwrap();

        // check that we actually use dictionaries and don't hydrate them
        assert_eq!(encoded.dictionaries.len(), 2);

        let batch2 = encoder.read(encoded).unwrap();
        assert_eq!(batch, batch2);
    }

    fn schema() -> Schema {
        Schema::new_with_metadata(
            vec![
                Field::new("f1", DataType::Int64, true)
                    .with_metadata(HashMap::from([("k".to_owned(), "v".to_owned())])),
                Field::new(
                    "f2",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    false,
                ),
                Field::new(
                    "f3",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    false,
                ),
                Field::new(
                    "f4",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    false,
                ),
            ],
            HashMap::from([("foo".to_owned(), "bar".to_owned())]),
        )
    }

    fn int64array() -> ArrayRef {
        Arc::new(Int64Array::from(vec![None, Some(1i64), Some(2i64)]))
    }

    fn dictarray1() -> ArrayRef {
        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        builder.append("foo").unwrap();
        builder.append("foo").unwrap();
        builder.append("bar").unwrap();
        Arc::new(builder.finish())
    }

    fn dictarray2() -> ArrayRef {
        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        builder.append("fo").unwrap();
        builder.append("fo").unwrap();
        builder.append("ba").unwrap();
        Arc::new(builder.finish())
    }

    fn unalign_buffer(data: &[u8], alignment: usize, offset: usize) -> Bytes {
        assert!(alignment > 0);
        assert!(alignment.is_power_of_two());
        assert!(offset < alignment);

        let memsize = data.len() + alignment;
        let mut mem = Vec::<u8>::with_capacity(memsize);

        let actual_offset = get_offset(mem.as_ptr(), alignment);
        let padding = if actual_offset <= offset {
            offset - actual_offset
        } else {
            alignment - actual_offset + offset
        };
        assert!(padding < alignment);

        mem.resize(padding, 0);
        mem.extend_from_slice(data);
        assert_eq!(get_offset(mem[padding..].as_ptr(), alignment), offset);

        let b = Bytes::from(mem);
        let b = b.slice(padding..);
        assert_eq!(get_offset(b.as_ptr(), alignment), offset);
        assert_eq!(b.as_ref(), data);

        b
    }

    fn get_offset(ptr: *const u8, alignment: usize) -> usize {
        (alignment - ptr.align_offset(alignment)) % alignment
    }
}
