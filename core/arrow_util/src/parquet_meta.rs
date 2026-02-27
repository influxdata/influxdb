//! Utils for parquet metadata.

use arrow::datatypes::SchemaRef;
use base64::Engine;
use parquet::{arrow::ARROW_SCHEMA_META_KEY, file::metadata::KeyValue};

/// Encodes the Arrow schema into the IPC format, and base64 encodes it.
///
/// This method is copied from a private method in the arrow-rs upstream code.
/// (Refer: <https://github.com/apache/arrow-rs/blob/2905ce6796cad396241fc50164970dbf1237440a/parquet/src/arrow/schema/mod.rs#L178-L193>)
///
/// See <https://github.com/apache/arrow-rs/issues/6177> for discussion
///
/// TODO: Replace with upstream call when
/// <https://github.com/apache/arrow-rs/pull/6916> is available
fn encode_arrow_schema(schema: &SchemaRef) -> String {
    let options = arrow_ipc::writer::IpcWriteOptions::default();
    let data_gen = arrow_ipc::writer::IpcDataGenerator::default();
    let error_on_replacement = true;
    let mut tracker = arrow_ipc::writer::DictionaryTracker::new(error_on_replacement);
    let mut serialized_schema =
        data_gen.schema_to_bytes_with_dictionary_tracker(schema, &mut tracker, &options);

    // manually prepending the length to the schema as arrow uses the legacy IPC format
    // TODO: change after addressing ARROW-9777
    let schema_len = serialized_schema.ipc_message.len();
    let mut len_prefix_schema = Vec::with_capacity(schema_len + 8);
    len_prefix_schema.append(&mut vec![255u8, 255, 255, 255]);
    len_prefix_schema.append((schema_len as u32).to_le_bytes().to_vec().as_mut());
    len_prefix_schema.append(&mut serialized_schema.ipc_message);

    base64::prelude::BASE64_STANDARD.encode(&len_prefix_schema)
}

/// When encoding to parquet, the ArrowWriter persists the arrow schema, keyed to
/// [`ARROW_SCHEMA_META_KEY`] in the parquet metadata.
///
/// This occurs as the default behavior when using the ArrowWriter in single threaded writes.
/// (refer to: <https://github.com/apache/arrow-rs/blob/2905ce6796cad396241fc50164970dbf1237440a/parquet/src/arrow/arrow_writer/mod.rs#L188-L190>)
///
/// TODO: file upstream ticket to make this default behavior uniform across other parquet encoders
/// including the parallel writer.
/// See <https://github.com/apache/arrow-rs/issues/6177>
pub fn add_encoded_arrow_schema_to_metadata(arrow_schema: SchemaRef, meta: &mut Vec<KeyValue>) {
    let encoded = encode_arrow_schema(&arrow_schema);

    let schema_kv = KeyValue {
        key: ARROW_SCHEMA_META_KEY.to_string(),
        value: Some(encoded),
    };

    // check if ARROW:schema exists, and overwrite it
    let schema_meta = meta
        .iter()
        .enumerate()
        .find(|(_, kv)| kv.key.as_str() == ARROW_SCHEMA_META_KEY);
    match schema_meta {
        Some((i, _)) => {
            meta.remove(i);
            meta.push(schema_kv);
        }
        None => {
            meta.push(schema_kv);
        }
    }
}
