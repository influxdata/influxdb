use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringBuilder, TimestampNanosecondBuilder},
    record_batch::RecordBatch,
};
use data_types::{NamespaceId, PartitionId, SequenceNumber, SequencerId, TableId, Timestamp};
use iox_time::Time;
use object_store::DynObjectStore;
use parquet_file::{metadata::IoxMetadata, storage::ParquetStorage};
use schema::{builder::SchemaBuilder, InfluxFieldType, TIME_COLUMN_NAME};

#[tokio::test]
async fn test_decoded_iox_metadata() {
    // A representative IOx data sample (with a time column, an invariant upheld
    // in the IOx write path)
    let data = [
        (
            TIME_COLUMN_NAME,
            to_timestamp_array(&[
                // NOTE: not ordered to ensure min/max is derived, not head/tail
                1646917692000000000,
                1653311292000000000,
                1647695292000000000,
            ]),
        ),
        (
            "some_field",
            to_string_array(&["bananas", "platanos", "manzana"]),
        ),
    ];

    // And the metadata the batch would be encoded with if it came through the
    // IOx write path.
    let meta = IoxMetadata {
        object_store_id: Default::default(),
        creation_timestamp: Time::from_timestamp_nanos(42),
        namespace_id: NamespaceId::new(1),
        namespace_name: "bananas".into(),
        sequencer_id: SequencerId::new(2),
        table_id: TableId::new(3),
        table_name: "platanos".into(),
        partition_id: PartitionId::new(4),
        partition_key: "potato".into(),
        min_sequence_number: SequenceNumber::new(10),
        max_sequence_number: SequenceNumber::new(11),
        compaction_level: 1,
        sort_key: None,
    };

    let batch = RecordBatch::try_from_iter(data).unwrap();
    let stream = futures::stream::iter([Ok(batch.clone())]);

    let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());
    let storage = ParquetStorage::new(object_store);

    let (iox_parquet_meta, file_size) = storage
        .upload(stream, &meta)
        .await
        .expect("failed to serialise & persist record batch");

    // Sanity check - can't assert the actual value.
    assert!(file_size > 0);

    // Decode the IOx metadata embedded in the parquet file metadata.
    let decoded = iox_parquet_meta
        .decode()
        .expect("failed to decode parquet file metadata");

    // And verify the metadata matches the expected values.
    assert_eq!(
        decoded.row_count(),
        3,
        "row count statistics does not match input row count"
    );

    let got = decoded
        .read_iox_metadata_new()
        .expect("failed to deserialise embedded IOx metadata");
    assert_eq!(
        got, meta,
        "embedded metadata does not match original metadata"
    );
}

#[tokio::test]
async fn test_derive_parquet_file_params() {
    // A representative IOx data sample (with a time column, an invariant upheld
    // in the IOx write path)
    let data = vec![
        to_string_array(&["bananas", "platanos", "manzana"]),
        to_timestamp_array(&[
            // NOTE: not ordered to ensure min/max extracted, not head/tail
            1646917692000000000,
            1653311292000000000,
            1647695292000000000,
        ]),
    ];

    // And the metadata the batch would be encoded with if it came through the
    // IOx write path.
    let meta = IoxMetadata {
        object_store_id: Default::default(),
        creation_timestamp: Time::from_timestamp_nanos(1234),
        namespace_id: NamespaceId::new(1),
        namespace_name: "bananas".into(),
        sequencer_id: SequencerId::new(2),
        table_id: TableId::new(3),
        table_name: "platanos".into(),
        partition_id: PartitionId::new(4),
        partition_key: "potato".into(),
        min_sequence_number: SequenceNumber::new(10),
        max_sequence_number: SequenceNumber::new(11),
        compaction_level: 1,
        sort_key: None,
    };

    // Build a schema that contains the IOx metadata, ensuring it is correctly
    // populated in the final parquet file's metadata.
    let schema = SchemaBuilder::new()
        .influx_field("some_field", InfluxFieldType::String)
        .timestamp()
        .build()
        .expect("could not create schema")
        .as_arrow();

    let batch = RecordBatch::try_new(schema, data).unwrap();
    let stream = futures::stream::iter([Ok(batch.clone())]);

    let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());
    let storage = ParquetStorage::new(object_store);

    let (iox_parquet_meta, file_size) = storage
        .upload(stream, &meta)
        .await
        .expect("failed to serialise & persist record batch");

    // Use the IoxParquetMetaData and original IoxMetadata to derive a
    // ParquetFileParams.
    let catalog_data = meta.to_parquet_file(file_size, &iox_parquet_meta);

    // And verify the resulting statistics used in the catalog.
    //
    // NOTE: thrift-encoded metadata not checked
    assert_eq!(catalog_data.sequencer_id, meta.sequencer_id);
    assert_eq!(catalog_data.namespace_id, meta.namespace_id);
    assert_eq!(catalog_data.table_id, meta.table_id);
    assert_eq!(catalog_data.partition_id, meta.partition_id);
    assert_eq!(catalog_data.object_store_id, meta.object_store_id);
    assert_eq!(catalog_data.min_sequence_number, meta.min_sequence_number);
    assert_eq!(catalog_data.max_sequence_number, meta.max_sequence_number);
    assert_eq!(catalog_data.file_size_bytes, file_size as i64);
    assert_eq!(catalog_data.compaction_level, meta.compaction_level);
    assert_eq!(catalog_data.created_at, Timestamp::new(1234));
    assert_eq!(catalog_data.row_count, 3);
    assert_eq!(catalog_data.min_time, Timestamp::new(1646917692000000000));
    assert_eq!(catalog_data.max_time, Timestamp::new(1653311292000000000));
}

fn to_string_array(strs: &[&str]) -> ArrayRef {
    let mut builder = StringBuilder::new(strs.len());
    for s in strs {
        builder.append_value(s).expect("appending string");
    }
    Arc::new(builder.finish())
}

fn to_timestamp_array(timestamps: &[i64]) -> ArrayRef {
    let mut builder = TimestampNanosecondBuilder::new(timestamps.len());
    builder
        .append_slice(timestamps)
        .expect("failed to append timestamp values");
    Arc::new(builder.finish())
}
