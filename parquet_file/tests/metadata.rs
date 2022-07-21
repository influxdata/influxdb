use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{ArrayRef, StringBuilder, TimestampNanosecondBuilder},
    record_batch::RecordBatch,
};
use data_types::{
    ColumnId, CompactionLevel, NamespaceId, PartitionId, SequenceNumber, SequencerId, TableId,
    Timestamp,
};
use iox_time::Time;
use object_store::DynObjectStore;
use parquet_file::{metadata::IoxMetadata, storage::ParquetStorage};
use schema::{builder::SchemaBuilder, sort::SortKey, InfluxFieldType, TIME_COLUMN_NAME};

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
        ("null_field", null_array(3)),
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
        max_sequence_number: SequenceNumber::new(11),
        compaction_level: CompactionLevel::FileNonOverlapped,
        sort_key: None,
    };

    let batch = RecordBatch::try_from_iter(data).unwrap();
    let stream = futures::stream::iter([Ok(batch.clone())]);

    let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());
    let storage = ParquetStorage::new(object_store);

    let (iox_parquet_meta, file_size) = storage
        .upload(stream, &meta)
        .await
        .expect("failed to serialize & persist record batch");

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

    // Repro of 4714
    let row_group_meta = decoded.parquet_row_group_metadata();
    println!("ow_group_meta: {:#?}", row_group_meta);
    assert_eq!(row_group_meta.len(), 1);
    assert_eq!(row_group_meta[0].columns().len(), 3); // time and some_field
    assert!(row_group_meta[0].column(0).statistics().is_some()); // There is statistics for "time"
    assert!(row_group_meta[0].column(1).statistics().is_some()); // There is statistics for "some_field"
    assert!(row_group_meta[0].column(2).statistics().is_some()); // There is statistics for "null_field"

    let schema = decoded.read_schema().unwrap();
    let (_, field) = schema.field(0);
    assert_eq!(field.name(), "time");
    println!("schema: {:#?}", schema);

    let col_summary = decoded
        .read_statistics(&*schema)
        .expect("Invalid Statistics");
    assert!(col_summary.is_empty()); // TODO: must NOT be empty after the fix of 4714

    let got = decoded
        .read_iox_metadata_new()
        .expect("failed to deserialize embedded IOx metadata");
    assert_eq!(
        got, meta,
        "embedded metadata does not match original metadata"
    );
}

// Ensure that attempting to write an empty parquet file causes a panic for a
// human to investigate why it is happening.
//
// The idea is that currently it is a logical error to be producing empty
// parquet files at all - this might not always be the case, in which case
// removing this panic behaviour is perfectly fine too!
//
// Relates to "https://github.com/influxdata/influxdb_iox/issues/4695"
#[tokio::test]
#[should_panic = "serialised empty parquet file"]
async fn test_empty_parquet_file_panic() {
    // A representative IOx data sample (with a time column, an invariant upheld
    // in the IOx write path)
    let data = [
        (
            TIME_COLUMN_NAME,
            to_timestamp_array(&[]), // No data on purpose to reproduce the panic bug
        ),
        ("some_field", to_string_array(&[])),
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
        max_sequence_number: SequenceNumber::new(11),
        compaction_level: CompactionLevel::FileNonOverlapped,
        sort_key: None,
    };

    let batch = RecordBatch::try_from_iter(data).unwrap();
    let stream = futures::stream::iter([Ok(batch.clone())]);

    let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());
    let storage = ParquetStorage::new(object_store);

    // Serialising empty data should cause a panic for human investigation.
    let _ = storage.upload(stream, &meta).await;
}

#[tokio::test]
async fn test_decoded_many_columns_with_null_cols_iox_metadata() {
    // Increase these values to have larger test
    let num_cols = 10;
    let num_rows = 20;
    let num_repeats = 5;

    let mut data = Vec::with_capacity(num_cols);

    let t = 1646917692000000000;
    let mut time_arr: Vec<i64> = Vec::with_capacity(num_rows);
    let mut string_arr = Vec::with_capacity(num_rows);

    // Make long string data
    fn make_long_str(len: usize) -> String {
        "Long String Data".repeat(len)
    }
    let str = make_long_str(num_repeats);

    // Data of time and string columns
    for i in 0..num_rows {
        time_arr.push(t + i as i64);
        string_arr.push(str.as_str());
    }

    // First column is time
    data.push((TIME_COLUMN_NAME.to_string(), to_timestamp_array(&time_arr)));
    // Second column contains all nulls
    data.push(("column_name_1".to_string(), null_array(num_rows)));
    // Names of other columns
    fn make_col_name(i: usize) -> String {
        "column_name_".to_string() + i.to_string().as_str()
    }
    // Data of the rest of the columns
    for i in 2..num_cols {
        let col = make_col_name(i);
        let col_data = (col, to_string_array(&string_arr));
        data.push(col_data);
    }

    // And the metadata the batch would be encoded with if it came through the
    // IOx write path.

    // sort key includes all columns with time column last
    let mut sort_key_data = Vec::with_capacity(num_cols);
    for i in 1..num_cols {
        let col = make_col_name(i);
        sort_key_data.push(col);
    }
    sort_key_data.push(TIME_COLUMN_NAME.to_string());
    let sort_key = SortKey::from_columns(sort_key_data);

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
        max_sequence_number: SequenceNumber::new(11),
        compaction_level: CompactionLevel::FileNonOverlapped,
        sort_key: Some(sort_key),
    };

    let batch = RecordBatch::try_from_iter(data).unwrap();
    let stream = futures::stream::iter([Ok(batch.clone())]);

    let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());
    let storage = ParquetStorage::new(object_store);

    let (iox_parquet_meta, file_size) = storage
        .upload(stream, &meta)
        .await
        .expect("failed to serialize & persist record batch");

    // Sanity check - can't assert the actual value.
    assert!(file_size > 0);

    // Decode the IOx metadata embedded in the parquet file metadata.
    let decoded = iox_parquet_meta
        .decode()
        .expect("failed to decode parquet file metadata");

    // And verify the metadata matches the expected values.
    assert_eq!(
        decoded.row_count(),
        num_rows,
        "row count statistics does not match input row count"
    );

    let schema = decoded.read_schema().unwrap();
    let (_, field) = schema.field(0);
    assert_eq!(field.name(), "time");

    let col_summary = decoded
        .read_statistics(&*schema)
        .expect("Invalid Statistics");
    assert!(col_summary.is_empty()); // TODO: must NOT be empty after the fix of 4714

    let got = decoded
        .read_iox_metadata_new()
        .expect("failed to deserialize embedded IOx metadata");
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
    let partition_id = PartitionId::new(4);
    let meta = IoxMetadata {
        object_store_id: Default::default(),
        creation_timestamp: Time::from_timestamp_nanos(1234),
        namespace_id: NamespaceId::new(1),
        namespace_name: "bananas".into(),
        sequencer_id: SequencerId::new(2),
        table_id: TableId::new(3),
        table_name: "platanos".into(),
        partition_id,
        partition_key: "potato".into(),
        max_sequence_number: SequenceNumber::new(11),
        compaction_level: CompactionLevel::FileNonOverlapped,
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
        .expect("failed to serialize & persist record batch");

    // Use the IoxParquetMetaData and original IoxMetadata to derive a
    // ParquetFileParams.
    let column_id_map: HashMap<String, ColumnId> = HashMap::from([
        ("some_field".into(), ColumnId::new(1)),
        ("time".into(), ColumnId::new(2)),
    ]);
    let catalog_data = meta.to_parquet_file(partition_id, file_size, &iox_parquet_meta, |name| {
        *column_id_map.get(name).unwrap()
    });

    // And verify the resulting statistics used in the catalog.
    //
    // NOTE: thrift-encoded metadata not checked
    // TODO: check thrift-encoded metadata which may be the issue of bug 4695
    assert_eq!(catalog_data.sequencer_id, meta.sequencer_id);
    assert_eq!(catalog_data.namespace_id, meta.namespace_id);
    assert_eq!(catalog_data.table_id, meta.table_id);
    assert_eq!(catalog_data.partition_id, meta.partition_id);
    assert_eq!(catalog_data.object_store_id, meta.object_store_id);
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

fn null_array(num: usize) -> ArrayRef {
    let mut builder = StringBuilder::new(num);
    for _i in 0..num {
        builder.append_null().expect("failed to append null values");
    }
    Arc::new(builder.finish())
}
