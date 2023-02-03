use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{ArrayRef, StringArray, TimestampNanosecondArray},
    record_batch::RecordBatch,
};
use data_types::{
    ColumnId, CompactionLevel, NamespaceId, PartitionId, SequenceNumber, ShardId, TableId,
    Timestamp,
};
use datafusion_util::MemoryStream;
use iox_time::Time;
use object_store::DynObjectStore;
use parquet_file::{
    metadata::IoxMetadata,
    serialize::CodecError,
    storage::{ParquetStorage, StorageId, UploadError},
};
use schema::{
    builder::SchemaBuilder, sort::SortKey, InfluxColumnType, InfluxFieldType, TIME_COLUMN_NAME,
};

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
            InfluxColumnType::Timestamp,
        ),
        (
            "some_field",
            to_string_array(&["bananas", "platanos", "manzana"]),
            InfluxColumnType::Field(InfluxFieldType::String),
        ),
        (
            "null_field",
            null_string_array(3),
            InfluxColumnType::Field(InfluxFieldType::String),
        ),
    ];

    // And the metadata the batch would be encoded with if it came through the
    // IOx write path.
    let meta = IoxMetadata {
        object_store_id: Default::default(),
        creation_timestamp: Time::from_timestamp_nanos(42),
        namespace_id: NamespaceId::new(1),
        namespace_name: "bananas".into(),
        shard_id: ShardId::new(2),
        table_id: TableId::new(3),
        table_name: "platanos".into(),
        partition_id: PartitionId::new(4),
        partition_key: "potato".into(),
        max_sequence_number: SequenceNumber::new(11),
        compaction_level: CompactionLevel::FileNonOverlapped,
        sort_key: None,
        max_l0_created_at: Time::from_timestamp_nanos(42),
    };

    let mut schema_builder = SchemaBuilder::new();
    for (name, _array, column_type) in &data {
        schema_builder.influx_column(name, *column_type);
    }
    let schema = schema_builder.build().unwrap();

    let batch = RecordBatch::try_new(
        schema.as_arrow(),
        data.into_iter()
            .map(|(_name, array, _column_type)| array)
            .collect(),
    )
    .unwrap();
    let stream = Box::pin(MemoryStream::new(vec![batch.clone()]));

    let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());
    let storage = ParquetStorage::new(object_store, StorageId::from("iox"));

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
    println!("ow_group_meta: {row_group_meta:#?}");
    assert_eq!(row_group_meta.len(), 1);
    assert_eq!(row_group_meta[0].columns().len(), 3); // time and some_field
    assert!(row_group_meta[0].column(0).statistics().is_some()); // There is statistics for "time"
    assert!(row_group_meta[0].column(1).statistics().is_some()); // There is statistics for "some_field"
    assert!(row_group_meta[0].column(2).statistics().is_some()); // There is statistics for "null_field"

    let schema = decoded.read_schema().unwrap();
    let (_, field) = schema.field(0);
    assert_eq!(field.name(), "time");
    println!("schema: {schema:#?}");

    let col_summary = decoded
        .read_statistics(&schema)
        .expect("Invalid Statistics");
    assert_eq!(col_summary.len(), 3);

    let got = decoded
        .read_iox_metadata_new()
        .expect("failed to deserialize embedded IOx metadata");
    assert_eq!(
        got, meta,
        "embedded metadata does not match original metadata"
    );
}

// Ensure that attempting to write an empty parquet file causes a error to be
// raised. The caller can then decide if this is acceptable plan output or a
// bug.
//
// It used to be considered a logical error to be producing empty parquet files
// at all - we have previously identified cases of useless work being performed
// by inducing a panic when observing a parquet file with 0 rows, however we now
// tolerate 0 row outputs as the compactor can perform multiple splits at once,
// which is problematic when a single chunk can overlap multiple split points:
//
//                  ────────────── Time ────────────▶
//
//                          │                │
//                  ┌────────────────────────────────┐
//                  │       │    Chunk 1     │       │
//                  └────────────────────────────────┘
//                          │                │
//
//                          │                │
//
//                      Split T1         Split T2
//
// If this chunk has an unusual distribution of writes over the time range it
// covers, we can wind up with the split between T1 and T2 containing no data.
// For example, if all the data is either before T1, or after T2 we can wind up
// with a split plan such as this, where the middle sub-section contains no
// data:
//
//                          │                │
//                  ┌█████──────────────────────█████┐
//                  │█████  │    Chunk 1     │  █████│
//                  └█████──────────────────────█████┘
//                          │                │
//
//                          │                │
//
//                      Split T1         Split T2
//
// It is not possible to use the chunk statistics (min/max timestamps) to
// determine this empty sub-section will result ahead of time, therefore the
// parquet encoder must tolerate it and raise a non-fatal error instead of
// panicking.
//
// Relates to:
//      * https://github.com/influxdata/influxdb_iox/issues/4695
//      * https://github.com/influxdata/conductor/issues/1121
#[tokio::test]
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
        shard_id: ShardId::new(2),
        table_id: TableId::new(3),
        table_name: "platanos".into(),
        partition_id: PartitionId::new(4),
        partition_key: "potato".into(),
        max_sequence_number: SequenceNumber::new(11),
        compaction_level: CompactionLevel::FileNonOverlapped,
        sort_key: None,
        max_l0_created_at: Time::from_timestamp_nanos(42),
    };

    let batch = RecordBatch::try_from_iter(data).unwrap();
    let stream = Box::pin(MemoryStream::new(vec![batch.clone()]));

    let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());
    let storage = ParquetStorage::new(object_store, StorageId::from("iox"));

    // Serialising empty data should cause a panic for human investigation.
    let err = storage
        .upload(stream, &meta)
        .await
        .expect_err("empty file should raise an error");

    assert!(matches!(err, UploadError::Serialise(CodecError::NoRows)));
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
    data.push((
        TIME_COLUMN_NAME.to_string(),
        to_timestamp_array(&time_arr),
        InfluxColumnType::Timestamp,
    ));
    // Second column contains all nulls
    data.push((
        "column_name_1".to_string(),
        null_string_array(num_rows),
        InfluxColumnType::Field(InfluxFieldType::String),
    ));
    // Names of other columns
    fn make_col_name(i: usize) -> String {
        "column_name_".to_string() + i.to_string().as_str()
    }
    // Data of the rest of the columns
    for i in 2..num_cols {
        let col = make_col_name(i);
        let col_data = (
            col,
            to_string_array(&string_arr),
            InfluxColumnType::Field(InfluxFieldType::String),
        );
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
        shard_id: ShardId::new(2),
        table_id: TableId::new(3),
        table_name: "platanos".into(),
        partition_id: PartitionId::new(4),
        partition_key: "potato".into(),
        max_sequence_number: SequenceNumber::new(11),
        compaction_level: CompactionLevel::FileNonOverlapped,
        sort_key: Some(sort_key),
        max_l0_created_at: Time::from_timestamp_nanos(42),
    };

    let mut schema_builder = SchemaBuilder::new();
    for (name, _array, column_type) in &data {
        schema_builder.influx_column(name, *column_type);
    }
    let schema = schema_builder.build().unwrap();

    let batch = RecordBatch::try_new(
        schema.as_arrow(),
        data.into_iter()
            .map(|(_name, array, _column_type)| array)
            .collect(),
    )
    .unwrap();
    let stream = Box::pin(MemoryStream::new(vec![batch.clone()]));

    let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());
    let storage = ParquetStorage::new(object_store, StorageId::from("iox"));

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
        .read_statistics(&schema)
        .expect("Invalid Statistics");
    assert_eq!(col_summary.len(), num_cols);

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
        shard_id: ShardId::new(2),
        table_id: TableId::new(3),
        table_name: "platanos".into(),
        partition_id,
        partition_key: "potato".into(),
        max_sequence_number: SequenceNumber::new(11),
        compaction_level: CompactionLevel::FileNonOverlapped,
        sort_key: None,
        max_l0_created_at: Time::from_timestamp_nanos(1234),
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
    let stream = Box::pin(MemoryStream::new(vec![batch.clone()]));

    let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());
    let storage = ParquetStorage::new(object_store, StorageId::from("iox"));

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
    assert_eq!(catalog_data.shard_id, meta.shard_id);
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
    assert_eq!(catalog_data.max_l0_created_at, Timestamp::new(1234));
}

fn to_string_array(strs: &[&str]) -> ArrayRef {
    let array: StringArray = strs.iter().map(|s| Some(*s)).collect();
    Arc::new(array)
}

fn to_timestamp_array(timestamps: &[i64]) -> ArrayRef {
    let array: TimestampNanosecondArray = timestamps.iter().map(|v| Some(*v)).collect();
    Arc::new(array)
}

fn null_string_array(num: usize) -> ArrayRef {
    let array: StringArray = std::iter::repeat(None as Option<&str>).take(num).collect();
    Arc::new(array)
}
