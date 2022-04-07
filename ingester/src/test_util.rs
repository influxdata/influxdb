//! Test setups and data for ingester crate
#![allow(missing_docs)]

use crate::data::{
    IngesterData, NamespaceData, PartitionData, PersistingBatch, QueryableBatch, SequencerData,
    SnapshotBatch, TableData,
};
use arrow::record_batch::RecordBatch;
use arrow_util::assert_batches_eq;
use bitflags::bitflags;
use data_types2::{
    KafkaPartition, NamespaceId, PartitionId, SequenceNumber, SequencerId, TableId, Timestamp,
    Tombstone, TombstoneId,
};
use iox_catalog::{
    interface::{Catalog, INITIAL_COMPACTION_LEVEL},
    mem::MemCatalog,
};
use parquet_file::metadata::IoxMetadata;
use query::test::{raw_data, TestChunk};
use schema::sort::SortKey;
use std::{collections::BTreeMap, sync::Arc};
use time::{SystemProvider, Time, TimeProvider};
use uuid::Uuid;

/// Create a persisting batch, some tombstones and corresponding metadata for them after compaction
pub async fn make_persisting_batch_with_meta() -> (Arc<PersistingBatch>, Vec<Tombstone>, IoxMetadata)
{
    // record batches of input data
    let batches = create_batches_with_influxtype_different_columns_different_order().await;

    // tombstones
    let tombstones = vec![
        create_tombstone(
            1,
            1,
            1,
            100,                          // delete's seq_number
            0,                            // min time of data to get deleted
            200000,                       // max time of data to get deleted
            "tag2=CT and field_int=1000", // delete predicate
        ),
        create_tombstone(
            1, 1, 1, 101,        // delete's seq_number
            0,          // min time of data to get deleted
            200000,     // max time of data to get deleted
            "tag1!=MT", // delete predicate
        ),
    ];

    // IDs set to the persisting batch and its compacted metadata
    let uuid = Uuid::new_v4();
    let namespace_name = "test_namespace";
    let partition_key = "test_partition_key";
    let table_name = "test_table";
    let seq_id = 1;
    let seq_num_start: i64 = 1;
    let seq_num_end: i64 = seq_num_start + 1; // 2 batches
    let namespace_id = 1;
    let table_id = 1;
    let partition_id = 1;
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    let row_count = row_count.try_into().unwrap();

    // make the persisting batch
    let persisting_batch = make_persisting_batch(
        seq_id,
        seq_num_start,
        table_id,
        table_name,
        partition_id,
        uuid,
        batches,
        tombstones.clone(),
    );

    // make metadata
    let time_provider = Arc::new(SystemProvider::new());
    let meta = make_meta(
        uuid,
        time_provider.now(),
        seq_id,
        namespace_id,
        namespace_name,
        table_id,
        table_name,
        partition_id,
        partition_key,
        5,
        7000,
        seq_num_start,
        seq_num_end,
        row_count,
        INITIAL_COMPACTION_LEVEL,
        Some(SortKey::from_columns(vec!["tag1", "tag2", "time"])),
    );

    (persisting_batch, tombstones, meta)
}

/// Create tombstone for testing
pub fn create_tombstone(
    id: i64,
    table_id: i32,
    seq_id: i16,
    seq_num: i64,
    min_time: i64,
    max_time: i64,
    predicate: &str,
) -> Tombstone {
    Tombstone {
        id: TombstoneId::new(id),
        table_id: TableId::new(table_id),
        sequencer_id: SequencerId::new(seq_id),
        sequence_number: SequenceNumber::new(seq_num),
        min_time: Timestamp::new(min_time),
        max_time: Timestamp::new(max_time),
        serialized_predicate: predicate.to_string(),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn make_meta(
    object_store_id: Uuid,
    creation_timestamp: Time,
    sequencer_id: i16,
    namespace_id: i32,
    namespace_name: &str,
    table_id: i32,
    table_name: &str,
    partition_id: i64,
    partition_key: &str,
    min_time: i64,
    max_time: i64,
    min_sequence_number: i64,
    max_sequence_number: i64,
    row_count: i64,
    compaction_level: i16,
    sort_key: Option<SortKey>,
) -> IoxMetadata {
    IoxMetadata {
        object_store_id,
        creation_timestamp,
        sequencer_id: SequencerId::new(sequencer_id),
        namespace_id: NamespaceId::new(namespace_id),
        namespace_name: Arc::from(namespace_name),
        table_id: TableId::new(table_id),
        table_name: Arc::from(table_name),
        partition_id: PartitionId::new(partition_id),
        partition_key: Arc::from(partition_key),
        time_of_first_write: Time::from_timestamp_nanos(min_time),
        time_of_last_write: Time::from_timestamp_nanos(max_time),
        min_sequence_number: SequenceNumber::new(min_sequence_number),
        max_sequence_number: SequenceNumber::new(max_sequence_number),
        row_count,
        compaction_level,
        sort_key,
    }
}

#[allow(clippy::too_many_arguments)]
pub fn make_persisting_batch(
    seq_id: i16,
    seq_num_start: i64,
    table_id: i32,
    table_name: &str,
    partition_id: i64,
    object_store_id: Uuid,
    batches: Vec<Arc<RecordBatch>>,
    tombstones: Vec<Tombstone>,
) -> Arc<PersistingBatch> {
    let queryable_batch =
        make_queryable_batch_with_deletes(table_name, seq_num_start, batches, tombstones);

    Arc::new(PersistingBatch {
        sequencer_id: SequencerId::new(seq_id),
        table_id: TableId::new(table_id),
        partition_id: PartitionId::new(partition_id),
        object_store_id,
        data: queryable_batch,
    })
}

pub fn make_queryable_batch(
    table_name: &str,
    seq_num_start: i64,
    batches: Vec<Arc<RecordBatch>>,
) -> Arc<QueryableBatch> {
    make_queryable_batch_with_deletes(table_name, seq_num_start, batches, vec![])
}

pub fn make_queryable_batch_with_deletes(
    table_name: &str,
    seq_num_start: i64,
    batches: Vec<Arc<RecordBatch>>,
    tombstones: Vec<Tombstone>,
) -> Arc<QueryableBatch> {
    // make snapshots for the batches
    let mut snapshots = vec![];
    let mut seq_num = seq_num_start;
    for batch in batches {
        let seq = SequenceNumber::new(seq_num);
        snapshots.push(Arc::new(make_snapshot_batch(batch, seq, seq)));
        seq_num += 1;
    }

    Arc::new(QueryableBatch::new(table_name, snapshots, tombstones))
}

pub fn make_snapshot_batch(
    batch: Arc<RecordBatch>,
    min: SequenceNumber,
    max: SequenceNumber,
) -> SnapshotBatch {
    SnapshotBatch {
        min_sequencer_number: min,
        max_sequencer_number: max,
        data: batch,
    }
}

pub async fn create_one_row_record_batch_with_influxtype() -> Vec<Arc<RecordBatch>> {
    let chunk1 = Arc::new(
        TestChunk::new("t")
            .with_id(1)
            .with_time_column()
            .with_tag_column("tag1")
            .with_i64_field_column("field_int")
            .with_one_row_of_data(),
    );
    let batches = raw_data(&[chunk1]).await;

    // Make sure all data in one record batch
    assert_eq!(batches.len(), 1);

    // verify data
    let expected = vec![
        "+-----------+------+-----------------------------+",
        "| field_int | tag1 | time                        |",
        "+-----------+------+-----------------------------+",
        "| 1000      | MA   | 1970-01-01T00:00:00.000001Z |",
        "+-----------+------+-----------------------------+",
    ];
    assert_batches_eq!(&expected, &batches);

    let batches: Vec<_> = batches.iter().map(|r| Arc::new(r.clone())).collect();
    batches
}

pub async fn create_one_record_batch_with_influxtype_no_duplicates() -> Vec<Arc<RecordBatch>> {
    let chunk1 = Arc::new(
        TestChunk::new("t")
            .with_id(1)
            .with_time_column()
            .with_tag_column("tag1")
            .with_i64_field_column("field_int")
            .with_three_rows_of_data(),
    );
    let batches = raw_data(&[chunk1]).await;

    // Make sure all data in one record batch
    assert_eq!(batches.len(), 1);

    // verify data
    let expected = vec![
        "+-----------+------+-----------------------------+",
        "| field_int | tag1 | time                        |",
        "+-----------+------+-----------------------------+",
        "| 1000      | WA   | 1970-01-01T00:00:00.000008Z |",
        "| 10        | VT   | 1970-01-01T00:00:00.000010Z |",
        "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
        "+-----------+------+-----------------------------+",
    ];
    assert_batches_eq!(&expected, &batches);

    let batches: Vec<_> = batches.iter().map(|r| Arc::new(r.clone())).collect();
    batches
}

pub async fn create_one_record_batch_with_influxtype_duplicates() -> Vec<Arc<RecordBatch>> {
    let chunk1 = Arc::new(
        TestChunk::new("t")
            .with_id(1)
            .with_time_column() //_with_full_stats(
            .with_tag_column("tag1")
            .with_i64_field_column("field_int")
            .with_ten_rows_of_data_some_duplicates(),
    );
    let batches = raw_data(&[chunk1]).await;

    // Make sure all data in one record batch
    assert_eq!(batches.len(), 1);

    // verify data
    let expected = vec![
        "+-----------+------+--------------------------------+",
        "| field_int | tag1 | time                           |",
        "+-----------+------+--------------------------------+",
        "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
        "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
        "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
        "| 5         | MT   | 1970-01-01T00:00:00.000000005Z |",
        "| 1000      | MT   | 1970-01-01T00:00:00.000002Z    |",
        "| 20        | MT   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | 1970-01-01T00:00:00.000000500Z |",
        "| 10        | AL   | 1970-01-01T00:00:00.000000050Z |",
        "| 30        | MT   | 1970-01-01T00:00:00.000000005Z |",
        "+-----------+------+--------------------------------+",
    ];
    assert_batches_eq!(&expected, &batches);

    let batches: Vec<_> = batches.iter().map(|r| Arc::new(r.clone())).collect();
    batches
}

/// RecordBatches with knowledge of influx metadata
pub async fn create_batches_with_influxtype() -> Vec<Arc<RecordBatch>> {
    // Use the available TestChunk to create chunks and then convert them to raw RecordBatches
    let mut batches = vec![];

    // chunk1 with 10 rows and 3 columns
    let chunk1 = Arc::new(
        TestChunk::new("t")
            .with_id(1)
            .with_time_column()
            .with_tag_column("tag1")
            .with_i64_field_column("field_int")
            .with_ten_rows_of_data_some_duplicates(),
    );
    let batch1 = raw_data(&[chunk1]).await[0].clone();
    let expected = vec![
        "+-----------+------+--------------------------------+",
        "| field_int | tag1 | time                           |",
        "+-----------+------+--------------------------------+",
        "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
        "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
        "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
        "| 5         | MT   | 1970-01-01T00:00:00.000000005Z |",
        "| 1000      | MT   | 1970-01-01T00:00:00.000002Z    |",
        "| 20        | MT   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | 1970-01-01T00:00:00.000000500Z |",
        "| 10        | AL   | 1970-01-01T00:00:00.000000050Z |",
        "| 30        | MT   | 1970-01-01T00:00:00.000000005Z |",
        "+-----------+------+--------------------------------+",
    ];
    assert_batches_eq!(&expected, &[batch1.clone()]);
    batches.push(Arc::new(batch1));

    // chunk2 having duplicate data with chunk 1
    let chunk2 = Arc::new(
        TestChunk::new("t")
            .with_id(2)
            .with_time_column()
            .with_tag_column("tag1")
            .with_i64_field_column("field_int")
            .with_five_rows_of_data(),
    );
    let batch2 = raw_data(&[chunk2]).await[0].clone();
    let expected = vec![
        "+-----------+------+--------------------------------+",
        "| field_int | tag1 | time                           |",
        "+-----------+------+--------------------------------+",
        "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
        "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
        "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
        "| 5         | MT   | 1970-01-01T00:00:00.000005Z    |",
        "+-----------+------+--------------------------------+",
    ];
    assert_batches_eq!(&expected, &[batch2.clone()]);
    batches.push(Arc::new(batch2));

    // verify data from both batches
    let expected = vec![
        "+-----------+------+--------------------------------+",
        "| field_int | tag1 | time                           |",
        "+-----------+------+--------------------------------+",
        "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
        "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
        "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
        "| 5         | MT   | 1970-01-01T00:00:00.000000005Z |",
        "| 1000      | MT   | 1970-01-01T00:00:00.000002Z    |",
        "| 20        | MT   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | 1970-01-01T00:00:00.000000500Z |",
        "| 10        | AL   | 1970-01-01T00:00:00.000000050Z |",
        "| 30        | MT   | 1970-01-01T00:00:00.000000005Z |",
        "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
        "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
        "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
        "| 5         | MT   | 1970-01-01T00:00:00.000005Z    |",
        "+-----------+------+--------------------------------+",
    ];
    let b: Vec<_> = batches.iter().map(|b| (**b).clone()).collect();
    assert_batches_eq!(&expected, &b);

    batches
}

/// RecordBatches with knowledge of influx metadata
pub async fn create_batches_with_influxtype_different_columns() -> Vec<Arc<RecordBatch>> {
    // Use the available TestChunk to create chunks and then convert them to raw RecordBatches
    let mut batches = vec![];

    // chunk1 with 10 rows and 3 columns
    let chunk1 = Arc::new(
        TestChunk::new("t")
            .with_id(1)
            .with_time_column()
            .with_tag_column("tag1")
            .with_i64_field_column("field_int")
            .with_ten_rows_of_data_some_duplicates(),
    );
    let batch1 = raw_data(&[chunk1]).await[0].clone();
    let expected = vec![
        "+-----------+------+--------------------------------+",
        "| field_int | tag1 | time                           |",
        "+-----------+------+--------------------------------+",
        "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
        "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
        "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
        "| 5         | MT   | 1970-01-01T00:00:00.000000005Z |",
        "| 1000      | MT   | 1970-01-01T00:00:00.000002Z    |",
        "| 20        | MT   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | 1970-01-01T00:00:00.000000500Z |",
        "| 10        | AL   | 1970-01-01T00:00:00.000000050Z |",
        "| 30        | MT   | 1970-01-01T00:00:00.000000005Z |",
        "+-----------+------+--------------------------------+",
    ];
    assert_batches_eq!(&expected, &[batch1.clone()]);
    batches.push(Arc::new(batch1));

    // chunk2 having duplicate data with chunk 1
    // mmore columns
    let chunk2 = Arc::new(
        TestChunk::new("t")
            .with_id(2)
            .with_time_column()
            .with_tag_column("tag1")
            .with_i64_field_column("field_int")
            .with_tag_column("tag2")
            .with_i64_field_column("field_int2")
            .with_five_rows_of_data(),
    );
    let batch2 = raw_data(&[chunk2]).await[0].clone();
    let expected = vec![
        "+-----------+------------+------+------+--------------------------------+",
        "| field_int | field_int2 | tag1 | tag2 | time                           |",
        "+-----------+------------+------+------+--------------------------------+",
        "| 1000      | 1000       | MT   | CT   | 1970-01-01T00:00:00.000001Z    |",
        "| 10        | 10         | MT   | AL   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | 70         | CT   | CT   | 1970-01-01T00:00:00.000000100Z |",
        "| 100       | 100        | AL   | MA   | 1970-01-01T00:00:00.000000050Z |",
        "| 5         | 5          | MT   | AL   | 1970-01-01T00:00:00.000005Z    |",
        "+-----------+------------+------+------+--------------------------------+",
    ];
    assert_batches_eq!(&expected, &[batch2.clone()]);
    batches.push(Arc::new(batch2));

    batches
}

/// RecordBatches with knowledge of influx metadata
pub async fn create_batches_with_influxtype_different_columns_different_order(
) -> Vec<Arc<RecordBatch>> {
    // Use the available TestChunk to create chunks and then convert them to raw RecordBatches
    let mut batches = vec![];

    // chunk1 with 10 rows and 3 columns
    let chunk1 = Arc::new(
        TestChunk::new("t")
            .with_id(1)
            .with_time_column()
            .with_tag_column("tag1")
            .with_i64_field_column("field_int")
            .with_tag_column("tag2")
            .with_ten_rows_of_data_some_duplicates(),
    );
    let batch1 = raw_data(&[chunk1]).await[0].clone();
    let expected = vec![
        "+-----------+------+------+--------------------------------+",
        "| field_int | tag1 | tag2 | time                           |",
        "+-----------+------+------+--------------------------------+",
        "| 1000      | MT   | CT   | 1970-01-01T00:00:00.000001Z    |",
        "| 10        | MT   | AL   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | CT   | 1970-01-01T00:00:00.000000100Z |",
        "| 100       | AL   | MA   | 1970-01-01T00:00:00.000000050Z |",
        "| 5         | MT   | AL   | 1970-01-01T00:00:00.000000005Z |",
        "| 1000      | MT   | CT   | 1970-01-01T00:00:00.000002Z    |",
        "| 20        | MT   | AL   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | CT   | 1970-01-01T00:00:00.000000500Z |",
        "| 10        | AL   | MA   | 1970-01-01T00:00:00.000000050Z |",
        "| 30        | MT   | AL   | 1970-01-01T00:00:00.000000005Z |",
        "+-----------+------+------+--------------------------------+",
    ];
    assert_batches_eq!(&expected, &[batch1.clone()]);
    batches.push(Arc::new(batch1.clone()));

    // chunk2 having duplicate data with chunk 1
    // mmore columns
    let chunk2 = Arc::new(
        TestChunk::new("t")
            .with_id(2)
            .with_time_column()
            .with_tag_column("tag2")
            .with_i64_field_column("field_int")
            .with_five_rows_of_data(),
    );
    let batch2 = raw_data(&[chunk2]).await[0].clone();
    let expected = vec![
        "+-----------+------+--------------------------------+",
        "| field_int | tag2 | time                           |",
        "+-----------+------+--------------------------------+",
        "| 1000      | CT   | 1970-01-01T00:00:00.000001Z    |",
        "| 10        | AL   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
        "| 100       | MA   | 1970-01-01T00:00:00.000000050Z |",
        "| 5         | AL   | 1970-01-01T00:00:00.000005Z    |",
        "+-----------+------+--------------------------------+",
    ];
    assert_batches_eq!(&expected, &[batch2.clone()]);
    batches.push(Arc::new(batch2));

    batches
}

/// Has 2 tag columns; tag1 has a lower cardinality (3) than tag3 (4)
pub async fn create_batches_with_influxtype_different_cardinality() -> Vec<Arc<RecordBatch>> {
    // Use the available TestChunk to create chunks and then convert them to raw RecordBatches
    let mut batches = vec![];

    let chunk1 = Arc::new(
        TestChunk::new("t")
            .with_id(1)
            .with_time_column()
            .with_tag_column("tag1")
            .with_i64_field_column("field_int")
            .with_tag_column("tag3")
            .with_four_rows_of_data(),
    );
    let batch1 = raw_data(&[chunk1]).await[0].clone();
    let expected = vec![
        "+-----------+------+------+-----------------------------+",
        "| field_int | tag1 | tag3 | time                        |",
        "+-----------+------+------+-----------------------------+",
        "| 1000      | WA   | TX   | 1970-01-01T00:00:00.000028Z |",
        "| 10        | VT   | PR   | 1970-01-01T00:00:00.000210Z |",
        "| 70        | UT   | OR   | 1970-01-01T00:00:00.000220Z |",
        "| 50        | VT   | AL   | 1970-01-01T00:00:00.000210Z |",
        "+-----------+------+------+-----------------------------+",
    ];
    assert_batches_eq!(&expected, &[batch1.clone()]);
    batches.push(Arc::new(batch1.clone()));

    let chunk2 = Arc::new(
        TestChunk::new("t")
            .with_id(2)
            .with_time_column()
            .with_tag_column("tag1")
            .with_tag_column("tag3")
            .with_i64_field_column("field_int")
            .with_four_rows_of_data(),
    );
    let batch2 = raw_data(&[chunk2]).await[0].clone();
    let expected = vec![
        "+-----------+------+------+-----------------------------+",
        "| field_int | tag1 | tag3 | time                        |",
        "+-----------+------+------+-----------------------------+",
        "| 1000      | WA   | TX   | 1970-01-01T00:00:00.000028Z |",
        "| 10        | VT   | PR   | 1970-01-01T00:00:00.000210Z |",
        "| 70        | UT   | OR   | 1970-01-01T00:00:00.000220Z |",
        "| 50        | VT   | AL   | 1970-01-01T00:00:00.000210Z |",
        "+-----------+------+------+-----------------------------+",
    ];
    assert_batches_eq!(&expected, &[batch2.clone()]);
    batches.push(Arc::new(batch2));

    batches
}

/// RecordBatches with knowledge of influx metadata
pub async fn create_batches_with_influxtype_same_columns_different_type() -> Vec<Arc<RecordBatch>> {
    // Use the available TestChunk to create chunks and then convert them to raw RecordBatches
    let mut batches = vec![];

    // chunk1
    let chunk1 = Arc::new(
        TestChunk::new("t")
            .with_id(1)
            .with_time_column()
            .with_tag_column("tag1")
            .with_i64_field_column("field_int")
            .with_three_rows_of_data(),
    );
    let batch1 = raw_data(&[chunk1]).await[0].clone();
    let expected = vec![
        "+-----------+------+-----------------------------+",
        "| field_int | tag1 | time                        |",
        "+-----------+------+-----------------------------+",
        "| 1000      | WA   | 1970-01-01T00:00:00.000008Z |",
        "| 10        | VT   | 1970-01-01T00:00:00.000010Z |",
        "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
        "+-----------+------+-----------------------------+",
    ];
    assert_batches_eq!(&expected, &[batch1.clone()]);
    batches.push(Arc::new(batch1));

    // chunk2 having duplicate data with chunk 1
    // mmore columns
    let chunk2 = Arc::new(
        TestChunk::new("t")
            .with_id(2)
            .with_time_column()
            .with_u64_column("field_int") //  u64 type but on existing name "field_int" used for i64 in chunk 1
            .with_tag_column("tag2")
            .with_three_rows_of_data(),
    );
    let batch2 = raw_data(&[chunk2]).await[0].clone();
    let expected = vec![
        "+-----------+------+-----------------------------+",
        "| field_int | tag2 | time                        |",
        "+-----------+------+-----------------------------+",
        "| 1000      | SC   | 1970-01-01T00:00:00.000008Z |",
        "| 10        | NC   | 1970-01-01T00:00:00.000010Z |",
        "| 70        | RI   | 1970-01-01T00:00:00.000020Z |",
        "+-----------+------+-----------------------------+",
    ];
    assert_batches_eq!(&expected, &[batch2.clone()]);
    batches.push(Arc::new(batch2));

    batches
}

pub const TEST_NAMESPACE: &str = "test_namespace";
pub const TEST_NAMESPACE_EMPTY: &str = "test_namespace_empty";
pub const TEST_TABLE: &str = "test_table";
pub const TEST_TABLE_EMPTY: &str = "test_table_empty";
pub const TEST_PARTITION_1: &str = "test+partition_1";
pub const TEST_PARTITION_2: &str = "test+partition_2";

bitflags! {
    /// Make the same in-memory data but data are split between:
    ///    . one or two partition
    ///    . The first partition will have a choice to have data in either
    ///       . buffer only
    ///       . snapshot only
    ///       . persisting only
    ///       . buffer + snapshot
    ///       . buffer + persisting
    ///       . snapshot + persisting
    ///       . buffer + snapshot + persisting
    ///    . If the second partittion exists, it only has data in its buffer
    pub struct DataLocation: u8 {
        const BUFFER = 0b001;
        const SNAPSHOT = 0b010;
        const PERSISTING = 0b100;
        const BUFFER_SNAPSHOT = Self::BUFFER.bits | Self::SNAPSHOT.bits;
        const BUFFER_PERSISTING = Self::BUFFER.bits | Self::PERSISTING.bits;
        const SNAPSHOT_PERSISTING = Self::SNAPSHOT.bits | Self::PERSISTING.bits;
        const BUFFER_SNAPSHOT_PERSISTING = Self::BUFFER.bits | Self::SNAPSHOT.bits | Self::PERSISTING.bits;
    }
}

/// This function produces one scenario but with the parameter combination (2*7),
/// you will be able to produce 14 scenarios by calling it in 2 loops
pub fn make_ingester_data(two_partitions: bool, loc: DataLocation) -> IngesterData {
    // Whatever data because they won't be used in the tests
    let metrics: Arc<metric::Registry> = Default::default();
    let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
    let object_store = Arc::new(object_store::ObjectStoreImpl::new_in_memory());
    let exec = Arc::new(query::exec::Executor::new(1));

    // Make data for one sequencer/shard and two tables
    let seq_id = SequencerId::new(1);
    let empty_table_id = TableId::new(1);
    let data_table_id = TableId::new(2);

    // Make partitions per requested
    let partitions = make_partitions(two_partitions, loc, seq_id, data_table_id, TEST_TABLE);

    // Two tables: one empty and one with data of one or two partitions
    let mut tables = BTreeMap::new();
    let empty_tbl = Arc::new(tokio::sync::RwLock::new(TableData::new(
        empty_table_id,
        None,
    )));
    let data_tbl = Arc::new(tokio::sync::RwLock::new(TableData::new_for_test(
        data_table_id,
        None,
        partitions,
    )));
    tables.insert(TEST_TABLE_EMPTY.to_string(), empty_tbl);
    tables.insert(TEST_TABLE.to_string(), data_tbl);

    // Two namespaces: one empty and one with data of 2 tables
    let mut namespaces = BTreeMap::new();
    let empty_ns = Arc::new(NamespaceData::new(NamespaceId::new(1), &*metrics));
    let data_ns = Arc::new(NamespaceData::new_for_test(NamespaceId::new(2), tables));
    namespaces.insert(TEST_NAMESPACE_EMPTY.to_string(), empty_ns);
    namespaces.insert(TEST_NAMESPACE.to_string(), data_ns);

    // One sequencer/shard that contains 2 namespaces
    let kafka_partition = KafkaPartition::new(0);
    let seq_data = SequencerData::new_for_test(kafka_partition, namespaces);
    let mut sequencers = BTreeMap::new();
    sequencers.insert(seq_id, seq_data);

    // Ingester data that inlcudes one sequencer/shard
    IngesterData {
        object_store,
        catalog,
        sequencers,
        exec,
        backoff_config: backoff::BackoffConfig::default(),
    }
}

pub async fn make_ingester_data_with_tombstones(loc: DataLocation) -> IngesterData {
    // Whatever data because they won't be used in the tests
    let metrics: Arc<metric::Registry> = Default::default();
    let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(metrics));
    let object_store = Arc::new(object_store::ObjectStoreImpl::new_in_memory());
    let exec = Arc::new(query::exec::Executor::new(1));

    // Make data for one sequencer/shard and two tables
    let seq_id = SequencerId::new(1);
    let data_table_id = TableId::new(2);

    // Make partitions per requested
    let partitions =
        make_one_partition_with_tombstones(&exec, loc, seq_id, data_table_id, TEST_TABLE).await;

    // Two tables: one empty and one with data of one or two partitions
    let mut tables = BTreeMap::new();
    let data_tbl = TableData::new_for_test(data_table_id, None, partitions);
    tables.insert(
        TEST_TABLE.to_string(),
        Arc::new(tokio::sync::RwLock::new(data_tbl)),
    );

    // Two namespaces: one empty and one with data of 2 tables
    let mut namespaces = BTreeMap::new();
    let data_ns = Arc::new(NamespaceData::new_for_test(NamespaceId::new(2), tables));
    namespaces.insert(TEST_NAMESPACE.to_string(), data_ns);

    // One sequencer/shard that contains 1 namespace
    let kafka_partition = KafkaPartition::new(0);
    let seq_data = SequencerData::new_for_test(kafka_partition, namespaces);
    let mut sequencers = BTreeMap::new();
    sequencers.insert(seq_id, seq_data);

    // Ingester data that inlcudes one sequencer/shard
    IngesterData {
        object_store,
        catalog,
        sequencers,
        exec,
        backoff_config: backoff::BackoffConfig::default(),
    }
}

/// Make data for one or two partitions per requested
pub(crate) fn make_partitions(
    two_partitions: bool,
    loc: DataLocation,
    sequencer_id: SequencerId,
    table_id: TableId,
    table_name: &str,
) -> BTreeMap<String, PartitionData> {
    // In-memory data includes these rows but split between 4 groups go into
    // different batches of parittion 1 or partittion 2  as requeted
    // let expected = vec![
    //         "+------------+-----+------+--------------------------------+",
    //         "| city       | day | temp | time                           |",
    //         "+------------+-----+------+--------------------------------+",
    //         "| Andover    | tue | 56   | 1970-01-01T00:00:00.000000030Z |", // in group 1 - seq_num: 2
    //         "| Andover    | mon |      | 1970-01-01T00:00:00.000000046Z |", // in group 2 - seq_num: 3
    //         "| Boston     | sun | 60   | 1970-01-01T00:00:00.000000036Z |", // in group 1 - seq_num: 1
    //         "| Boston     | mon |      | 1970-01-01T00:00:00.000000038Z |", // in group 3 - seq_num: 5
    //         "| Medford    | sun | 55   | 1970-01-01T00:00:00.000000022Z |", // in group 4 - seq_num: 7
    //         "| Medford    | wed |      | 1970-01-01T00:00:00.000000026Z |", // in group 2 - seq_num: 4
    //         "| Reading    | mon | 58   | 1970-01-01T00:00:00.000000040Z |", // in group 4 - seq_num: 8
    //         "| Wilmington | mon |      | 1970-01-01T00:00:00.000000035Z |", // in group 3 - seq_num: 6
    //         "+------------+-----+------+--------------------------------+",
    //     ];

    let mut partitions = BTreeMap::new();

    // ------------------------------------------
    // Build the first partition
    let partition_id = PartitionId::new(1);
    let (mut p1, seq_num) =
        make_first_partition_data(partition_id, loc, sequencer_id, table_id, table_name);

    // ------------------------------------------
    // Build the second partition if asked

    let mut seq_num = seq_num.get();
    if two_partitions {
        let partition_id = PartitionId::new(2);
        let mut p2 = PartitionData::new(partition_id);
        // Group 4: in buffer of p2
        // Fill `buffer`
        seq_num += 1;
        let (_, mb) = mutable_batch_lp::test_helpers::lp_to_mutable_batch(
            r#"test_table,city=Medford day="sun",temp=55 22"#,
        );
        p2.buffer_write(SequenceNumber::new(seq_num), mb).unwrap();
        seq_num += 1;
        let (_, mb) = mutable_batch_lp::test_helpers::lp_to_mutable_batch(
            r#"test_table,city=Reading day="mon",temp=58 40"#,
        );
        p2.buffer_write(SequenceNumber::new(seq_num), mb).unwrap();

        partitions.insert(TEST_PARTITION_2.to_string(), p2);
    } else {
        // Group 4: in buffer of p1
        // Fill `buffer`
        seq_num += 1;
        let (_, mb) = mutable_batch_lp::test_helpers::lp_to_mutable_batch(
            r#"test_table,city=Medford day="sun",temp=55 22"#,
        );
        p1.buffer_write(SequenceNumber::new(seq_num), mb).unwrap();
        seq_num += 1;
        let (_, mb) = mutable_batch_lp::test_helpers::lp_to_mutable_batch(
            r#"test_table,city=Reading day="mon",temp=58 40"#,
        );
        p1.buffer_write(SequenceNumber::new(seq_num), mb).unwrap();
    }

    partitions.insert(TEST_PARTITION_1.to_string(), p1);
    partitions
}

/// Make data for one partition with tombstones
pub(crate) async fn make_one_partition_with_tombstones(
    exec: &query::exec::Executor,
    loc: DataLocation,
    sequencer_id: SequencerId,
    table_id: TableId,
    table_name: &str,
) -> BTreeMap<String, PartitionData> {
    // In-memory data includes these rows but split between 4 groups go into
    // different batches of parittion 1 or partittion 2  as requeted
    // let expected = vec![
    //         "+------------+-----+------+--------------------------------+",
    //         "| city       | day | temp | time                           |",
    //         "+------------+-----+------+--------------------------------+",
    //         "| Andover    | tue | 56   | 1970-01-01T00:00:00.000000030Z |", // in group 1 - seq_num: 2
    //         "| Andover    | mon |      | 1970-01-01T00:00:00.000000046Z |", // in group 2 - seq_num: 3
    //         "| Boston     | sun | 60   | 1970-01-01T00:00:00.000000036Z |", // in group 1 - seq_num: 1  --> will get deleted
    //         "| Boston     | mon |      | 1970-01-01T00:00:00.000000038Z |", // in group 3 - seq_num: 5  --> will get deleted
    //         "| Medford    | sun | 55   | 1970-01-01T00:00:00.000000022Z |", // in group 4 - seq_num: 8  (after the tombstone's seq num)
    //         "| Medford    | wed |      | 1970-01-01T00:00:00.000000026Z |", // in group 2 - seq_num: 4
    //         "| Reading    | mon | 58   | 1970-01-01T00:00:00.000000040Z |", // in group 4 - seq_num: 9
    //         "| Wilmington | mon |      | 1970-01-01T00:00:00.000000035Z |", // in group 3 - seq_num: 6
    //         "+------------+-----+------+--------------------------------+",
    //     ];

    let partition_id = PartitionId::new(1);
    let (mut p1, seq_num) =
        make_first_partition_data(partition_id, loc, sequencer_id, table_id, table_name);

    // Add tombtones
    // Depending on where the existing data is, they (buffer & snapshot) will be either moved to a new sanpshot after
    // appying the tombstone or (persisting) stay where they are and the tomstobes is kept to get applied later
    // ------------------------------------------
    // Delete
    let mut seq_num = seq_num.get();
    seq_num += 1;
    let ts = create_tombstone(
        2,                  // tombstone id
        table_id.get(),     // table id
        sequencer_id.get(), // sequencer id
        seq_num,            // delete's seq_number
        10,                 // min time of data to get deleted
        50,                 // max time of data to get deleted
        "city=Boston",      // delete predicate
    );
    p1.buffer_tombstone(exec, table_name, ts).await;

    // Group 4: in buffer of p1 after the tombstone
    // Fill `buffer`
    seq_num += 1;
    let (_, mb) = mutable_batch_lp::test_helpers::lp_to_mutable_batch(
        r#"test_table,city=Medford day="sun",temp=55 22"#,
    );
    p1.buffer_write(SequenceNumber::new(seq_num), mb).unwrap();
    seq_num += 1;
    let (_, mb) = mutable_batch_lp::test_helpers::lp_to_mutable_batch(
        r#"test_table,city=Reading day="mon",temp=58 40"#,
    );
    p1.buffer_write(SequenceNumber::new(seq_num), mb).unwrap();

    let mut partitions = BTreeMap::new();
    partitions.insert(TEST_PARTITION_1.to_string(), p1);

    partitions
}

fn make_first_partition_data(
    partition_id: PartitionId,
    loc: DataLocation,
    sequencer_id: SequencerId,
    table_id: TableId,
    table_name: &str,
) -> (PartitionData, SequenceNumber) {
    // In-memory data inlcudes these rows but split between 3 groups go into
    // different batches of parittion p1
    // let expected = vec![
    //         "+------------+-----+------+--------------------------------+",
    //         "| city       | day | temp | time                           |",
    //         "+------------+-----+------+--------------------------------+",
    //         "| Andover    | tue | 56   | 1970-01-01T00:00:00.000000030Z |", // in group 1 - seq_num: 2
    //         "| Andover    | mon |      | 1970-01-01T00:00:00.000000046Z |", // in group 2 - seq_num: 3
    //         "| Boston     | sun | 60   | 1970-01-01T00:00:00.000000036Z |", // in group 1 - seq_num: 1
    //         "| Boston     | mon |      | 1970-01-01T00:00:00.000000038Z |", // in group 3 - seq_num: 5
    //         "| Medford    | wed |      | 1970-01-01T00:00:00.000000026Z |", // in group 2 - seq_num: 4
    //         "| Wilmington | mon |      | 1970-01-01T00:00:00.000000035Z |", // in group 3 - seq_num: 6
    //         "+------------+-----+------+--------------------------------+",
    //     ];

    // ------------------------------------------
    // Build the first partition
    let mut p1 = PartitionData::new(partition_id);
    let mut seq_num = 0;

    // --------------------
    // Group 1
    // Fill `buffer`
    let (_, mb) = mutable_batch_lp::test_helpers::lp_to_mutable_batch(
        r#"test_table,city=Boston day="sun",temp=60 36"#,
    );
    seq_num += 1;
    p1.buffer_write(SequenceNumber::new(seq_num), mb).unwrap();
    seq_num += 1;
    let (_, mb) = mutable_batch_lp::test_helpers::lp_to_mutable_batch(
        r#"test_table,city=Andover day="tue",temp=56 30"#,
    );
    p1.buffer_write(SequenceNumber::new(seq_num), mb).unwrap();

    if loc.contains(DataLocation::PERSISTING) {
        // Move group 1 data to persisting
        p1.snapshot_to_persisting_batch(sequencer_id, table_id, partition_id, table_name);
    } else if loc.contains(DataLocation::SNAPSHOT) {
        // move group 1 data to snapshot
        p1.snapshot().unwrap();
    } else {
    } // keep it in buffer

    // --------------------
    // Group 2
    // Fill `buffer`
    seq_num += 1;
    let (_, mb) = mutable_batch_lp::test_helpers::lp_to_mutable_batch(
        r#"test_table,city=Andover day="mon" 46"#,
    );
    p1.buffer_write(SequenceNumber::new(seq_num), mb).unwrap();
    seq_num += 1;
    let (_, mb) = mutable_batch_lp::test_helpers::lp_to_mutable_batch(
        r#"test_table,city=Medford day="wed" 26"#,
    );
    p1.buffer_write(SequenceNumber::new(seq_num), mb).unwrap();

    if loc.contains(DataLocation::SNAPSHOT) {
        // move group 2 data to snapshot
        p1.snapshot().unwrap();
    } else {
    } // keep it in buffer

    // --------------------
    // Group 3: always in buffer
    // Fill `buffer`
    seq_num += 1;
    let (_, mb) = mutable_batch_lp::test_helpers::lp_to_mutable_batch(
        r#"test_table,city=Boston day="mon" 38"#,
    );
    p1.buffer_write(SequenceNumber::new(seq_num), mb).unwrap();
    seq_num += 1;
    let (_, mb) = mutable_batch_lp::test_helpers::lp_to_mutable_batch(
        r#"test_table,city=Wilmington day="mon" 35"#,
    );
    p1.buffer_write(SequenceNumber::new(seq_num), mb).unwrap();

    (p1, SequenceNumber::new(seq_num))
}
