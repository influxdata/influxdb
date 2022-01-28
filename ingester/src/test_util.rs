//! Test setups and data for ingetser crate

use crate::data::{PersistingBatch, QueryableBatch, SnapshotBatch};
use arrow::record_batch::RecordBatch;
use arrow_util::assert_batches_eq;
use iox_catalog::interface::{
    NamespaceId, PartitionId, SequenceNumber, SequencerId, TableId, Timestamp, Tombstone,
    TombstoneId,
};
use parquet_file::metadata::IoxMetadata;
use query::test::{raw_data, TestChunk};
use std::sync::Arc;
use time::{SystemProvider, Time, TimeProvider};
use uuid::Uuid;

/// Create a persting batch, some tombstones and corresponding metadata fot them after compaction
pub async fn make_persisting_batch_with_meta() -> (Arc<PersistingBatch>, Vec<Tombstone>, IoxMetadata)
{
    // record bacthes of input data
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

///
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
    }
}

///
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

///
pub fn make_queryable_batch(
    table_name: &str,
    seq_num_start: i64,
    batches: Vec<Arc<RecordBatch>>,
) -> Arc<QueryableBatch> {
    make_queryable_batch_with_deletes(table_name, seq_num_start, batches, vec![])
}

///
pub fn make_queryable_batch_with_deletes(
    table_name: &str,
    seq_num_start: i64,
    batches: Vec<Arc<RecordBatch>>,
    tombstones: Vec<Tombstone>,
) -> Arc<QueryableBatch> {
    // make snapshots for the bacthes
    let mut snapshots = vec![];
    let mut seq_num = seq_num_start;
    for batch in batches {
        let seq = SequenceNumber::new(seq_num);
        snapshots.push(make_snapshot_batch(batch, seq, seq));
        seq_num += 1;
    }

    Arc::new(QueryableBatch::new(table_name, snapshots, tombstones))
}

///
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
///
pub async fn create_one_record_batch_with_influxtype_no_duplicates() -> Vec<Arc<RecordBatch>> {
    let chunk1 = Arc::new(
        TestChunk::new("t")
            .with_id(1)
            .with_time_column() //_with_full_stats(
            .with_tag_column("tag1")
            .with_i64_field_column("field_int")
            .with_three_rows_of_data(),
    );
    let batches = raw_data(&[chunk1]).await;

    // Make sure all dat ain one record batch
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

///
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

    // Make sure all dat ain one record batch
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
