//! Test setups and data for ingester crate

#![allow(missing_docs)]
#![cfg(test)]

use std::{sync::Arc, time::Duration};

use arrow::record_batch::RecordBatch;
use arrow_util::assert_batches_eq;
use data_types::{
    NamespaceId, PartitionKey, Sequence, SequenceNumber, ShardId, ShardIndex, TableId,
};
use dml::{DmlMeta, DmlOperation, DmlWrite};
use iox_catalog::{interface::Catalog, mem::MemCatalog};
use iox_query::test::{raw_data, TestChunk};
use iox_time::{SystemProvider, Time};
use mutable_batch_lp::lines_to_batches;
use object_store::memory::InMemory;

use crate::{
    data::IngesterData,
    lifecycle::{LifecycleConfig, LifecycleManager},
};

pub(crate) async fn create_one_row_record_batch_with_influxtype() -> Vec<Arc<RecordBatch>> {
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

pub(crate) async fn create_one_record_batch_with_influxtype_no_duplicates() -> Vec<Arc<RecordBatch>>
{
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

pub(crate) async fn create_one_record_batch_with_influxtype_duplicates() -> Vec<Arc<RecordBatch>> {
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
pub(crate) async fn create_batches_with_influxtype() -> Vec<Arc<RecordBatch>> {
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
pub(crate) async fn create_batches_with_influxtype_different_columns() -> Vec<Arc<RecordBatch>> {
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
pub(crate) async fn create_batches_with_influxtype_different_columns_different_order(
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
pub(crate) async fn create_batches_with_influxtype_different_cardinality() -> Vec<Arc<RecordBatch>>
{
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
pub(crate) async fn create_batches_with_influxtype_same_columns_different_type(
) -> Vec<Arc<RecordBatch>> {
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

pub(crate) const TEST_NAMESPACE: &str = "test_namespace";
pub(crate) const TEST_TABLE: &str = "test_table";
pub(crate) const TEST_PARTITION_1: &str = "test+partition_1";
pub(crate) const TEST_PARTITION_2: &str = "test+partition_2";

/// This function produces one scenario but with the parameter combination (2*7),
/// you will be able to produce 14 scenarios by calling it in 2 loops
pub(crate) async fn make_ingester_data(
    two_partitions: bool,
) -> (IngesterData, NamespaceId, TableId) {
    // Whatever data because they won't be used in the tests
    let metrics: Arc<metric::Registry> = Default::default();
    let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
    let object_store = Arc::new(InMemory::new());
    let exec = Arc::new(iox_query::exec::Executor::new_testing());
    let lifecycle = LifecycleManager::new(
        LifecycleConfig::new(
            200_000_000,
            100_000_000,
            100_000_000,
            Duration::from_secs(100_000_000),
            Duration::from_secs(100_000_000),
            100_000_000,
        ),
        Arc::clone(&metrics),
        Arc::new(SystemProvider::default()),
    );

    // Make data for one shard and two tables
    let shard_index = ShardIndex::new(1);
    let (shard_id, ns_id, table_id) =
        populate_catalog(&*catalog, shard_index, TEST_NAMESPACE, TEST_TABLE).await;

    let ingester = IngesterData::new(
        object_store,
        Arc::clone(&catalog),
        [(shard_id, shard_index)],
        exec,
        backoff::BackoffConfig::default(),
        metrics,
    )
    .await
    .expect("failed to initialise ingester");

    // Make partitions per requested
    let ops = make_partitions(two_partitions, shard_index, table_id, ns_id);

    // Apply all ops
    for op in ops {
        ingester
            .buffer_operation(shard_id, op, &lifecycle.handle())
            .await
            .unwrap();
    }

    (ingester, ns_id, table_id)
}

/// Make data for one or two partitions per requested
pub(crate) fn make_partitions(
    two_partitions: bool,
    shard_index: ShardIndex,
    table_id: TableId,
    ns_id: NamespaceId,
) -> Vec<DmlOperation> {
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

    // ------------------------------------------
    // Build the first partition
    let (mut ops, seq_num) = make_first_partition_data(
        &PartitionKey::from(TEST_PARTITION_1),
        shard_index,
        table_id,
        ns_id,
    );

    // ------------------------------------------
    // Build the second partition if asked

    let mut seq_num = seq_num.get();
    if two_partitions {
        // Group 4: in buffer of p2
        ops.push(DmlOperation::Write(make_write_op(
            &PartitionKey::from(TEST_PARTITION_2),
            shard_index,
            ns_id,
            TEST_TABLE,
            table_id,
            seq_num,
            r#"test_table,city=Medford day="sun",temp=55 22"#,
        )));
        seq_num += 1;

        ops.push(DmlOperation::Write(make_write_op(
            &PartitionKey::from(TEST_PARTITION_2),
            shard_index,
            ns_id,
            TEST_TABLE,
            table_id,
            seq_num,
            r#"test_table,city=Reading day="mon",temp=58 40"#,
        )));
    } else {
        // Group 4: in buffer of p1
        ops.push(DmlOperation::Write(make_write_op(
            &PartitionKey::from(TEST_PARTITION_1),
            shard_index,
            ns_id,
            TEST_TABLE,
            table_id,
            seq_num,
            r#"test_table,city=Medford day="sun",temp=55 22"#,
        )));
        seq_num += 1;

        ops.push(DmlOperation::Write(make_write_op(
            &PartitionKey::from(TEST_PARTITION_1),
            shard_index,
            ns_id,
            TEST_TABLE,
            table_id,
            seq_num,
            r#"test_table,city=Reading day="mon",temp=58 40"#,
        )));
    }

    ops
}

pub(crate) fn make_write_op(
    partition_key: &PartitionKey,
    shard_index: ShardIndex,
    namespace_id: NamespaceId,
    table_name: &str,
    table_id: TableId,
    sequence_number: i64,
    lines: &str,
) -> DmlWrite {
    let mut tables_by_name = lines_to_batches(lines, 0).unwrap();
    assert_eq!(tables_by_name.len(), 1);

    let tables_by_id = [(table_id, tables_by_name.remove(table_name).unwrap())]
        .into_iter()
        .collect();
    DmlWrite::new(
        namespace_id,
        tables_by_id,
        partition_key.clone(),
        DmlMeta::sequenced(
            Sequence {
                shard_index,
                sequence_number: SequenceNumber::new(sequence_number),
            },
            Time::MIN,
            None,
            42,
        ),
    )
}

fn make_first_partition_data(
    partition_key: &PartitionKey,
    shard_index: ShardIndex,
    table_id: TableId,
    ns_id: NamespaceId,
) -> (Vec<DmlOperation>, SequenceNumber) {
    // In-memory data includes these rows but split between 3 groups go into
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

    let mut out = Vec::default();

    // ------------------------------------------
    // Build the first partition
    let mut seq_num = 0;

    // --------------------
    // Group 1
    out.push(DmlOperation::Write(make_write_op(
        partition_key,
        shard_index,
        ns_id,
        TEST_TABLE,
        table_id,
        seq_num,
        r#"test_table,city=Boston day="sun",temp=60 36"#,
    )));
    seq_num += 1;

    out.push(DmlOperation::Write(make_write_op(
        partition_key,
        shard_index,
        ns_id,
        TEST_TABLE,
        table_id,
        seq_num,
        r#"test_table,city=Andover day="tue",temp=56 30"#,
    )));
    seq_num += 1;

    // --------------------
    // Group 2
    out.push(DmlOperation::Write(make_write_op(
        partition_key,
        shard_index,
        ns_id,
        TEST_TABLE,
        table_id,
        seq_num,
        r#"test_table,city=Andover day="mon" 46"#,
    )));
    seq_num += 1;

    out.push(DmlOperation::Write(make_write_op(
        partition_key,
        shard_index,
        ns_id,
        TEST_TABLE,
        table_id,
        seq_num,
        r#"test_table,city=Medford day="wed" 26"#,
    )));
    seq_num += 1;

    // --------------------
    // Group 3: always in buffer
    // Fill `buffer`
    out.push(DmlOperation::Write(make_write_op(
        partition_key,
        shard_index,
        ns_id,
        TEST_TABLE,
        table_id,
        seq_num,
        r#"test_table,city=Boston day="mon" 38"#,
    )));
    seq_num += 1;

    out.push(DmlOperation::Write(make_write_op(
        partition_key,
        shard_index,
        ns_id,
        TEST_TABLE,
        table_id,
        seq_num,
        r#"test_table,city=Wilmington day="mon" 35"#,
    )));
    seq_num += 1;

    (out, SequenceNumber::new(seq_num))
}

pub(crate) async fn populate_catalog(
    catalog: &dyn Catalog,
    shard_index: ShardIndex,
    namespace: &str,
    table: &str,
) -> (ShardId, NamespaceId, TableId) {
    let mut c = catalog.repositories().await;
    let topic = c.topics().create_or_get("kafka-topic").await.unwrap();
    let query_pool = c.query_pools().create_or_get("query-pool").await.unwrap();
    let ns_id = c
        .namespaces()
        .create(namespace, None, topic.id, query_pool.id)
        .await
        .unwrap()
        .id;
    let table_id = c.tables().create_or_get(table, ns_id).await.unwrap().id;
    let shard_id = c
        .shards()
        .create_or_get(&topic, shard_index)
        .await
        .unwrap()
        .id;
    (shard_id, ns_id, table_id)
}
