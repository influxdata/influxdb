use std::collections::BTreeMap;

use data_types::{
    NamespaceId, PartitionKey, Sequence, SequenceNumber, ShardId, ShardIndex, TableId,
};
use dml::{DmlMeta, DmlWrite};
use iox_catalog::interface::Catalog;
use mutable_batch_lp::lines_to_batches;
use schema::Projection;

/// Construct a [`DmlWrite`] with the specified parameters, for LP that contains
/// a single table identified by `table_id`.
///
/// # Panics
///
/// This method panics if `lines` contains data for more than one table.
#[track_caller]
pub(crate) fn make_write_op(
    partition_key: &PartitionKey,
    namespace_id: NamespaceId,
    table_name: &str,
    table_id: TableId,
    sequence_number: i64,
    lines: &str,
) -> DmlWrite {
    let mut tables_by_name = lines_to_batches(lines, 0).expect("invalid LP");
    assert_eq!(
        tables_by_name.len(),
        1,
        "make_write_op only supports 1 table in the LP"
    );

    let tables_by_id = [(
        table_id,
        tables_by_name
            .remove(table_name)
            .expect("table_name does not exist in LP"),
    )]
    .into_iter()
    .collect();

    DmlWrite::new(
        namespace_id,
        tables_by_id,
        partition_key.clone(),
        DmlMeta::sequenced(
            Sequence {
                shard_index: ShardIndex::new(i32::MAX),
                sequence_number: SequenceNumber::new(sequence_number),
            },
            iox_time::Time::MIN,
            None,
            42,
        ),
    )
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

/// Assert `a` and `b` have identical metadata, and that when converting
/// them to Arrow batches they produces identical output.
#[track_caller]
pub(crate) fn assert_dml_writes_eq(a: DmlWrite, b: DmlWrite) {
    assert_eq!(a.namespace_id(), b.namespace_id(), "namespace");
    assert_eq!(a.table_count(), b.table_count(), "table count");
    assert_eq!(a.min_timestamp(), b.min_timestamp(), "min timestamp");
    assert_eq!(a.max_timestamp(), b.max_timestamp(), "max timestamp");
    assert_eq!(a.partition_key(), b.partition_key(), "partition key");

    // Assert sequence numbers were reassigned
    let seq_a = a.meta().sequence().map(|s| s.sequence_number);
    let seq_b = b.meta().sequence().map(|s| s.sequence_number);
    assert_eq!(seq_a, seq_b, "sequence numbers differ");

    let a = a.into_tables().collect::<BTreeMap<_, _>>();
    let b = b.into_tables().collect::<BTreeMap<_, _>>();

    a.into_iter().zip(b.into_iter()).for_each(|(a, b)| {
        assert_eq!(a.0, b.0, "table IDs differ - a table is missing!");
        assert_eq!(
            a.1.to_arrow(Projection::All)
                .expect("failed projection for a"),
            b.1.to_arrow(Projection::All)
                .expect("failed projection for b"),
            "table data differs"
        );
    })
}
