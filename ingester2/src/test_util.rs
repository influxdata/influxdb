use data_types::{
    NamespaceId, PartitionKey, Sequence, SequenceNumber, ShardId, ShardIndex, TableId,
};
use dml::{DmlMeta, DmlWrite};
use iox_catalog::interface::Catalog;
use mutable_batch_lp::lines_to_batches;

/// Construct a [`DmlWrite`] with the specified parameters, for LP that contains
/// a single table identified by `table_id`.
///
/// # Panics
///
/// This method panics if `lines` contains data for more than one table.
pub(crate) fn make_write_op(
    partition_key: &PartitionKey,
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

    assert_eq!(shard_id, crate::TRANSITION_SHARD_ID);

    (shard_id, ns_id, table_id)
}
