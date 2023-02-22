//! This module implements an in-memory implementation of the iox_catalog interface. It can be
//! used for testing or for an IOx designed to run without catalog persistence.

use crate::{
    interface::{
        sealed::TransactionFinalize, CasFailure, Catalog, ColumnRepo, ColumnTypeMismatchSnafu,
        Error, NamespaceRepo, ParquetFileRepo, PartitionRepo, ProcessedTombstoneRepo,
        QueryPoolRepo, RepoCollection, Result, ShardRepo, SoftDeletedRows, TableRepo,
        TombstoneRepo, TopicMetadataRepo, Transaction,
    },
    metrics::MetricDecorator,
    DEFAULT_MAX_COLUMNS_PER_TABLE, DEFAULT_MAX_TABLES,
};
use async_trait::async_trait;
use data_types::{
    Column, ColumnId, ColumnType, ColumnTypeCount, CompactionLevel, Namespace, NamespaceId,
    ParquetFile, ParquetFileId, ParquetFileParams, Partition, PartitionId, PartitionKey,
    PartitionParam, ProcessedTombstone, QueryPool, QueryPoolId, SequenceNumber, Shard, ShardId,
    ShardIndex, SkippedCompaction, Table, TableId, TablePartition, Timestamp, Tombstone,
    TombstoneId, TopicId, TopicMetadata,
};
use iox_time::{SystemProvider, TimeProvider};
use observability_deps::tracing::warn;
use snafu::ensure;
use sqlx::types::Uuid;
use std::{
    collections::{HashMap, HashSet},
    convert::TryFrom,
    fmt::{Display, Formatter},
    sync::Arc,
};
use tokio::sync::{Mutex, OwnedMutexGuard};

/// In-memory catalog that implements the `RepoCollection` and individual repo traits from
/// the catalog interface.
pub struct MemCatalog {
    metrics: Arc<metric::Registry>,
    collections: Arc<Mutex<MemCollections>>,
    time_provider: Arc<dyn TimeProvider>,
}

impl MemCatalog {
    /// return new initialized `MemCatalog`
    pub fn new(metrics: Arc<metric::Registry>) -> Self {
        Self {
            metrics,
            collections: Default::default(),
            time_provider: Arc::new(SystemProvider::new()),
        }
    }
}

impl std::fmt::Debug for MemCatalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemCatalog").finish_non_exhaustive()
    }
}

#[derive(Default, Debug, Clone)]
struct MemCollections {
    topics: Vec<TopicMetadata>,
    query_pools: Vec<QueryPool>,
    namespaces: Vec<Namespace>,
    tables: Vec<Table>,
    columns: Vec<Column>,
    shards: Vec<Shard>,
    partitions: Vec<Partition>,
    skipped_compactions: Vec<SkippedCompaction>,
    tombstones: Vec<Tombstone>,
    parquet_files: Vec<ParquetFile>,
    processed_tombstones: Vec<ProcessedTombstone>,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum MemTxnInner {
    Txn {
        guard: OwnedMutexGuard<MemCollections>,
        stage: MemCollections,
        finalized: bool,
    },
    NoTxn {
        collections: OwnedMutexGuard<MemCollections>,
    },
}

/// transaction bound to an in-memory catalog.
#[derive(Debug)]
pub struct MemTxn {
    inner: MemTxnInner,
    time_provider: Arc<dyn TimeProvider>,
}

impl MemTxn {
    fn stage(&mut self) -> &mut MemCollections {
        match &mut self.inner {
            MemTxnInner::Txn { stage, .. } => stage,
            MemTxnInner::NoTxn { collections } => collections,
        }
    }
}

impl Drop for MemTxn {
    fn drop(&mut self) {
        match self.inner {
            MemTxnInner::Txn { finalized, .. } if !finalized => {
                warn!("Dropping MemTxn w/o finalizing (commit or abort)");
            }
            _ => {}
        }
    }
}

impl Display for MemCatalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Memory")
    }
}

#[async_trait]
impl Catalog for MemCatalog {
    async fn setup(&self) -> Result<(), Error> {
        // nothing to do
        Ok(())
    }

    async fn start_transaction(&self) -> Result<Box<dyn Transaction>, Error> {
        let guard = Arc::clone(&self.collections).lock_owned().await;
        let stage = guard.clone();
        Ok(Box::new(MetricDecorator::new(
            MemTxn {
                inner: MemTxnInner::Txn {
                    guard,
                    stage,
                    finalized: false,
                },
                time_provider: self.time_provider(),
            },
            Arc::clone(&self.metrics),
        )))
    }

    async fn repositories(&self) -> Box<dyn RepoCollection> {
        let collections = Arc::clone(&self.collections).lock_owned().await;
        Box::new(MetricDecorator::new(
            MemTxn {
                inner: MemTxnInner::NoTxn { collections },
                time_provider: self.time_provider(),
            },
            Arc::clone(&self.metrics),
        ))
    }

    fn metrics(&self) -> Arc<metric::Registry> {
        Arc::clone(&self.metrics)
    }

    fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider)
    }
}

#[async_trait]
impl TransactionFinalize for MemTxn {
    async fn commit_inplace(&mut self) -> Result<(), Error> {
        match &mut self.inner {
            MemTxnInner::Txn {
                guard,
                stage,
                finalized,
            } => {
                assert!(!*finalized);
                **guard = std::mem::take(stage);
                *finalized = true;
            }
            MemTxnInner::NoTxn { .. } => {
                panic!("cannot commit oneshot");
            }
        }
        Ok(())
    }

    async fn abort_inplace(&mut self) -> Result<(), Error> {
        match &mut self.inner {
            MemTxnInner::Txn { finalized, .. } => {
                assert!(!*finalized);
                *finalized = true;
            }
            MemTxnInner::NoTxn { .. } => {
                panic!("cannot abort oneshot");
            }
        }
        Ok(())
    }
}

#[async_trait]
impl RepoCollection for MemTxn {
    fn topics(&mut self) -> &mut dyn TopicMetadataRepo {
        self
    }

    fn query_pools(&mut self) -> &mut dyn QueryPoolRepo {
        self
    }

    fn namespaces(&mut self) -> &mut dyn NamespaceRepo {
        self
    }

    fn tables(&mut self) -> &mut dyn TableRepo {
        self
    }

    fn columns(&mut self) -> &mut dyn ColumnRepo {
        self
    }

    fn shards(&mut self) -> &mut dyn ShardRepo {
        self
    }

    fn partitions(&mut self) -> &mut dyn PartitionRepo {
        self
    }

    fn tombstones(&mut self) -> &mut dyn TombstoneRepo {
        self
    }

    fn parquet_files(&mut self) -> &mut dyn ParquetFileRepo {
        self
    }

    fn processed_tombstones(&mut self) -> &mut dyn ProcessedTombstoneRepo {
        self
    }
}

#[async_trait]
impl TopicMetadataRepo for MemTxn {
    async fn create_or_get(&mut self, name: &str) -> Result<TopicMetadata> {
        let stage = self.stage();

        let topic = match stage.topics.iter().find(|t| t.name == name) {
            Some(t) => t,
            None => {
                let topic = TopicMetadata {
                    id: TopicId::new(stage.topics.len() as i64 + 1),
                    name: name.to_string(),
                };
                stage.topics.push(topic);
                stage.topics.last().unwrap()
            }
        };

        Ok(topic.clone())
    }

    async fn get_by_name(&mut self, name: &str) -> Result<Option<TopicMetadata>> {
        let stage = self.stage();

        let topic = stage.topics.iter().find(|t| t.name == name).cloned();
        Ok(topic)
    }
}

#[async_trait]
impl QueryPoolRepo for MemTxn {
    async fn create_or_get(&mut self, name: &str) -> Result<QueryPool> {
        let stage = self.stage();

        let pool = match stage.query_pools.iter().find(|t| t.name == name) {
            Some(t) => t,
            None => {
                let pool = QueryPool {
                    id: QueryPoolId::new(stage.query_pools.len() as i64 + 1),
                    name: name.to_string(),
                };
                stage.query_pools.push(pool);
                stage.query_pools.last().unwrap()
            }
        };

        Ok(pool.clone())
    }
}

#[async_trait]
impl NamespaceRepo for MemTxn {
    async fn create(
        &mut self,
        name: &str,
        retention_period_ns: Option<i64>,
        topic_id: TopicId,
        query_pool_id: QueryPoolId,
    ) -> Result<Namespace> {
        let stage = self.stage();

        if stage.namespaces.iter().any(|n| n.name == name) {
            return Err(Error::NameExists {
                name: name.to_string(),
            });
        }

        let namespace = Namespace {
            id: NamespaceId::new(stage.namespaces.len() as i64 + 1),
            name: name.to_string(),
            topic_id,
            query_pool_id,
            max_tables: DEFAULT_MAX_TABLES,
            max_columns_per_table: DEFAULT_MAX_COLUMNS_PER_TABLE,
            retention_period_ns,
            deleted_at: None,
        };
        stage.namespaces.push(namespace);
        Ok(stage.namespaces.last().unwrap().clone())
    }

    async fn list(&mut self, deleted: SoftDeletedRows) -> Result<Vec<Namespace>> {
        let stage = self.stage();

        Ok(filter_namespace_soft_delete(&stage.namespaces, deleted)
            .cloned()
            .collect())
    }

    async fn get_by_id(
        &mut self,
        id: NamespaceId,
        deleted: SoftDeletedRows,
    ) -> Result<Option<Namespace>> {
        let stage = self.stage();

        Ok(filter_namespace_soft_delete(&stage.namespaces, deleted)
            .find(|n| n.id == id)
            .cloned())
    }

    async fn get_by_name(
        &mut self,
        name: &str,
        deleted: SoftDeletedRows,
    ) -> Result<Option<Namespace>> {
        let stage = self.stage();

        Ok(filter_namespace_soft_delete(&stage.namespaces, deleted)
            .find(|n| n.name == name)
            .cloned())
    }

    // performs a cascading delete of all things attached to the namespace, then deletes the
    // namespace
    async fn soft_delete(&mut self, name: &str) -> Result<()> {
        let timestamp = self.time_provider.now();
        let stage = self.stage();
        // get namespace by name
        match stage.namespaces.iter_mut().find(|n| n.name == name) {
            Some(n) => {
                n.deleted_at = Some(Timestamp::from(timestamp));
                Ok(())
            }
            None => Err(Error::NamespaceNotFoundByName {
                name: name.to_string(),
            }),
        }
    }

    async fn update_table_limit(&mut self, name: &str, new_max: i32) -> Result<Namespace> {
        let stage = self.stage();
        match stage.namespaces.iter_mut().find(|n| n.name == name) {
            Some(n) => {
                n.max_tables = new_max;
                Ok(n.clone())
            }
            None => Err(Error::NamespaceNotFoundByName {
                name: name.to_string(),
            }),
        }
    }

    async fn update_column_limit(&mut self, name: &str, new_max: i32) -> Result<Namespace> {
        let stage = self.stage();
        match stage.namespaces.iter_mut().find(|n| n.name == name) {
            Some(n) => {
                n.max_columns_per_table = new_max;
                Ok(n.clone())
            }
            None => Err(Error::NamespaceNotFoundByName {
                name: name.to_string(),
            }),
        }
    }

    async fn update_retention_period(
        &mut self,
        name: &str,
        retention_period_ns: Option<i64>,
    ) -> Result<Namespace> {
        let stage = self.stage();
        match stage.namespaces.iter_mut().find(|n| n.name == name) {
            Some(n) => {
                n.retention_period_ns = retention_period_ns;
                Ok(n.clone())
            }
            None => Err(Error::NamespaceNotFoundByName {
                name: name.to_string(),
            }),
        }
    }
}

#[async_trait]
impl TableRepo for MemTxn {
    async fn create_or_get(&mut self, name: &str, namespace_id: NamespaceId) -> Result<Table> {
        let stage = self.stage();

        // this block is just to ensure the mem impl correctly creates TableCreateLimitError in
        // tests, we don't care about any of the errors it is discarding
        stage
            .namespaces
            .iter()
            .find(|n| n.id == namespace_id)
            .cloned()
            .ok_or_else(|| Error::NamespaceNotFoundByName {
                // we're never going to use this error, this is just for flow control,
                // so it doesn't matter that we only have the ID, not the name
                name: "".to_string(),
            })
            .and_then(|n| {
                let max_tables = n.max_tables;
                let tables_count = stage
                    .tables
                    .iter()
                    .filter(|t| t.namespace_id == namespace_id)
                    .count();
                if tables_count >= max_tables.try_into().unwrap() {
                    return Err(Error::TableCreateLimitError {
                        table_name: name.to_string(),
                        namespace_id,
                    });
                }
                Ok(())
            })?;

        let table = match stage
            .tables
            .iter()
            .find(|t| t.name == name && t.namespace_id == namespace_id)
        {
            Some(t) => t,
            None => {
                let table = Table {
                    id: TableId::new(stage.tables.len() as i64 + 1),
                    namespace_id,
                    name: name.to_string(),
                };
                stage.tables.push(table);
                stage.tables.last().unwrap()
            }
        };

        Ok(table.clone())
    }

    async fn get_by_id(&mut self, table_id: TableId) -> Result<Option<Table>> {
        let stage = self.stage();

        Ok(stage.tables.iter().find(|t| t.id == table_id).cloned())
    }

    async fn get_by_namespace_and_name(
        &mut self,
        namespace_id: NamespaceId,
        name: &str,
    ) -> Result<Option<Table>> {
        let stage = self.stage();

        Ok(stage
            .tables
            .iter()
            .find(|t| t.namespace_id == namespace_id && t.name == name)
            .cloned())
    }

    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Table>> {
        let stage = self.stage();

        let tables: Vec<_> = stage
            .tables
            .iter()
            .filter(|t| t.namespace_id == namespace_id)
            .cloned()
            .collect();
        Ok(tables)
    }

    async fn list(&mut self) -> Result<Vec<Table>> {
        let stage = self.stage();
        Ok(stage.tables.clone())
    }
}

#[async_trait]
impl ColumnRepo for MemTxn {
    async fn create_or_get(
        &mut self,
        name: &str,
        table_id: TableId,
        column_type: ColumnType,
    ) -> Result<Column> {
        let stage = self.stage();

        // this block is just to ensure the mem impl correctly creates ColumnCreateLimitError in
        // tests, we don't care about any of the errors it is discarding
        stage
            .tables
            .iter()
            .find(|t| t.id == table_id)
            .cloned()
            .ok_or(Error::TableNotFound { id: table_id }) // error never used, this is just for flow control
            .and_then(|t| {
                stage
                    .namespaces
                    .iter()
                    .find(|n| n.id == t.namespace_id)
                    .cloned()
                    .ok_or_else(|| Error::NamespaceNotFoundByName {
                        // we're never going to use this error, this is just for flow control,
                        // so it doesn't matter that we only have the ID, not the name
                        name: "".to_string(),
                    })
                    .and_then(|n| {
                        let max_columns_per_table = n.max_columns_per_table;
                        let columns_count = stage
                            .columns
                            .iter()
                            .filter(|t| t.table_id == table_id)
                            .count();
                        if columns_count >= max_columns_per_table.try_into().unwrap() {
                            return Err(Error::ColumnCreateLimitError {
                                column_name: name.to_string(),
                                table_id,
                            });
                        }
                        Ok(())
                    })?;
                Ok(())
            })?;

        let column = match stage
            .columns
            .iter()
            .find(|t| t.name == name && t.table_id == table_id)
        {
            Some(c) => {
                ensure!(
                    column_type == c.column_type,
                    ColumnTypeMismatchSnafu {
                        name,
                        existing: c.column_type,
                        new: column_type
                    }
                );
                c
            }
            None => {
                let column = Column {
                    id: ColumnId::new(stage.columns.len() as i64 + 1),
                    table_id,
                    name: name.to_string(),
                    column_type,
                };
                stage.columns.push(column);
                stage.columns.last().unwrap()
            }
        };

        Ok(column.clone())
    }

    async fn create_or_get_many_unchecked(
        &mut self,
        table_id: TableId,
        columns: HashMap<&str, ColumnType>,
    ) -> Result<Vec<Column>> {
        // Explicitly NOT using `create_or_get` in this function: the Postgres catalog doesn't
        // check column limits when inserting many columns because it's complicated and expensive,
        // and for testing purposes the in-memory catalog needs to match its functionality.

        let stage = self.stage();

        let out: Vec<_> = columns
            .iter()
            .map(|(&column_name, &column_type)| {
                match stage
                    .columns
                    .iter()
                    .find(|t| t.name == column_name && t.table_id == table_id)
                {
                    Some(c) => {
                        ensure!(
                            column_type == c.column_type,
                            ColumnTypeMismatchSnafu {
                                name: column_name,
                                existing: c.column_type,
                                new: column_type
                            }
                        );
                        Ok(c.clone())
                    }
                    None => {
                        let new_column = Column {
                            id: ColumnId::new(stage.columns.len() as i64 + 1),
                            table_id,
                            name: column_name.to_string(),
                            column_type,
                        };
                        stage.columns.push(new_column);
                        Ok(stage.columns.last().unwrap().clone())
                    }
                }
            })
            .collect::<Result<Vec<Column>>>()?;

        Ok(out)
    }

    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Column>> {
        let stage = self.stage();

        let table_ids: Vec<_> = stage
            .tables
            .iter()
            .filter(|t| t.namespace_id == namespace_id)
            .map(|t| t.id)
            .collect();
        let columns: Vec<_> = stage
            .columns
            .iter()
            .filter(|c| table_ids.contains(&c.table_id))
            .cloned()
            .collect();

        Ok(columns)
    }

    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Column>> {
        let stage = self.stage();

        let columns: Vec<_> = stage
            .columns
            .iter()
            .filter(|c| c.table_id == table_id)
            .cloned()
            .collect();

        Ok(columns)
    }

    async fn list(&mut self) -> Result<Vec<Column>> {
        let stage = self.stage();
        Ok(stage.columns.clone())
    }

    async fn list_type_count_by_table_id(
        &mut self,
        table_id: TableId,
    ) -> Result<Vec<ColumnTypeCount>> {
        let stage = self.stage();

        let columns = stage
            .columns
            .iter()
            .filter(|c| c.table_id == table_id)
            .map(|c| c.column_type)
            .collect::<Vec<_>>();

        let mut cols = HashMap::new();
        for c in columns {
            cols.entry(c)
                .and_modify(|counter| *counter += 1)
                .or_insert(1);
        }

        let column_type_counts = cols
            .iter()
            .map(|c| ColumnTypeCount {
                col_type: *c.0,
                count: *c.1,
            })
            .collect::<Vec<_>>();

        Ok(column_type_counts)
    }
}

#[async_trait]
impl ShardRepo for MemTxn {
    async fn create_or_get(
        &mut self,
        topic: &TopicMetadata,
        shard_index: ShardIndex,
    ) -> Result<Shard> {
        let stage = self.stage();

        let shard = match stage
            .shards
            .iter()
            .find(|s| s.topic_id == topic.id && s.shard_index == shard_index)
        {
            Some(t) => t,
            None => {
                let shard = Shard {
                    id: ShardId::new(stage.shards.len() as i64 + 1),
                    topic_id: topic.id,
                    shard_index,
                    min_unpersisted_sequence_number: SequenceNumber::new(0),
                };
                stage.shards.push(shard);
                stage.shards.last().unwrap()
            }
        };

        Ok(*shard)
    }

    async fn get_by_topic_id_and_shard_index(
        &mut self,
        topic_id: TopicId,
        shard_index: ShardIndex,
    ) -> Result<Option<Shard>> {
        let stage = self.stage();

        let shard = stage
            .shards
            .iter()
            .find(|s| s.topic_id == topic_id && s.shard_index == shard_index)
            .cloned();
        Ok(shard)
    }

    async fn list(&mut self) -> Result<Vec<Shard>> {
        let stage = self.stage();

        Ok(stage.shards.clone())
    }

    async fn list_by_topic(&mut self, topic: &TopicMetadata) -> Result<Vec<Shard>> {
        let stage = self.stage();

        let shards: Vec<_> = stage
            .shards
            .iter()
            .filter(|s| s.topic_id == topic.id)
            .cloned()
            .collect();
        Ok(shards)
    }

    async fn update_min_unpersisted_sequence_number(
        &mut self,
        shard_id: ShardId,
        sequence_number: SequenceNumber,
    ) -> Result<()> {
        let stage = self.stage();

        if let Some(s) = stage.shards.iter_mut().find(|s| s.id == shard_id) {
            s.min_unpersisted_sequence_number = sequence_number
        };

        Ok(())
    }
}

#[async_trait]
impl PartitionRepo for MemTxn {
    async fn create_or_get(
        &mut self,
        key: PartitionKey,
        shard_id: ShardId,
        table_id: TableId,
    ) -> Result<Partition> {
        let stage = self.stage();

        let partition =
            match stage.partitions.iter().find(|p| {
                p.partition_key == key && p.shard_id == shard_id && p.table_id == table_id
            }) {
                Some(p) => p,
                None => {
                    let p = Partition {
                        id: PartitionId::new(stage.partitions.len() as i64 + 1),
                        shard_id,
                        table_id,
                        partition_key: key,
                        sort_key: vec![],
                        persisted_sequence_number: None,
                        new_file_at: None,
                    };
                    stage.partitions.push(p);
                    stage.partitions.last().unwrap()
                }
            };

        Ok(partition.clone())
    }

    async fn get_by_id(&mut self, partition_id: PartitionId) -> Result<Option<Partition>> {
        let stage = self.stage();

        Ok(stage
            .partitions
            .iter()
            .find(|p| p.id == partition_id)
            .cloned())
    }

    async fn list_by_namespace(&mut self, namespace_id: NamespaceId) -> Result<Vec<Partition>> {
        let stage = self.stage();

        let table_ids: HashSet<_> = stage
            .tables
            .iter()
            .filter_map(|table| (table.namespace_id == namespace_id).then_some(table.id))
            .collect();
        let partitions: Vec<_> = stage
            .partitions
            .iter()
            .filter(|p| table_ids.contains(&p.table_id))
            .cloned()
            .collect();
        Ok(partitions)
    }

    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Partition>> {
        let stage = self.stage();

        let partitions: Vec<_> = stage
            .partitions
            .iter()
            .filter(|p| p.table_id == table_id)
            .cloned()
            .collect();
        Ok(partitions)
    }

    async fn list_ids(&mut self) -> Result<Vec<PartitionId>> {
        let stage = self.stage();

        let partitions: Vec<_> = stage.partitions.iter().map(|p| p.id).collect();

        Ok(partitions)
    }

    async fn cas_sort_key(
        &mut self,
        partition_id: PartitionId,
        old_sort_key: Option<Vec<String>>,
        new_sort_key: &[&str],
    ) -> Result<Partition, CasFailure<Vec<String>>> {
        let stage = self.stage();
        let old_sort_key = old_sort_key.unwrap_or_default();
        match stage.partitions.iter_mut().find(|p| p.id == partition_id) {
            Some(p) if p.sort_key == old_sort_key => {
                p.sort_key = new_sort_key.iter().map(|s| s.to_string()).collect();
                Ok(p.clone())
            }
            Some(p) => return Err(CasFailure::ValueMismatch(p.sort_key.clone())),
            None => Err(CasFailure::QueryError(Error::PartitionNotFound {
                id: partition_id,
            })),
        }
    }

    async fn record_skipped_compaction(
        &mut self,
        partition_id: PartitionId,
        reason: &str,
        num_files: usize,
        limit_num_files: usize,
        limit_num_files_first_in_partition: usize,
        estimated_bytes: u64,
        limit_bytes: u64,
    ) -> Result<()> {
        let reason = reason.to_string();
        let skipped_at = Timestamp::from(self.time_provider.now());

        let stage = self.stage();
        match stage
            .skipped_compactions
            .iter_mut()
            .find(|s| s.partition_id == partition_id)
        {
            Some(s) => {
                s.reason = reason;
                s.skipped_at = skipped_at;
                s.num_files = num_files as i64;
                s.limit_num_files = limit_num_files as i64;
                s.limit_num_files_first_in_partition = limit_num_files_first_in_partition as i64;
                s.estimated_bytes = estimated_bytes as i64;
                s.limit_bytes = limit_bytes as i64;
            }
            None => stage.skipped_compactions.push(SkippedCompaction {
                partition_id,
                reason,
                skipped_at,
                num_files: num_files as i64,
                limit_num_files: limit_num_files as i64,
                limit_num_files_first_in_partition: limit_num_files_first_in_partition as i64,
                estimated_bytes: estimated_bytes as i64,
                limit_bytes: limit_bytes as i64,
            }),
        }
        Ok(())
    }

    async fn get_in_skipped_compaction(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<SkippedCompaction>> {
        let stage = self.stage();
        Ok(stage
            .skipped_compactions
            .iter()
            .find(|s| s.partition_id == partition_id)
            .cloned())
    }

    async fn list_skipped_compactions(&mut self) -> Result<Vec<SkippedCompaction>> {
        let stage = self.stage();
        Ok(stage.skipped_compactions.clone())
    }

    async fn delete_skipped_compactions(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<SkippedCompaction>> {
        use std::mem;

        let stage = self.stage();
        let skipped_compactions = mem::take(&mut stage.skipped_compactions);
        let (mut removed, remaining) = skipped_compactions
            .into_iter()
            .partition(|sc| sc.partition_id == partition_id);
        stage.skipped_compactions = remaining;

        match removed.pop() {
            Some(sc) if removed.is_empty() => Ok(Some(sc)),
            Some(_) => unreachable!("There must be exactly one skipped compaction per partition"),
            None => Ok(None),
        }
    }

    async fn update_persisted_sequence_number(
        &mut self,
        partition_id: PartitionId,
        sequence_number: SequenceNumber,
    ) -> Result<()> {
        let stage = self.stage();
        match stage.partitions.iter_mut().find(|p| p.id == partition_id) {
            Some(p) => {
                p.persisted_sequence_number = Some(sequence_number);
                Ok(())
            }
            None => Err(Error::PartitionNotFound { id: partition_id }),
        }
    }

    async fn most_recent_n(&mut self, n: usize) -> Result<Vec<Partition>> {
        let stage = self.stage();
        Ok(stage.partitions.iter().rev().take(n).cloned().collect())
    }

    async fn most_recent_n_in_shards(
        &mut self,
        n: usize,
        shards: &[ShardId],
    ) -> Result<Vec<Partition>> {
        let stage = self.stage();
        Ok(stage
            .partitions
            .iter()
            .rev()
            .filter(|p| shards.contains(&p.shard_id))
            .take(n)
            .cloned()
            .collect())
    }

    async fn partitions_with_recent_created_files(
        &mut self,
        time_in_the_past: Timestamp,
        max_num_partitions: usize,
    ) -> Result<Vec<PartitionParam>> {
        let stage = self.stage();

        let partitions: Vec<_> = stage
            .partitions
            .iter()
            .filter(|p| p.new_file_at > Some(time_in_the_past))
            .map(|p| {
                // get namesapce_id of this partition
                let namespace_id = stage
                    .tables
                    .iter()
                    .find(|t| t.id == p.table_id)
                    .map(|t| t.namespace_id)
                    .unwrap_or(NamespaceId::new(1));

                PartitionParam {
                    partition_id: p.id,
                    table_id: p.table_id,
                    shard_id: ShardId::new(1), // this is unused and will be removed when we remove shard_id
                    namespace_id,
                }
            })
            .take(max_num_partitions)
            .collect();

        Ok(partitions)
    }

    async fn partitions_to_compact(&mut self, recent_time: Timestamp) -> Result<Vec<PartitionId>> {
        let stage = self.stage();

        let partitions: Vec<_> = stage
            .partitions
            .iter()
            .filter(|p| p.new_file_at > Some(recent_time))
            .map(|p| p.id)
            .collect();

        Ok(partitions)
    }
}

#[async_trait]
impl TombstoneRepo for MemTxn {
    async fn create_or_get(
        &mut self,
        table_id: TableId,
        shard_id: ShardId,
        sequence_number: SequenceNumber,
        min_time: Timestamp,
        max_time: Timestamp,
        predicate: &str,
    ) -> Result<Tombstone> {
        let stage = self.stage();

        let tombstone = match stage.tombstones.iter().find(|t| {
            t.table_id == table_id && t.shard_id == shard_id && t.sequence_number == sequence_number
        }) {
            Some(t) => t,
            None => {
                let t = Tombstone {
                    id: TombstoneId::new(stage.tombstones.len() as i64 + 1),
                    table_id,
                    shard_id,
                    sequence_number,
                    min_time,
                    max_time,
                    serialized_predicate: predicate.to_string(),
                };
                stage.tombstones.push(t);
                stage.tombstones.last().unwrap()
            }
        };

        Ok(tombstone.clone())
    }

    async fn list_by_namespace(&mut self, namespace_id: NamespaceId) -> Result<Vec<Tombstone>> {
        let stage = self.stage();

        let table_ids: HashSet<_> = stage
            .tables
            .iter()
            .filter_map(|table| (table.namespace_id == namespace_id).then_some(table.id))
            .collect();
        let tombstones: Vec<_> = stage
            .tombstones
            .iter()
            .filter(|t| table_ids.contains(&t.table_id))
            .cloned()
            .collect();
        Ok(tombstones)
    }

    async fn list_by_table(&mut self, table_id: TableId) -> Result<Vec<Tombstone>> {
        let stage = self.stage();

        let tombstones: Vec<_> = stage
            .tombstones
            .iter()
            .filter(|t| t.table_id == table_id)
            .cloned()
            .collect();
        Ok(tombstones)
    }

    async fn get_by_id(&mut self, id: TombstoneId) -> Result<Option<Tombstone>> {
        let stage = self.stage();

        Ok(stage.tombstones.iter().find(|t| t.id == id).cloned())
    }

    async fn list_tombstones_by_shard_greater_than(
        &mut self,
        shard_id: ShardId,
        sequence_number: SequenceNumber,
    ) -> Result<Vec<Tombstone>> {
        let stage = self.stage();

        let tombstones: Vec<_> = stage
            .tombstones
            .iter()
            .filter(|t| t.shard_id == shard_id && t.sequence_number > sequence_number)
            .cloned()
            .collect();
        Ok(tombstones)
    }

    async fn remove(&mut self, tombstone_ids: &[TombstoneId]) -> Result<()> {
        let stage = self.stage();

        // remove the processed tombstones first
        stage
            .processed_tombstones
            .retain(|pt| !tombstone_ids.iter().any(|id| *id == pt.tombstone_id));

        // remove the tombstones
        stage
            .tombstones
            .retain(|ts| !tombstone_ids.iter().any(|id| *id == ts.id));

        Ok(())
    }

    async fn list_tombstones_for_time_range(
        &mut self,
        shard_id: ShardId,
        table_id: TableId,
        sequence_number: SequenceNumber,
        min_time: Timestamp,
        max_time: Timestamp,
    ) -> Result<Vec<Tombstone>> {
        let stage = self.stage();

        let tombstones: Vec<_> = stage
            .tombstones
            .iter()
            .filter(|t| {
                t.shard_id == shard_id
                    && t.table_id == table_id
                    && t.sequence_number > sequence_number
                    && ((t.min_time <= min_time && t.max_time >= min_time)
                        || (t.min_time > min_time && t.min_time <= max_time))
            })
            .cloned()
            .collect();
        Ok(tombstones)
    }
}

#[async_trait]
impl ParquetFileRepo for MemTxn {
    async fn create(&mut self, parquet_file_params: ParquetFileParams) -> Result<ParquetFile> {
        let stage = self.stage();

        if stage
            .parquet_files
            .iter()
            .any(|f| f.object_store_id == parquet_file_params.object_store_id)
        {
            return Err(Error::FileExists {
                object_store_id: parquet_file_params.object_store_id,
            });
        }

        let parquet_file = ParquetFile::from_params(
            parquet_file_params,
            ParquetFileId::new(stage.parquet_files.len() as i64 + 1),
        );
        let compaction_level = parquet_file.compaction_level;
        let created_at = parquet_file.created_at;
        let partition_id = parquet_file.partition_id;
        stage.parquet_files.push(parquet_file);

        // Update the new_file_at field its partition to the time of created_at
        // Only update if the compaction level is not Final which signal more compaction needed
        if compaction_level < CompactionLevel::Final {
            let partition = stage
                .partitions
                .iter_mut()
                .find(|p| p.id == partition_id)
                .ok_or(Error::PartitionNotFound { id: partition_id })?;
            partition.new_file_at = Some(created_at);
        }

        Ok(stage.parquet_files.last().unwrap().clone())
    }

    async fn flag_for_delete(&mut self, id: ParquetFileId) -> Result<()> {
        let marked_at = Timestamp::from(self.time_provider.now());
        let stage = self.stage();

        match stage.parquet_files.iter_mut().find(|p| p.id == id) {
            Some(f) => f.to_delete = Some(marked_at),
            None => return Err(Error::ParquetRecordNotFound { id }),
        }

        Ok(())
    }

    async fn flag_for_delete_by_retention(&mut self) -> Result<Vec<ParquetFileId>> {
        let now = Timestamp::from(self.time_provider.now());
        let stage = self.stage();

        Ok(stage
            .parquet_files
            .iter_mut()
            // don't flag if already flagged for deletion
            .filter(|f| f.to_delete.is_none())
            .filter_map(|f| {
                // table retention, if it exists, overrides namespace retention
                // TODO - include check of table retention period once implemented
                stage
                    .namespaces
                    .iter()
                    .find(|n| n.id == f.namespace_id)
                    .and_then(|ns| {
                        ns.retention_period_ns.and_then(|rp| {
                            if f.max_time < now - rp {
                                f.to_delete = Some(now);
                                Some(f.id)
                            } else {
                                None
                            }
                        })
                    })
            })
            .collect())
    }

    async fn list_by_shard_greater_than(
        &mut self,
        shard_id: ShardId,
        sequence_number: SequenceNumber,
    ) -> Result<Vec<ParquetFile>> {
        let stage = self.stage();

        let files: Vec<_> = stage
            .parquet_files
            .iter()
            .filter(|f| f.shard_id == shard_id && f.max_sequence_number > sequence_number)
            .cloned()
            .collect();
        Ok(files)
    }

    async fn list_by_namespace_not_to_delete(
        &mut self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<ParquetFile>> {
        let stage = self.stage();

        let table_ids: HashSet<_> = stage
            .tables
            .iter()
            .filter_map(|table| (table.namespace_id == namespace_id).then_some(table.id))
            .collect();
        let parquet_files: Vec<_> = stage
            .parquet_files
            .iter()
            .filter(|f| table_ids.contains(&f.table_id) && f.to_delete.is_none())
            .cloned()
            .collect();
        Ok(parquet_files)
    }

    async fn list_by_table_not_to_delete(&mut self, table_id: TableId) -> Result<Vec<ParquetFile>> {
        let stage = self.stage();

        let parquet_files: Vec<_> = stage
            .parquet_files
            .iter()
            .filter(|f| table_id == f.table_id && f.to_delete.is_none())
            .cloned()
            .collect();
        Ok(parquet_files)
    }

    async fn delete_old(&mut self, older_than: Timestamp) -> Result<Vec<ParquetFile>> {
        let stage = self.stage();

        let (delete, keep): (Vec<_>, Vec<_>) = stage.parquet_files.iter().cloned().partition(
            |f| matches!(f.to_delete, Some(marked_deleted) if marked_deleted < older_than),
        );

        stage.parquet_files = keep;

        Ok(delete)
    }

    async fn delete_old_ids_only(&mut self, older_than: Timestamp) -> Result<Vec<ParquetFileId>> {
        let delete = self
            .delete_old(older_than)
            .await
            .unwrap()
            .into_iter()
            .map(|f| f.id)
            .collect();
        Ok(delete)
    }

    async fn level_0(&mut self, shard_id: ShardId) -> Result<Vec<ParquetFile>> {
        let stage = self.stage();

        Ok(stage
            .parquet_files
            .iter()
            .filter(|f| {
                f.shard_id == shard_id
                    && f.compaction_level == CompactionLevel::Initial
                    && f.to_delete.is_none()
            })
            .cloned()
            .collect())
    }

    async fn level_1(
        &mut self,
        table_partition: TablePartition,
        min_time: Timestamp,
        max_time: Timestamp,
    ) -> Result<Vec<ParquetFile>> {
        let stage = self.stage();

        Ok(stage
            .parquet_files
            .iter()
            .filter(|f| {
                f.shard_id == table_partition.shard_id
                    && f.table_id == table_partition.table_id
                    && f.partition_id == table_partition.partition_id
                    && f.compaction_level == CompactionLevel::FileNonOverlapped
                    && f.to_delete.is_none()
                    && ((f.min_time <= min_time && f.max_time >= min_time)
                        || (f.min_time > min_time && f.min_time <= max_time))
            })
            .cloned()
            .collect())
    }

    async fn recent_highest_throughput_partitions(
        &mut self,
        shard_id: Option<ShardId>,
        time_in_the_past: Timestamp,
        min_num_files: usize,
        num_partitions: usize,
    ) -> Result<Vec<PartitionParam>> {
        let recent_time = time_in_the_past;

        let stage = self.stage();

        // Get partition info of selected files
        let partitions = stage
            .parquet_files
            .iter()
            .filter(|f| {
                let shard_matches_if_specified = if let Some(shard_id) = shard_id {
                    f.shard_id == shard_id
                } else {
                    true
                };

                shard_matches_if_specified
                    && f.created_at > recent_time
                    && f.compaction_level == CompactionLevel::Initial
                    && f.to_delete.is_none()
            })
            .map(|pf| PartitionParam {
                partition_id: pf.partition_id,
                shard_id: pf.shard_id,
                namespace_id: pf.namespace_id,
                table_id: pf.table_id,
            })
            .collect::<Vec<_>>();

        // Count num of files per partition by simply count the number of partition duplicates
        let mut partition_duplicate_count: HashMap<PartitionParam, usize> =
            HashMap::with_capacity(partitions.len());
        for p in partitions {
            let count = partition_duplicate_count.entry(p).or_insert(0);
            *count += 1;
        }

        // Partitions with select file count >= min_num_files that haven't been skipped by the
        // compactor
        let skipped_partitions: Vec<_> = stage
            .skipped_compactions
            .iter()
            .map(|s| s.partition_id)
            .collect();
        let mut partitions = partition_duplicate_count
            .iter()
            .filter(|(_, v)| v >= &&min_num_files)
            .filter(|(p, _)| !skipped_partitions.contains(&p.partition_id))
            .collect::<Vec<_>>();

        // Sort partitions by file count
        partitions.sort_by(|a, b| b.1.cmp(a.1));

        // only return top partitions
        let partitions = partitions
            .into_iter()
            .map(|(k, _)| *k)
            .take(num_partitions)
            .collect::<Vec<_>>();

        Ok(partitions)
    }

    async fn partitions_with_small_l1_file_count(
        &mut self,
        shard_id: Option<ShardId>,
        small_size_threshold_bytes: i64,
        min_small_file_count: usize,
        num_partitions: usize,
    ) -> Result<Vec<PartitionParam>> {
        let stage = self.stage();
        let skipped_partitions: Vec<_> = stage
            .skipped_compactions
            .iter()
            .map(|s| s.partition_id)
            .collect();
        // get a list of files for the shard that are under the size threshold and don't belong to
        // a partition that has been skipped by the compactor
        let relevant_parquet_files = stage
            .parquet_files
            .iter()
            .filter(|f| {
                let shard_matches_if_specified = if let Some(shard_id) = shard_id {
                    f.shard_id == shard_id
                } else {
                    true
                };

                shard_matches_if_specified
                    && f.compaction_level == CompactionLevel::FileNonOverlapped
                    && f.file_size_bytes < small_size_threshold_bytes
                    && !skipped_partitions.contains(&f.partition_id)
            })
            .collect::<Vec<_>>();
        // count the number of files per partition & use that to retain only a list of counts that
        // are above our threshold. the keys then become our partition candidates
        let mut partition_small_file_count: HashMap<PartitionParam, usize> =
            HashMap::with_capacity(relevant_parquet_files.len());
        for pf in relevant_parquet_files {
            let key = PartitionParam {
                partition_id: pf.partition_id,
                shard_id: pf.shard_id,
                namespace_id: pf.namespace_id,
                table_id: pf.table_id,
            };
            if pf.to_delete.is_none() {
                let count = partition_small_file_count.entry(key).or_insert(0);
                *count += 1;
            }
        }
        partition_small_file_count.retain(|_key, c| *c >= min_small_file_count);
        let mut partitions = partition_small_file_count.iter().collect::<Vec<_>>();
        // sort and return top N
        partitions.sort_by(|a, b| b.1.cmp(a.1));
        Ok(partitions
            .into_iter()
            .map(|(k, _)| *k)
            .take(num_partitions)
            .collect::<Vec<_>>())
    }

    async fn most_cold_files_partitions(
        &mut self,
        shard_id: Option<ShardId>,
        time_in_the_past: Timestamp,
        num_partitions: usize,
    ) -> Result<Vec<PartitionParam>> {
        let stage = self.stage();
        let relevant_parquet_files = stage
            .parquet_files
            .iter()
            .filter(|f| {
                let shard_matches_if_specified = if let Some(shard_id) = shard_id {
                    f.shard_id == shard_id
                } else {
                    true
                };

                shard_matches_if_specified
                    && (f.compaction_level == CompactionLevel::Initial
                        || f.compaction_level == CompactionLevel::FileNonOverlapped)
            })
            .collect::<Vec<_>>();

        // Count num of files per partition by simply count the number of partition duplicates
        let mut partition_duplicate_count: HashMap<PartitionParam, i32> =
            HashMap::with_capacity(relevant_parquet_files.len());
        let mut partition_max_created_at = HashMap::with_capacity(relevant_parquet_files.len());
        for pf in relevant_parquet_files {
            let key = PartitionParam {
                partition_id: pf.partition_id,
                shard_id: pf.shard_id,
                namespace_id: pf.namespace_id,
                table_id: pf.table_id,
            };

            if pf.to_delete.is_none() {
                let count = partition_duplicate_count.entry(key).or_insert(0);
                *count += 1;
            }

            let created_at = if pf.compaction_level == CompactionLevel::Initial {
                // the file is level-0, use its created_at time even if it is deleted
                Some(pf.created_at)
            } else if pf.to_delete.is_none() {
                // non deleted level-1,  make it `time_in_the_past - 1` to have this partition always the cold one
                Some(time_in_the_past - 1)
            } else {
                // This is the case of deleted level-1
                None
            };

            if let Some(created_at) = created_at {
                let max_created_at = partition_max_created_at.entry(key).or_insert(created_at);
                *max_created_at = std::cmp::max(*max_created_at, created_at);
                if created_at > *max_created_at {
                    *max_created_at = created_at;
                }
            }
        }

        // Sort partitions whose max created at is older than the limit by their file count
        let mut partitions = partition_duplicate_count
            .iter()
            .filter(|(k, _v)| partition_max_created_at.get(k).unwrap() < &time_in_the_past)
            .collect::<Vec<_>>();
        partitions.sort_by(|a, b| b.1.cmp(a.1));

        // Return top partitions with most file counts that haven't been skipped by the compactor
        let skipped_partitions: Vec<_> = stage
            .skipped_compactions
            .iter()
            .map(|s| s.partition_id)
            .collect();
        let partitions = partitions
            .into_iter()
            .map(|(k, _)| *k)
            .filter(|pf| !skipped_partitions.contains(&pf.partition_id))
            .map(|pf| PartitionParam {
                partition_id: pf.partition_id,
                shard_id: pf.shard_id,
                namespace_id: pf.namespace_id,
                table_id: pf.table_id,
            })
            .take(num_partitions)
            .collect::<Vec<_>>();

        Ok(partitions)
    }

    async fn list_by_partition_not_to_delete(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Vec<ParquetFile>> {
        let stage = self.stage();

        Ok(stage
            .parquet_files
            .iter()
            .filter(|f| f.partition_id == partition_id && f.to_delete.is_none())
            .cloned()
            .collect())
    }

    async fn update_compaction_level(
        &mut self,
        parquet_file_ids: &[ParquetFileId],
        compaction_level: CompactionLevel,
    ) -> Result<Vec<ParquetFileId>> {
        let stage = self.stage();

        let mut updated = Vec::with_capacity(parquet_file_ids.len());

        for f in stage
            .parquet_files
            .iter_mut()
            .filter(|p| parquet_file_ids.contains(&p.id))
        {
            f.compaction_level = compaction_level;
            updated.push(f.id);
        }

        Ok(updated)
    }

    async fn exist(&mut self, id: ParquetFileId) -> Result<bool> {
        let stage = self.stage();

        Ok(stage.parquet_files.iter().any(|f| f.id == id))
    }

    async fn count(&mut self) -> Result<i64> {
        let stage = self.stage();

        let count = stage.parquet_files.len();
        let count_i64 = i64::try_from(count);
        if count_i64.is_err() {
            return Err(Error::InvalidValue { value: count });
        }
        Ok(count_i64.unwrap())
    }

    async fn count_by_overlaps_with_level_0(
        &mut self,
        table_id: TableId,
        shard_id: ShardId,
        min_time: Timestamp,
        max_time: Timestamp,
        sequence_number: SequenceNumber,
    ) -> Result<i64> {
        let stage = self.stage();

        let count = stage
            .parquet_files
            .iter()
            .filter(|f| {
                f.shard_id == shard_id
                    && f.table_id == table_id
                    && f.max_sequence_number < sequence_number
                    && f.to_delete.is_none()
                    && f.compaction_level == CompactionLevel::Initial
                    && ((f.min_time <= min_time && f.max_time >= min_time)
                        || (f.min_time > min_time && f.min_time <= max_time))
            })
            .count();

        i64::try_from(count).map_err(|_| Error::InvalidValue { value: count })
    }

    async fn count_by_overlaps_with_level_1(
        &mut self,
        table_id: TableId,
        shard_id: ShardId,
        min_time: Timestamp,
        max_time: Timestamp,
    ) -> Result<i64> {
        let stage = self.stage();

        let count = stage
            .parquet_files
            .iter()
            .filter(|f| {
                f.shard_id == shard_id
                    && f.table_id == table_id
                    && f.to_delete.is_none()
                    && f.compaction_level == CompactionLevel::FileNonOverlapped
                    && ((f.min_time <= min_time && f.max_time >= min_time)
                        || (f.min_time > min_time && f.min_time <= max_time))
            })
            .count();

        i64::try_from(count).map_err(|_| Error::InvalidValue { value: count })
    }

    async fn get_by_object_store_id(
        &mut self,
        object_store_id: Uuid,
    ) -> Result<Option<ParquetFile>> {
        let stage = self.stage();

        Ok(stage
            .parquet_files
            .iter()
            .find(|f| f.object_store_id.eq(&object_store_id))
            .cloned())
    }
}

#[async_trait]
impl ProcessedTombstoneRepo for MemTxn {
    async fn create(
        &mut self,
        parquet_file_id: ParquetFileId,
        tombstone_id: TombstoneId,
    ) -> Result<ProcessedTombstone> {
        let stage = self.stage();

        // check if the parquet file available
        if !stage.parquet_files.iter().any(|f| f.id == parquet_file_id) {
            return Err(Error::FileNotFound {
                id: parquet_file_id.get(),
            });
        }

        // check if tombstone exists
        if !stage.tombstones.iter().any(|f| f.id == tombstone_id) {
            return Err(Error::TombstoneNotFound {
                id: tombstone_id.get(),
            });
        }

        if stage
            .processed_tombstones
            .iter()
            .any(|pt| pt.tombstone_id == tombstone_id && pt.parquet_file_id == parquet_file_id)
        {
            // The tombstone was already processed for this file
            return Err(Error::ProcessTombstoneExists {
                parquet_file_id: parquet_file_id.get(),
                tombstone_id: tombstone_id.get(),
            });
        }

        let processed_tombstone = ProcessedTombstone {
            tombstone_id,
            parquet_file_id,
        };
        stage.processed_tombstones.push(processed_tombstone);

        Ok(processed_tombstone)
    }

    async fn exist(
        &mut self,
        parquet_file_id: ParquetFileId,
        tombstone_id: TombstoneId,
    ) -> Result<bool> {
        let stage = self.stage();

        Ok(stage
            .processed_tombstones
            .iter()
            .any(|f| f.parquet_file_id == parquet_file_id && f.tombstone_id == tombstone_id))
    }

    async fn count(&mut self) -> Result<i64> {
        let stage = self.stage();

        let count = stage.processed_tombstones.len();
        let count_i64 = i64::try_from(count);
        if count_i64.is_err() {
            return Err(Error::InvalidValue { value: count });
        }
        Ok(count_i64.unwrap())
    }

    async fn count_by_tombstone_id(&mut self, tombstone_id: TombstoneId) -> Result<i64> {
        let stage = self.stage();

        let count = stage
            .processed_tombstones
            .iter()
            .filter(|p| p.tombstone_id == tombstone_id)
            .count();

        i64::try_from(count).map_err(|_| Error::InvalidValue { value: count })
    }
}

fn filter_namespace_soft_delete<'a>(
    v: impl IntoIterator<Item = &'a Namespace>,
    deleted: SoftDeletedRows,
) -> impl Iterator<Item = &'a Namespace> {
    v.into_iter().filter(move |v| match deleted {
        SoftDeletedRows::AllRows => true,
        SoftDeletedRows::ExcludeDeleted => v.deleted_at.is_none(),
        SoftDeletedRows::OnlyDeleted => v.deleted_at.is_some(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_catalog() {
        crate::interface::test_helpers::test_catalog(|| async {
            let metrics = Arc::new(metric::Registry::default());
            let x: Arc<dyn Catalog> = Arc::new(MemCatalog::new(metrics));
            x
        })
        .await;
    }
}
