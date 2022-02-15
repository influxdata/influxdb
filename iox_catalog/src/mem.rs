//! This module implements an in-memory implementation of the iox_catalog interface. It can be
//! used for testing or for an IOx designed to run without catalog persistence.

use crate::interface::{
    sealed::TransactionFinalize, Catalog, Column, ColumnId, ColumnRepo, ColumnType, Error,
    KafkaPartition, KafkaTopic, KafkaTopicId, KafkaTopicRepo, Namespace, NamespaceId,
    NamespaceRepo, ParquetFile, ParquetFileId, ParquetFileRepo, Partition, PartitionId,
    PartitionInfo, PartitionRepo, ProcessedTombstone, ProcessedTombstoneRepo, QueryPool,
    QueryPoolId, QueryPoolRepo, RepoCollection, Result, SequenceNumber, Sequencer, SequencerId,
    SequencerRepo, Table, TableId, TableRepo, Timestamp, Tombstone, TombstoneId, TombstoneRepo,
    Transaction,
};
use async_trait::async_trait;
use observability_deps::tracing::warn;
use std::convert::TryFrom;
use std::fmt::Formatter;
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedMutexGuard};
use uuid::Uuid;

/// In-memory catalog that implements the `RepoCollection` and individual repo traits from
/// the catalog interface.
#[derive(Default)]
pub struct MemCatalog {
    collections: Arc<Mutex<MemCollections>>,
}

impl MemCatalog {
    /// return new initialized `MemCatalog`
    pub fn new() -> Self {
        Self::default()
    }
}

impl std::fmt::Debug for MemCatalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemCatalog").finish_non_exhaustive()
    }
}

#[derive(Default, Debug, Clone)]
struct MemCollections {
    kafka_topics: Vec<KafkaTopic>,
    query_pools: Vec<QueryPool>,
    namespaces: Vec<Namespace>,
    tables: Vec<Table>,
    columns: Vec<Column>,
    sequencers: Vec<Sequencer>,
    partitions: Vec<Partition>,
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

#[async_trait]
impl Catalog for MemCatalog {
    async fn setup(&self) -> Result<(), Error> {
        // nothing to do
        Ok(())
    }

    async fn start_transaction(&self) -> Result<Box<dyn Transaction>, Error> {
        let guard = Arc::clone(&self.collections).lock_owned().await;
        let stage = guard.clone();
        Ok(Box::new(MemTxn {
            inner: MemTxnInner::Txn {
                guard,
                stage,
                finalized: false,
            },
        }))
    }

    async fn repositories(&self) -> Box<dyn RepoCollection> {
        let collections = Arc::clone(&self.collections).lock_owned().await;
        Box::new(MemTxn {
            inner: MemTxnInner::NoTxn { collections },
        })
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
    fn kafka_topics(&mut self) -> &mut dyn KafkaTopicRepo {
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

    fn sequencers(&mut self) -> &mut dyn SequencerRepo {
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
impl KafkaTopicRepo for MemTxn {
    async fn create_or_get(&mut self, name: &str) -> Result<KafkaTopic> {
        let stage = self.stage();

        let topic = match stage.kafka_topics.iter().find(|t| t.name == name) {
            Some(t) => t,
            None => {
                let topic = KafkaTopic {
                    id: KafkaTopicId::new(stage.kafka_topics.len() as i32 + 1),
                    name: name.to_string(),
                };
                stage.kafka_topics.push(topic);
                stage.kafka_topics.last().unwrap()
            }
        };

        Ok(topic.clone())
    }

    async fn get_by_name(&mut self, name: &str) -> Result<Option<KafkaTopic>> {
        let stage = self.stage();

        let kafka_topic = stage.kafka_topics.iter().find(|t| t.name == name).cloned();
        Ok(kafka_topic)
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
                    id: QueryPoolId::new(stage.query_pools.len() as i16 + 1),
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
        retention_duration: &str,
        kafka_topic_id: KafkaTopicId,
        query_pool_id: QueryPoolId,
    ) -> Result<Namespace> {
        let stage = self.stage();

        if stage.namespaces.iter().any(|n| n.name == name) {
            return Err(Error::NameExists {
                name: name.to_string(),
            });
        }

        let namespace = Namespace {
            id: NamespaceId::new(stage.namespaces.len() as i32 + 1),
            name: name.to_string(),
            kafka_topic_id,
            query_pool_id,
            retention_duration: Some(retention_duration.to_string()),
        };
        stage.namespaces.push(namespace);
        Ok(stage.namespaces.last().unwrap().clone())
    }

    async fn get_by_name(&mut self, name: &str) -> Result<Option<Namespace>> {
        let stage = self.stage();

        Ok(stage.namespaces.iter().find(|n| n.name == name).cloned())
    }
}

#[async_trait]
impl TableRepo for MemTxn {
    async fn create_or_get(&mut self, name: &str, namespace_id: NamespaceId) -> Result<Table> {
        let stage = self.stage();

        let table = match stage
            .tables
            .iter()
            .find(|t| t.name == name && t.namespace_id == namespace_id)
        {
            Some(t) => t,
            None => {
                let table = Table {
                    id: TableId::new(stage.tables.len() as i32 + 1),
                    namespace_id,
                    name: name.to_string(),
                };
                stage.tables.push(table);
                stage.tables.last().unwrap()
            }
        };

        Ok(table.clone())
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

        let column = match stage
            .columns
            .iter()
            .find(|t| t.name == name && t.table_id == table_id)
        {
            Some(c) => {
                if column_type as i16 != c.column_type {
                    return Err(Error::ColumnTypeMismatch {
                        name: name.to_string(),
                        existing: ColumnType::try_from(c.column_type).unwrap().to_string(),
                        new: column_type.to_string(),
                    });
                }

                c
            }
            None => {
                let column = Column {
                    id: ColumnId::new(stage.columns.len() as i32 + 1),
                    table_id,
                    name: name.to_string(),
                    column_type: column_type as i16,
                };
                stage.columns.push(column);
                stage.columns.last().unwrap()
            }
        };

        Ok(column.clone())
    }

    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Column>> {
        let stage = self.stage();

        let table_ids: Vec<_> = stage
            .tables
            .iter()
            .filter(|t| t.namespace_id == namespace_id)
            .map(|t| t.id)
            .collect();
        println!("tables: {:?}", stage.tables);
        println!("table_ids: {:?}", table_ids);
        let columns: Vec<_> = stage
            .columns
            .iter()
            .filter(|c| table_ids.contains(&c.table_id))
            .cloned()
            .collect();

        Ok(columns)
    }
}

#[async_trait]
impl SequencerRepo for MemTxn {
    async fn create_or_get(
        &mut self,
        topic: &KafkaTopic,
        partition: KafkaPartition,
    ) -> Result<Sequencer> {
        let stage = self.stage();

        let sequencer = match stage
            .sequencers
            .iter()
            .find(|s| s.kafka_topic_id == topic.id && s.kafka_partition == partition)
        {
            Some(t) => t,
            None => {
                let sequencer = Sequencer {
                    id: SequencerId::new(stage.sequencers.len() as i16 + 1),
                    kafka_topic_id: topic.id,
                    kafka_partition: partition,
                    min_unpersisted_sequence_number: 0,
                };
                stage.sequencers.push(sequencer);
                stage.sequencers.last().unwrap()
            }
        };

        Ok(*sequencer)
    }

    async fn get_by_topic_id_and_partition(
        &mut self,
        topic_id: KafkaTopicId,
        partition: KafkaPartition,
    ) -> Result<Option<Sequencer>> {
        let stage = self.stage();

        let sequencer = stage
            .sequencers
            .iter()
            .find(|s| s.kafka_topic_id == topic_id && s.kafka_partition == partition)
            .cloned();
        Ok(sequencer)
    }

    async fn list(&mut self) -> Result<Vec<Sequencer>> {
        let stage = self.stage();

        Ok(stage.sequencers.clone())
    }

    async fn list_by_kafka_topic(&mut self, topic: &KafkaTopic) -> Result<Vec<Sequencer>> {
        let stage = self.stage();

        let sequencers: Vec<_> = stage
            .sequencers
            .iter()
            .filter(|s| s.kafka_topic_id == topic.id)
            .cloned()
            .collect();
        Ok(sequencers)
    }
}

#[async_trait]
impl PartitionRepo for MemTxn {
    async fn create_or_get(
        &mut self,
        key: &str,
        sequencer_id: SequencerId,
        table_id: TableId,
    ) -> Result<Partition> {
        let stage = self.stage();

        let partition = match stage.partitions.iter().find(|p| {
            p.partition_key == key && p.sequencer_id == sequencer_id && p.table_id == table_id
        }) {
            Some(p) => p,
            None => {
                let p = Partition {
                    id: PartitionId::new(stage.partitions.len() as i64 + 1),
                    sequencer_id,
                    table_id,
                    partition_key: key.to_string(),
                };
                stage.partitions.push(p);
                stage.partitions.last().unwrap()
            }
        };

        Ok(partition.clone())
    }

    async fn list_by_sequencer(&mut self, sequencer_id: SequencerId) -> Result<Vec<Partition>> {
        let stage = self.stage();

        let partitions: Vec<_> = stage
            .partitions
            .iter()
            .filter(|p| p.sequencer_id == sequencer_id)
            .cloned()
            .collect();
        Ok(partitions)
    }

    async fn partition_info_by_id(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<PartitionInfo>> {
        let stage = self.stage();

        let partition = stage
            .partitions
            .iter()
            .find(|p| p.id == partition_id)
            .cloned();

        if let Some(partition) = partition {
            let table = stage
                .tables
                .iter()
                .find(|t| t.id == partition.table_id)
                .cloned();
            if let Some(table) = table {
                let namespace = stage
                    .namespaces
                    .iter()
                    .find(|n| n.id == table.namespace_id)
                    .cloned();
                if let Some(namespace) = namespace {
                    return Ok(Some(PartitionInfo {
                        namespace_name: namespace.name,
                        table_name: table.name,
                        partition,
                    }));
                }
            }
        }

        Ok(None)
    }
}

#[async_trait]
impl TombstoneRepo for MemTxn {
    async fn create_or_get(
        &mut self,
        table_id: TableId,
        sequencer_id: SequencerId,
        sequence_number: SequenceNumber,
        min_time: Timestamp,
        max_time: Timestamp,
        predicate: &str,
    ) -> Result<Tombstone> {
        let stage = self.stage();

        let tombstone = match stage.tombstones.iter().find(|t| {
            t.table_id == table_id
                && t.sequencer_id == sequencer_id
                && t.sequence_number == sequence_number
        }) {
            Some(t) => t,
            None => {
                let t = Tombstone {
                    id: TombstoneId::new(stage.tombstones.len() as i64 + 1),
                    table_id,
                    sequencer_id,
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

    async fn list_tombstones_by_sequencer_greater_than(
        &mut self,
        sequencer_id: SequencerId,
        sequence_number: SequenceNumber,
    ) -> Result<Vec<Tombstone>> {
        let stage = self.stage();

        let tombstones: Vec<_> = stage
            .tombstones
            .iter()
            .filter(|t| t.sequencer_id == sequencer_id && t.sequence_number > sequence_number)
            .cloned()
            .collect();
        Ok(tombstones)
    }
}

#[async_trait]
impl ParquetFileRepo for MemTxn {
    async fn create(
        &mut self,
        sequencer_id: SequencerId,
        table_id: TableId,
        partition_id: PartitionId,
        object_store_id: Uuid,
        min_sequence_number: SequenceNumber,
        max_sequence_number: SequenceNumber,
        min_time: Timestamp,
        max_time: Timestamp,
    ) -> Result<ParquetFile> {
        let stage = self.stage();

        if stage
            .parquet_files
            .iter()
            .any(|f| f.object_store_id == object_store_id)
        {
            return Err(Error::FileExists { object_store_id });
        }

        let parquet_file = ParquetFile {
            id: ParquetFileId::new(stage.parquet_files.len() as i64 + 1),
            sequencer_id,
            table_id,
            partition_id,
            object_store_id,
            min_sequence_number,
            max_sequence_number,
            min_time,
            max_time,
            to_delete: false,
        };
        stage.parquet_files.push(parquet_file);
        Ok(*stage.parquet_files.last().unwrap())
    }

    async fn flag_for_delete(&mut self, id: ParquetFileId) -> Result<()> {
        let stage = self.stage();

        match stage.parquet_files.iter_mut().find(|p| p.id == id) {
            Some(f) => f.to_delete = true,
            None => return Err(Error::ParquetRecordNotFound { id }),
        }

        Ok(())
    }

    async fn list_by_sequencer_greater_than(
        &mut self,
        sequencer_id: SequencerId,
        sequence_number: SequenceNumber,
    ) -> Result<Vec<ParquetFile>> {
        let stage = self.stage();

        let files: Vec<_> = stage
            .parquet_files
            .iter()
            .filter(|f| f.sequencer_id == sequencer_id && f.max_sequence_number > sequence_number)
            .cloned()
            .collect();
        Ok(files)
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

        // check if tomstone exists
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
            // The tombstone was already proccessed for this file
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_catalog() {
        crate::interface::test_helpers::test_catalog(Arc::new(MemCatalog::new())).await;
    }
}
