//! This module implements an in-memory implementation of the iox_catalog interface. It can be
//! used for testing or for an IOx designed to run without catalog persistence.

use crate::interface::{
    Catalog, Column, ColumnId, ColumnRepo, ColumnType, Error, KafkaPartition, KafkaTopic,
    KafkaTopicId, KafkaTopicRepo, Namespace, NamespaceId, NamespaceRepo, ParquetFile,
    ParquetFileId, ParquetFileRepo, Partition, PartitionId, PartitionRepo, ProcessedTombstone,
    ProcessedTombstoneRepo, QueryPool, QueryPoolId, QueryPoolRepo, Result, SequenceNumber,
    Sequencer, SequencerId, SequencerRepo, Table, TableId, TableRepo, Timestamp, Tombstone,
    TombstoneId, TombstoneRepo,
};
use async_trait::async_trait;
use sqlx::{Postgres, Transaction};
use std::convert::TryFrom;
use std::fmt::Formatter;
use std::sync::Mutex;
use uuid::Uuid;

/// In-memory catalog that implements the `RepoCollection` and individual repo traits from
/// the catalog interface.
#[derive(Default)]
pub struct MemCatalog {
    collections: Mutex<MemCollections>,
}

impl MemCatalog {
    /// return new initialized `MemCatalog`
    pub fn new() -> Self {
        Self::default()
    }

    // Since this is test catalog that do not handle transaction
    // this is a help function to fake `rollback` work
    fn remove_parquet_file(&self, object_store_id: Uuid) {
        let mut collections = self.collections.lock().expect("mutex poisoned");
        collections
            .parquet_files
            .retain(|f| f.object_store_id != object_store_id);
    }
}

impl std::fmt::Debug for MemCatalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let c = self.collections.lock().expect("mutex poisoned");
        write!(f, "MemCatalog[ {:?} ]", c)
    }
}

#[derive(Default, Debug)]
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

#[async_trait]
impl Catalog for MemCatalog {
    async fn setup(&self) -> Result<(), Error> {
        // nothing to do
        Ok(())
    }

    fn kafka_topics(&self) -> &dyn KafkaTopicRepo {
        self
    }

    fn query_pools(&self) -> &dyn QueryPoolRepo {
        self
    }

    fn namespaces(&self) -> &dyn NamespaceRepo {
        self
    }

    fn tables(&self) -> &dyn TableRepo {
        self
    }

    fn columns(&self) -> &dyn ColumnRepo {
        self
    }

    fn sequencers(&self) -> &dyn SequencerRepo {
        self
    }

    fn partitions(&self) -> &dyn PartitionRepo {
        self
    }

    fn tombstones(&self) -> &dyn TombstoneRepo {
        self
    }

    fn parquet_files(&self) -> &dyn ParquetFileRepo {
        self
    }

    fn processed_tombstones(&self) -> &dyn ProcessedTombstoneRepo {
        self
    }

    async fn add_parquet_file_with_tombstones(
        &self,
        parquet_file: &ParquetFile,
        tombstones: &[Tombstone],
    ) -> Result<(ParquetFile, Vec<ProcessedTombstone>), Error> {
        // The activities in this file must either be all succeed or all fail

        // Create a parquet file in the catalog first
        let parquet = self
            .parquet_files()
            .create(
                None,
                parquet_file.sequencer_id,
                parquet_file.table_id,
                parquet_file.partition_id,
                parquet_file.object_store_id,
                parquet_file.min_sequence_number,
                parquet_file.max_sequence_number,
                parquet_file.min_time,
                parquet_file.max_time,
            )
            .await?;

        // Now the parquet available, let create its dependent processed tombstones
        let processed_tombstones = self
            .processed_tombstones()
            .create_many(None, parquet.id, tombstones)
            .await;

        if let Err(error) = processed_tombstones {
            // failed to insert processed tombstone, remove the above
            // inserted parquet file from the catalog
            self.remove_parquet_file(parquet.object_store_id);
            return Err(error);
        }
        let processed_tombstones = processed_tombstones.unwrap();

        Ok((parquet, processed_tombstones))
    }
}

#[async_trait]
impl KafkaTopicRepo for MemCatalog {
    async fn create_or_get(&self, name: &str) -> Result<KafkaTopic> {
        let mut collections = self.collections.lock().expect("mutex poisoned");

        let topic = match collections.kafka_topics.iter().find(|t| t.name == name) {
            Some(t) => t,
            None => {
                let topic = KafkaTopic {
                    id: KafkaTopicId::new(collections.kafka_topics.len() as i32 + 1),
                    name: name.to_string(),
                };
                collections.kafka_topics.push(topic);
                collections.kafka_topics.last().unwrap()
            }
        };

        Ok(topic.clone())
    }

    async fn get_by_name(&self, name: &str) -> Result<Option<KafkaTopic>> {
        let collections = self.collections.lock().expect("mutex poisoned");
        let kafka_topic = collections
            .kafka_topics
            .iter()
            .find(|t| t.name == name)
            .cloned();
        Ok(kafka_topic)
    }
}

#[async_trait]
impl QueryPoolRepo for MemCatalog {
    async fn create_or_get(&self, name: &str) -> Result<QueryPool> {
        let mut collections = self.collections.lock().expect("mutex poisoned");

        let pool = match collections.query_pools.iter().find(|t| t.name == name) {
            Some(t) => t,
            None => {
                let pool = QueryPool {
                    id: QueryPoolId::new(collections.query_pools.len() as i16 + 1),
                    name: name.to_string(),
                };
                collections.query_pools.push(pool);
                collections.query_pools.last().unwrap()
            }
        };

        Ok(pool.clone())
    }
}

#[async_trait]
impl NamespaceRepo for MemCatalog {
    async fn create(
        &self,
        name: &str,
        retention_duration: &str,
        kafka_topic_id: KafkaTopicId,
        query_pool_id: QueryPoolId,
    ) -> Result<Namespace> {
        let mut collections = self.collections.lock().expect("mutex poisoned");
        if collections.namespaces.iter().any(|n| n.name == name) {
            return Err(Error::NameExists {
                name: name.to_string(),
            });
        }

        let namespace = Namespace {
            id: NamespaceId::new(collections.namespaces.len() as i32 + 1),
            name: name.to_string(),
            kafka_topic_id,
            query_pool_id,
            retention_duration: Some(retention_duration.to_string()),
        };
        collections.namespaces.push(namespace);
        Ok(collections.namespaces.last().unwrap().clone())
    }

    async fn get_by_name(&self, name: &str) -> Result<Option<Namespace>> {
        let collections = self.collections.lock().expect("mutex poisoned");
        Ok(collections
            .namespaces
            .iter()
            .find(|n| n.name == name)
            .cloned())
    }
}

#[async_trait]
impl TableRepo for MemCatalog {
    async fn create_or_get(&self, name: &str, namespace_id: NamespaceId) -> Result<Table> {
        let mut collections = self.collections.lock().expect("mutex poisoned");

        let table = match collections
            .tables
            .iter()
            .find(|t| t.name == name && t.namespace_id == namespace_id)
        {
            Some(t) => t,
            None => {
                let table = Table {
                    id: TableId::new(collections.tables.len() as i32 + 1),
                    namespace_id,
                    name: name.to_string(),
                };
                collections.tables.push(table);
                collections.tables.last().unwrap()
            }
        };

        Ok(table.clone())
    }

    async fn list_by_namespace_id(&self, namespace_id: NamespaceId) -> Result<Vec<Table>> {
        let collections = self.collections.lock().expect("mutex poisoned");
        let tables: Vec<_> = collections
            .tables
            .iter()
            .filter(|t| t.namespace_id == namespace_id)
            .cloned()
            .collect();
        Ok(tables)
    }
}

#[async_trait]
impl ColumnRepo for MemCatalog {
    async fn create_or_get(
        &self,
        name: &str,
        table_id: TableId,
        column_type: ColumnType,
    ) -> Result<Column> {
        let mut collections = self.collections.lock().expect("mutex poisoned");

        let column = match collections
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
                    id: ColumnId::new(collections.columns.len() as i32 + 1),
                    table_id,
                    name: name.to_string(),
                    column_type: column_type as i16,
                };
                collections.columns.push(column);
                collections.columns.last().unwrap()
            }
        };

        Ok(column.clone())
    }

    async fn list_by_namespace_id(&self, namespace_id: NamespaceId) -> Result<Vec<Column>> {
        let collections = self.collections.lock().expect("mutex poisoned");

        let table_ids: Vec<_> = collections
            .tables
            .iter()
            .filter(|t| t.namespace_id == namespace_id)
            .map(|t| t.id)
            .collect();
        println!("tables: {:?}", collections.tables);
        println!("table_ids: {:?}", table_ids);
        let columns: Vec<_> = collections
            .columns
            .iter()
            .filter(|c| table_ids.contains(&c.table_id))
            .cloned()
            .collect();

        Ok(columns)
    }
}

#[async_trait]
impl SequencerRepo for MemCatalog {
    async fn create_or_get(
        &self,
        topic: &KafkaTopic,
        partition: KafkaPartition,
    ) -> Result<Sequencer> {
        let mut collections = self.collections.lock().expect("mutex poisoned");

        let sequencer = match collections
            .sequencers
            .iter()
            .find(|s| s.kafka_topic_id == topic.id && s.kafka_partition == partition)
        {
            Some(t) => t,
            None => {
                let sequencer = Sequencer {
                    id: SequencerId::new(collections.sequencers.len() as i16 + 1),
                    kafka_topic_id: topic.id,
                    kafka_partition: partition,
                    min_unpersisted_sequence_number: 0,
                };
                collections.sequencers.push(sequencer);
                collections.sequencers.last().unwrap()
            }
        };

        Ok(*sequencer)
    }

    async fn get_by_topic_id_and_partition(
        &self,
        topic_id: KafkaTopicId,
        partition: KafkaPartition,
    ) -> Result<Option<Sequencer>> {
        let collections = self.collections.lock().expect("mutex poisoned");
        let sequencer = collections
            .sequencers
            .iter()
            .find(|s| s.kafka_topic_id == topic_id && s.kafka_partition == partition)
            .cloned();
        Ok(sequencer)
    }

    async fn list(&self) -> Result<Vec<Sequencer>> {
        let collections = self.collections.lock().expect("mutex poisoned");
        Ok(collections.sequencers.clone())
    }

    async fn list_by_kafka_topic(&self, topic: &KafkaTopic) -> Result<Vec<Sequencer>> {
        let collections = self.collections.lock().expect("mutex poisoned");
        let sequencers: Vec<_> = collections
            .sequencers
            .iter()
            .filter(|s| s.kafka_topic_id == topic.id)
            .cloned()
            .collect();
        Ok(sequencers)
    }
}

#[async_trait]
impl PartitionRepo for MemCatalog {
    async fn create_or_get(
        &self,
        key: &str,
        sequencer_id: SequencerId,
        table_id: TableId,
    ) -> Result<Partition> {
        let mut collections = self.collections.lock().expect("mutex poisoned");
        let partition = match collections.partitions.iter().find(|p| {
            p.partition_key == key && p.sequencer_id == sequencer_id && p.table_id == table_id
        }) {
            Some(p) => p,
            None => {
                let p = Partition {
                    id: PartitionId::new(collections.partitions.len() as i64 + 1),
                    sequencer_id,
                    table_id,
                    partition_key: key.to_string(),
                };
                collections.partitions.push(p);
                collections.partitions.last().unwrap()
            }
        };

        Ok(partition.clone())
    }

    async fn list_by_sequencer(&self, sequencer_id: SequencerId) -> Result<Vec<Partition>> {
        let collections = self.collections.lock().expect("mutex poisoned");
        let partitions: Vec<_> = collections
            .partitions
            .iter()
            .filter(|p| p.sequencer_id == sequencer_id)
            .cloned()
            .collect();
        Ok(partitions)
    }
}

#[async_trait]
impl TombstoneRepo for MemCatalog {
    async fn create_or_get(
        &self,
        table_id: TableId,
        sequencer_id: SequencerId,
        sequence_number: SequenceNumber,
        min_time: Timestamp,
        max_time: Timestamp,
        predicate: &str,
    ) -> Result<Tombstone> {
        let mut collections = self.collections.lock().expect("mutex poisoned");
        let tombstone = match collections.tombstones.iter().find(|t| {
            t.table_id == table_id
                && t.sequencer_id == sequencer_id
                && t.sequence_number == sequence_number
        }) {
            Some(t) => t,
            None => {
                let t = Tombstone {
                    id: TombstoneId::new(collections.tombstones.len() as i64 + 1),
                    table_id,
                    sequencer_id,
                    sequence_number,
                    min_time,
                    max_time,
                    serialized_predicate: predicate.to_string(),
                };
                collections.tombstones.push(t);
                collections.tombstones.last().unwrap()
            }
        };

        Ok(tombstone.clone())
    }

    async fn list_tombstones_by_sequencer_greater_than(
        &self,
        sequencer_id: SequencerId,
        sequence_number: SequenceNumber,
    ) -> Result<Vec<Tombstone>> {
        let collections = self.collections.lock().expect("mutex poisoned");
        let tombstones: Vec<_> = collections
            .tombstones
            .iter()
            .filter(|t| t.sequencer_id == sequencer_id && t.sequence_number > sequence_number)
            .cloned()
            .collect();
        Ok(tombstones)
    }
}

#[async_trait]
impl ParquetFileRepo for MemCatalog {
    async fn create(
        &self,
        _txt: Option<&mut Transaction<'_, Postgres>>,
        sequencer_id: SequencerId,
        table_id: TableId,
        partition_id: PartitionId,
        object_store_id: Uuid,
        min_sequence_number: SequenceNumber,
        max_sequence_number: SequenceNumber,
        min_time: Timestamp,
        max_time: Timestamp,
    ) -> Result<ParquetFile> {
        let mut collections = self.collections.lock().expect("mutex poisoned");
        if collections
            .parquet_files
            .iter()
            .any(|f| f.object_store_id == object_store_id)
        {
            return Err(Error::FileExists { object_store_id });
        }

        let parquet_file = ParquetFile {
            id: ParquetFileId::new(collections.parquet_files.len() as i64 + 1),
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
        collections.parquet_files.push(parquet_file);
        Ok(*collections.parquet_files.last().unwrap())
    }

    async fn flag_for_delete(&self, id: ParquetFileId) -> Result<()> {
        let mut collections = self.collections.lock().expect("mutex poisoned");

        match collections.parquet_files.iter_mut().find(|p| p.id == id) {
            Some(f) => f.to_delete = true,
            None => return Err(Error::ParquetRecordNotFound { id }),
        }

        Ok(())
    }

    async fn list_by_sequencer_greater_than(
        &self,
        sequencer_id: SequencerId,
        sequence_number: SequenceNumber,
    ) -> Result<Vec<ParquetFile>> {
        let collections = self.collections.lock().expect("mutex poisoned");
        let files: Vec<_> = collections
            .parquet_files
            .iter()
            .filter(|f| f.sequencer_id == sequencer_id && f.max_sequence_number > sequence_number)
            .cloned()
            .collect();
        Ok(files)
    }

    async fn exist(&self, id: ParquetFileId) -> Result<bool> {
        let collections = self.collections.lock().expect("mutex poisoned");
        Ok(collections.parquet_files.iter().any(|f| f.id == id))
    }

    async fn count(&self) -> Result<i64> {
        let collections = self.collections.lock().expect("mutex poisoned");
        let count = collections.parquet_files.len();
        let count_i64 = i64::try_from(count);
        if count_i64.is_err() {
            return Err(Error::InvalidValue { value: count });
        }
        Ok(count_i64.unwrap())
    }
}

#[async_trait]
impl ProcessedTombstoneRepo for MemCatalog {
    async fn create_many(
        &self,
        _txt: Option<&mut Transaction<'_, Postgres>>,
        parquet_file_id: ParquetFileId,
        tombstones: &[Tombstone],
    ) -> Result<Vec<ProcessedTombstone>> {
        let mut collections = self.collections.lock().expect("mutex poisoned");

        // check if the parquet file available
        if !collections
            .parquet_files
            .iter()
            .any(|f| f.id == parquet_file_id)
        {
            return Err(Error::FileNotFound {
                id: parquet_file_id.get(),
            });
        }

        let mut processed_tombstones = vec![];
        for tombstone in tombstones {
            // check if tomstone exists
            if !collections.tombstones.iter().any(|f| f.id == tombstone.id) {
                return Err(Error::TombstoneNotFound {
                    id: tombstone.id.get(),
                });
            }

            if collections
                .processed_tombstones
                .iter()
                .any(|pt| pt.tombstone_id == tombstone.id && pt.parquet_file_id == parquet_file_id)
            {
                // The tombstone was already proccessed for this file
                return Err(Error::ProcessTombstoneExists {
                    parquet_file_id: parquet_file_id.get(),
                    tombstone_id: tombstone.id.get(),
                });
            }

            let processed_tombstone = ProcessedTombstone {
                tombstone_id: tombstone.id,
                parquet_file_id,
            };
            processed_tombstones.push(processed_tombstone);
        }

        // save for returning
        let return_processed_tombstones = processed_tombstones.clone();

        // Add to the catalog
        collections
            .processed_tombstones
            .append(&mut processed_tombstones);

        Ok(return_processed_tombstones)
    }

    async fn exist(
        &self,
        parquet_file_id: ParquetFileId,
        tombstone_id: TombstoneId,
    ) -> Result<bool> {
        let collections = self.collections.lock().expect("mutex poisoned");
        Ok(collections
            .processed_tombstones
            .iter()
            .any(|f| f.parquet_file_id == parquet_file_id && f.tombstone_id == tombstone_id))
    }

    async fn count(&self) -> Result<i64> {
        let collections = self.collections.lock().expect("mutex poisoned");
        let count = collections.processed_tombstones.len();
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
