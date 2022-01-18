//! This module implements an in-memory implementation of the iox_catalog interface. It can be
//! used for testing or for an IOx designed to run without catalog persistence.

use crate::interface::{
    Column, ColumnRepo, ColumnType, Error, KafkaTopic, KafkaTopicRepo, Namespace, NamespaceRepo,
    QueryPool, QueryPoolRepo, RepoCollection, Result, Sequencer, SequencerRepo, Table, TableRepo,
};
use async_trait::async_trait;
use std::convert::TryFrom;
use std::fmt::Formatter;
use std::sync::{Arc, Mutex};

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
}

impl RepoCollection for Arc<MemCatalog> {
    fn kafka_topic(&self) -> Arc<dyn KafkaTopicRepo + Sync + Send> {
        Self::clone(self) as Arc<dyn KafkaTopicRepo + Sync + Send>
    }

    fn query_pool(&self) -> Arc<dyn QueryPoolRepo + Sync + Send> {
        Self::clone(self) as Arc<dyn QueryPoolRepo + Sync + Send>
    }

    fn namespace(&self) -> Arc<dyn NamespaceRepo + Sync + Send> {
        Self::clone(self) as Arc<dyn NamespaceRepo + Sync + Send>
    }

    fn table(&self) -> Arc<dyn TableRepo + Sync + Send> {
        Self::clone(self) as Arc<dyn TableRepo + Sync + Send>
    }

    fn column(&self) -> Arc<dyn ColumnRepo + Sync + Send> {
        Self::clone(self) as Arc<dyn ColumnRepo + Sync + Send>
    }

    fn sequencer(&self) -> Arc<dyn SequencerRepo + Sync + Send> {
        Self::clone(self) as Arc<dyn SequencerRepo + Sync + Send>
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
                    id: collections.kafka_topics.len() as i32 + 1,
                    name: name.to_string(),
                };
                collections.kafka_topics.push(topic);
                collections.kafka_topics.last().unwrap()
            }
        };

        Ok(topic.clone())
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
                    id: collections.query_pools.len() as i16 + 1,
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
        kafka_topic_id: i32,
        query_pool_id: i16,
    ) -> Result<Namespace> {
        let mut collections = self.collections.lock().expect("mutex poisoned");
        if collections.namespaces.iter().any(|n| n.name == name) {
            return Err(Error::NameExists {
                name: name.to_string(),
            });
        }

        let namespace = Namespace {
            id: collections.namespaces.len() as i32 + 1,
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
    async fn create_or_get(&self, name: &str, namespace_id: i32) -> Result<Table> {
        let mut collections = self.collections.lock().expect("mutex poisoned");

        let table = match collections.tables.iter().find(|t| t.name == name) {
            Some(t) => t,
            None => {
                let table = Table {
                    id: collections.tables.len() as i32 + 1,
                    namespace_id,
                    name: name.to_string(),
                };
                collections.tables.push(table);
                collections.tables.last().unwrap()
            }
        };

        Ok(table.clone())
    }

    async fn list_by_namespace_id(&self, namespace_id: i32) -> Result<Vec<Table>> {
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
        table_id: i32,
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
                    id: collections.columns.len() as i32 + 1,
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

    async fn list_by_namespace_id(&self, namespace_id: i32) -> Result<Vec<Column>> {
        let mut columns = vec![];

        let collections = self.collections.lock().expect("mutex poisoned");
        for t in collections
            .tables
            .iter()
            .filter(|t| t.namespace_id == namespace_id)
        {
            for c in collections.columns.iter().filter(|c| c.table_id == t.id) {
                columns.push(c.clone());
            }
        }

        Ok(columns)
    }
}

#[async_trait]
impl SequencerRepo for MemCatalog {
    async fn create_or_get(&self, topic: &KafkaTopic, partition: i32) -> Result<Sequencer> {
        let mut collections = self.collections.lock().expect("mutex poisoned");

        let sequencer = match collections
            .sequencers
            .iter()
            .find(|s| s.kafka_topic_id == topic.id && s.kafka_partition == partition)
        {
            Some(t) => t,
            None => {
                let sequencer = Sequencer {
                    id: collections.sequencers.len() as i16 + 1,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mem_repo() {
        let f = || Arc::new(MemCatalog::new());

        crate::interface::test_helpers::test_repo(f).await;
    }
}
