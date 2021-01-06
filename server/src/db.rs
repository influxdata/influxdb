//! This module contains the main IOx database object that points to all the
//! data

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use async_trait::async_trait;
use data_types::{data::ReplicatedWrite, database_rules::DatabaseRules};
use mutable_buffer::MutableBufferDb;
use query::{Database, PartitionChunk, SQLDatabase};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};

use crate::buffer::Buffer;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Mutable Buffer Chunk Error: {}", source))]
    MutableBufferChunk {
        source: mutable_buffer::chunk::Error,
    },

    #[snafu(display("Cannot write to this database: no mutable buffer configured"))]
    DatatbaseNotWriteable {},

    #[snafu(display("Cannot read to this database: no mutable buffer configured"))]
    DatabaseNotReadable {},

    #[snafu(display("Error rolling partition: {}", source))]
    RollingPartition {
        source: mutable_buffer::database::Error,
    },

    #[snafu(display("Error querying mutable buffer: {}", source))]
    MutableBufferRead {
        source: mutable_buffer::database::Error,
    },

    #[snafu(display("Error writing to mutable buffer: {}", source))]
    MutableBufferWrite {
        source: mutable_buffer::database::Error,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Serialize, Deserialize)]
/// This is the main IOx Database object. It is the root object of any
/// specific InfluxDB IOx instance
pub struct Db {
    #[serde(flatten)]
    pub rules: DatabaseRules,
    #[serde(skip)]
    pub local_store: Option<Arc<MutableBufferDb>>,
    #[serde(skip)]
    wal_buffer: Option<Buffer>,
    #[serde(skip)]
    sequence: AtomicU64,
}
impl Db {
    pub fn new(
        rules: DatabaseRules,
        local_store: Option<Arc<MutableBufferDb>>,
        wal_buffer: Option<Buffer>,
        sequence: AtomicU64,
    ) -> Self {
        Self {
            rules,
            local_store,
            wal_buffer,
            sequence,
        }
    }

    /// Rolls over the active chunk in the database's specified partition
    pub async fn rollover_partition(&self, partition_key: &str) -> Result<Arc<DBChunk>> {
        if let Some(local_store) = self.local_store.as_ref() {
            local_store
                .rollover_partition(partition_key)
                .await
                .context(RollingPartition)
                .map(|c| Arc::new(DBChunk::MutableBuffer(c)))
        } else {
            DatatbaseNotWriteable {}.fail()
        }
    }
}

impl PartialEq for Db {
    fn eq(&self, other: &Self) -> bool {
        self.rules == other.rules
    }
}
impl Eq for Db {}

impl Db {
    pub fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::SeqCst)
    }
}

/// A IOx DatabaseChunk can come from one of three places:
/// MutableBuffer, ReadBuffer, or a ParquetFile
#[derive(Debug)]
pub enum DBChunk {
    MutableBuffer(Arc<mutable_buffer::chunk::Chunk>),
    ReadBuffer,  // TODO add appropriate type here
    ParquetFile, // TODO add appropriate type here
}

impl PartitionChunk for DBChunk {
    type Error = Error;

    fn key(&self) -> &str {
        match self {
            Self::MutableBuffer(chunk) => chunk.key(),
            Self::ReadBuffer => unimplemented!("read buffer not implemented"),
            Self::ParquetFile => unimplemented!("parquet file not implemented"),
        }
    }

    fn id(&self) -> u64 {
        match self {
            Self::MutableBuffer(chunk) => chunk.id(),
            Self::ReadBuffer => unimplemented!("read buffer not implemented"),
            Self::ParquetFile => unimplemented!("parquet file not implemented"),
        }
    }

    fn table_stats(&self) -> Result<Vec<data_types::partition_metadata::Table>, Self::Error> {
        match self {
            Self::MutableBuffer(chunk) => chunk.table_stats().context(MutableBufferChunk),
            Self::ReadBuffer => unimplemented!("read buffer not implemented"),
            Self::ParquetFile => unimplemented!("parquet file not implemented"),
        }
    }

    fn table_to_arrow(
        &self,
        dst: &mut Vec<arrow_deps::arrow::record_batch::RecordBatch>,
        table_name: &str,
        columns: &[&str],
    ) -> Result<(), Self::Error> {
        match self {
            Self::MutableBuffer(chunk) => chunk
                .table_to_arrow(dst, table_name, columns)
                .context(MutableBufferChunk),
            Self::ReadBuffer => unimplemented!("read buffer not implemented"),
            Self::ParquetFile => unimplemented!("parquet file not implemented"),
        }
    }
}

#[async_trait]
impl Database for Db {
    type Error = Error;

    // Note that most of these traits will eventually be removed from
    // this trait. For now, pass them directly on to the local store

    async fn store_replicated_write(&self, write: &ReplicatedWrite) -> Result<(), Self::Error> {
        self.local_store
            .as_ref()
            .context(DatatbaseNotWriteable)?
            .store_replicated_write(write)
            .await
            .context(MutableBufferWrite)
    }

    async fn table_names(
        &self,
        predicate: query::predicate::Predicate,
    ) -> Result<query::exec::StringSetPlan, Self::Error> {
        self.local_store
            .as_ref()
            .context(DatabaseNotReadable)?
            .table_names(predicate)
            .await
            .context(MutableBufferRead)
    }

    async fn tag_column_names(
        &self,
        predicate: query::predicate::Predicate,
    ) -> Result<query::exec::StringSetPlan, Self::Error> {
        self.local_store
            .as_ref()
            .context(DatabaseNotReadable)?
            .tag_column_names(predicate)
            .await
            .context(MutableBufferRead)
    }

    async fn field_column_names(
        &self,
        predicate: query::predicate::Predicate,
    ) -> Result<query::exec::FieldListPlan, Self::Error> {
        self.local_store
            .as_ref()
            .context(DatabaseNotReadable)?
            .field_column_names(predicate)
            .await
            .context(MutableBufferRead)
    }

    async fn column_values(
        &self,
        column_name: &str,
        predicate: query::predicate::Predicate,
    ) -> Result<query::exec::StringSetPlan, Self::Error> {
        self.local_store
            .as_ref()
            .context(DatabaseNotReadable)?
            .column_values(column_name, predicate)
            .await
            .context(MutableBufferRead)
    }

    async fn query_series(
        &self,
        predicate: query::predicate::Predicate,
    ) -> Result<query::exec::SeriesSetPlans, Self::Error> {
        self.local_store
            .as_ref()
            .context(DatabaseNotReadable)?
            .query_series(predicate)
            .await
            .context(MutableBufferRead)
    }

    async fn query_groups(
        &self,
        predicate: query::predicate::Predicate,
        gby_agg: query::group_by::GroupByAndAggregate,
    ) -> Result<query::exec::SeriesSetPlans, Self::Error> {
        self.local_store
            .as_ref()
            .context(DatabaseNotReadable)?
            .query_groups(predicate, gby_agg)
            .await
            .context(MutableBufferRead)
    }
}

#[async_trait]
impl SQLDatabase for Db {
    type Error = Error;

    async fn query(
        &self,
        query: &str,
    ) -> Result<Vec<arrow_deps::arrow::record_batch::RecordBatch>, Self::Error> {
        self.local_store
            .as_ref()
            .context(DatabaseNotReadable)?
            .query(query)
            .await
            .context(MutableBufferRead)
    }

    async fn table_to_arrow(
        &self,
        table_name: &str,
        columns: &[&str],
    ) -> Result<Vec<arrow_deps::arrow::record_batch::RecordBatch>, Self::Error> {
        self.local_store
            .as_ref()
            .context(DatabaseNotReadable)?
            .table_to_arrow(table_name, columns)
            .await
            .context(MutableBufferRead)
    }

    async fn partition_keys(&self) -> Result<Vec<String>, Self::Error> {
        self.local_store
            .as_ref()
            .context(DatabaseNotReadable)?
            .partition_keys()
            .await
            .context(MutableBufferRead)
    }

    async fn table_names_for_partition(
        &self,
        partition_key: &str,
    ) -> Result<Vec<String>, Self::Error> {
        self.local_store
            .as_ref()
            .context(DatabaseNotReadable)?
            .table_names_for_partition(partition_key)
            .await
            .context(MutableBufferRead)
    }
}
