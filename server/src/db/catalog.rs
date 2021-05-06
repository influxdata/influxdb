//! This module contains the implementation of the InfluxDB IOx Metadata catalog
use std::any::Any;
use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};

use snafu::{OptionExt, Snafu};

use chunk::Chunk;
use data_types::chunk::ChunkSummary;
use data_types::database_rules::{Order, Sort, SortOrder};
use data_types::error::ErrorLogger;
use data_types::partition_metadata::PartitionSummary;
use datafusion::{catalog::schema::SchemaProvider, datasource::TableProvider};
use internal_types::selection::Selection;
use partition::Partition;
use query::{
    exec::stringset::StringSet,
    predicate::Predicate,
    provider::{self, ProviderBuilder},
    PartitionChunk,
};
use tracker::{LockTracker, RwLock};

pub mod chunk;
pub mod partition;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("unknown partition: {}", partition_key))]
    UnknownPartition { partition_key: String },

    #[snafu(display("unknown table: {}:{}", partition_key, table_name))]
    UnknownTable {
        partition_key: String,
        table_name: String,
    },

    #[snafu(display("unknown chunk: {}:{}:{}", partition_key, table_name, chunk_id))]
    UnknownChunk {
        partition_key: String,
        table_name: String,
        chunk_id: u32,
    },

    #[snafu(display(
        "Internal unexpected chunk state for {}:{}:{}  during {}. Expected {}, got {}",
        partition_key,
        table_name,
        chunk_id,
        operation,
        expected,
        actual
    ))]
    InternalChunkState {
        partition_key: String,
        table_name: String,
        chunk_id: u32,
        operation: String,
        expected: String,
        actual: String,
    },

    #[snafu(display("Can not open chunk {}:{} : {}", partition_key, chunk_id, source))]
    OpenChunk {
        partition_key: String,
        chunk_id: u32,
        source: mutable_buffer::chunk::Error,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// InfluxDB IOx Metadata Catalog
///
/// The Catalog stores information such as which chunks exist, what
/// state they are in, and what objects on object store are used, etc.
///
/// The catalog is also responsible for (eventually) persisting this
/// information
#[derive(Default, Debug)]
pub struct Catalog {
    /// key is partition_key
    partitions: RwLock<BTreeMap<String, Arc<RwLock<Partition>>>>,

    /// Lock tracker for partition-level locks
    lock_tracker: LockTracker,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    /// List all partitions in this database
    pub fn partitions(&self) -> impl Iterator<Item = Arc<RwLock<Partition>>> {
        let partitions = self.partitions.read();
        partitions.values().cloned().collect::<Vec<_>>().into_iter()
    }

    /// Get a specific partition by name, returning `None` if there is no such
    /// partition
    pub fn partition(&self, partition_key: impl AsRef<str>) -> Option<Arc<RwLock<Partition>>> {
        let partition_key = partition_key.as_ref();
        let partitions = self.partitions.read();
        partitions.get(partition_key).cloned()
    }

    /// List all partition keys in this database
    pub fn partition_keys(&self) -> Vec<String> {
        self.partitions.read().keys().cloned().collect()
    }

    /// Gets or creates a new partition in the catalog and returns
    /// a reference to it
    pub fn get_or_create_partition(
        &self,
        partition_key: impl Into<String>,
    ) -> Arc<RwLock<Partition>> {
        let partition_key = partition_key.into();

        let mut partitions = self.partitions.write();
        let entry = partitions.entry(partition_key);
        match entry {
            Entry::Vacant(entry) => {
                let partition = Partition::new(entry.key());
                let partition = Arc::new(self.lock_tracker.new_lock(partition));
                entry.insert(Arc::clone(&partition));
                partition
            }
            Entry::Occupied(entry) => Arc::clone(entry.get()),
        }
    }

    /// Return the specified partition or an error if there is no such
    /// partition
    pub fn valid_partition(&self, partition_key: &str) -> Result<Arc<RwLock<Partition>>> {
        let partitions = self.partitions.read();
        partitions
            .get(partition_key)
            .cloned()
            .context(UnknownPartition { partition_key })
    }

    /// Returns a list of partition summaries
    pub fn partition_summaries(&self) -> Vec<PartitionSummary> {
        self.partitions
            .read()
            .values()
            .map(|partition| partition.read().summary())
            .collect()
    }

    pub fn chunk_summaries(&self) -> Vec<ChunkSummary> {
        self.filtered_chunks(&Predicate::default(), Chunk::summary)
    }

    /// Returns all chunks within the catalog in an arbitrary order
    pub fn chunks(&self) -> Vec<Arc<RwLock<Chunk>>> {
        let mut chunks = Vec::new();
        let partitions = self.partitions.read();

        for partition in partitions.values() {
            let partition = partition.read();
            chunks.extend(partition.chunks().cloned())
        }
        chunks
    }

    /// Returns the chunks in the requested sort order
    pub fn chunks_sorted_by(&self, sort_rules: &SortOrder) -> Vec<Arc<RwLock<Chunk>>> {
        let mut chunks = self.chunks();

        match &sort_rules.sort {
            // The first write is technically not the created time but is in practice close enough
            Sort::CreatedAtTime => chunks.sort_by_cached_key(|x| x.read().time_of_first_write()),
            Sort::LastWriteTime => chunks.sort_by_cached_key(|x| x.read().time_of_last_write()),
            Sort::Column(_name, _data_type, _val) => {
                unimplemented!()
            }
        }

        if sort_rules.order == Order::Desc {
            chunks.reverse();
        }

        chunks
    }

    /// Calls `map` with every chunk matching `predicate` and returns a
    /// collection of the results
    pub fn filtered_chunks<F, C>(&self, predicate: &Predicate, map: F) -> Vec<C>
    where
        F: Fn(&Chunk) -> C + Copy,
    {
        let mut chunks = Vec::new();
        let partitions = self.partitions.read();

        let partitions = match &predicate.partition_key {
            None => itertools::Either::Left(partitions.values()),
            Some(partition_key) => {
                itertools::Either::Right(partitions.get(partition_key).into_iter())
            }
        };

        for partition in partitions {
            let partition = partition.read();
            chunks.extend(partition.filtered_chunks(predicate).map(|chunk| {
                let chunk = chunk.read();
                // TODO: Filter chunks
                map(&chunk)
            }))
        }
        chunks
    }
}

impl SchemaProvider for Catalog {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn table_names(&self) -> Vec<String> {
        let mut names = StringSet::new();

        self.partitions().for_each(|partition| {
            let partition = partition.read();
            partition.chunks().for_each(|chunk| {
                names.insert(chunk.read().table_name().to_string());
            })
        });

        names.into_iter().collect()
    }

    fn table(&self, table_name: &str) -> Option<Arc<dyn TableProvider>> {
        let mut builder = ProviderBuilder::new(table_name);
        let partitions = self.partitions.read();

        for partition in partitions.values() {
            let partition = partition.read();
            for chunk in partition.chunks() {
                let chunk = chunk.read();

                if chunk.table_name() == table_name {
                    let chunk = super::DbChunk::snapshot(&chunk);

                    // This should only fail if the table doesn't exist which isn't possible
                    let schema = chunk
                        .table_schema(table_name, Selection::All)
                        .expect("cannot fail");

                    // This is unfortunate - a table with incompatible chunks ceases to
                    // be visible to the query engine
                    builder = builder
                        .add_chunk(chunk, schema)
                        .log_if_error("Adding chunks to table")
                        .ok()?
                }
            }
        }

        match builder.build() {
            Ok(provider) => Some(Arc::new(provider)),
            Err(provider::Error::InternalNoChunks { .. }) => None,
            Err(e) => panic!("unexpected error: {:?}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::server_id::ServerId;
    use entry::{test_helpers::lp_to_entry, ClockValue};
    use query::predicate::PredicateBuilder;
    use std::convert::TryFrom;
    use tracker::MemRegistry;

    fn create_open_chunk(partition: &Arc<RwLock<Partition>>, table: &str, registry: &MemRegistry) {
        let entry = lp_to_entry(&format!("{} bar=1 10", table));
        let write = entry.partition_writes().unwrap().remove(0);
        let batch = write.table_batches().remove(0);
        let mut partition = partition.write();
        partition
            .create_open_chunk(
                batch,
                ClockValue::try_from(5).unwrap(),
                ServerId::try_from(1).unwrap(),
                registry,
            )
            .unwrap();
    }

    #[test]
    fn partition_get() {
        let catalog = Catalog::new();
        catalog.get_or_create_partition("p1");
        catalog.get_or_create_partition("p2");

        let p1 = catalog.partition("p1").unwrap();
        assert_eq!(p1.read().key(), "p1");

        let p2 = catalog.partition("p2").unwrap();
        assert_eq!(p2.read().key(), "p2");

        let p3 = catalog.partition("p3");
        assert!(p3.is_none());
    }

    #[test]
    fn partition_list() {
        let catalog = Catalog::new();

        assert_eq!(catalog.partitions().count(), 0);

        catalog.get_or_create_partition("p1");
        catalog.get_or_create_partition("p2");
        catalog.get_or_create_partition("p3");

        let mut partition_keys: Vec<String> = catalog
            .partitions()
            .map(|p| p.read().key().into())
            .collect();
        partition_keys.sort_unstable();

        assert_eq!(partition_keys, vec!["p1", "p2", "p3"]);
    }

    #[test]
    fn chunk_create() {
        let registry = MemRegistry::new();
        let catalog = Catalog::new();
        let p1 = catalog.get_or_create_partition("p1");

        create_open_chunk(&p1, "table1", &registry);
        create_open_chunk(&p1, "table1", &registry);
        create_open_chunk(&p1, "table2", &registry);

        let p1 = p1.write();

        let c1_0 = p1.chunk("table1", 0).unwrap();
        assert_eq!(c1_0.read().table_name(), "table1");
        assert_eq!(c1_0.read().key(), "p1");
        assert_eq!(c1_0.read().id(), 0);

        let c1_1 = p1.chunk("table1", 1).unwrap();
        assert_eq!(c1_1.read().table_name(), "table1");
        assert_eq!(c1_1.read().key(), "p1");
        assert_eq!(c1_1.read().id(), 1);

        let c2_0 = p1.chunk("table2", 0).unwrap();
        assert_eq!(c2_0.read().table_name(), "table2");
        assert_eq!(c2_0.read().key(), "p1");
        assert_eq!(c2_0.read().id(), 0);

        let err = p1.chunk("table1", 100).unwrap_err();
        assert_eq!(err.to_string(), "unknown chunk: p1:table1:100");

        let err = p1.chunk("table3", 0).unwrap_err();
        assert_eq!(err.to_string(), "unknown table: p1:table3");
    }

    #[test]
    fn chunk_list() {
        let registry = MemRegistry::new();
        let catalog = Catalog::new();

        let p1 = catalog.get_or_create_partition("p1");
        create_open_chunk(&p1, "table1", &registry);
        create_open_chunk(&p1, "table1", &registry);
        create_open_chunk(&p1, "table2", &registry);

        let p2 = catalog.get_or_create_partition("p2");
        create_open_chunk(&p2, "table1", &registry);

        assert_eq!(
            chunk_strings(&catalog),
            vec![
                "Chunk p1:table1:0",
                "Chunk p1:table1:1",
                "Chunk p1:table2:0",
                "Chunk p2:table1:0"
            ]
        );

        assert_eq!(
            partition_chunk_strings(&catalog, "p1"),
            vec![
                "Chunk p1:table1:0",
                "Chunk p1:table1:1",
                "Chunk p1:table2:0"
            ]
        );
        assert_eq!(
            partition_chunk_strings(&catalog, "p2"),
            vec!["Chunk p2:table1:0"]
        );
    }

    fn chunk_strings(catalog: &Catalog) -> Vec<String> {
        let mut chunks: Vec<String> = catalog
            .partitions()
            .flat_map(|p| {
                let p = p.read();
                p.chunks()
                    .map(|c| {
                        let c = c.read();
                        format!("Chunk {}:{}:{}", c.key(), c.table_name(), c.id())
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
            })
            .collect();

        chunks.sort_unstable();
        chunks
    }

    fn partition_chunk_strings(catalog: &Catalog, partition_key: &str) -> Vec<String> {
        let p = catalog.partition(partition_key).unwrap();
        let p = p.read();

        let mut chunks: Vec<String> = p
            .chunks()
            .map(|c| {
                let c = c.read();
                format!("Chunk {}:{}:{}", c.key(), c.table_name(), c.id())
            })
            .collect();

        chunks.sort_unstable();
        chunks
    }

    #[test]
    fn chunk_drop() {
        let registry = MemRegistry::new();
        let catalog = Catalog::new();

        let p1 = catalog.get_or_create_partition("p1");
        create_open_chunk(&p1, "table1", &registry);
        create_open_chunk(&p1, "table1", &registry);
        create_open_chunk(&p1, "table2", &registry);

        let p2 = catalog.get_or_create_partition("p2");
        create_open_chunk(&p2, "table1", &registry);

        assert_eq!(chunk_strings(&catalog).len(), 4);

        {
            let mut p1 = p1.write();
            p1.drop_chunk("table2", 0).unwrap();
            p1.chunk("table2", 0).unwrap_err(); // chunk is gone
        }
        assert_eq!(chunk_strings(&catalog).len(), 3);

        {
            let mut p1 = p1.write();
            p1.drop_chunk("table1", 1).unwrap();
            p1.chunk("table1", 1).unwrap_err(); // chunk is gone
        }
        assert_eq!(chunk_strings(&catalog).len(), 2);

        {
            let mut p2 = p1.write();
            p2.drop_chunk("table1", 0).unwrap();
            p2.chunk("table1", 0).unwrap_err(); // chunk is gone
        }
        assert_eq!(chunk_strings(&catalog).len(), 1);
    }

    #[test]
    fn chunk_drop_non_existent_chunk() {
        let registry = MemRegistry::new();
        let catalog = Catalog::new();
        let p3 = catalog.get_or_create_partition("p3");
        create_open_chunk(&p3, "table1", &registry);

        let mut p3 = p3.write();

        let err = p3.drop_chunk("table2", 0).unwrap_err();
        assert_eq!(err.to_string(), "unknown table: p3:table2");

        let err = p3.drop_chunk("table1", 1).unwrap_err();
        assert_eq!(err.to_string(), "unknown chunk: p3:table1:1");
    }

    #[test]
    fn chunk_recreate_dropped() {
        let registry = MemRegistry::new();
        let catalog = Catalog::new();

        let p1 = catalog.get_or_create_partition("p1");
        create_open_chunk(&p1, "table1", &registry);
        create_open_chunk(&p1, "table1", &registry);
        assert_eq!(
            chunk_strings(&catalog),
            vec!["Chunk p1:table1:0", "Chunk p1:table1:1"]
        );

        {
            let mut p1 = p1.write();
            p1.drop_chunk("table1", 0).unwrap();
        }
        assert_eq!(chunk_strings(&catalog), vec!["Chunk p1:table1:1"]);

        // should be ok to "re-create", it gets another chunk_id though
        create_open_chunk(&p1, "table1", &registry);
        assert_eq!(
            chunk_strings(&catalog),
            vec!["Chunk p1:table1:1", "Chunk p1:table1:2"]
        );
    }

    #[test]
    fn filtered_chunks() {
        let registry = MemRegistry::new();
        let catalog = Catalog::new();

        let p1 = catalog.get_or_create_partition("p1");
        let p2 = catalog.get_or_create_partition("p2");
        create_open_chunk(&p1, "table1", &registry);
        create_open_chunk(&p1, "table2", &registry);
        create_open_chunk(&p2, "table2", &registry);

        let a = catalog.filtered_chunks(&Predicate::default(), |_| ());

        let b = catalog.filtered_chunks(&PredicateBuilder::new().table("table1").build(), |_| ());

        let c = catalog.filtered_chunks(&PredicateBuilder::new().table("table2").build(), |_| ());

        let d = catalog.filtered_chunks(
            &PredicateBuilder::new()
                .table("table2")
                .partition_key("p2")
                .build(),
            |_| (),
        );

        assert_eq!(a.len(), 3);
        assert_eq!(b.len(), 1);
        assert_eq!(c.len(), 2);
        assert_eq!(d.len(), 1);
    }
}
