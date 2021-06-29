//! This module contains the implementation of the InfluxDB IOx Metadata catalog
use std::collections::BTreeSet;
use std::sync::Arc;

use hashbrown::{HashMap, HashSet};

use data_types::chunk_metadata::ChunkSummary;
use data_types::partition_metadata::{PartitionSummary, TableSummary};
use data_types::{
    chunk_metadata::DetailedChunkSummary,
    database_rules::{Order, Sort, SortOrder},
};
use snafu::Snafu;
use tracker::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};

use self::chunk::CatalogChunk;
use self::metrics::CatalogMetrics;
use self::partition::Partition;
use self::table::Table;

pub mod chunk;
mod metrics;
pub mod partition;
pub mod table;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("table '{}' not found", table))]
    TableNotFound { table: String },

    #[snafu(display("partition '{}' not found in table '{}'", partition, table))]
    PartitionNotFound { partition: String, table: String },

    #[snafu(display(
        "chunk: {} not found in partition '{}' and table '{}'",
        chunk_id,
        partition,
        table
    ))]
    ChunkNotFound {
        chunk_id: u32,
        partition: String,
        table: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Specify which tables are to be matched when filtering
/// catalog chunks
#[derive(Debug, Clone, Copy)]
pub enum TableNameFilter<'a> {
    /// Include all tables
    AllTables,
    /// Only include tables that appear in the named set
    NamedTables(&'a BTreeSet<String>),
}

impl<'a> From<Option<&'a BTreeSet<String>>> for TableNameFilter<'a> {
    /// Creates a [`TableNameFilter`] from an [`Option`].
    ///
    /// If the Option is `None`, all table names will be included in
    /// the results.
    ///
    /// If the Option is `Some(set)`, only table names which apear in
    /// `set` will be included in the results.
    ///
    /// Note `Some(empty set)` will not match anything
    fn from(v: Option<&'a BTreeSet<String>>) -> Self {
        match v {
            Some(names) => Self::NamedTables(names),
            None => Self::AllTables,
        }
    }
}

/// InfluxDB IOx Metadata Catalog
///
/// The Catalog stores information such as which chunks exist, what
/// state they are in, and what objects on object store are used, etc.
///
/// The catalog is also responsible for (eventually) persisting this
/// information
#[derive(Debug)]
pub struct Catalog {
    db_name: Arc<str>,

    /// key is table name
    ///
    /// TODO: Remove this unnecessary additional layer of locking
    tables: RwLock<HashMap<Arc<str>, Table>>,

    metrics: CatalogMetrics,

    pub(crate) metrics_registry: Arc<::metrics::MetricRegistry>,
    pub(crate) metric_labels: Vec<::metrics::KeyValue>,
}

impl Catalog {
    #[cfg(test)]
    fn test() -> Self {
        let registry = Arc::new(::metrics::MetricRegistry::new());
        Self::new(
            Arc::from("test"),
            registry.register_domain("catalog"),
            registry,
            vec![],
        )
    }

    pub fn new(
        db_name: Arc<str>,
        metrics_domain: ::metrics::Domain,
        metrics_registry: Arc<::metrics::MetricRegistry>,
        metric_labels: Vec<::metrics::KeyValue>,
    ) -> Self {
        let metrics = CatalogMetrics::new(metrics_domain);

        Self {
            db_name,
            tables: Default::default(),
            metrics,
            metrics_registry,
            metric_labels,
        }
    }

    /// List all partitions in this database
    pub fn partitions(&self) -> Vec<Arc<RwLock<Partition>>> {
        self.tables
            .read()
            .values()
            .flat_map(|table| table.partitions().cloned())
            .collect()
    }

    /// Get a specific table by name, returning `None` if there is no such table
    pub fn table(&self, table_name: impl AsRef<str>) -> Result<MappedRwLockReadGuard<'_, Table>> {
        let table_name = table_name.as_ref();
        RwLockReadGuard::try_map(self.tables.read(), |tables| tables.get(table_name)).map_err(
            |_| Error::TableNotFound {
                table: table_name.to_string(),
            },
        )
    }

    /// Get a specific partition by name, returning an error if it can't be found
    pub fn partition(
        &self,
        table_name: impl AsRef<str>,
        partition_key: impl AsRef<str>,
    ) -> Result<Arc<RwLock<Partition>>> {
        let table_name = table_name.as_ref();
        let partition_key = partition_key.as_ref();

        self.table(table_name)?
            .partition(partition_key)
            .cloned()
            .ok_or_else(|| Error::PartitionNotFound {
                partition: partition_key.to_string(),
                table: table_name.to_string(),
            })
    }

    /// Get a specific chunk returning an error if it can't be found
    pub fn chunk(
        &self,
        table_name: impl AsRef<str>,
        partition_key: impl AsRef<str>,
        chunk_id: u32,
    ) -> Result<Arc<RwLock<CatalogChunk>>> {
        let table_name = table_name.as_ref();
        let partition_key = partition_key.as_ref();

        self.partition(table_name, partition_key)?
            .read()
            .chunk(chunk_id)
            .cloned()
            .ok_or_else(|| Error::ChunkNotFound {
                partition: partition_key.to_string(),
                table: table_name.to_string(),
                chunk_id,
            })
    }

    /// List all partition keys in this database
    pub fn partition_keys(&self) -> HashSet<String> {
        let mut set = HashSet::new();
        let tables = self.tables.read();
        for table in tables.values() {
            for partition in table.partition_keys() {
                set.get_or_insert_with(partition.as_ref(), ToString::to_string);
            }
        }
        set
    }

    /// Gets or creates a new partition in the catalog and returns
    /// a reference to it
    pub fn get_or_create_partition(
        &self,
        table_name: impl AsRef<str>,
        partition_key: impl AsRef<str>,
    ) -> Arc<RwLock<Partition>> {
        let mut tables = self.tables.write();
        let (_, table) = tables
            .raw_entry_mut()
            .from_key(table_name.as_ref())
            .or_insert_with(|| {
                let table_name = Arc::from(table_name.as_ref());
                let table = Table::new(
                    Arc::clone(&self.db_name),
                    Arc::clone(&table_name),
                    self.metrics.new_table_metrics(table_name.as_ref()),
                );

                (table_name, table)
            });

        let partition = table.get_or_create_partition(partition_key);
        Arc::clone(&partition)
    }

    /// Returns a list of summaries for each partition.
    pub fn partition_summaries(&self) -> Vec<PartitionSummary> {
        self.tables
            .read()
            .values()
            .flat_map(|table| table.partition_summaries())
            .collect()
    }

    pub fn chunk_summaries(&self) -> Vec<ChunkSummary> {
        let partition_key = None;
        let table_names = TableNameFilter::AllTables;
        self.filtered_chunks(table_names, partition_key, CatalogChunk::summary)
    }

    pub fn detailed_chunk_summaries(&self) -> Vec<(Arc<TableSummary>, DetailedChunkSummary)> {
        let partition_key = None;
        let table_names = TableNameFilter::AllTables;
        // TODO: Having two summaries with overlapping information seems unfortunate
        self.filtered_chunks(table_names, partition_key, |chunk| {
            (chunk.table_summary(), chunk.detailed_summary())
        })
    }

    /// Returns all chunks within the catalog in an arbitrary order
    pub fn chunks(&self) -> Vec<Arc<RwLock<CatalogChunk>>> {
        let mut chunks = Vec::new();
        let tables = self.tables.read();

        for table in tables.values() {
            for partition in table.partitions() {
                let partition = partition.read();
                chunks.extend(partition.chunks().cloned())
            }
        }
        chunks
    }

    /// Returns the chunks in the requested sort order
    pub fn chunks_sorted_by(&self, sort_rules: &SortOrder) -> Vec<Arc<RwLock<CatalogChunk>>> {
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

    /// Calls `map` with every chunk and returns a collection of the results
    ///
    /// If `partition_key` is Some(partition_key) only returns chunks
    /// from the specified partition.
    ///
    /// `table_names` specifies which tables to include
    pub fn filtered_chunks<F, C>(
        &self,
        table_names: TableNameFilter<'_>,
        partition_key: Option<&str>,
        map: F,
    ) -> Vec<C>
    where
        F: Fn(&CatalogChunk) -> C + Copy,
    {
        let tables = self.tables.read();
        let tables = match table_names {
            TableNameFilter::AllTables => itertools::Either::Left(tables.values()),
            TableNameFilter::NamedTables(named_tables) => itertools::Either::Right(
                named_tables
                    .iter()
                    .flat_map(|table_name| tables.get(table_name.as_str()).into_iter()),
            ),
        };

        let partitions = tables.flat_map(|table| match partition_key {
            Some(partition_key) => {
                itertools::Either::Left(table.partition(partition_key).into_iter())
            }
            None => itertools::Either::Right(table.partitions()),
        });

        let mut chunks = Vec::with_capacity(partitions.size_hint().1.unwrap_or_default());
        for partition in partitions {
            let partition = partition.read();
            chunks.extend(partition.chunks().map(|chunk| {
                let chunk = chunk.read();
                map(&chunk)
            }))
        }
        chunks
    }

    /// Return a list of all table names in the catalog
    pub fn table_names(&self) -> Vec<String> {
        self.tables.read().keys().map(ToString::to_string).collect()
    }

    pub fn metrics(&self) -> &CatalogMetrics {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use entry::{test_helpers::lp_to_entry, Sequence};

    use super::*;

    fn create_open_chunk(partition: &Arc<RwLock<Partition>>) {
        let mut partition = partition.write();
        let table = partition.table_name();
        let entry = lp_to_entry(&format!("{} bar=1 10", table));
        let write = entry.partition_writes().unwrap().remove(0);
        let batch = write.table_batches().remove(0);

        let mut mb_chunk = mutable_buffer::chunk::MBChunk::new(
            batch.name(),
            mutable_buffer::chunk::ChunkMetrics::new_unregistered(),
        );

        let sequence = Some(Sequence::new(1, 5));
        mb_chunk
            .write_table_batch(sequence.as_ref(), batch)
            .unwrap();

        partition.create_open_chunk(mb_chunk);
    }

    #[test]
    fn partition_get() {
        let catalog = Catalog::test();
        catalog.get_or_create_partition("foo", "p1");
        catalog.get_or_create_partition("foo", "p2");

        let p1 = catalog.partition("foo", "p1").unwrap();
        assert_eq!(p1.read().key(), "p1");

        let p2 = catalog.partition("foo", "p2").unwrap();
        assert_eq!(p2.read().key(), "p2");

        let err = catalog.partition("foo", "p3").unwrap_err();
        assert_eq!(err.to_string(), "partition 'p3' not found in table 'foo'");
    }

    #[test]
    fn partition_list() {
        let catalog = Catalog::test();

        assert_eq!(catalog.partitions().len(), 0);

        catalog.get_or_create_partition("t1", "p1");
        catalog.get_or_create_partition("t2", "p2");
        catalog.get_or_create_partition("t1", "p3");

        let mut partition_keys: Vec<String> = catalog
            .partitions()
            .into_iter()
            .map(|p| p.read().key().into())
            .collect();
        partition_keys.sort_unstable();

        assert_eq!(partition_keys, vec!["p1", "p2", "p3"]);
    }

    #[test]
    fn chunk_create() {
        let catalog = Catalog::test();
        let p1 = catalog.get_or_create_partition("t1", "p1");
        let p2 = catalog.get_or_create_partition("t2", "p2");

        create_open_chunk(&p1);
        create_open_chunk(&p1);
        create_open_chunk(&p2);

        let p1 = p1.write();
        let p2 = p2.write();

        let c1_0 = p1.chunk(0).unwrap();
        assert_eq!(c1_0.read().table_name().as_ref(), "t1");
        assert_eq!(c1_0.read().key(), "p1");
        assert_eq!(c1_0.read().id(), 0);

        let c1_1 = p1.chunk(1).unwrap();
        assert_eq!(c1_1.read().table_name().as_ref(), "t1");
        assert_eq!(c1_1.read().key(), "p1");
        assert_eq!(c1_1.read().id(), 1);

        let c2_0 = p2.chunk(0).unwrap();
        assert_eq!(c2_0.read().table_name().as_ref(), "t2");
        assert_eq!(c2_0.read().key(), "p2");
        assert_eq!(c2_0.read().id(), 0);

        assert!(p1.chunk(100).is_none());
    }

    #[test]
    fn chunk_list() {
        let catalog = Catalog::test();

        let p1 = catalog.get_or_create_partition("table1", "p1");
        let p2 = catalog.get_or_create_partition("table2", "p1");
        create_open_chunk(&p1);
        create_open_chunk(&p1);
        create_open_chunk(&p2);

        let p3 = catalog.get_or_create_partition("table1", "p2");
        create_open_chunk(&p3);

        assert_eq!(
            chunk_strings(&catalog),
            vec![
                "Chunk p1:table1:0",
                "Chunk p1:table1:1",
                "Chunk p1:table2:0",
                "Chunk p2:table1:0"
            ]
        );
    }

    fn chunk_strings(catalog: &Catalog) -> Vec<String> {
        let mut chunks: Vec<String> = catalog
            .partitions()
            .into_iter()
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

    #[test]
    fn chunk_drop() {
        let catalog = Catalog::test();

        let p1 = catalog.get_or_create_partition("p1", "table1");
        let p2 = catalog.get_or_create_partition("p1", "table2");
        create_open_chunk(&p1);
        create_open_chunk(&p1);
        create_open_chunk(&p2);

        let p3 = catalog.get_or_create_partition("p2", "table1");
        create_open_chunk(&p3);

        assert_eq!(chunk_strings(&catalog).len(), 4);

        {
            let mut p2 = p2.write();
            p2.drop_chunk(0).unwrap();
            assert!(p2.chunk(0).is_none()); // chunk is gone
        }
        assert_eq!(chunk_strings(&catalog).len(), 3);

        {
            let mut p1 = p1.write();
            p1.drop_chunk(1).unwrap();
            assert!(p1.chunk(1).is_none()); // chunk is gone
        }
        assert_eq!(chunk_strings(&catalog).len(), 2);

        {
            let mut p1 = p1.write();
            p1.drop_chunk(0).unwrap();
            assert!(p1.chunk(0).is_none()); // chunk is gone
        }
        assert_eq!(chunk_strings(&catalog).len(), 1);
    }

    #[test]
    fn chunk_drop_non_existent_chunk() {
        let catalog = Catalog::test();
        let p3 = catalog.get_or_create_partition("table1", "p3");
        create_open_chunk(&p3);

        let mut p3 = p3.write();
        let err = p3.drop_chunk(2).unwrap_err();

        assert!(matches!(err, partition::Error::ChunkNotFound { .. }))
    }

    #[test]
    fn chunk_recreate_dropped() {
        let catalog = Catalog::test();

        let p1 = catalog.get_or_create_partition("table1", "p1");
        create_open_chunk(&p1);
        create_open_chunk(&p1);
        assert_eq!(
            chunk_strings(&catalog),
            vec!["Chunk p1:table1:0", "Chunk p1:table1:1"]
        );

        {
            let mut p1 = p1.write();
            p1.drop_chunk(0).unwrap();
        }
        assert_eq!(chunk_strings(&catalog), vec!["Chunk p1:table1:1"]);

        // should be ok to "re-create", it gets another chunk_id though
        create_open_chunk(&p1);
        assert_eq!(
            chunk_strings(&catalog),
            vec!["Chunk p1:table1:1", "Chunk p1:table1:2"]
        );
    }

    #[test]
    fn filtered_chunks() {
        use TableNameFilter::*;
        let catalog = Catalog::test();

        let p1 = catalog.get_or_create_partition("table1", "p1");
        let p2 = catalog.get_or_create_partition("table2", "p1");
        let p3 = catalog.get_or_create_partition("table2", "p2");
        create_open_chunk(&p1);
        create_open_chunk(&p2);
        create_open_chunk(&p3);

        let a = catalog.filtered_chunks(AllTables, None, |_| ());

        let b = catalog.filtered_chunks(NamedTables(&make_set("table1")), None, |_| ());

        let c = catalog.filtered_chunks(NamedTables(&make_set("table2")), None, |_| ());

        let d = catalog.filtered_chunks(NamedTables(&make_set("table2")), Some("p2"), |_| ());

        assert_eq!(a.len(), 3);
        assert_eq!(b.len(), 1);
        assert_eq!(c.len(), 2);
        assert_eq!(d.len(), 1);
    }

    fn make_set(s: impl Into<String>) -> BTreeSet<String> {
        std::iter::once(s.into()).collect()
    }
}
