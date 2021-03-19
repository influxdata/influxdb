use data_types::database_rules::{PartitionSort, PartitionSortRules};
use generated_types::wal;
use internal_types::data::ReplicatedWrite;

use crate::{chunk::Chunk, partition::Partition};

use std::collections::HashMap;
use std::sync::Arc;

use data_types::database_rules::Order;
use snafu::{ResultExt, Snafu};
use std::sync::RwLock;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error in {}: {}", source_module, source))]
    PassThrough {
        source_module: &'static str,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Error dropping chunk from partition '{}': {}", partition_key, source))]
    DroppingChunk {
        partition_key: String,
        source: crate::partition::Error,
    },

    #[snafu(display("replicated write from writer {} missing payload", writer))]
    MissingPayload { writer: u32 },
}

impl From<crate::table::Error> for Error {
    fn from(e: crate::table::Error) -> Self {
        Self::PassThrough {
            source_module: "Table",
            source: Box::new(e),
        }
    }
}

impl From<crate::chunk::Error> for Error {
    fn from(e: crate::chunk::Error) -> Self {
        Self::PassThrough {
            source_module: "Chunk",
            source: Box::new(e),
        }
    }
}

impl From<crate::partition::Error> for Error {
    fn from(e: crate::partition::Error) -> Self {
        Self::PassThrough {
            source_module: "Partition",
            source: Box::new(e),
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Default)]
/// This implements the mutable buffer. See the module doc comments
/// for more details.
pub struct MutableBufferDb {
    pub name: String,

    /// Maps partition keys to partitions which hold the actual data
    partitions: RwLock<HashMap<String, Arc<RwLock<Partition>>>>,
}

impl MutableBufferDb {
    /// New creates a new in-memory only write buffer database
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }

    /// returns the id of the current open chunk in the specified partition
    pub fn open_chunk_id(&self, partition_key: &str) -> u32 {
        let partition = self.get_partition(partition_key);
        let partition = partition.read().expect("mutex poisoned");

        partition.open_chunk_id()
    }

    /// Directs the writes from batch into the appropriate partitions
    fn write_entries_to_partitions(&self, batch: &wal::WriteBufferBatch<'_>) -> Result<()> {
        if let Some(entries) = batch.entries() {
            for entry in entries {
                let key = entry
                    .partition_key()
                    .expect("partition key should have been inserted");

                let partition = self.get_partition(key);
                let mut partition = partition.write().expect("mutex poisoned");
                partition.write_entry(&entry)?
            }
        }

        Ok(())
    }

    /// Rolls over the active chunk in this partititon.  Returns the
    /// previously open (now closed) Chunk
    pub fn rollover_partition(&self, partition_key: &str) -> Result<Arc<Chunk>> {
        let partition = self.get_partition(partition_key);
        let mut partition = partition.write().expect("mutex poisoned");
        Ok(partition.rollover_chunk())
    }

    /// return the specified chunk from the partition
    /// Returns None if no such chunk exists.
    pub fn get_chunk(&self, partition_key: &str, chunk_id: u32) -> Option<Arc<Chunk>> {
        let partition = self.get_partition(partition_key);
        let partition = partition.read().expect("mutex poisoned");
        partition.get_chunk(chunk_id).ok()
    }

    /// drop the the specified chunk from the partition
    pub fn drop_chunk(&self, partition_key: &str, chunk_id: u32) -> Result<Arc<Chunk>> {
        let partition = self.get_partition(partition_key);
        let mut partition = partition.write().expect("mutex poisoned");
        partition
            .drop_chunk(chunk_id)
            .context(DroppingChunk { partition_key })
    }

    /// drop the specified partition
    pub fn drop_partition(&self, partition_key: &str) -> Option<Arc<RwLock<Partition>>> {
        self.partitions
            .write()
            .expect("mutex poisoned")
            .remove(partition_key)
    }

    /// The approximate size in memory of all data in the mutable buffer, in
    /// bytes
    pub fn size(&self) -> usize {
        let partitions = self
            .partitions
            .read()
            .expect("lock poisoned")
            .values()
            .cloned()
            .collect::<Vec<_>>();

        let mut size = 0;
        for p in partitions {
            size += p.read().expect("lock poisoned").size();
        }

        size
    }

    /// Returns the partitions in the requested sort order
    pub fn partitions_sorted_by(
        &self,
        sort_rules: &PartitionSortRules,
    ) -> Vec<Arc<RwLock<Partition>>> {
        let mut partitions: Vec<_> = {
            let partitions = self.partitions.read().expect("poisoned mutex");
            partitions.values().map(Arc::clone).collect()
        };

        match &sort_rules.sort {
            PartitionSort::CreatedAtTime => {
                partitions.sort_by_cached_key(|p| p.read().expect("mutex poisoned").created_at);
            }
            PartitionSort::LastWriteTime => {
                partitions.sort_by_cached_key(|p| p.read().expect("mutex poisoned").last_write_at);
            }
            PartitionSort::Column(_name, _data_type, _val) => {
                unimplemented!()
            }
        }

        if sort_rules.order == Order::Desc {
            partitions.reverse();
        }

        partitions
    }

    pub async fn store_replicated_write(&self, write: &ReplicatedWrite) -> Result<()> {
        match write.write_buffer_batch() {
            Some(b) => self.write_entries_to_partitions(&b)?,
            None => {
                return MissingPayload {
                    writer: write.to_fb().writer(),
                }
                .fail()
            }
        };

        Ok(())
    }

    /// Return the partition keys for data in this DB
    pub fn partition_keys(&self) -> Result<Vec<String>> {
        let partitions = self.partitions.read().expect("mutex poisoned");
        let keys = partitions.keys().cloned().collect();
        Ok(keys)
    }

    /// Return the list of chunks, in order of id, for the specified
    /// partition_key
    pub fn chunks(&self, partition_key: &str) -> Vec<Arc<Chunk>> {
        let partition = self.get_partition(partition_key);
        let partition = partition.read().expect("mutex poisoned");
        partition.chunks()
    }
}

impl MutableBufferDb {
    /// returns the number of partitions in this database
    pub fn len(&self) -> usize {
        let partitions = self.partitions.read().expect("mutex poisoned");
        partitions.len()
    }

    /// returns true if the database has no partititons
    pub fn is_empty(&self) -> bool {
        let partitions = self.partitions.read().expect("mutex poisoned");
        partitions.is_empty()
    }

    /// Retrieve (or create) the partition for the specified partition key
    fn get_partition(&self, partition_key: &str) -> Arc<RwLock<Partition>> {
        // until we think this code is likely to be a contention hot
        // spot, simply use a write lock even when often a read lock
        // would do.
        let mut partitions = self.partitions.write().expect("mutex poisoned");

        if let Some(partition) = partitions.get(partition_key) {
            Arc::clone(&partition)
        } else {
            let partition = Arc::new(RwLock::new(Partition::new(partition_key)));
            partitions.insert(partition_key.to_string(), Arc::clone(&partition));
            partition
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use data_types::database_rules::{Order, Partitioner};
    use internal_types::{data::lines_to_replicated_write, selection::Selection};

    use arrow_deps::arrow::array::{Array, StringArray};
    use influxdb_line_protocol::{parse_lines, ParsedLine};

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T = (), E = TestError> = std::result::Result<T, E>;

    #[tokio::test]
    async fn missing_tags_are_null() -> Result {
        let db = MutableBufferDb::new("mydb");

        // Note the `region` tag is introduced in the second line, so
        // the values in prior rows for the region column are
        // null. Likewise the `core` tag is introduced in the third
        // line so the prior columns are null
        let lines = vec![
            "cpu,region=west user=23.2 10",
            "cpu, user=10.0 11",
            "cpu,core=one user=10.0 11",
        ];
        let partition_key = "1970-01-01T00";

        write_lines_to_partition(&db, &lines, partition_key).await;

        let chunk = db.get_chunk(partition_key, 0).unwrap();
        let mut batches = Vec::new();
        let selection = Selection::Some(&["region", "core"]);
        chunk
            .table_to_arrow(&mut batches, "cpu", selection)
            .unwrap();
        let columns = batches[0].columns();

        assert_eq!(
            2,
            columns.len(),
            "Got only two columns in partiton: {:#?}",
            columns
        );

        let region_col = columns[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Get region column as a string");

        assert_eq!(region_col.len(), 3);
        assert_eq!(region_col.value(0), "west", "region_col: {:?}", region_col);
        assert!(!region_col.is_null(0), "is_null(0): {:?}", region_col);
        assert!(region_col.is_null(1), "is_null(1): {:?}", region_col);
        assert!(region_col.is_null(2), "is_null(1): {:?}", region_col);

        let host_col = columns[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Get host column as a string");

        assert_eq!(host_col.len(), 3);
        assert!(host_col.is_null(0), "is_null(0): {:?}", host_col);
        assert!(host_col.is_null(1), "is_null(1): {:?}", host_col);
        assert!(!host_col.is_null(2), "is_null(2): {:?}", host_col);
        assert_eq!(host_col.value(2), "one", "host_col: {:?}", host_col);

        Ok(())
    }

    #[tokio::test]
    async fn db_size() {
        let db = MutableBufferDb::new("column_namedb");

        let lp_data = vec![
            "h2o,state=MA,city=Boston temp=70.4 50",
            "h2o,state=MA,city=Boston other_temp=70.4 250",
            "h2o,state=CA,city=Boston other_temp=72.4 350",
            "o2,state=MA,city=Boston temp=53.4,reading=51 50",
        ]
        .join("\n");

        let lines: Vec<_> = parse_lines(&lp_data).map(|l| l.unwrap()).collect();
        write_lp(&db, &lines).await;

        assert_eq!(429, db.size());
    }

    #[tokio::test]
    async fn partitions_sorted_by_times() {
        let db = MutableBufferDb::new("foo");
        write_lines_to_partition(&db, &["cpu val=1 2"], "p1").await;
        write_lines_to_partition(&db, &["mem val=2 1"], "p2").await;
        write_lines_to_partition(&db, &["cpu val=1 2"], "p1").await;
        write_lines_to_partition(&db, &["mem val=2 1"], "p2").await;

        let sort_rules = PartitionSortRules {
            order: Order::Desc,
            sort: PartitionSort::LastWriteTime,
        };
        let partitions = db.partitions_sorted_by(&sort_rules);
        assert_eq!(partitions[0].read().unwrap().key(), "p2");
        assert_eq!(partitions[1].read().unwrap().key(), "p1");

        let sort_rules = PartitionSortRules {
            order: Order::Asc,
            sort: PartitionSort::CreatedAtTime,
        };
        let partitions = db.partitions_sorted_by(&sort_rules);
        assert_eq!(partitions[0].read().unwrap().key(), "p1");
        assert_eq!(partitions[1].read().unwrap().key(), "p2");
    }

    /// write lines into this database
    async fn write_lp(database: &MutableBufferDb, lp: &[ParsedLine<'_>]) {
        write_lp_to_partition(database, lp, "test_partition_key").await
    }

    async fn write_lines_to_partition(
        database: &MutableBufferDb,
        lines: &[&str],
        partition_key: impl Into<String>,
    ) {
        let lines_string = lines.join("\n");
        let lp: Vec<_> = parse_lines(&lines_string).map(|l| l.unwrap()).collect();
        write_lp_to_partition(database, &lp, partition_key).await
    }

    /// Writes lines the the given partition
    async fn write_lp_to_partition(
        database: &MutableBufferDb,
        lines: &[ParsedLine<'_>],
        partition_key: impl Into<String>,
    ) {
        let writer_id = 0;
        let sequence_number = 0;
        let partitioner = TestPartitioner {
            key: partition_key.into(),
        };
        let replicated_write =
            lines_to_replicated_write(writer_id, sequence_number, &lines, &partitioner);

        database
            .store_replicated_write(&replicated_write)
            .await
            .unwrap()
    }

    // Outputs a set partition key for testing. Used for parsing line protocol into
    // ReplicatedWrite and setting an explicit partition key for all writes therein.
    struct TestPartitioner {
        key: String,
    }

    impl Partitioner for TestPartitioner {
        fn partition_key(
            &self,
            _line: &ParsedLine<'_>,
            _default_time: &DateTime<Utc>,
        ) -> data_types::database_rules::Result<String> {
            Ok(self.key.clone())
        }
    }
}
