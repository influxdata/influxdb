use std::{collections::BTreeSet, sync::Arc};

use arrow::record_batch::RecordBatch;
use parking_lot::Mutex;
use snafu::{ResultExt, Snafu};

use data_types::partition_metadata::TableSummary;
use entry::TableBatch;
use internal_types::selection::Selection;
use metrics::GaugeValue;

use crate::chunk::snapshot::ChunkSnapshot;
use crate::table::Table;

pub mod snapshot;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error writing table '{}': {}", table_name, source))]
    TableWrite {
        table_name: String,
        source: crate::table::Error,
    },

    #[snafu(display("Table Error in '{}': {}", table_name, source))]
    NamedTableError {
        table_name: String,
        source: crate::table::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct ChunkMetrics {
    /// keep track of memory used by chunk
    memory_bytes: GaugeValue,
}

impl ChunkMetrics {
    /// Creates an instance of ChunkMetrics that isn't registered with a central
    /// metrics registry. Observations made to instruments on this ChunkMetrics instance
    /// will therefore not be visible to other ChunkMetrics instances or metric instruments
    /// created on a metrics domain, and vice versa
    pub fn new_unregistered() -> Self {
        Self {
            memory_bytes: GaugeValue::new_unregistered(),
        }
    }

    pub fn new(_metrics: &metrics::Domain, memory_bytes: GaugeValue) -> Self {
        Self { memory_bytes }
    }
}

/// Represents a Chunk of data (a horizontal subset of a table) in
/// the mutable store.
#[derive(Debug)]
pub struct Chunk {
    /// The name of this table
    table_name: Arc<str>,

    /// The table stored in this chunk
    table: Table,

    /// Metrics tracked by this chunk
    metrics: ChunkMetrics,

    /// Cached chunk snapshot
    ///
    /// Note: This is a mutex to allow mutation within
    /// `Chunk::snapshot()` which only takes an immutable borrow
    snapshot: Mutex<Option<Arc<ChunkSnapshot>>>,
}

impl Chunk {
    pub fn new(table_name: impl AsRef<str>, metrics: ChunkMetrics) -> Self {
        let table_name = Arc::from(table_name.as_ref());
        let table = Table::new(Arc::clone(&table_name));

        let mut chunk = Self {
            table_name,
            table,
            metrics,
            snapshot: Mutex::new(None),
        };
        chunk.metrics.memory_bytes.set(chunk.size());
        chunk
    }

    /// Write the contents of a [`TableBatch`] into this Chunk.
    ///
    /// Panics if the batch specifies a different name for the table in this Chunk
    pub fn write_table_batch(
        &mut self,
        sequencer_id: u32,
        sequence_number: u64,
        batch: TableBatch<'_>,
    ) -> Result<()> {
        let table_name = batch.name();
        assert_eq!(
            table_name,
            self.table_name.as_ref(),
            "can only insert table batch for a single table to chunk"
        );

        let columns = batch.columns();
        self.table
            .write_columns(sequencer_id, sequence_number, columns)
            .context(TableWrite { table_name })?;

        // Invalidate chunk snapshot
        *self
            .snapshot
            .try_lock()
            .expect("concurrent readers/writers to MBChunk") = None;

        self.metrics.memory_bytes.set(self.size());

        Ok(())
    }

    /// Returns a queryable snapshot of this chunk
    #[cfg(not(feature = "nocache"))]
    pub fn snapshot(&self) -> Arc<ChunkSnapshot> {
        let mut guard = self.snapshot.lock();
        if let Some(snapshot) = &*guard {
            return Arc::clone(snapshot);
        }

        let snapshot = Arc::new(ChunkSnapshot::new(
            self,
            self.metrics.memory_bytes.clone_empty(),
        ));
        *guard = Some(Arc::clone(&snapshot));
        snapshot
    }

    /// Returns a queryable snapshot of this chunk
    #[cfg(feature = "nocache")]
    pub fn snapshot(&self) -> Arc<ChunkSnapshot> {
        Arc::new(ChunkSnapshot::new(
            self,
            self.metrics.memory_bytes.clone_empty(),
        ))
    }

    /// Return the name of the table in this chunk
    pub fn table_name(&self) -> &Arc<str> {
        &self.table_name
    }

    /// Convert the table specified in this chunk into some number of
    /// record batches, appended to dst
    pub fn table_to_arrow(
        &self,
        dst: &mut Vec<RecordBatch>,
        selection: Selection<'_>,
    ) -> Result<()> {
        dst.push(self.table.to_arrow(selection).context(NamedTableError {
            table_name: self.table_name.as_ref(),
        })?);
        Ok(())
    }

    /// Returns a vec of the summary statistics of the tables in this chunk
    pub fn table_summary(&self) -> TableSummary {
        TableSummary {
            name: self.table_name.to_string(),
            columns: self.table.stats(),
        }
    }

    /// Return the approximate memory size of the chunk, in bytes including the
    /// dictionary, tables, and their rows.
    ///
    /// Note: This does not include the size of any cached ChunkSnapshot
    pub fn size(&self) -> usize {
        // TODO: Better accounting of non-column data (#1565)
        self.table.size() + self.table_name.len()
    }

    /// Returns an iterator over (column_name, estimated_size) for all
    /// columns in this chunk.
    pub fn column_sizes(&self) -> impl Iterator<Item = (&str, usize)> + '_ {
        self.table.column_sizes()
    }

    /// Return the number of rows in this chunk
    pub fn rows(&self) -> usize {
        self.table.row_count()
    }
}

pub mod test_helpers {
    use entry::test_helpers::lp_to_entry;

    use super::*;

    /// A helper that will write line protocol string to the passed in Chunk.
    /// All data will be under a single partition with a clock value and
    /// server id of 1.
    pub fn write_lp_to_chunk(lp: &str, chunk: &mut Chunk) -> Result<()> {
        let entry = lp_to_entry(lp);

        for w in entry.partition_writes().unwrap() {
            let table_batches = w.table_batches();
            // ensure they are all to the same table
            let table_names: BTreeSet<String> =
                table_batches.iter().map(|b| b.name().to_string()).collect();

            assert!(
                table_names.len() <= 1,
                "Can only write 0 or one tables to chunk. Found {:?}",
                table_names
            );

            for batch in table_batches {
                chunk.write_table_batch(1, 5, batch)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use arrow_util::assert_batches_eq;
    use data_types::partition_metadata::{ColumnSummary, InfluxDbType, StatValues, Statistics};

    use super::test_helpers::write_lp_to_chunk;
    use super::*;

    #[test]
    fn writes_table_batches() {
        let mut chunk = Chunk::new("cpu", ChunkMetrics::new_unregistered());

        let lp = vec!["cpu,host=a val=23 1", "cpu,host=b val=2 1"].join("\n");

        write_lp_to_chunk(&lp, &mut chunk).unwrap();

        assert_batches_eq!(
            vec![
                "+------+-------------------------------+-----+",
                "| host | time                          | val |",
                "+------+-------------------------------+-----+",
                "| a    | 1970-01-01 00:00:00.000000001 | 23  |",
                "| b    | 1970-01-01 00:00:00.000000001 | 2   |",
                "+------+-------------------------------+-----+",
            ],
            &chunk_to_batches(&chunk)
        );
    }

    #[test]
    fn writes_table_3_batches() {
        let mut chunk = Chunk::new("cpu", ChunkMetrics::new_unregistered());

        let lp = vec!["cpu,host=a val=23 1", "cpu,host=b val=2 1"].join("\n");

        write_lp_to_chunk(&lp, &mut chunk).unwrap();

        let lp = vec!["cpu,host=c val=11 1"].join("\n");

        write_lp_to_chunk(&lp, &mut chunk).unwrap();

        let lp = vec!["cpu,host=a val=14 2"].join("\n");

        write_lp_to_chunk(&lp, &mut chunk).unwrap();

        assert_batches_eq!(
            vec![
                "+------+-------------------------------+-----+",
                "| host | time                          | val |",
                "+------+-------------------------------+-----+",
                "| a    | 1970-01-01 00:00:00.000000001 | 23  |",
                "| b    | 1970-01-01 00:00:00.000000001 | 2   |",
                "| c    | 1970-01-01 00:00:00.000000001 | 11  |",
                "| a    | 1970-01-01 00:00:00.000000002 | 14  |",
                "+------+-------------------------------+-----+",
            ],
            &chunk_to_batches(&chunk)
        );
    }

    #[test]
    fn test_summary() {
        let mut chunk = Chunk::new("cpu", ChunkMetrics::new_unregistered());
        let lp = r#"
            cpu,host=a val=23 1
            cpu,host=b,env=prod val=2 1
            cpu,host=c,env=stage val=11 1
            cpu,host=a,env=prod val=14 2
        "#;
        write_lp_to_chunk(&lp, &mut chunk).unwrap();

        let summary = chunk.table_summary();
        assert_eq!(
            summary,
            TableSummary {
                name: "cpu".to_string(),
                columns: vec![
                    ColumnSummary {
                        name: "env".to_string(),
                        influxdb_type: Some(InfluxDbType::Tag),
                        stats: Statistics::String(StatValues {
                            min: Some("prod".to_string()),
                            max: Some("stage".to_string()),
                            count: 3,
                            distinct_count: Some(NonZeroU64::new(3).unwrap())
                        })
                    },
                    ColumnSummary {
                        name: "host".to_string(),
                        influxdb_type: Some(InfluxDbType::Tag),
                        stats: Statistics::String(StatValues {
                            min: Some("a".to_string()),
                            max: Some("c".to_string()),
                            count: 4,
                            distinct_count: Some(NonZeroU64::new(3).unwrap())
                        })
                    },
                    ColumnSummary {
                        name: "time".to_string(),
                        influxdb_type: Some(InfluxDbType::Timestamp),
                        stats: Statistics::I64(StatValues {
                            min: Some(1),
                            max: Some(2),
                            count: 4,
                            distinct_count: None
                        })
                    },
                    ColumnSummary {
                        name: "val".to_string(),
                        influxdb_type: Some(InfluxDbType::Field),
                        stats: Statistics::F64(StatValues {
                            min: Some(2.),
                            max: Some(23.),
                            count: 4,
                            distinct_count: None
                        })
                    },
                ]
            }
        )
    }

    #[test]
    #[cfg(not(feature = "nocache"))]
    fn test_snapshot() {
        let mut chunk = Chunk::new("cpu", ChunkMetrics::new_unregistered());

        let lp = vec!["cpu,host=a val=23 1", "cpu,host=b val=2 1"].join("\n");

        write_lp_to_chunk(&lp, &mut chunk).unwrap();
        let s1 = chunk.snapshot();
        let s2 = chunk.snapshot();

        write_lp_to_chunk(&lp, &mut chunk).unwrap();
        let s3 = chunk.snapshot();
        let s4 = chunk.snapshot();

        assert_eq!(Arc::as_ptr(&s1), Arc::as_ptr(&s2));
        assert_ne!(Arc::as_ptr(&s1), Arc::as_ptr(&s3));
        assert_eq!(Arc::as_ptr(&s3), Arc::as_ptr(&s4));
    }

    fn chunk_to_batches(chunk: &Chunk) -> Vec<RecordBatch> {
        let mut batches = vec![];
        chunk.table_to_arrow(&mut batches, Selection::All).unwrap();
        batches
    }
}
