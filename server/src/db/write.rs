use entry::SequencedEntry;
use hashbrown::HashMap;
use mutable_batch::{MutableBatch, PartitionWrite};
use mutable_batch_entry::{entry_to_batches, Result};
use write_buffer::meta::WriteMeta;

/// A collection of writes to potentially multiple tables within the same database
#[derive(Debug)]
pub struct DbWrite {
    /// Writes to individual tables keyed by table name
    tables: HashMap<String, MutableBatch>,
    /// Write metadata
    meta: WriteMeta,
}

impl DbWrite {
    /// Create a new [`DbWrite`] from a [`SequencedEntry`]
    pub fn from_entry(entry: &SequencedEntry) -> Result<Self> {
        let meta = WriteMeta::new(
            entry.sequence().cloned(),
            entry.producer_wallclock_timestamp(),
            entry.span_context().cloned(),
        );

        let tables = entry_to_batches(entry.entry())?;

        Ok(Self { tables, meta })
    }

    /// Metadata associated with this write
    pub fn meta(&self) -> &WriteMeta {
        &self.meta
    }

    /// Returns an iterator over the per-table writes within this [`DbWrite`]
    /// in no particular order
    pub fn tables(&self) -> impl Iterator<Item = (&str, &MutableBatch)> + '_ {
        self.tables.iter().map(|(k, v)| (k.as_str(), v))
    }
}

/// A [`WriteFilter`] provides the ability to mask rows from a [`PartitionWrite`]
/// before they are written to a partition.
///
/// This important for replay where it needs to apply per-partition filtering
/// despite [`DbWrite`] not having a notion of partitioning
pub trait WriteFilter {
    fn filter_write<'a>(
        &self,
        table_name: &str,
        partition_key: &str,
        write: PartitionWrite<'a>,
    ) -> Option<PartitionWrite<'a>>;
}

#[derive(Debug, Default, Copy, Clone)]
pub struct WriteFilterNone {}

impl WriteFilter for WriteFilterNone {
    fn filter_write<'a>(
        &self,
        _table_name: &str,
        _partition_key: &str,
        write: PartitionWrite<'a>,
    ) -> Option<PartitionWrite<'a>> {
        Some(write)
    }
}
