use mutable_batch::PartitionWrite;

/// A [`WriteFilter`] provides the ability to mask rows from a [`PartitionWrite`]
/// before they are written to a partition.
///
/// This important for replay where it needs to apply per-partition filtering
/// despite [`mutable_batch::payload::DbWrite`] not having a notion of partitioning
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
