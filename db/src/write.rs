use crate::catalog::chunk::CatalogChunk;
use mutable_batch::PartitionWrite;

/// A [`WriteFilter`] provides the ability to mask rows from a [`PartitionWrite`]
/// before they are written to a partition.
///
/// This important for replay where it needs to apply per-partition filtering
/// despite [`dml::DmlWrite`] not having a notion of partitioning
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

/// A [`DeleteFilter`] provides the ability to exclude chunks from having a delete applied
///
/// This is important for replay where it needs to prevent deletes from being applied to chunks
/// containing writes sequenced after the delete
pub trait DeleteFilter: Copy {
    /// Returns true if the delete should be applied to this chunk
    fn filter_chunk(&self, chunk: &CatalogChunk) -> bool;
}

#[derive(Debug, Default, Copy, Clone)]
pub struct DeleteFilterNone {}

impl DeleteFilter for DeleteFilterNone {
    fn filter_chunk(&self, _chunk: &CatalogChunk) -> bool {
        true
    }
}
