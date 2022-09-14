//! Handle for parquet files using within the compactor.

use data_types::{
    ColumnSet, CompactionLevel, NamespaceId, ParquetFile, ParquetFileId, PartitionId,
    SequenceNumber, ShardId, TableId, Timestamp,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactorParquetFile {
    inner: ParquetFile,
    estimated_arrow_bytes: u64,
    size_override: Option<i64>,
}

impl CompactorParquetFile {
    pub fn new(inner: ParquetFile, estimated_arrow_bytes: u64) -> Self {
        Self {
            inner,
            estimated_arrow_bytes,
            size_override: None,
        }
    }

    pub(crate) fn new_with_size_override(
        inner: ParquetFile,
        estimated_arrow_bytes: u64,
        size: i64,
    ) -> Self {
        let mut this = Self::new(inner, estimated_arrow_bytes);
        this.size_override = Some(size);
        this
    }

    #[cfg(test)]
    pub(crate) fn with_size_override(f: ParquetFile, size: i64) -> Self {
        let mut this = Self::new(f, 0);
        this.size_override = Some(size);
        this
    }

    pub fn id(&self) -> ParquetFileId {
        self.inner.id
    }

    pub fn file_size_bytes(&self) -> i64 {
        self.size_override.unwrap_or(self.inner.file_size_bytes)
    }

    pub fn estimated_arrow_bytes(&self) -> u64 {
        self.estimated_arrow_bytes
    }

    pub fn compaction_level(&self) -> CompactionLevel {
        self.inner.compaction_level
    }

    pub fn column_set(&self) -> &ColumnSet {
        &self.inner.column_set
    }

    pub fn max_sequence_number(&self) -> SequenceNumber {
        self.inner.max_sequence_number
    }

    pub fn partition_id(&self) -> PartitionId {
        self.inner.partition_id
    }

    pub fn row_count(&self) -> i64 {
        self.inner.row_count
    }

    pub fn min_time(&self) -> Timestamp {
        self.inner.min_time
    }

    pub fn max_time(&self) -> Timestamp {
        self.inner.max_time
    }

    pub fn shard_id(&self) -> ShardId {
        self.inner.shard_id
    }

    pub fn namespace_id(&self) -> NamespaceId {
        self.inner.namespace_id
    }

    pub fn table_id(&self) -> TableId {
        self.inner.table_id
    }
}

impl From<CompactorParquetFile> for ParquetFile {
    fn from(f: CompactorParquetFile) -> Self {
        f.inner
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use iox_tests::util::TestParquetFile;

    impl From<TestParquetFile> for CompactorParquetFile {
        fn from(tpf: TestParquetFile) -> Self {
            let TestParquetFile {
                parquet_file,
                size_override,
                ..
            } = tpf;

            match size_override {
                Some(size) => Self::with_size_override(parquet_file, size),
                None => Self::new(parquet_file, 0),
            }
        }
    }
}
