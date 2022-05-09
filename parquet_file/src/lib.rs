#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

pub mod chunk;
pub mod metadata;
pub mod storage;

use data_types::{NamespaceId, PartitionId, SequencerId, TableId};
use object_store::{
    path::{parsed::DirsAndFileName, ObjectStorePath, Path},
    DynObjectStore,
};
use uuid::Uuid;

/// Location of a Parquet file within a database's object store.
/// The exact format is an implementation detail and is subject to change.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct ParquetFilePath {
    namespace_id: NamespaceId,
    table_id: TableId,
    sequencer_id: SequencerId,
    partition_id: PartitionId,
    object_store_id: Uuid,
}

impl ParquetFilePath {
    /// Create parquet file path relevant for the NG storage layout.
    pub fn new(
        namespace_id: NamespaceId,
        table_id: TableId,
        sequencer_id: SequencerId,
        partition_id: PartitionId,
        object_store_id: Uuid,
    ) -> Self {
        Self {
            namespace_id,
            table_id,
            sequencer_id,
            partition_id,
            object_store_id,
        }
    }

    /// Get absolute storage location.
    fn absolute_dirs_and_file_name(&self) -> DirsAndFileName {
        let Self {
            namespace_id,
            table_id,
            sequencer_id,
            partition_id,
            object_store_id,
        } = self;

        let mut result = DirsAndFileName::default();
        result.push_all_dirs(&[
            namespace_id.to_string().as_str(),
            table_id.to_string().as_str(),
            sequencer_id.to_string().as_str(),
            partition_id.to_string().as_str(),
        ]);
        result.set_file_name(format!("{}.parquet", object_store_id));
        result
    }

    /// Get object-store specific absolute path.
    pub fn object_store_path(&self, object_store: &DynObjectStore) -> Path {
        object_store.path_from_dirs_and_filename(self.absolute_dirs_and_file_name())
    }
}

impl From<&Self> for ParquetFilePath {
    fn from(borrowed: &Self) -> Self {
        *borrowed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parquet_file_absolute_dirs_and_file_path() {
        let pfp = ParquetFilePath::new(
            NamespaceId::new(1),
            TableId::new(2),
            SequencerId::new(3),
            PartitionId::new(4),
            Uuid::nil(),
        );
        let dirs_and_file_name = pfp.absolute_dirs_and_file_name();
        assert_eq!(
            dirs_and_file_name.to_string(),
            "1/2/3/4/00000000-0000-0000-0000-000000000000.parquet".to_string(),
        );
    }
}
