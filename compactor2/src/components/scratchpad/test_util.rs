use std::{collections::HashSet, sync::Arc};

use data_types::{NamespaceId, PartitionId, ShardId, TableId};
use object_store::{memory::InMemory, DynObjectStore};
use parquet_file::ParquetFilePath;
use uuid::Uuid;

use crate::test_util::list_object_store;

pub fn stores() -> (
    Arc<DynObjectStore>,
    Arc<DynObjectStore>,
    Arc<DynObjectStore>,
) {
    (
        Arc::new(InMemory::new()),
        Arc::new(InMemory::new()),
        Arc::new(InMemory::new()),
    )
}

pub fn file_path(i: u128) -> ParquetFilePath {
    ParquetFilePath::new(
        NamespaceId::new(1),
        TableId::new(1),
        ShardId::new(1),
        PartitionId::new(1),
        Uuid::from_u128(i),
    )
}

#[track_caller]
pub async fn assert_content<const N: usize>(
    store: &Arc<DynObjectStore>,
    files: [&ParquetFilePath; N],
) {
    let expected = files
        .iter()
        .map(|f| f.object_store_path())
        .collect::<HashSet<_>>();
    assert_eq!(expected.len(), N, "duplicate files in expected clause");

    let actual = list_object_store(store).await;
    assert_eq!(actual, expected);
}
