use std::{collections::HashSet, sync::Arc};

use data_types::{NamespaceId, PartitionId, ShardId, TableId};
use futures::TryStreamExt;
use object_store::{memory::InMemory, path::Path, DynObjectStore};
use parquet_file::ParquetFilePath;
use uuid::Uuid;

pub fn stores() -> (Arc<DynObjectStore>, Arc<DynObjectStore>) {
    (Arc::new(InMemory::new()), Arc::new(InMemory::new()))
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

pub async fn get_content(store: &Arc<DynObjectStore>) -> HashSet<Path> {
    store
        .list(None)
        .await
        .unwrap()
        .map_ok(|f| f.location)
        .try_collect::<HashSet<_>>()
        .await
        .unwrap()
}

pub async fn assert_content<const N: usize>(
    store: &Arc<DynObjectStore>,
    files: [&ParquetFilePath; N],
) {
    let expected = files
        .iter()
        .map(|f| f.object_store_path())
        .collect::<HashSet<_>>();
    let actual = get_content(store).await;
    assert_eq!(actual, expected);
}
