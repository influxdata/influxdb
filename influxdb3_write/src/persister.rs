//! This is the implementation of the `Persister` used to write data from the buffer to object
//! storage.

use crate::catalog::Catalog;
use crate::catalog::InnerCatalog;
use crate::paths::CatalogFilePath;
use crate::paths::SegmentInfoFilePath;
use crate::{PersistedCatalog, PersistedSegment, Persister, SegmentId};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::StreamExt;
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use std::sync::Arc;

#[derive(Debug)]
pub struct PersisterImpl {
    object_store: Arc<dyn ObjectStore>,
}

impl PersisterImpl {
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self { object_store }
    }
}

#[async_trait]
impl Persister for PersisterImpl {
    async fn load_catalog(&self) -> crate::Result<Option<PersistedCatalog>> {
        let mut list = self
            .object_store
            .list(Some(&CatalogFilePath::dir()))
            .await?;
        let mut catalog_path: Option<ObjPath> = None;
        while let Some(item) = list.next().await {
            let item = item?;
            catalog_path = match catalog_path {
                Some(old_path) => {
                    let new_catalog_name = item
                        .location
                        .filename()
                        .expect("catalog names are utf-8 encoded");
                    let old_catalog_name = old_path
                        .filename()
                        .expect("catalog names are utf-8 encoded");

                    // We order catalogs by number starting with u32::MAX and
                    // then decrease it, therefore if the new catalog file name
                    // is less than the old one this is the path we want
                    if new_catalog_name < old_catalog_name {
                        Some(item.location)
                    } else {
                        Some(old_path)
                    }
                }
                None => Some(item.location),
            };
        }

        match catalog_path {
            None => Ok(None),
            Some(path) => {
                let bytes = self.object_store.get(&path).await?.bytes().await?;
                let catalog: InnerCatalog = serde_json::from_slice(&bytes)?;
                let file_name = path.filename().expect("catalog names are utf-8 encoded");
                let parsed_number = file_name
                    .trim_end_matches(format!(".{}", crate::paths::CATALOG_FILE_EXTENSION).as_str())
                    .parse::<u32>()?;
                let segment_id = SegmentId::new(u32::MAX - parsed_number);
                Ok(Some(PersistedCatalog {
                    segment_id,
                    catalog,
                }))
            }
        }
    }

    async fn load_segments(&self, most_recent_n: usize) -> crate::Result<Vec<PersistedSegment>> {
        let segment_list = self
            .object_store
            .list(Some(&SegmentInfoFilePath::dir()))
            .await?
            .collect::<Vec<_>>()
            .await;

        // Why not collect into a Result<Vec<ObjectMeta>, object_store::Error>>
        // like we could with Iterators? Well because it's a stream it ends up
        // using different traits and can't really do that. So we need to loop
        // through to return any errors that might have occurred, then do an
        // unstable sort (which is faster and we know won't have any
        // duplicates) since these can arrive out of order, and then issue gets
        // on the n most recent segments that we want and is returned in order
        // of the moste recent to least.
        let mut list = Vec::new();
        for segment in segment_list {
            list.push(segment?);
        }

        list.sort_unstable_by(|a, b| a.location.cmp(&b.location));

        let len = list.len();
        let range = if len <= most_recent_n {
            0..len
        } else {
            0..most_recent_n
        };

        let mut output = Vec::new();
        for item in &list[range] {
            let bytes = self.object_store.get(&item.location).await?.bytes().await?;
            output.push(serde_json::from_slice(&bytes)?);
        }

        Ok(output)
    }

    async fn persist_catalog(&self, segment_id: SegmentId, catalog: Catalog) -> crate::Result<()> {
        let catalog_path = CatalogFilePath::new(segment_id);
        let json = serde_json::to_vec_pretty(&catalog.into_inner())?;
        self.object_store
            .put(catalog_path.as_ref(), Bytes::from(json))
            .await?;
        Ok(())
    }

    async fn persist_segment(&self, persisted_segment: PersistedSegment) -> crate::Result<()> {
        let segment_file_path = SegmentInfoFilePath::new(persisted_segment.segment_id);
        let json = serde_json::to_vec_pretty(&persisted_segment)?;
        self.object_store
            .put(segment_file_path.as_ref(), Bytes::from(json))
            .await?;
        Ok(())
    }

    fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }
}

#[cfg(test)]
use {object_store::local::LocalFileSystem, std::collections::HashMap};

#[tokio::test]
async fn persist_catalog() {
    let local_disk = LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
    let persister = PersisterImpl::new(Arc::new(local_disk));
    let catalog = Catalog::new();
    let _ = catalog.db_or_create("my_db");

    persister
        .persist_catalog(SegmentId::new(0), catalog)
        .await
        .unwrap();
}

#[tokio::test]
async fn persist_and_load_newest_catalog() {
    let local_disk = LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
    let persister = PersisterImpl::new(Arc::new(local_disk));
    let catalog = Catalog::new();
    let _ = catalog.db_or_create("my_db");

    persister
        .persist_catalog(SegmentId::new(0), catalog)
        .await
        .unwrap();

    let catalog = Catalog::new();
    let _ = catalog.db_or_create("my_second_db");

    persister
        .persist_catalog(SegmentId::new(1), catalog)
        .await
        .unwrap();

    let catalog = persister
        .load_catalog()
        .await
        .expect("loading the catalog did not cause an error")
        .expect("there was a catalog to load");

    assert_eq!(catalog.segment_id, SegmentId::new(1));
    assert!(catalog.catalog.db_exists("my_second_db"));
    assert!(!catalog.catalog.db_exists("my_db"));
}

#[tokio::test]
async fn persist_segment_info_file() {
    let local_disk = LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
    let persister = PersisterImpl::new(Arc::new(local_disk));
    let info_file = PersistedSegment {
        segment_id: SegmentId::new(0),
        segment_wal_size_bytes: 0,
        databases: HashMap::new(),
        segment_min_time: 0,
        segment_max_time: 1,
        segment_row_count: 0,
        segment_parquet_size_bytes: 0,
    };

    persister.persist_segment(info_file).await.unwrap();
}

#[tokio::test]
async fn persist_and_load_segment_info_files() {
    let local_disk = LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
    let persister = PersisterImpl::new(Arc::new(local_disk));
    let info_file = PersistedSegment {
        segment_id: SegmentId::new(0),
        segment_wal_size_bytes: 0,
        databases: HashMap::new(),
        segment_min_time: 0,
        segment_max_time: 1,
        segment_row_count: 0,
        segment_parquet_size_bytes: 0,
    };
    let info_file_2 = PersistedSegment {
        segment_id: SegmentId::new(1),
        segment_wal_size_bytes: 0,
        databases: HashMap::new(),
        segment_min_time: 0,
        segment_max_time: 1,
        segment_row_count: 0,
        segment_parquet_size_bytes: 0,
    };
    let info_file_3 = PersistedSegment {
        segment_id: SegmentId::new(2),
        segment_wal_size_bytes: 0,
        databases: HashMap::new(),
        segment_min_time: 0,
        segment_max_time: 1,
        segment_row_count: 0,
        segment_parquet_size_bytes: 0,
    };

    persister.persist_segment(info_file).await.unwrap();
    persister.persist_segment(info_file_2).await.unwrap();
    persister.persist_segment(info_file_3).await.unwrap();

    let segments = persister.load_segments(2).await.unwrap();
    assert_eq!(segments.len(), 2);
    // The most recent one is first
    assert_eq!(segments[0].segment_id.0, 2);
    assert_eq!(segments[1].segment_id.0, 1);
}

#[tokio::test]
async fn persist_and_load_segment_info_files_with_fewer_than_requested() {
    let local_disk = LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
    let persister = PersisterImpl::new(Arc::new(local_disk));
    let info_file = PersistedSegment {
        segment_id: SegmentId::new(0),
        segment_wal_size_bytes: 0,
        databases: HashMap::new(),
        segment_min_time: 0,
        segment_max_time: 1,
        segment_row_count: 0,
        segment_parquet_size_bytes: 0,
    };
    persister.persist_segment(info_file).await.unwrap();
    let segments = persister.load_segments(2).await.unwrap();
    // We asked for the most recent 2 but there should only be 1
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].segment_id.0, 0);
}
