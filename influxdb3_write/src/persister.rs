//! This is the implementation of the `Persister` used to write data from the buffer to object
//! storage.

use crate::catalog::Catalog;
use crate::catalog::InnerCatalog;
use crate::paths::CatalogFilePath;
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

    async fn load_segments(&self, _most_recent_n: usize) -> crate::Result<Vec<PersistedSegment>> {
        todo!()
    }

    async fn persist_catalog(&self, segment_id: SegmentId, catalog: Catalog) -> crate::Result<()> {
        let catalog_path = CatalogFilePath::new(segment_id);
        let json = serde_json::to_vec_pretty(&catalog.into_inner())?;
        self.object_store
            .put(catalog_path.as_ref(), Bytes::from(json))
            .await?;
        Ok(())
    }

    async fn persist_segment(&self, _persisted_segment: PersistedSegment) -> crate::Result<()> {
        todo!()
    }

    fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }
}

#[cfg(test)]
use object_store::local::LocalFileSystem;

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
