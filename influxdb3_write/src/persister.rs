//! This is the implementation of the `Persister` used to write data from the buffer to object
//! storage.

use std::sync::Arc;
use async_trait::async_trait;
use object_store::ObjectStore;
use crate::{PersistedCatalog, PersistedSegment, Persister, SegmentId};
use crate::catalog::Catalog;

#[derive(Debug)]
pub struct PersisterImpl {
    #[allow(dead_code)]
    object_store: Arc<dyn ObjectStore>,
}

impl PersisterImpl {
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            object_store,
        }
    }
}

#[async_trait]
impl Persister for PersisterImpl {
    async fn load_catalog(&self) -> crate::Result<Option<PersistedCatalog>> {
        todo!()
    }

    async fn load_segments(&self, _most_recent_n: usize) -> crate::Result<Vec<PersistedSegment>> {
        todo!()
    }

    async fn persist_catalog(&self, _segment_id: SegmentId, _catalog: Catalog) -> crate::Result<()> {
        todo!()
    }

    async fn persist_segment(&self, _persisted_segment: PersistedSegment) -> crate::Result<()> {
        todo!()
    }

    fn object_store(&self) -> Arc<dyn ObjectStore> {
        todo!()
    }
}