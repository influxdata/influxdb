//! A mock implementation of [`NamespaceResolver`].

#![allow(missing_docs)]

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use data_types::{NamespaceId, NamespaceName, NamespaceSchema};
use parking_lot::Mutex;

use super::NamespaceResolver;

use crate::test_helpers::new_empty_namespace_schema;

#[derive(Debug, Default)]
pub struct MockNamespaceResolver {
    map: Mutex<HashMap<NamespaceName<'static>, Arc<NamespaceSchema>>>,
}

impl MockNamespaceResolver {
    pub fn new(map: HashMap<NamespaceName<'static>, Arc<NamespaceSchema>>) -> Self {
        Self {
            map: Mutex::new(map),
        }
    }

    pub fn with_mapping(self, name: impl Into<String> + 'static, id: NamespaceId) -> Self {
        let name = NamespaceName::try_from(name.into()).unwrap();
        let empty_namespace_schema = Arc::new(new_empty_namespace_schema(id.get()));
        assert!(self
            .map
            .lock()
            .insert(name, empty_namespace_schema)
            .is_none());
        self
    }
}

#[async_trait]
impl NamespaceResolver for MockNamespaceResolver {
    async fn get_namespace_schema(
        &self,
        namespace: &NamespaceName<'static>,
    ) -> Result<Arc<NamespaceSchema>, super::Error> {
        Ok(Arc::clone(self.map.lock().get(namespace).ok_or(
            super::Error::Lookup(iox_catalog::interface::Error::NamespaceNotFoundByName {
                name: namespace.to_string(),
            }),
        )?))
    }
}
