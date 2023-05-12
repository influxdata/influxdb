//! A mock implementation of [`NamespaceResolver`].

#![allow(missing_docs)]

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use async_trait::async_trait;
use data_types::{NamespaceId, NamespaceName, NamespaceSchema};
use parking_lot::Mutex;

use super::NamespaceResolver;

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
        let empty_namespace_schema = Arc::new(new_empty_namespace_schema(id));
        assert!(self
            .map
            .lock()
            .insert(name, empty_namespace_schema)
            .is_none());
        self
    }
}

// Start a new `NamespaceSchema` with only the given ID; the rest of the fields are arbitrary.
fn new_empty_namespace_schema(id: NamespaceId) -> NamespaceSchema {
    NamespaceSchema {
        id,
        tables: BTreeMap::new(),
        max_columns_per_table: 500,
        max_tables: 200,
        retention_period_ns: None,
        partition_template: Default::default(),
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
