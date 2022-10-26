//! A mock implementation of [`NamespaceResolver`].

#![allow(missing_docs)]

use std::collections::HashMap;

use async_trait::async_trait;
use data_types::{DatabaseName, NamespaceId};
use parking_lot::Mutex;

use super::NamespaceResolver;

#[derive(Debug, Default)]
pub struct MockNamespaceResolver {
    map: Mutex<HashMap<DatabaseName<'static>, NamespaceId>>,
}

impl MockNamespaceResolver {
    pub fn new(map: HashMap<DatabaseName<'static>, NamespaceId>) -> Self {
        Self {
            map: Mutex::new(map),
        }
    }

    pub fn with_mapping(self, name: impl Into<String> + 'static, id: NamespaceId) -> Self {
        let name = DatabaseName::try_from(name.into()).unwrap();
        assert!(self.map.lock().insert(name, id).is_none());
        self
    }
}

#[async_trait]
impl NamespaceResolver for MockNamespaceResolver {
    /// Return the [`NamespaceId`] for the given [`DatabaseName`].
    async fn get_namespace_id(
        &self,
        namespace: &DatabaseName<'static>,
    ) -> Result<NamespaceId, super::Error> {
        Ok(*self
            .map
            .lock()
            .get(namespace)
            .expect("mock namespace resolver does not have ID"))
    }
}
