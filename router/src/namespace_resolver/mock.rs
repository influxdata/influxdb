//! A mock implementation of [`NamespaceResolver`].

#![allow(missing_docs)]

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use data_types::{NamespaceId, NamespaceName, PartitionTemplate};
use parking_lot::Mutex;

use super::NamespaceResolver;

#[derive(Debug, Default)]
pub struct MockNamespaceResolver {
    map: Mutex<HashMap<NamespaceName<'static>, NamespaceId>>,
}

impl MockNamespaceResolver {
    pub fn new(map: HashMap<NamespaceName<'static>, NamespaceId>) -> Self {
        Self {
            map: Mutex::new(map),
        }
    }

    pub fn with_mapping(self, name: impl Into<String> + 'static, id: NamespaceId) -> Self {
        let name = NamespaceName::try_from(name.into()).unwrap();
        assert!(self.map.lock().insert(name, id).is_none());
        self
    }
}

#[async_trait]
impl NamespaceResolver for MockNamespaceResolver {
    async fn get_namespace_info(
        &self,
        namespace: &NamespaceName<'static>,
    ) -> Result<(NamespaceId, Option<Arc<PartitionTemplate>>), super::Error> {
        Ok((
            *self.map.lock().get(namespace).ok_or(super::Error::Lookup(
                iox_catalog::interface::Error::NamespaceNotFoundByName {
                    name: namespace.to_string(),
                },
            ))?,
            None,
        ))
    }
}
