use std::{collections::HashMap, fmt::Display};

use async_trait::async_trait;
use data_types::{Namespace, NamespaceId, NamespaceSchema};

use super::NamespacesSource;

#[derive(Debug, Clone)]
pub struct NamespaceWrapper {
    pub ns: Namespace,
    pub schema: NamespaceSchema,
}

#[derive(Debug)]
pub struct MockNamespacesSource {
    namespaces: HashMap<NamespaceId, NamespaceWrapper>,
}

impl MockNamespacesSource {
    #[allow(dead_code)] // not used anywhere
    pub fn new(namespaces: HashMap<NamespaceId, NamespaceWrapper>) -> Self {
        Self { namespaces }
    }
}

impl Display for MockNamespacesSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mock")
    }
}

#[async_trait]
impl NamespacesSource for MockNamespacesSource {
    async fn fetch_by_id(&self, ns: NamespaceId) -> Option<Namespace> {
        let wrapper = self.namespaces.get(&ns);
        wrapper.map(|wrapper| wrapper.ns.clone())
    }

    async fn fetch_schema_by_id(&self, ns: NamespaceId) -> Option<NamespaceSchema> {
        let wrapper = self.namespaces.get(&ns);
        wrapper.map(|wrapper| wrapper.schema.clone())
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::NamespaceBuilder;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            MockNamespacesSource::new(HashMap::default()).to_string(),
            "mock",
        )
    }

    #[tokio::test]
    async fn test_fetch_namespace() {
        let ns_1 = NamespaceBuilder::new(1).build();
        let ns_2 = NamespaceBuilder::new(2).build();

        let namespaces = HashMap::from([
            (NamespaceId::new(1), ns_1.clone()),
            (NamespaceId::new(2), ns_2.clone()),
        ]);
        let source = MockNamespacesSource::new(namespaces);

        // different tables
        assert_eq!(
            source.fetch_by_id(NamespaceId::new(1)).await,
            Some(ns_1.clone().ns),
        );
        assert_eq!(source.fetch_by_id(NamespaceId::new(2)).await, Some(ns_2.ns),);

        // fetching does not drain
        assert_eq!(source.fetch_by_id(NamespaceId::new(1)).await, Some(ns_1.ns),);

        // unknown namespace => None result
        assert_eq!(source.fetch_by_id(NamespaceId::new(3)).await, None,);
    }

    #[tokio::test]
    async fn test_fetch_namespace_schema() {
        let ns_1 = NamespaceBuilder::new(1).build();
        let ns_2 = NamespaceBuilder::new(2).build();

        let namespaces = HashMap::from([
            (NamespaceId::new(1), ns_1.clone()),
            (NamespaceId::new(2), ns_2.clone()),
        ]);
        let source = MockNamespacesSource::new(namespaces);

        // different tables
        assert_eq!(
            source.fetch_schema_by_id(NamespaceId::new(1)).await,
            Some(ns_1.clone().schema),
        );
        assert_eq!(
            source.fetch_schema_by_id(NamespaceId::new(2)).await,
            Some(ns_2.schema),
        );

        // fetching does not drain
        assert_eq!(
            source.fetch_schema_by_id(NamespaceId::new(1)).await,
            Some(ns_1.schema),
        );

        // unknown namespace => None result
        assert_eq!(source.fetch_schema_by_id(NamespaceId::new(3)).await, None,);
    }
}
