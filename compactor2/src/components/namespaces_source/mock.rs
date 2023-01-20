use std::{collections::HashMap, fmt::Display};

use async_trait::async_trait;
use data_types::{NamespaceId, NamespaceSchema};

use super::NamespacesSource;

#[derive(Debug)]
pub struct MockNamespacesSource {
    namespaces: HashMap<NamespaceId, NamespaceSchema>,
}

impl MockNamespacesSource {
    #[allow(dead_code)] // not used anywhere
    pub fn new(namespaces: HashMap<NamespaceId, NamespaceSchema>) -> Self {
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
    async fn fetch(&self, ns: NamespaceId) -> Option<NamespaceSchema> {
        self.namespaces.get(&ns).cloned()
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
    async fn test_fetch() {
        let ns_1 = NamespaceBuilder::new(1).build();
        let ns_2 = NamespaceBuilder::new(2).build();

        let namespaces = HashMap::from([
            (NamespaceId::new(1), ns_1.clone()),
            (NamespaceId::new(2), ns_2.clone()),
        ]);
        let source = MockNamespacesSource::new(namespaces);

        // different tables
        assert_eq!(source.fetch(NamespaceId::new(1)).await, Some(ns_1.clone()),);
        assert_eq!(source.fetch(NamespaceId::new(2)).await, Some(ns_2),);

        // fetching does not drain
        assert_eq!(source.fetch(NamespaceId::new(1)).await, Some(ns_1),);

        // unknown namespace => None result
        assert_eq!(source.fetch(NamespaceId::new(3)).await, None,);
    }
}
