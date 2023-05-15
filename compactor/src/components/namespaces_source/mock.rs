use std::{collections::HashMap, fmt::Display};

use async_trait::async_trait;
use data_types::{Namespace, NamespaceId, NamespaceSchema};

use super::NamespacesSource;

#[derive(Debug, Clone)]
/// contains [`Namespace`] and a [`NamespaceSchema`]
pub struct NamespaceWrapper {
    /// namespace
    pub ns: Namespace,
    /// schema
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
    use std::collections::BTreeMap;

    use data_types::{Column, ColumnId, ColumnType, ColumnsByName, TableId, TableSchema};

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

    #[derive(Debug)]
    /// Build [`NamespaceWrapper`] for testing
    pub struct NamespaceBuilder {
        namespace: NamespaceWrapper,
    }

    impl NamespaceBuilder {
        pub fn new(id: i64) -> Self {
            let tables = BTreeMap::from([
                (
                    "table1".to_string(),
                    TableSchema {
                        id: TableId::new(1),
                        partition_template: Default::default(),
                        columns: ColumnsByName::new([
                            Column {
                                name: "col1".to_string(),
                                id: ColumnId::new(1),
                                column_type: ColumnType::I64,
                                table_id: TableId::new(1),
                            },
                            Column {
                                name: "col2".to_string(),
                                id: ColumnId::new(2),
                                column_type: ColumnType::String,
                                table_id: TableId::new(1),
                            },
                        ]),
                    },
                ),
                (
                    "table2".to_string(),
                    TableSchema {
                        id: TableId::new(2),
                        partition_template: Default::default(),
                        columns: ColumnsByName::new([
                            Column {
                                name: "col1".to_string(),
                                id: ColumnId::new(3),
                                column_type: ColumnType::I64,
                                table_id: TableId::new(2),
                            },
                            Column {
                                name: "col2".to_string(),
                                id: ColumnId::new(4),
                                column_type: ColumnType::String,
                                table_id: TableId::new(2),
                            },
                            Column {
                                name: "col3".to_string(),
                                id: ColumnId::new(5),
                                column_type: ColumnType::F64,
                                table_id: TableId::new(2),
                            },
                        ]),
                    },
                ),
            ]);

            let id = NamespaceId::new(id);
            Self {
                namespace: NamespaceWrapper {
                    ns: Namespace {
                        id,
                        name: "ns".to_string(),
                        max_tables: 10,
                        max_columns_per_table: 10,
                        retention_period_ns: None,
                        deleted_at: None,
                        partition_template: Default::default(),
                    },
                    schema: NamespaceSchema {
                        id,
                        tables,
                        max_columns_per_table: 10,
                        max_tables: 42,
                        retention_period_ns: None,
                        partition_template: Default::default(),
                    },
                },
            }
        }

        pub fn build(self) -> NamespaceWrapper {
            self.namespace
        }
    }
}
