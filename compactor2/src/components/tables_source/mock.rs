use std::{collections::HashMap, fmt::Display};

use async_trait::async_trait;
use data_types::{Table, TableId};

use super::TablesSource;

#[derive(Debug)]
pub struct MockTablesSource {
    tables: HashMap<TableId, Table>,
}

impl MockTablesSource {
    #[allow(dead_code)] // not used anywhere
    pub fn new(tables: HashMap<TableId, Table>) -> Self {
        Self { tables }
    }
}

impl Display for MockTablesSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mock")
    }
}

#[async_trait]
impl TablesSource for MockTablesSource {
    async fn fetch(&self, table: TableId) -> Option<Table> {
        self.tables.get(&table).cloned()
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::TableBuilder;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            MockTablesSource::new(HashMap::default()).to_string(),
            "mock",
        )
    }

    #[tokio::test]
    async fn test_fetch() {
        let t_1 = TableBuilder::new(1).with_name("table1").build();
        let t_2 = TableBuilder::new(2).with_name("table2").build();

        let tables = HashMap::from([
            (TableId::new(1), t_1.clone()),
            (TableId::new(2), t_2.clone()),
        ]);
        let source = MockTablesSource::new(tables);

        // different tables
        assert_eq!(source.fetch(TableId::new(1)).await, Some(t_1.clone()),);
        assert_eq!(source.fetch(TableId::new(2)).await, Some(t_2),);

        // fetching does not drain
        assert_eq!(source.fetch(TableId::new(1)).await, Some(t_1),);

        // unknown table => None result
        assert_eq!(source.fetch(TableId::new(3)).await, None,);
    }
}
