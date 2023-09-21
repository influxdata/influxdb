use std::{collections::HashMap, fmt::Display};

use async_trait::async_trait;
use data_types::{Column, TableId};

use super::ColumnsSource;

#[derive(Debug)]
pub struct MockColumnsSource {
    tables: HashMap<TableId, Vec<Column>>,
}

impl MockColumnsSource {
    #[allow(dead_code)] // not used anywhere
    pub fn new(tables: HashMap<TableId, Vec<Column>>) -> Self {
        Self { tables }
    }
}

impl Display for MockColumnsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mock")
    }
}

#[async_trait]
impl ColumnsSource for MockColumnsSource {
    async fn fetch(&self, table: TableId) -> Vec<Column> {
        self.tables.get(&table).cloned().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use data_types::ColumnType;
    use iox_tests::{ColumnBuilder, TableBuilder};

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            MockColumnsSource::new(HashMap::default()).to_string(),
            "mock",
        )
    }

    #[tokio::test]
    async fn test_fetch() {
        // // t_1 has one column and t_2 has no column
        let t1 = TableBuilder::new(1).with_name("table1").build();
        let t1_c1 = ColumnBuilder::new(1, t1.id.get())
            .with_name("time")
            .with_column_type(ColumnType::Time)
            .build();
        let t2 = TableBuilder::new(2).with_name("table2").build();

        let tables = HashMap::from([(t1.id, vec![t1_c1.clone()]), (t2.id, vec![])]);
        let source = MockColumnsSource::new(tables);

        // different tables
        assert_eq!(source.fetch(t1.id).await, vec![t1_c1.clone()],);
        assert_eq!(source.fetch(t2.id).await, vec![]);

        // fetching does not drain
        assert_eq!(source.fetch(t1.id).await, vec![t1_c1],);

        // unknown table => empty result
        assert_eq!(source.fetch(TableId::new(3)).await, vec![]);
    }
}
