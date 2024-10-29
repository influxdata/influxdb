use std::collections::BTreeMap;
use std::collections::BTreeSet;

use influxdb3_catalog::catalog::TableDefinition;
use influxdb3_id::ColumnId;
use influxdb3_id::DbId;
use influxdb3_id::TableId;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ProConfig {
    pub file_index_columns: BTreeMap<DbId, Index>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Index {
    pub db_columns: Vec<String>,
    pub table_columns: BTreeMap<TableId, Vec<ColumnId>>,
}

impl Index {
    pub fn new() -> Self {
        Self::default()
    }
}

impl super::Config for ProConfig {
    const PATH: &'static str = "/pro/config.json";
}

impl ProConfig {
    /// Get's all of the columns for the compactor to index on and deduplicates them
    /// so that only unique column names are passed in. This allows users to set the
    /// same columns at the DB and the Table level
    pub fn index_columns(&self, db_id: DbId, table_def: &TableDefinition) -> Option<Vec<ColumnId>> {
        let table_id = table_def.table_id;
        self.file_index_columns.get(&db_id).and_then(|db| {
            let mut set: BTreeSet<ColumnId> = BTreeSet::from_iter(
                db.db_columns
                    .clone()
                    .into_iter()
                    .map(|c| table_def.column_name_to_id(c).unwrap()),
            );

            for item in db.table_columns.get(&table_id).cloned().unwrap_or_default() {
                set.insert(item);
            }

            if set.is_empty() {
                None
            } else {
                Some(set.into_iter().collect::<Vec<_>>())
            }
        })
    }
}
