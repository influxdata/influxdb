use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::Context;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_catalog::catalog::InfluxColumnType;
use influxdb3_catalog::catalog::InfluxFieldType;
use influxdb3_catalog::catalog::TableDefinition;
use influxdb3_id::ColumnId;
use influxdb3_id::DbId;
use influxdb3_id::TableId;
use parking_lot::RwLock;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Semaphore;
use tokio::sync::SemaphorePermit;

#[derive(Debug, thiserror::Error)]
#[error("enterprise configuration error: {0}")]
pub struct EnterpriseConfigError(#[from] anyhow::Error);

/// Semaphore for only allowing single access when altering the config
static ENTERPRISE_CONFIG_WRITE_PERMIT: Semaphore = Semaphore::const_new(1);

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct EnterpriseConfig {
    inner: RwLock<EnterpriseConfigInner>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct EnterpriseConfigInner {
    file_index_columns: BTreeMap<DbId, Index>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Index {
    pub db_columns: Vec<Arc<str>>,
    pub table_columns: BTreeMap<TableId, Vec<ColumnId>>,
}

impl Index {
    pub fn new() -> Self {
        Self::default()
    }
}

impl super::Config for EnterpriseConfig {
    const PATH: &'static str = "/enterprise/config.json";
}

impl EnterpriseConfig {
    /// Get's all of the columns for the compactor to index on and deduplicates them
    /// so that only unique column names are passed in. This allows users to set the
    /// same columns at the DB and the Table level
    pub fn index_columns(
        &self,
        db_id: &DbId,
        table_def: &TableDefinition,
    ) -> Option<Vec<ColumnId>> {
        let table_id = table_def.table_id;
        let inner = self.inner.read();
        inner.file_index_columns.get(db_id).and_then(|db| {
            let mut set: BTreeSet<ColumnId> =
                BTreeSet::from_iter(db.db_columns.clone().into_iter().filter_map(|c| {
                    table_def
                        .column_definition(c)
                        .and_then(|def| match def.data_type {
                            InfluxColumnType::Tag
                            | InfluxColumnType::Field(InfluxFieldType::String) => Some(def.id),
                            _ => None,
                        })
                }));

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

    /// Collect a list of summaries of indexes in the config.
    ///
    /// This is used by the system tables to display index information
    pub fn index_summaries(&self, catalog: Arc<Catalog>) -> Vec<IndexSummary> {
        let inner = self.inner.read();

        let mut index_summaries = Vec::new();
        for (db_id, idx) in &inner.file_index_columns {
            let db_schema = catalog
                .db_schema_by_id(db_id)
                .expect("a valid database ID in the index");
            let line = IndexSummary {
                db: Arc::clone(&db_schema.name),
                table: None,
                columns: idx.db_columns.clone(),
                // We don't know the column ids for the db level so we leave this empty by default
                column_ids: Vec::new(),
            };

            if !line.columns.is_empty() {
                index_summaries.push(line.clone());
            }

            for (table_id, column_ids) in &idx.table_columns {
                let table_def = db_schema
                    .table_definition_by_id(table_id)
                    .expect("a valid table id in the index");
                let columns = column_ids
                    .iter()
                    .map(|id| {
                        table_def
                            .column_id_to_name(id)
                            .expect("a valid column id in the index")
                    })
                    .collect();
                let line = IndexSummary {
                    db: Arc::clone(&db_schema.name),
                    table: Some(Arc::clone(&table_def.table_name)),
                    columns,
                    column_ids: column_ids.clone(),
                };
                index_summaries.push(line);
            }
        }

        index_summaries
    }
}

/// Methods for making changes to the enterprise config
impl EnterpriseConfig {
    /// Acquire a permit to make changes to the config
    ///
    /// It is the responsibility of the caller to invoke this and hold the permit until the changes
    /// have been made to the config, _and_ the config persisted to object store.
    ///
    /// This ensures a) that only one change is made at a time and b) that we don't hold a write
    /// lock on the config while waiting for the object store.
    pub async fn write_permit(&self) -> Result<SemaphorePermit<'_>, EnterpriseConfigError> {
        ENTERPRISE_CONFIG_WRITE_PERMIT
            .acquire()
            .await
            .context("failed to get write permit on the enterprise config")
            .map_err(Into::into)
    }

    pub fn add_or_update_columns_for_db<S: AsRef<str>>(&self, db_id: DbId, columns: Vec<S>) {
        let mut inner = self.inner.write();
        inner
            .file_index_columns
            .entry(db_id)
            // if the db entry does exist add these columns for the db
            .and_modify(|idx| {
                idx.db_columns = columns.iter().map(AsRef::as_ref).map(Into::into).collect();
            })
            // if the db entry does not exist create a default Index
            // and add these columns
            .or_insert_with(|| {
                let mut idx = Index::new();
                idx.db_columns = columns.iter().map(AsRef::as_ref).map(Into::into).collect();
                idx
            });
    }

    pub fn add_or_update_columns_for_table(
        &self,
        db_id: DbId,
        table_id: TableId,
        columns: Vec<ColumnId>,
    ) {
        let mut inner = self.inner.write();
        inner
            .file_index_columns
            .entry(db_id)
            // If the db entry exists try to add those columns to the
            // table or if they don't exist yet create them
            .and_modify(|idx| {
                idx.table_columns
                    .entry(table_id)
                    .and_modify(|existing| {
                        existing.extend(columns.clone());
                    })
                    .or_insert_with(|| columns.clone());
            })
            // If the db entry does not exist create a default Index
            // and add those columns for that table
            .or_insert_with(|| {
                let mut idx = Index::default();
                idx.table_columns.insert(table_id, columns);

                idx
            });
    }

    pub fn remove_columns_for_db(&self, db_id: &DbId) -> Result<(), EnterpriseConfigError> {
        let mut inner = self.inner.write();
        inner
            .file_index_columns
            .remove(db_id)
            .context("database does not exist in the file index")?;
        Ok(())
    }

    pub fn remove_columns_for_table(
        &self,
        db_id: &DbId,
        table_id: &TableId,
    ) -> Result<(), EnterpriseConfigError> {
        let mut inner = self.inner.write();
        let Index { table_columns, .. } = inner
            .file_index_columns
            .get_mut(db_id)
            .context("database does not exist in the file index")?;
        table_columns
            .remove(table_id)
            .context("table does not exist in the index")?;
        Ok(())
    }
}

/// Summarizes an index in the config for reporting in system tables
#[derive(Debug, Clone)]
pub struct IndexSummary {
    pub db: Arc<str>,
    pub table: Option<Arc<str>>,
    pub columns: Vec<Arc<str>>,
    pub column_ids: Vec<ColumnId>,
}
