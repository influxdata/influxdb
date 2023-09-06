//! This module implements an in-memory implementation of the iox_catalog interface. It can be
//! used for testing or for an IOx designed to run without catalog persistence.

use crate::interface::{verify_sort_key_length, MAX_PARQUET_FILES_SELECTED_ONCE_FOR_DELETE};
use crate::{
    interface::{
        CasFailure, Catalog, ColumnRepo, ColumnTypeMismatchSnafu, Error, NamespaceRepo,
        ParquetFileRepo, PartitionRepo, RepoCollection, Result, SoftDeletedRows, TableRepo,
        MAX_PARQUET_FILES_SELECTED_ONCE_FOR_RETENTION,
    },
    metrics::MetricDecorator,
    DEFAULT_MAX_COLUMNS_PER_TABLE, DEFAULT_MAX_TABLES,
};
use async_trait::async_trait;
use data_types::SortedColumnSet;
use data_types::{
    partition_template::{
        NamespacePartitionTemplateOverride, TablePartitionTemplateOverride, TemplatePart,
    },
    Column, ColumnId, ColumnType, CompactionLevel, Namespace, NamespaceId, NamespaceName,
    NamespaceServiceProtectionLimitsOverride, ParquetFile, ParquetFileId, ParquetFileParams,
    Partition, PartitionHashId, PartitionId, PartitionKey, SkippedCompaction, Table, TableId,
    Timestamp, TransitionPartitionId,
};
use iox_time::{SystemProvider, TimeProvider};
use snafu::ensure;
use sqlx::types::Uuid;
use std::{
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter},
    sync::Arc,
};
use tokio::sync::{Mutex, OwnedMutexGuard};

/// In-memory catalog that implements the `RepoCollection` and individual repo traits from
/// the catalog interface.
pub struct MemCatalog {
    metrics: Arc<metric::Registry>,
    collections: Arc<Mutex<MemCollections>>,
    time_provider: Arc<dyn TimeProvider>,
}

impl MemCatalog {
    /// return new initialized `MemCatalog`
    pub fn new(metrics: Arc<metric::Registry>) -> Self {
        Self {
            metrics,
            collections: Default::default(),
            time_provider: Arc::new(SystemProvider::new()),
        }
    }

    /// Add partition directly, for testing purposes only as it does not do any consistency or
    /// uniqueness checks
    pub async fn add_partition(&self, partition: Partition) {
        let mut collections = Arc::clone(&self.collections).lock_owned().await;
        collections.partitions.push(partition);
    }
}

impl std::fmt::Debug for MemCatalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemCatalog").finish_non_exhaustive()
    }
}

#[derive(Default, Debug, Clone)]
struct MemCollections {
    namespaces: Vec<Namespace>,
    tables: Vec<Table>,
    columns: Vec<Column>,
    partitions: Vec<Partition>,
    skipped_compactions: Vec<SkippedCompaction>,
    parquet_files: Vec<ParquetFile>,
}

/// transaction bound to an in-memory catalog.
#[derive(Debug)]
pub struct MemTxn {
    inner: OwnedMutexGuard<MemCollections>,
    time_provider: Arc<dyn TimeProvider>,
}

impl MemTxn {
    fn stage(&mut self) -> &mut MemCollections {
        &mut self.inner
    }
}

impl Display for MemCatalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Memory")
    }
}

#[async_trait]
impl Catalog for MemCatalog {
    async fn setup(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn repositories(&self) -> Box<dyn RepoCollection> {
        let collections = Arc::clone(&self.collections).lock_owned().await;
        Box::new(MetricDecorator::new(
            MemTxn {
                inner: collections,
                time_provider: self.time_provider(),
            },
            Arc::clone(&self.metrics),
        ))
    }

    #[cfg(test)]
    fn metrics(&self) -> Arc<metric::Registry> {
        Arc::clone(&self.metrics)
    }

    fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider)
    }
}

#[async_trait]
impl RepoCollection for MemTxn {
    fn namespaces(&mut self) -> &mut dyn NamespaceRepo {
        self
    }

    fn tables(&mut self) -> &mut dyn TableRepo {
        self
    }

    fn columns(&mut self) -> &mut dyn ColumnRepo {
        self
    }

    fn partitions(&mut self) -> &mut dyn PartitionRepo {
        self
    }

    fn parquet_files(&mut self) -> &mut dyn ParquetFileRepo {
        self
    }
}

#[async_trait]
impl NamespaceRepo for MemTxn {
    async fn create(
        &mut self,
        name: &NamespaceName<'_>,
        partition_template: Option<NamespacePartitionTemplateOverride>,
        retention_period_ns: Option<i64>,
        service_protection_limits: Option<NamespaceServiceProtectionLimitsOverride>,
    ) -> Result<Namespace> {
        let stage = self.stage();

        if stage.namespaces.iter().any(|n| n.name == name.as_str()) {
            return Err(Error::NameExists {
                name: name.to_string(),
            });
        }

        let max_tables = service_protection_limits.and_then(|l| l.max_tables);
        let max_columns_per_table = service_protection_limits.and_then(|l| l.max_columns_per_table);

        let namespace = Namespace {
            id: NamespaceId::new(stage.namespaces.len() as i64 + 1),
            name: name.to_string(),
            max_tables: max_tables.unwrap_or(DEFAULT_MAX_TABLES),
            max_columns_per_table: max_columns_per_table.unwrap_or(DEFAULT_MAX_COLUMNS_PER_TABLE),
            retention_period_ns,
            deleted_at: None,
            partition_template: partition_template.unwrap_or_default(),
        };
        stage.namespaces.push(namespace);
        Ok(stage.namespaces.last().unwrap().clone())
    }

    async fn list(&mut self, deleted: SoftDeletedRows) -> Result<Vec<Namespace>> {
        let stage = self.stage();

        Ok(filter_namespace_soft_delete(&stage.namespaces, deleted)
            .cloned()
            .collect())
    }

    async fn get_by_id(
        &mut self,
        id: NamespaceId,
        deleted: SoftDeletedRows,
    ) -> Result<Option<Namespace>> {
        let stage = self.stage();

        Ok(filter_namespace_soft_delete(&stage.namespaces, deleted)
            .find(|n| n.id == id)
            .cloned())
    }

    async fn get_by_name(
        &mut self,
        name: &str,
        deleted: SoftDeletedRows,
    ) -> Result<Option<Namespace>> {
        let stage = self.stage();

        Ok(filter_namespace_soft_delete(&stage.namespaces, deleted)
            .find(|n| n.name == name)
            .cloned())
    }

    // performs a cascading delete of all things attached to the namespace, then deletes the
    // namespace
    async fn soft_delete(&mut self, name: &str) -> Result<()> {
        let timestamp = self.time_provider.now();
        let stage = self.stage();
        // get namespace by name
        match stage.namespaces.iter_mut().find(|n| n.name == name) {
            Some(n) => {
                n.deleted_at = Some(Timestamp::from(timestamp));
                Ok(())
            }
            None => Err(Error::NamespaceNotFoundByName {
                name: name.to_string(),
            }),
        }
    }

    async fn update_table_limit(&mut self, name: &str, new_max: i32) -> Result<Namespace> {
        let stage = self.stage();
        match stage.namespaces.iter_mut().find(|n| n.name == name) {
            Some(n) => {
                n.max_tables = new_max;
                Ok(n.clone())
            }
            None => Err(Error::NamespaceNotFoundByName {
                name: name.to_string(),
            }),
        }
    }

    async fn update_column_limit(&mut self, name: &str, new_max: i32) -> Result<Namespace> {
        let stage = self.stage();
        match stage.namespaces.iter_mut().find(|n| n.name == name) {
            Some(n) => {
                n.max_columns_per_table = new_max;
                Ok(n.clone())
            }
            None => Err(Error::NamespaceNotFoundByName {
                name: name.to_string(),
            }),
        }
    }

    async fn update_retention_period(
        &mut self,
        name: &str,
        retention_period_ns: Option<i64>,
    ) -> Result<Namespace> {
        let stage = self.stage();
        match stage.namespaces.iter_mut().find(|n| n.name == name) {
            Some(n) => {
                n.retention_period_ns = retention_period_ns;
                Ok(n.clone())
            }
            None => Err(Error::NamespaceNotFoundByName {
                name: name.to_string(),
            }),
        }
    }
}

#[async_trait]
impl TableRepo for MemTxn {
    async fn create(
        &mut self,
        name: &str,
        partition_template: TablePartitionTemplateOverride,
        namespace_id: NamespaceId,
    ) -> Result<Table> {
        let table = {
            let stage = self.stage();

            // this block is just to ensure the mem impl correctly creates TableCreateLimitError in
            // tests, we don't care about any of the errors it is discarding
            stage
                .namespaces
                .iter()
                .find(|n| n.id == namespace_id)
                .cloned()
                .ok_or_else(|| Error::NamespaceNotFoundByName {
                    // we're never going to use this error, this is just for flow control,
                    // so it doesn't matter that we only have the ID, not the name
                    name: "".to_string(),
                })
                .and_then(|n| {
                    let max_tables = n.max_tables;
                    let tables_count = stage
                        .tables
                        .iter()
                        .filter(|t| t.namespace_id == namespace_id)
                        .count();
                    if tables_count >= max_tables.try_into().unwrap() {
                        return Err(Error::TableCreateLimitError {
                            table_name: name.to_string(),
                            namespace_id,
                        });
                    }
                    Ok(())
                })?;

            match stage
                .tables
                .iter()
                .find(|t| t.name == name && t.namespace_id == namespace_id)
            {
                Some(_t) => {
                    return Err(Error::TableNameExists {
                        name: name.to_string(),
                        namespace_id,
                    })
                }
                None => {
                    let table = Table {
                        id: TableId::new(stage.tables.len() as i64 + 1),
                        namespace_id,
                        name: name.to_string(),
                        partition_template,
                    };
                    stage.tables.push(table);
                    stage.tables.last().unwrap()
                }
            }
        };

        let table = table.clone();

        // Partitioning is only supported for tags, so create tag columns for all `TagValue`
        // partition template parts. It's important this happens within the table creation
        // transaction so that there isn't a possibility of a concurrent write creating these
        // columns with an unsupported type.
        for template_part in table.partition_template.parts() {
            if let TemplatePart::TagValue(tag_name) = template_part {
                self.columns()
                    .create_or_get(tag_name, table.id, ColumnType::Tag)
                    .await?;
            }
        }

        Ok(table)
    }

    async fn get_by_id(&mut self, table_id: TableId) -> Result<Option<Table>> {
        let stage = self.stage();

        Ok(stage.tables.iter().find(|t| t.id == table_id).cloned())
    }

    async fn get_by_namespace_and_name(
        &mut self,
        namespace_id: NamespaceId,
        name: &str,
    ) -> Result<Option<Table>> {
        let stage = self.stage();

        Ok(stage
            .tables
            .iter()
            .find(|t| t.namespace_id == namespace_id && t.name == name)
            .cloned())
    }

    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Table>> {
        let stage = self.stage();

        let tables: Vec<_> = stage
            .tables
            .iter()
            .filter(|t| t.namespace_id == namespace_id)
            .cloned()
            .collect();
        Ok(tables)
    }

    async fn list(&mut self) -> Result<Vec<Table>> {
        let stage = self.stage();
        Ok(stage.tables.clone())
    }
}

#[async_trait]
impl ColumnRepo for MemTxn {
    async fn create_or_get(
        &mut self,
        name: &str,
        table_id: TableId,
        column_type: ColumnType,
    ) -> Result<Column> {
        let stage = self.stage();

        // this block is just to ensure the mem impl correctly creates ColumnCreateLimitError in
        // tests, we don't care about any of the errors it is discarding
        stage
            .tables
            .iter()
            .find(|t| t.id == table_id)
            .cloned()
            .ok_or(Error::TableNotFound { id: table_id }) // error never used, this is just for flow control
            .and_then(|t| {
                stage
                    .namespaces
                    .iter()
                    .find(|n| n.id == t.namespace_id)
                    .cloned()
                    .ok_or_else(|| Error::NamespaceNotFoundByName {
                        // we're never going to use this error, this is just for flow control,
                        // so it doesn't matter that we only have the ID, not the name
                        name: "".to_string(),
                    })
                    .and_then(|n| {
                        let max_columns_per_table = n.max_columns_per_table;
                        let columns_count = stage
                            .columns
                            .iter()
                            .filter(|t| t.table_id == table_id)
                            .count();
                        if columns_count >= max_columns_per_table.try_into().unwrap() {
                            return Err(Error::ColumnCreateLimitError {
                                column_name: name.to_string(),
                                table_id,
                            });
                        }
                        Ok(())
                    })?;
                Ok(())
            })?;

        let column = match stage
            .columns
            .iter()
            .find(|t| t.name == name && t.table_id == table_id)
        {
            Some(c) => {
                ensure!(
                    column_type == c.column_type,
                    ColumnTypeMismatchSnafu {
                        name,
                        existing: c.column_type,
                        new: column_type
                    }
                );
                c
            }
            None => {
                let column = Column {
                    id: ColumnId::new(stage.columns.len() as i64 + 1),
                    table_id,
                    name: name.to_string(),
                    column_type,
                };
                stage.columns.push(column);
                stage.columns.last().unwrap()
            }
        };

        Ok(column.clone())
    }

    async fn create_or_get_many_unchecked(
        &mut self,
        table_id: TableId,
        columns: HashMap<&str, ColumnType>,
    ) -> Result<Vec<Column>> {
        // Explicitly NOT using `create_or_get` in this function: the Postgres catalog doesn't
        // check column limits when inserting many columns because it's complicated and expensive,
        // and for testing purposes the in-memory catalog needs to match its functionality.

        let stage = self.stage();

        let out: Vec<_> = columns
            .iter()
            .map(|(&column_name, &column_type)| {
                match stage
                    .columns
                    .iter()
                    .find(|t| t.name == column_name && t.table_id == table_id)
                {
                    Some(c) => {
                        ensure!(
                            column_type == c.column_type,
                            ColumnTypeMismatchSnafu {
                                name: column_name,
                                existing: c.column_type,
                                new: column_type
                            }
                        );
                        Ok(c.clone())
                    }
                    None => {
                        let new_column = Column {
                            id: ColumnId::new(stage.columns.len() as i64 + 1),
                            table_id,
                            name: column_name.to_string(),
                            column_type,
                        };
                        stage.columns.push(new_column);
                        Ok(stage.columns.last().unwrap().clone())
                    }
                }
            })
            .collect::<Result<Vec<Column>>>()?;

        Ok(out)
    }

    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Column>> {
        let stage = self.stage();

        let table_ids: Vec<_> = stage
            .tables
            .iter()
            .filter(|t| t.namespace_id == namespace_id)
            .map(|t| t.id)
            .collect();
        let columns: Vec<_> = stage
            .columns
            .iter()
            .filter(|c| table_ids.contains(&c.table_id))
            .cloned()
            .collect();

        Ok(columns)
    }

    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Column>> {
        let stage = self.stage();

        let columns: Vec<_> = stage
            .columns
            .iter()
            .filter(|c| c.table_id == table_id)
            .cloned()
            .collect();

        Ok(columns)
    }

    async fn list(&mut self) -> Result<Vec<Column>> {
        let stage = self.stage();
        Ok(stage.columns.clone())
    }
}

#[async_trait]
impl PartitionRepo for MemTxn {
    async fn create_or_get(&mut self, key: PartitionKey, table_id: TableId) -> Result<Partition> {
        let stage = self.stage();

        let partition = match stage
            .partitions
            .iter()
            .find(|p| p.partition_key == key && p.table_id == table_id)
        {
            Some(p) => p,
            None => {
                let p = Partition::new_in_memory_only(
                    PartitionId::new(stage.partitions.len() as i64 + 1),
                    table_id,
                    key,
                    vec![],
                    Some(SortedColumnSet::new(vec![])),
                    None,
                );
                stage.partitions.push(p);
                stage.partitions.last().unwrap()
            }
        };

        Ok(partition.clone())
    }

    async fn get_by_id(&mut self, partition_id: PartitionId) -> Result<Option<Partition>> {
        let stage = self.stage();

        Ok(stage
            .partitions
            .iter()
            .find(|p| p.id == partition_id)
            .cloned())
    }

    async fn get_by_id_batch(&mut self, partition_ids: Vec<PartitionId>) -> Result<Vec<Partition>> {
        let lookup = partition_ids.into_iter().collect::<HashSet<_>>();

        let stage = self.stage();

        Ok(stage
            .partitions
            .iter()
            .filter(|p| lookup.contains(&p.id))
            .cloned()
            .collect())
    }

    async fn get_by_hash_id(
        &mut self,
        partition_hash_id: &PartitionHashId,
    ) -> Result<Option<Partition>> {
        let stage = self.stage();

        Ok(stage
            .partitions
            .iter()
            .find(|p| {
                p.hash_id()
                    .map(|hash_id| hash_id == partition_hash_id)
                    .unwrap_or_default()
            })
            .cloned())
    }

    async fn get_by_hash_id_batch(
        &mut self,
        partition_hash_ids: &[&PartitionHashId],
    ) -> Result<Vec<Partition>> {
        let lookup = partition_hash_ids.iter().copied().collect::<HashSet<_>>();

        let stage = self.stage();

        Ok(stage
            .partitions
            .iter()
            .filter(|p| {
                p.hash_id()
                    .map(|hash_id| lookup.contains(hash_id))
                    .unwrap_or_default()
            })
            .cloned()
            .collect())
    }

    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Partition>> {
        let stage = self.stage();

        let partitions: Vec<_> = stage
            .partitions
            .iter()
            .filter(|p| p.table_id == table_id)
            .cloned()
            .collect();
        Ok(partitions)
    }

    async fn list_ids(&mut self) -> Result<Vec<PartitionId>> {
        let stage = self.stage();

        let partitions: Vec<_> = stage.partitions.iter().map(|p| p.id).collect();

        Ok(partitions)
    }

    async fn cas_sort_key(
        &mut self,
        partition_id: &TransitionPartitionId,
        old_sort_key: Option<Vec<String>>,
        new_sort_key: &[&str],
        new_sort_key_ids: &SortedColumnSet,
    ) -> Result<Partition, CasFailure<(Vec<String>, Option<SortedColumnSet>)>> {
        verify_sort_key_length(new_sort_key, new_sort_key_ids);

        let stage = self.stage();
        let old_sort_key = old_sort_key.unwrap_or_default();

        match stage.partitions.iter_mut().find(|p| match partition_id {
            TransitionPartitionId::Deterministic(hash_id) => {
                p.hash_id().map_or(false, |h| h == hash_id)
            }
            TransitionPartitionId::Deprecated(id) => p.id == *id,
        }) {
            Some(p) if p.sort_key == old_sort_key => {
                p.sort_key = new_sort_key.iter().map(|s| s.to_string()).collect();
                p.sort_key_ids = Some(new_sort_key_ids.clone());
                Ok(p.clone())
            }
            Some(p) => {
                return Err(CasFailure::ValueMismatch((
                    p.sort_key.clone(),
                    p.sort_key_ids.clone(),
                )));
            }
            None => Err(CasFailure::QueryError(Error::PartitionNotFound {
                id: partition_id.clone(),
            })),
        }
    }

    async fn record_skipped_compaction(
        &mut self,
        partition_id: PartitionId,
        reason: &str,
        num_files: usize,
        limit_num_files: usize,
        limit_num_files_first_in_partition: usize,
        estimated_bytes: u64,
        limit_bytes: u64,
    ) -> Result<()> {
        let reason = reason.to_string();
        let skipped_at = Timestamp::from(self.time_provider.now());

        let stage = self.stage();
        match stage
            .skipped_compactions
            .iter_mut()
            .find(|s| s.partition_id == partition_id)
        {
            Some(s) => {
                s.reason = reason;
                s.skipped_at = skipped_at;
                s.num_files = num_files as i64;
                s.limit_num_files = limit_num_files as i64;
                s.limit_num_files_first_in_partition = limit_num_files_first_in_partition as i64;
                s.estimated_bytes = estimated_bytes as i64;
                s.limit_bytes = limit_bytes as i64;
            }
            None => stage.skipped_compactions.push(SkippedCompaction {
                partition_id,
                reason,
                skipped_at,
                num_files: num_files as i64,
                limit_num_files: limit_num_files as i64,
                limit_num_files_first_in_partition: limit_num_files_first_in_partition as i64,
                estimated_bytes: estimated_bytes as i64,
                limit_bytes: limit_bytes as i64,
            }),
        }
        Ok(())
    }

    async fn get_in_skipped_compactions(
        &mut self,
        partition_ids: &[PartitionId],
    ) -> Result<Vec<SkippedCompaction>> {
        let stage = self.stage();
        let find: HashSet<&PartitionId> = partition_ids.iter().collect();
        Ok(stage
            .skipped_compactions
            .iter()
            .filter(|s| find.contains(&s.partition_id))
            .cloned()
            .collect())
    }

    async fn list_skipped_compactions(&mut self) -> Result<Vec<SkippedCompaction>> {
        let stage = self.stage();
        Ok(stage.skipped_compactions.clone())
    }

    async fn delete_skipped_compactions(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<SkippedCompaction>> {
        use std::mem;

        let stage = self.stage();
        let skipped_compactions = mem::take(&mut stage.skipped_compactions);
        let (mut removed, remaining) = skipped_compactions
            .into_iter()
            .partition(|sc| sc.partition_id == partition_id);
        stage.skipped_compactions = remaining;

        match removed.pop() {
            Some(sc) if removed.is_empty() => Ok(Some(sc)),
            Some(_) => unreachable!("There must be exactly one skipped compaction per partition"),
            None => Ok(None),
        }
    }

    async fn most_recent_n(&mut self, n: usize) -> Result<Vec<Partition>> {
        let stage = self.stage();
        Ok(stage.partitions.iter().rev().take(n).cloned().collect())
    }

    async fn partitions_new_file_between(
        &mut self,
        minimum_time: Timestamp,
        maximum_time: Option<Timestamp>,
    ) -> Result<Vec<PartitionId>> {
        let stage = self.stage();

        let partitions: Vec<_> = stage
            .partitions
            .iter()
            .filter(|p| {
                p.new_file_at > Some(minimum_time)
                    && maximum_time
                        .map(|max| p.new_file_at < Some(max))
                        .unwrap_or(true)
            })
            .map(|p| p.id)
            .collect();

        Ok(partitions)
    }

    async fn list_old_style(&mut self) -> Result<Vec<Partition>> {
        let stage = self.stage();

        let old_style: Vec<_> = stage
            .partitions
            .iter()
            .filter(|p| p.hash_id().is_none())
            .cloned()
            .collect();

        Ok(old_style)
    }
}

#[async_trait]
impl ParquetFileRepo for MemTxn {
    async fn create(&mut self, parquet_file_params: ParquetFileParams) -> Result<ParquetFile> {
        create_parquet_file(self.stage(), parquet_file_params).await
    }

    async fn list_all(&mut self) -> Result<Vec<ParquetFile>> {
        let stage = self.stage();

        Ok(stage.parquet_files.clone())
    }

    async fn flag_for_delete_by_retention(&mut self) -> Result<Vec<ParquetFileId>> {
        let now = Timestamp::from(self.time_provider.now());
        let stage = self.stage();

        Ok(stage
            .parquet_files
            .iter_mut()
            // don't flag if already flagged for deletion
            .filter(|f| f.to_delete.is_none())
            .filter_map(|f| {
                // table retention, if it exists, overrides namespace retention
                // TODO - include check of table retention period once implemented
                stage
                    .namespaces
                    .iter()
                    .find(|n| n.id == f.namespace_id)
                    .and_then(|ns| {
                        ns.retention_period_ns.and_then(|rp| {
                            if f.max_time < now - rp {
                                f.to_delete = Some(now);
                                Some(f.id)
                            } else {
                                None
                            }
                        })
                    })
            })
            .take(MAX_PARQUET_FILES_SELECTED_ONCE_FOR_RETENTION as usize)
            .collect())
    }

    async fn list_by_namespace_not_to_delete(
        &mut self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<ParquetFile>> {
        let stage = self.stage();

        let table_ids: HashSet<_> = stage
            .tables
            .iter()
            .filter_map(|table| (table.namespace_id == namespace_id).then_some(table.id))
            .collect();
        let parquet_files: Vec<_> = stage
            .parquet_files
            .iter()
            .filter(|f| table_ids.contains(&f.table_id) && f.to_delete.is_none())
            .cloned()
            .collect();
        Ok(parquet_files)
    }

    async fn list_by_table_not_to_delete(&mut self, table_id: TableId) -> Result<Vec<ParquetFile>> {
        let stage = self.stage();

        let parquet_files: Vec<_> = stage
            .parquet_files
            .iter()
            .filter(|f| table_id == f.table_id && f.to_delete.is_none())
            .cloned()
            .collect();
        Ok(parquet_files)
    }

    async fn delete_old_ids_only(&mut self, older_than: Timestamp) -> Result<Vec<ParquetFileId>> {
        let stage = self.stage();

        let (delete, keep): (Vec<_>, Vec<_>) = stage.parquet_files.iter().cloned().partition(
            |f| matches!(f.to_delete, Some(marked_deleted) if marked_deleted < older_than),
        );

        stage.parquet_files = keep;

        let delete = delete
            .into_iter()
            .take(MAX_PARQUET_FILES_SELECTED_ONCE_FOR_DELETE as usize)
            .map(|f| f.id)
            .collect();
        Ok(delete)
    }

    async fn list_by_partition_not_to_delete(
        &mut self,
        partition_id: &TransitionPartitionId,
    ) -> Result<Vec<ParquetFile>> {
        let stage = self.stage();

        let partition = stage
            .partitions
            .iter()
            .find(|p| match partition_id {
                TransitionPartitionId::Deterministic(hash_id) => p
                    .hash_id()
                    .map(|p_hash_id| p_hash_id == hash_id)
                    .unwrap_or(false),
                TransitionPartitionId::Deprecated(id) => id == &p.id,
            })
            .unwrap()
            .clone();

        Ok(stage
            .parquet_files
            .iter()
            .filter(|f| match &f.partition_id {
                TransitionPartitionId::Deterministic(hash_id) => partition
                    .hash_id()
                    .map(|p_hash_id| p_hash_id == hash_id)
                    .unwrap_or(false),
                TransitionPartitionId::Deprecated(id) => id == &partition.id,
            })
            .filter(|f| f.to_delete.is_none())
            .cloned()
            .collect())
    }

    async fn get_by_object_store_id(
        &mut self,
        object_store_id: Uuid,
    ) -> Result<Option<ParquetFile>> {
        let stage = self.stage();

        Ok(stage
            .parquet_files
            .iter()
            .find(|f| f.object_store_id.eq(&object_store_id))
            .cloned())
    }

    async fn exists_by_object_store_id_batch(
        &mut self,
        object_store_ids: Vec<Uuid>,
    ) -> Result<Vec<Uuid>> {
        let stage = self.stage();

        Ok(stage
            .parquet_files
            .iter()
            .filter(|f| object_store_ids.contains(&f.object_store_id))
            .map(|f| f.object_store_id)
            .collect())
    }

    async fn create_upgrade_delete(
        &mut self,
        delete: &[ParquetFileId],
        upgrade: &[ParquetFileId],
        create: &[ParquetFileParams],
        target_level: CompactionLevel,
    ) -> Result<Vec<ParquetFileId>> {
        let delete_set = delete.iter().copied().collect::<HashSet<_>>();
        let upgrade_set = upgrade.iter().copied().collect::<HashSet<_>>();

        assert!(
            delete_set.is_disjoint(&upgrade_set),
            "attempted to upgrade a file scheduled for delete"
        );

        let mut stage = self.inner.clone();

        for id in delete {
            let marked_at = Timestamp::from(self.time_provider.now());
            flag_for_delete(&mut stage, *id, marked_at).await?;
        }

        update_compaction_level(&mut stage, upgrade, target_level).await?;

        let mut ids = Vec::with_capacity(create.len());
        for file in create {
            let res = create_parquet_file(&mut stage, file.clone()).await?;
            ids.push(res.id);
        }

        *self.inner = stage;

        Ok(ids)
    }
}

fn filter_namespace_soft_delete<'a>(
    v: impl IntoIterator<Item = &'a Namespace>,
    deleted: SoftDeletedRows,
) -> impl Iterator<Item = &'a Namespace> {
    v.into_iter().filter(move |v| match deleted {
        SoftDeletedRows::AllRows => true,
        SoftDeletedRows::ExcludeDeleted => v.deleted_at.is_none(),
        SoftDeletedRows::OnlyDeleted => v.deleted_at.is_some(),
    })
}

// The following three functions are helpers to the create_upgrade_delete method.
// They are also used by the respective create/flag_for_delete/update_compaction_level methods.
async fn create_parquet_file(
    stage: &mut MemCollections,
    parquet_file_params: ParquetFileParams,
) -> Result<ParquetFile> {
    if stage
        .parquet_files
        .iter()
        .any(|f| f.object_store_id == parquet_file_params.object_store_id)
    {
        return Err(Error::FileExists {
            object_store_id: parquet_file_params.object_store_id,
        });
    }

    let parquet_file = ParquetFile::from_params(
        parquet_file_params,
        ParquetFileId::new(stage.parquet_files.len() as i64 + 1),
    );
    let created_at = parquet_file.created_at;
    let partition_id = parquet_file.partition_id.clone();
    stage.parquet_files.push(parquet_file);

    // Update the new_file_at field its partition to the time of created_at
    let partition = stage
        .partitions
        .iter_mut()
        .find(|p| p.transition_partition_id() == partition_id)
        .ok_or(Error::PartitionNotFound { id: partition_id })?;
    partition.new_file_at = Some(created_at);

    Ok(stage.parquet_files.last().unwrap().clone())
}

async fn flag_for_delete(
    stage: &mut MemCollections,
    id: ParquetFileId,
    marked_at: Timestamp,
) -> Result<()> {
    match stage.parquet_files.iter_mut().find(|p| p.id == id) {
        Some(f) => f.to_delete = Some(marked_at),
        None => return Err(Error::ParquetRecordNotFound { id }),
    }

    Ok(())
}

async fn update_compaction_level(
    stage: &mut MemCollections,
    parquet_file_ids: &[ParquetFileId],
    compaction_level: CompactionLevel,
) -> Result<Vec<ParquetFileId>> {
    let mut updated = Vec::with_capacity(parquet_file_ids.len());

    for f in stage
        .parquet_files
        .iter_mut()
        .filter(|p| parquet_file_ids.contains(&p.id))
    {
        f.compaction_level = compaction_level;
        updated.push(f.id);
    }

    Ok(updated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_catalog() {
        crate::interface::test_helpers::test_catalog(|| async {
            let metrics = Arc::new(metric::Registry::default());
            let x: Arc<dyn Catalog> = Arc::new(MemCatalog::new(metrics));
            x
        })
        .await;
    }
}
