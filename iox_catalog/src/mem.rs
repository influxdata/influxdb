//! This module implements an in-memory implementation of the iox_catalog interface. It can be
//! used for testing or for an IOx designed to run without catalog persistence.

use crate::{
    constants::{
        MAX_PARQUET_FILES_SELECTED_ONCE_FOR_DELETE, MAX_PARQUET_FILES_SELECTED_ONCE_FOR_RETENTION,
    },
    interface::{
        AlreadyExistsSnafu, CasFailure, Catalog, ColumnRepo, Error, NamespaceRepo, ParquetFileRepo,
        PartitionRepo, RepoCollection, Result, SoftDeletedRows, TableRepo,
    },
    metrics::MetricDecorator,
};
use async_trait::async_trait;
use data_types::snapshot::partition::PartitionSnapshot;
use data_types::snapshot::table::TableSnapshot;
use data_types::{
    partition_template::{
        NamespacePartitionTemplateOverride, TablePartitionTemplateOverride, TemplatePart,
    },
    Column, ColumnId, ColumnType, CompactionLevel, MaxColumnsPerTable, MaxTables, Namespace,
    NamespaceId, NamespaceName, NamespaceServiceProtectionLimitsOverride, ObjectStoreId,
    ParquetFile, ParquetFileId, ParquetFileParams, Partition, PartitionHashId, PartitionId,
    PartitionKey, SkippedCompaction, SortKeyIds, Table, TableId, Timestamp,
};
use iox_time::TimeProvider;
use parking_lot::Mutex;
use snafu::ensure;
use std::ops::Deref;
use std::{
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter},
    ops::DerefMut,
    sync::Arc,
};

/// In-memory catalog that implements the `RepoCollection` and individual repo traits from
/// the catalog interface.
pub struct MemCatalog {
    metrics: Arc<metric::Registry>,
    collections: Arc<Mutex<MemCollections>>,
    time_provider: Arc<dyn TimeProvider>,
}

impl MemCatalog {
    /// return new initialized [`MemCatalog`]
    pub fn new(metrics: Arc<metric::Registry>, time_provider: Arc<dyn TimeProvider>) -> Self {
        Self {
            metrics,
            collections: Default::default(),
            time_provider,
        }
    }

    /// Add partition directly, for testing purposes only as it does not do any consistency or
    /// uniqueness checks
    pub fn add_partition(&self, partition: Partition) {
        let mut stage = self.collections.lock();
        stage.partitions.push(partition.into());
    }
}

impl std::fmt::Debug for MemCatalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemCatalog").finish_non_exhaustive()
    }
}

/// A wrapper around `T` adding a generation number
#[derive(Debug, Clone)]
struct Versioned<T> {
    generation: u64,
    value: T,
}

impl<T> Deref for Versioned<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> DerefMut for Versioned<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl<T> From<T> for Versioned<T> {
    fn from(value: T) -> Self {
        Self {
            generation: 0,
            value,
        }
    }
}

#[derive(Default, Debug, Clone)]
struct MemCollections {
    namespaces: Vec<Namespace>,
    tables: Vec<Versioned<Table>>,
    columns: Vec<Column>,
    partitions: Vec<Versioned<Partition>>,
    skipped_compactions: Vec<SkippedCompaction>,
    parquet_files: Vec<ParquetFile>,
}

/// transaction bound to an in-memory catalog.
#[derive(Debug)]
pub struct MemTxn {
    collections: Arc<Mutex<MemCollections>>,
    time_provider: Arc<dyn TimeProvider>,
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

    fn repositories(&self) -> Box<dyn RepoCollection> {
        let collections = Arc::clone(&self.collections);
        Box::new(MetricDecorator::new(
            MemTxn {
                collections,
                time_provider: self.time_provider(),
            },
            Arc::clone(&self.metrics),
            self.time_provider(),
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
        let mut stage = self.collections.lock();

        if stage.namespaces.iter().any(|n| n.name == name.as_str()) {
            return Err(Error::AlreadyExists {
                descr: name.to_string(),
            });
        }

        let max_tables = service_protection_limits
            .and_then(|l| l.max_tables)
            .unwrap_or_default();
        let max_columns_per_table = service_protection_limits
            .and_then(|l| l.max_columns_per_table)
            .unwrap_or_default();

        let namespace = Namespace {
            id: NamespaceId::new(stage.namespaces.len() as i64 + 1),
            name: name.to_string(),
            max_tables,
            max_columns_per_table,
            retention_period_ns,
            deleted_at: None,
            partition_template: partition_template.unwrap_or_default(),
        };
        stage.namespaces.push(namespace);
        Ok(stage.namespaces.last().unwrap().clone())
    }

    async fn list(&mut self, deleted: SoftDeletedRows) -> Result<Vec<Namespace>> {
        let stage = self.collections.lock();

        Ok(filter_namespace_soft_delete(&stage.namespaces, deleted)
            .cloned()
            .collect())
    }

    async fn get_by_id(
        &mut self,
        id: NamespaceId,
        deleted: SoftDeletedRows,
    ) -> Result<Option<Namespace>> {
        let stage = self.collections.lock();

        let res = filter_namespace_soft_delete(&stage.namespaces, deleted)
            .find(|n| n.id == id)
            .cloned();

        Ok(res)
    }

    async fn get_by_name(
        &mut self,
        name: &str,
        deleted: SoftDeletedRows,
    ) -> Result<Option<Namespace>> {
        let stage = self.collections.lock();

        let res = filter_namespace_soft_delete(&stage.namespaces, deleted)
            .find(|n| n.name == name)
            .cloned();

        Ok(res)
    }

    // performs a cascading delete of all things attached to the namespace, then deletes the
    // namespace
    async fn soft_delete(&mut self, name: &str) -> Result<()> {
        let mut stage = self.collections.lock();
        let timestamp = self.time_provider.now();
        // get namespace by name
        match stage.namespaces.iter_mut().find(|n| n.name == name) {
            Some(n) => {
                n.deleted_at = Some(Timestamp::from(timestamp));
                Ok(())
            }
            None => Err(Error::NotFound {
                descr: name.to_string(),
            }),
        }
    }

    async fn update_table_limit(&mut self, name: &str, new_max: MaxTables) -> Result<Namespace> {
        let mut stage = self.collections.lock();
        match stage.namespaces.iter_mut().find(|n| n.name == name) {
            Some(n) => {
                n.max_tables = new_max;
                Ok(n.clone())
            }
            None => Err(Error::NotFound {
                descr: name.to_string(),
            }),
        }
    }

    async fn update_column_limit(
        &mut self,
        name: &str,
        new_max: MaxColumnsPerTable,
    ) -> Result<Namespace> {
        let mut stage = self.collections.lock();
        match stage.namespaces.iter_mut().find(|n| n.name == name) {
            Some(n) => {
                n.max_columns_per_table = new_max;
                Ok(n.clone())
            }
            None => Err(Error::NotFound {
                descr: name.to_string(),
            }),
        }
    }

    async fn update_retention_period(
        &mut self,
        name: &str,
        retention_period_ns: Option<i64>,
    ) -> Result<Namespace> {
        let mut stage = self.collections.lock();
        match stage.namespaces.iter_mut().find(|n| n.name == name) {
            Some(n) => {
                n.retention_period_ns = retention_period_ns;
                Ok(n.clone())
            }
            None => Err(Error::NotFound {
                descr: name.to_string(),
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
        let mut stage = self.collections.lock();

        let table = {
            // this block is just to ensure the mem impl correctly creates TableCreateLimitError in
            // tests, we don't care about any of the errors it is discarding
            stage
                .namespaces
                .iter()
                .find(|n| n.id == namespace_id)
                .cloned()
                .ok_or_else(|| Error::NotFound {
                    // we're never going to use this error, this is just for flow control,
                    // so it doesn't matter that we only have the ID, not the name
                    descr: "".to_string(),
                })
                .and_then(|n| {
                    let max_tables = n.max_tables;
                    let tables_count = stage
                        .tables
                        .iter()
                        .filter(|t| t.namespace_id == namespace_id)
                        .count();
                    if tables_count >= max_tables.get() {
                        return Err(Error::LimitExceeded {
                            descr: format!(
                                "couldn't create table {}; limit reached on namespace {}",
                                name, namespace_id
                            ),
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
                    return Err(Error::AlreadyExists {
                        descr: format!("table '{name}' in namespace {namespace_id}"),
                    })
                }
                None => {
                    let table = Table {
                        id: TableId::new(stage.tables.len() as i64 + 1),
                        namespace_id,
                        name: name.to_string(),
                        partition_template,
                    };
                    stage.tables.push(table.into());
                    stage.tables.last().unwrap().value.clone()
                }
            }
        };

        // Partitioning is only supported for tags, so create tag columns for all `TagValue`
        // partition template parts. It's important this happens within the table creation
        // transaction so that there isn't a possibility of a concurrent write creating these
        // columns with an unsupported type.
        for template_part in table.partition_template.parts() {
            if let TemplatePart::TagValue(tag_name) = template_part {
                create_or_get_column(&mut stage, tag_name, table.id, ColumnType::Tag)?;
            }
        }

        Ok(table)
    }

    async fn get_by_id(&mut self, table_id: TableId) -> Result<Option<Table>> {
        let stage = self.collections.lock();

        let mut tables = stage.tables.iter();
        Ok(tables.find(|t| t.id == table_id).map(|v| v.value.clone()))
    }

    async fn get_by_namespace_and_name(
        &mut self,
        namespace_id: NamespaceId,
        name: &str,
    ) -> Result<Option<Table>> {
        let stage = self.collections.lock();

        let mut tables = stage.tables.iter();
        let search = tables.find(|t| t.namespace_id == namespace_id && t.name == name);
        Ok(search.map(|v| v.value.clone()))
    }

    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Table>> {
        let stage = self.collections.lock();

        let tables = stage.tables.iter();
        let filtered = tables.filter(|t| t.namespace_id == namespace_id);
        let tables: Vec<_> = filtered.map(|v| v.value.clone()).collect();
        Ok(tables)
    }

    async fn list(&mut self) -> Result<Vec<Table>> {
        let stage = self.collections.lock();
        Ok(stage.tables.iter().map(|v| v.value.clone()).collect())
    }

    async fn snapshot(&mut self, table_id: TableId) -> Result<TableSnapshot> {
        let mut guard = self.collections.lock();

        let (table, generation) = {
            let mut tables = guard.tables.iter_mut();
            let search = tables.find(|x| x.id == table_id);
            let table = search.ok_or_else(|| Error::NotFound {
                descr: table_id.to_string(),
            })?;

            let generation = table.generation;
            table.generation += 1;
            (table.value.clone(), generation)
        };

        let columns = guard
            .columns
            .iter()
            .filter(|x| x.table_id == table_id)
            .cloned()
            .collect();

        let partitions = guard
            .partitions
            .iter()
            .filter(|x| x.table_id == table_id)
            .map(|v| v.value.clone())
            .collect();

        Ok(TableSnapshot::encode(
            table, partitions, columns, generation,
        )?)
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
        let mut stage = self.collections.lock();
        create_or_get_column(&mut stage, name, table_id, column_type)
    }

    async fn create_or_get_many_unchecked(
        &mut self,
        table_id: TableId,
        columns: HashMap<&str, ColumnType>,
    ) -> Result<Vec<Column>> {
        // Explicitly NOT using `create_or_get` in this function: the Postgres catalog doesn't
        // check column limits when inserting many columns because it's complicated and expensive,
        // and for testing purposes the in-memory catalog needs to match its functionality.

        let mut stage = self.collections.lock();

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
                            AlreadyExistsSnafu {
                                descr: format!(
                                    "column {} is type {} but schema update has type {}",
                                    column_name, c.column_type, column_type
                                ),
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
        let stage = self.collections.lock();

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
        let stage = self.collections.lock();

        let columns: Vec<_> = stage
            .columns
            .iter()
            .filter(|c| c.table_id == table_id)
            .cloned()
            .collect();

        Ok(columns)
    }

    async fn list(&mut self) -> Result<Vec<Column>> {
        let stage = self.collections.lock();
        Ok(stage.columns.clone())
    }
}

#[async_trait]
impl PartitionRepo for MemTxn {
    async fn create_or_get(&mut self, key: PartitionKey, table_id: TableId) -> Result<Partition> {
        let mut stage = self.collections.lock();

        let partition = match stage
            .partitions
            .iter()
            .find(|p| p.partition_key == key && p.table_id == table_id)
        {
            Some(p) => p,
            None => {
                let hash_id = PartitionHashId::new(table_id, &key);
                let p = Partition::new_catalog_only(
                    PartitionId::new(stage.partitions.len() as i64 + 1),
                    Some(hash_id),
                    table_id,
                    key,
                    SortKeyIds::default(),
                    None,
                );
                stage.partitions.push(p.into());
                stage.partitions.last().unwrap()
            }
        };

        Ok(partition.value.clone())
    }

    async fn get_by_id_batch(&mut self, partition_ids: &[PartitionId]) -> Result<Vec<Partition>> {
        let lookup = partition_ids.iter().collect::<HashSet<_>>();

        let stage = self.collections.lock();

        Ok(stage
            .partitions
            .iter()
            .filter(|p| lookup.contains(&p.id))
            .map(|x| x.value.clone())
            .collect())
    }

    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Partition>> {
        let stage = self.collections.lock();

        let partitions: Vec<_> = stage
            .partitions
            .iter()
            .filter(|p| p.table_id == table_id)
            .map(|x| x.value.clone())
            .collect();
        Ok(partitions)
    }

    async fn list_ids(&mut self) -> Result<Vec<PartitionId>> {
        let stage = self.collections.lock();

        let partitions: Vec<_> = stage.partitions.iter().map(|p| p.id).collect();

        Ok(partitions)
    }

    async fn cas_sort_key(
        &mut self,
        partition_id: PartitionId,
        old_sort_key_ids: Option<&SortKeyIds>,
        new_sort_key_ids: &SortKeyIds,
    ) -> Result<Partition, CasFailure<SortKeyIds>> {
        let mut stage = self.collections.lock();

        match stage.partitions.iter_mut().find(|p| p.id == partition_id) {
            Some(p) if p.sort_key_ids() == old_sort_key_ids => {
                p.set_sort_key_ids(new_sort_key_ids);
                Ok(p.value.clone())
            }
            Some(p) => {
                return Err(CasFailure::ValueMismatch(
                    p.sort_key_ids().cloned().unwrap_or_default(),
                ));
            }
            None => Err(CasFailure::QueryError(Error::NotFound {
                descr: partition_id.to_string(),
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
        let mut stage = self.collections.lock();

        let reason = reason.to_string();
        let skipped_at = Timestamp::from(self.time_provider.now());

        let sc = SkippedCompaction {
            partition_id,
            reason,
            skipped_at,
            num_files: num_files as i64,
            limit_num_files: limit_num_files as i64,
            limit_num_files_first_in_partition: limit_num_files_first_in_partition as i64,
            estimated_bytes: estimated_bytes as i64,
            limit_bytes: limit_bytes as i64,
        };

        match stage
            .skipped_compactions
            .iter_mut()
            .find(|s| s.partition_id == partition_id)
        {
            Some(s) => {
                *s = sc;
            }
            None => stage.skipped_compactions.push(sc),
        }
        Ok(())
    }

    async fn get_in_skipped_compactions(
        &mut self,
        partition_ids: &[PartitionId],
    ) -> Result<Vec<SkippedCompaction>> {
        let stage = self.collections.lock();
        let find: HashSet<&PartitionId> = partition_ids.iter().collect();
        Ok(stage
            .skipped_compactions
            .iter()
            .filter(|s| find.contains(&s.partition_id))
            .cloned()
            .collect())
    }

    async fn list_skipped_compactions(&mut self) -> Result<Vec<SkippedCompaction>> {
        let stage = self.collections.lock();
        Ok(stage.skipped_compactions.clone())
    }

    async fn delete_skipped_compactions(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<SkippedCompaction>> {
        use std::mem;

        let mut stage = self.collections.lock();
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
        let stage = self.collections.lock();
        let iter = stage.partitions.iter().rev().take(n);
        Ok(iter.map(|x| x.value.clone()).collect())
    }

    async fn partitions_new_file_between(
        &mut self,
        minimum_time: Timestamp,
        maximum_time: Option<Timestamp>,
    ) -> Result<Vec<PartitionId>> {
        let stage = self.collections.lock();

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
        let stage = self.collections.lock();

        let old_style: Vec<_> = stage
            .partitions
            .iter()
            .filter(|p| p.hash_id().is_none())
            .map(|x| x.value.clone())
            .collect();

        Ok(old_style)
    }

    async fn snapshot(&mut self, partition_id: PartitionId) -> Result<PartitionSnapshot> {
        let mut guard = self.collections.lock();
        let (partition, generation) = {
            let search = guard.partitions.iter_mut().find(|x| x.id == partition_id);
            let partition = search.ok_or_else(|| Error::NotFound {
                descr: format!("Partition {partition_id} not found"),
            })?;

            let generation = partition.generation;
            partition.generation += 1;
            (partition.value.clone(), generation)
        };

        let files = guard
            .parquet_files
            .iter()
            .filter(|x| x.partition_id == partition_id && x.to_delete.is_none())
            .cloned()
            .collect();

        let search = guard.tables.iter().find(|x| x.id == partition.table_id);
        let table = search.ok_or_else(|| Error::NotFound {
            descr: format!("Table {} not found", partition.table_id),
        })?;

        let sc = guard
            .skipped_compactions
            .iter()
            .find(|sc| sc.partition_id == partition_id)
            .cloned();

        Ok(PartitionSnapshot::encode(
            table.namespace_id,
            partition,
            files,
            sc,
            generation,
        )?)
    }
}

#[async_trait]
impl ParquetFileRepo for MemTxn {
    async fn flag_for_delete_by_retention(&mut self) -> Result<Vec<(PartitionId, ObjectStoreId)>> {
        let mut stage = self.collections.lock();
        let now = Timestamp::from(self.time_provider.now());
        let stage = stage.deref_mut();

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
                                Some((f.partition_id, f.object_store_id))
                            } else {
                                None
                            }
                        })
                    })
            })
            .take(MAX_PARQUET_FILES_SELECTED_ONCE_FOR_RETENTION as usize)
            .collect())
    }

    async fn delete_old_ids_only(&mut self, older_than: Timestamp) -> Result<Vec<ObjectStoreId>> {
        let mut stage = self.collections.lock();

        let (delete, keep): (Vec<_>, Vec<_>) = stage.parquet_files.iter().cloned().partition(
            |f| matches!(f.to_delete, Some(marked_deleted) if marked_deleted < older_than),
        );

        stage.parquet_files = keep;

        let delete = delete
            .into_iter()
            .take(MAX_PARQUET_FILES_SELECTED_ONCE_FOR_DELETE as usize)
            .map(|f| f.object_store_id)
            .collect();
        Ok(delete)
    }

    async fn list_by_partition_not_to_delete_batch(
        &mut self,
        partition_ids: Vec<PartitionId>,
    ) -> Result<Vec<ParquetFile>> {
        let partition_ids = partition_ids.into_iter().collect::<HashSet<_>>();
        let stage = self.collections.lock();

        Ok(stage
            .parquet_files
            .iter()
            .filter(|f| partition_ids.contains(&f.partition_id) && f.to_delete.is_none())
            .cloned()
            .collect())
    }

    async fn get_by_object_store_id(
        &mut self,
        object_store_id: ObjectStoreId,
    ) -> Result<Option<ParquetFile>> {
        let stage = self.collections.lock();

        Ok(stage
            .parquet_files
            .iter()
            .find(|f| f.object_store_id.eq(&object_store_id))
            .cloned())
    }

    async fn exists_by_object_store_id_batch(
        &mut self,
        object_store_ids: Vec<ObjectStoreId>,
    ) -> Result<Vec<ObjectStoreId>> {
        let stage = self.collections.lock();

        Ok(stage
            .parquet_files
            .iter()
            .filter(|f| object_store_ids.contains(&f.object_store_id))
            .map(|f| f.object_store_id)
            .collect())
    }

    async fn create_upgrade_delete(
        &mut self,
        partition_id: PartitionId,
        delete: &[ObjectStoreId],
        upgrade: &[ObjectStoreId],
        create: &[ParquetFileParams],
        target_level: CompactionLevel,
    ) -> Result<Vec<ParquetFileId>> {
        let delete_set = delete.iter().copied().collect::<HashSet<_>>();
        let upgrade_set = upgrade.iter().copied().collect::<HashSet<_>>();

        assert!(
            delete_set.is_disjoint(&upgrade_set),
            "attempted to upgrade a file scheduled for delete"
        );

        let mut collections = self.collections.lock();
        let mut stage = collections.clone();

        for id in delete {
            let marked_at = Timestamp::from(self.time_provider.now());
            flag_for_delete(&mut stage, partition_id, *id, marked_at)?;
        }

        update_compaction_level(&mut stage, partition_id, upgrade, target_level)?;

        let mut ids = Vec::with_capacity(create.len());
        for file in create {
            if file.partition_id != partition_id {
                return Err(Error::External {
                    source: format!("Inconsistent ParquetFileParams, expected PartitionId({partition_id}) got PartitionId({})", file.partition_id).into(),
                });
            }
            let res = create_parquet_file(&mut stage, file.clone())?;
            ids.push(res.id);
        }

        *collections = stage;

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

fn create_or_get_column(
    stage: &mut MemCollections,
    name: &str,
    table_id: TableId,
    column_type: ColumnType,
) -> Result<Column> {
    // this block is just to ensure the mem impl correctly creates ColumnCreateLimitError in
    // tests, we don't care about any of the errors it is discarding
    stage
        .tables
        .iter()
        .find(|t| t.id == table_id)
        .cloned()
        .ok_or(Error::NotFound {
            descr: format!("table: {}", table_id),
        }) // error never used, this is just for flow control
        .and_then(|t| {
            stage
                .namespaces
                .iter()
                .find(|n| n.id == t.namespace_id)
                .cloned()
                .ok_or_else(|| Error::NotFound {
                    // we're never going to use this error, this is just for flow control,
                    // so it doesn't matter that we only have the ID, not the name
                    descr: "".to_string(),
                })
                .and_then(|n| {
                    let max_columns_per_table = n.max_columns_per_table;
                    let columns_count = stage
                        .columns
                        .iter()
                        .filter(|t| t.table_id == table_id)
                        .count();
                    if columns_count >= max_columns_per_table.get() {
                        return Err(Error::LimitExceeded {
                            descr: format!(
                                "couldn't create column {} in table {}; limit reached on namespace",
                                name, table_id
                            ),
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
                AlreadyExistsSnafu {
                    descr: format!(
                        "column {} is type {} but schema update has type {}",
                        name, c.column_type, column_type
                    ),
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

// The following three functions are helpers to the create_upgrade_delete method.
// They are also used by the respective create/flag_for_delete/update_compaction_level methods.
fn create_parquet_file(
    stage: &mut MemCollections,
    parquet_file_params: ParquetFileParams,
) -> Result<ParquetFile> {
    if stage
        .parquet_files
        .iter()
        .any(|f| f.object_store_id == parquet_file_params.object_store_id)
    {
        return Err(Error::AlreadyExists {
            descr: parquet_file_params.object_store_id.to_string(),
        });
    }

    let parquet_file = ParquetFile::from_params(
        parquet_file_params,
        ParquetFileId::new(stage.parquet_files.len() as i64 + 1),
    );
    let created_at = parquet_file.created_at;
    let partition_id = parquet_file.partition_id;
    stage.parquet_files.push(parquet_file);

    // Update the new_file_at field its partition to the time of created_at
    let partition = stage
        .partitions
        .iter_mut()
        .find(|p| p.id == partition_id)
        .ok_or(Error::NotFound {
            descr: partition_id.to_string(),
        })?;
    partition.new_file_at = Some(created_at);

    Ok(stage.parquet_files.last().unwrap().clone())
}

fn flag_for_delete(
    stage: &mut MemCollections,
    partition_id: PartitionId,
    id: ObjectStoreId,
    marked_at: Timestamp,
) -> Result<()> {
    match stage
        .parquet_files
        .iter_mut()
        .find(|p| p.object_store_id == id && p.partition_id == partition_id)
    {
        Some(f) if f.to_delete.is_none() => f.to_delete = Some(marked_at),
        _ => {
            return Err(Error::NotFound {
                descr: format!("parquet file {id} not found for delete"),
            })
        }
    }

    Ok(())
}

fn update_compaction_level(
    stage: &mut MemCollections,
    partition_id: PartitionId,
    object_store_ids: &[ObjectStoreId],
    compaction_level: CompactionLevel,
) -> Result<Vec<ObjectStoreId>> {
    let all_ids = stage
        .parquet_files
        .iter()
        .filter(|f| f.partition_id == partition_id && f.to_delete.is_none())
        .map(|f| f.object_store_id)
        .collect::<HashSet<_>>();
    for id in object_store_ids {
        if !all_ids.contains(id) {
            return Err(Error::NotFound {
                descr: format!("parquet file {id} not found for upgrade"),
            });
        }
    }

    let update_ids = object_store_ids.iter().copied().collect::<HashSet<_>>();
    let mut updated = Vec::with_capacity(object_store_ids.len());
    for f in stage
        .parquet_files
        .iter_mut()
        .filter(|p| update_ids.contains(&p.object_store_id) && p.partition_id == partition_id)
    {
        f.compaction_level = compaction_level;
        updated.push(f.object_store_id);
    }

    Ok(updated)
}

#[cfg(test)]
mod tests {
    use iox_time::SystemProvider;

    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_catalog() {
        crate::interface_tests::test_catalog(|| async {
            let metrics = Arc::new(metric::Registry::default());
            let time_provider = Arc::new(SystemProvider::new());
            let x: Arc<dyn Catalog> = Arc::new(MemCatalog::new(metrics, time_provider));
            x
        })
        .await;
    }
}
