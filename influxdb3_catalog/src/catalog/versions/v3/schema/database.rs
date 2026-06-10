use std::{sync::Arc, time::Duration};

use data_types::{Namespace, NamespaceId};
use influxdb3_id::{DbId, TableId, TriggerId};
use iox_time::{Time, TimeProvider};
use schema::Schema;

use crate::Repository;
use crate::catalog::{
    DeletedSchema, DeletionStatus, IfNotDeleted,
    versions::v3::{
        deletes::DeletionScope,
        schema::{
            cache::{DistinctCacheDefinition, LastCacheDefinition},
            retention::RetentionPeriod,
            table::TableDefinition,
            trigger::{TriggerDefinition, TriggerSpecificationDefinition},
        },
    },
};
use crate::resource::CatalogResource;

/// Definition of a database in the catalog
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct DatabaseSchema {
    /// Unique identifier for the database
    pub id: DbId,
    /// Unique user-provided name for the database
    pub name: Arc<str>,
    /// Tables contained in the database
    pub tables: Repository<TableId, TableDefinition>,
    /// Retention period for the database
    pub retention_period: RetentionPeriod,
    /// Processing engine triggers configured on the database
    pub processing_engine_triggers: Repository<TriggerId, TriggerDefinition>,
    /// Whether this database has been flagged as deleted
    pub deleted: bool,
    /// The time when the database is scheduled to be hard deleted.
    pub hard_delete_time: Option<Time>,
    /// The scope of the hard delete request, if any.
    pub hard_delete_scope: Option<DeletionScope>,
}

impl CatalogResource for DatabaseSchema {
    type Identifier = DbId;

    const CATEGORY: &'static str = "databases";

    fn id(&self) -> Self::Identifier {
        self.id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }
}

impl DatabaseSchema {
    pub fn new(id: DbId, name: Arc<str>) -> Self {
        Self {
            id,
            name,
            tables: Repository::new(),
            retention_period: RetentionPeriod::Indefinite,
            processing_engine_triggers: Repository::new(),
            deleted: false,
            hard_delete_time: None,
            hard_delete_scope: None,
        }
    }

    pub fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }

    pub fn table_count(&self) -> usize {
        self.tables.iter().filter(|table| !table.1.deleted).count()
    }

    /// Insert a [`TableDefinition`] to the `tables` map and also update the `table_map` and
    /// increment the database next id.
    ///
    /// # Implementation Note
    ///
    /// This method is intended for table definitions being inserted from a log, where the `TableId`
    /// is known, but the table does not yet exist in this instance of the `DatabaseSchema`, i.e.,
    /// on catalog initialization/replay.
    pub fn insert_table_from_log(&mut self, table_id: TableId, table_def: Arc<TableDefinition>) {
        self.tables
            .insert(table_id, table_def)
            .expect("table inserted from the log should not already exist");
    }

    pub fn table_schema_by_id(&self, table_id: &TableId) -> Option<Schema> {
        self.tables
            .get_by_id(table_id)
            .map(|table| table.influx_schema().clone())
    }

    pub fn table_definition(&self, table_name: impl AsRef<str>) -> Option<Arc<TableDefinition>> {
        self.tables.get_by_name(table_name.as_ref())
    }

    pub fn table_definition_by_id(&self, table_id: &TableId) -> Option<Arc<TableDefinition>> {
        self.tables.get_by_id(table_id)
    }

    pub fn table_ids(&self) -> Vec<TableId> {
        self.tables.id_iter().copied().collect()
    }

    pub fn table_names(&self) -> Vec<Arc<str>> {
        self.tables
            .resource_iter()
            .map(|td| Arc::clone(&td.table_name))
            .collect()
    }

    pub fn table_exists(&self, table_id: &TableId) -> bool {
        self.tables.get_by_id(table_id).is_some()
    }

    pub fn tables(&self) -> impl Iterator<Item = Arc<TableDefinition>> + use<'_> {
        self.tables.resource_iter().map(Arc::clone)
    }

    pub fn table_name_to_id(&self, table_name: impl AsRef<str>) -> Option<TableId> {
        self.tables.name_to_id(table_name.as_ref())
    }

    pub fn table_id_to_name(&self, table_id: &TableId) -> Option<Arc<str>> {
        self.tables.id_to_name(table_id)
    }

    pub fn list_distinct_caches(&self) -> Vec<Arc<DistinctCacheDefinition>> {
        self.tables
            .resource_iter()
            .filter(|t| !t.deleted)
            .flat_map(|t| t.distinct_caches.resource_iter())
            .cloned()
            .collect()
    }

    pub fn list_last_caches(&self) -> Vec<Arc<LastCacheDefinition>> {
        self.tables
            .resource_iter()
            .filter(|t| !t.deleted)
            .flat_map(|t| t.last_caches.resource_iter())
            .cloned()
            .collect()
    }

    pub fn trigger_count_by_type(&self) -> (u64, u64, u64, u64) {
        self.processing_engine_triggers.iter().fold(
            (0, 0, 0, 0),
            |(mut wal_count, mut all_wal_count, mut schedule_count, mut request_count),
             (_, trigger)| {
                match trigger.trigger {
                    // wal
                    TriggerSpecificationDefinition::SingleTableWalWrite { .. } => wal_count += 1,
                    TriggerSpecificationDefinition::AllTablesWalWrite => all_wal_count += 1,
                    // schedule
                    TriggerSpecificationDefinition::Schedule { .. }
                    | TriggerSpecificationDefinition::Every { .. } => schedule_count += 1,
                    // request
                    TriggerSpecificationDefinition::RequestPath { .. } => request_count += 1,
                };
                (wal_count, all_wal_count, schedule_count, request_count)
            },
        )
    }

    // Return the oldest allowable timestamp for the given table according to the
    // currently-available set of retention policies.
    pub fn get_retention_period_cutoff_ts_nanos(
        &self,
        now: Time,
        table_id: &TableId,
    ) -> Option<Time> {
        let table_value =
            self.table_definition_by_id(table_id)
                .and_then(|def| match def.retention_period {
                    RetentionPeriod::Duration(d) => Some(d),
                    RetentionPeriod::Indefinite => None,
                });
        let db_value = match self.retention_period {
            RetentionPeriod::Duration(d) => Some(d),
            RetentionPeriod::Indefinite => None,
        };
        let retention_period = match (db_value, table_value) {
            (_, Some(table)) => table,
            (Some(db), None) => db,
            (None, None) => return None,
        };

        now.checked_sub(retention_period)
    }

    /// Return the retention cutoff time and retention period for a given table.
    pub fn get_retention_cutoff_and_period(
        &self,
        now: Time,
        table_id: &TableId,
    ) -> Option<(Time, Duration)> {
        let table_value =
            self.table_definition_by_id(table_id)
                .and_then(|def| match def.retention_period {
                    RetentionPeriod::Duration(d) => Some(d),
                    RetentionPeriod::Indefinite => None,
                });
        let db_value = match self.retention_period {
            RetentionPeriod::Duration(d) => Some(d),
            RetentionPeriod::Indefinite => None,
        };
        let retention_period = match (db_value, table_value) {
            (_, Some(table)) => table,
            (Some(db), None) => db,
            (None, None) => return None,
        };

        now.checked_sub(retention_period)
            .map(|cutoff| (cutoff, retention_period))
    }

    /// Returns the deletion status of a table by its table ID
    ///
    /// If the table exists and is not deleted, returns `None`.
    pub fn table_deletion_status(
        &self,
        table_id: TableId,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Option<DeletionStatus> {
        match self.tables.get_by_id(&table_id) {
            Some(table_def) if table_def.deleted => Some(
                table_def
                    .hard_delete_time
                    .and_then(|time| {
                        time_provider
                            .now()
                            .checked_duration_since(time)
                            .map(DeletionStatus::Hard)
                    })
                    .unwrap_or(DeletionStatus::Soft),
            ),
            Some(_) => None,
            None => Some(DeletionStatus::NotFound),
        }
    }

    pub fn as_namespace(&self) -> Namespace {
        let retention_period_ns = match self.retention_period {
            RetentionPeriod::Indefinite => None,
            RetentionPeriod::Duration(duration) => {
                TryInto::<i64>::try_into(duration.as_nanos()).ok()
            }
        };
        Namespace {
            id: NamespaceId::new(self.id.get().into()),
            name: self.name.to_string(),
            retention_period_ns,
            deleted_at: self.hard_delete_time.map(Into::into),
            // NOTE(tjh): all of the below are IOx attributes that are not used in Core/Enterprise
            // and therefore are populated with defaults.
            max_tables: Default::default(),
            max_columns_per_table: Default::default(),
            partition_template: Default::default(),
            router_version: Default::default(),
            created_at: None,
        }
    }
}

impl DeletedSchema for DatabaseSchema {
    fn is_deleted(&self) -> bool {
        self.deleted
    }
}

impl IfNotDeleted for DatabaseSchema {
    type T = Self;

    fn if_not_deleted(self) -> Option<Self::T> {
        (!self.deleted).then_some(self)
    }
}
