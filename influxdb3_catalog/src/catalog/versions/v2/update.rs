use std::hash::Hash;
use std::ops::Add;
use std::sync::Arc;

use super::{
    CATALOG_WRITE_PERMIT, Catalog, CatalogSequenceNumber, CatalogWritePermit, ColumnDefinition,
    DatabaseSchema, FieldColumn, FieldFamilyDefinition, FieldFamilyMode, FieldFamilyName,
    NUM_FIELDS_PER_FAMILY_LIMIT, NUM_TAG_COLUMNS_LIMIT, NodeState, TableDefinition, TagColumn,
    TimestampColumn,
};
use crate::catalog::versions::v2::field::{
    FieldName, parse_field_name_auto, parse_field_name_aware,
};
use crate::catalog::{TIME_COLUMN_NAME, key};
use crate::log::versions::v4::ColumnDefinitionLog;
use crate::resource::CatalogResource;
use crate::{
    CatalogError, Result,
    catalog::{DEFAULT_OPERATOR_TOKEN_NAME, INTERNAL_DB_NAME},
    log::versions::v4::{
        AddColumnsLog, CatalogBatch, ClearRetentionPeriodLog, CreateDatabaseLog, CreateTableLog,
        DatabaseCatalogOp, DeleteDistinctCacheLog, DeleteLastCacheLog, DeleteOp,
        DeleteTokenDetails, DeleteTriggerLog, DistinctCacheDefinition, FieldDataType,
        FieldFamilyDefinitionLog, GenerationOp, LastCacheDefinition, LastCacheSize, LastCacheTtl,
        LastCacheValueColumnsDef, MaxAge, MaxCardinality, NodeCatalogOp, NodeMode,
        OrderedCatalogBatch, RegisterNodeLog, RetentionPeriod, SetGenerationDurationLog,
        SetRetentionPeriodLog, SoftDeleteDatabaseLog, SoftDeleteTableLog, StopNodeLog, TokenBatch,
        TokenCatalogOp, TriggerDefinition, TriggerIdentifier, TriggerSettings,
        TriggerSpecificationDefinition, ValidPluginFilename,
    },
    object_store::PersistCatalogResult,
};
use bimap::BiHashMap;
use hashbrown::HashMap;
use indexmap::IndexMap;
use influxdb3_id::{
    CatalogId, ColumnIdentifier, DbId, FieldFamilyId, FieldIdentifier, TableId, TagId,
};
use influxdb3_process::ProcessUuidGetter;
use iox_time::{Time, TimeProvider};
use observability_deps::tracing::{debug, error, info, trace};
use schema::{InfluxColumnType, InfluxFieldType};
use std::time::Duration;
use uuid::Uuid;

#[derive(Clone, Copy, Debug)]
pub enum HardDeletionTime {
    /// The object will never be hard deleted.
    Never,
    /// The object will be hard deleted after the default duration.
    Default,
    /// The object will be hard deleted at a specific timestamp.
    Timestamp(Time),
    /// The object will be hard deleted as soon as possible.
    Now,
}

impl HardDeletionTime {
    fn as_time(
        self,
        time_provider: &dyn TimeProvider,
        default: Duration,
    ) -> Option<iox_time::Time> {
        match self {
            HardDeletionTime::Never => None,
            HardDeletionTime::Default => Some(time_provider.now().add(default)),
            HardDeletionTime::Timestamp(time) => Some(time),
            HardDeletionTime::Now => Some(time_provider.now()),
        }
    }

    fn is_default(self) -> bool {
        matches!(self, HardDeletionTime::Default)
    }
}

impl std::fmt::Display for HardDeletionTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HardDeletionTime::Never => write!(f, "never"),
            HardDeletionTime::Default => write!(f, "default"),
            HardDeletionTime::Timestamp(time) => write!(f, "{time}"),
            HardDeletionTime::Now => write!(f, "now"),
        }
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct CreateDatabaseOptions {
    pub retention_period: Option<Duration>,
}

#[derive(Default, Debug, Clone, Copy)]
pub struct CreateTableOptions {
    pub field_family_mode: FieldFamilyMode,
}

impl Catalog {
    pub fn begin(&self, db_name: &str) -> Result<DatabaseCatalogTransaction> {
        debug!(db_name, "starting catalog transaction");
        let inner = self.inner.read();
        match self.db_schema(db_name) {
            Some(database_schema) => Ok(DatabaseCatalogTransaction {
                catalog_sequence: inner.sequence_number(),
                current_table_count: inner.table_count(),
                table_limit: self.num_tables_limit(),
                time_ns: self.time_provider.now().timestamp_nanos(),
                database_schema: Arc::clone(&database_schema),
                tables: Repo::new(),
                next_table_id: database_schema.tables.next_id,
                ops: vec![],
                columns_per_table_limit: self.num_columns_per_table_limit(),
            }),
            None => {
                if inner.database_count() >= self.num_dbs_limit() {
                    return Err(CatalogError::TooManyDbs(self.num_dbs_limit()));
                }
                drop(inner);
                let mut inner = self.inner.write();
                let database_id = inner.databases.get_and_increment_next_id();
                let database_name = Arc::from(db_name);
                let database_schema =
                    Arc::new(DatabaseSchema::new(database_id, Arc::clone(&database_name)));
                let retention_period = match database_schema.retention_period {
                    RetentionPeriod::Duration(duration) => Some(duration),
                    RetentionPeriod::Indefinite => None,
                };
                let time_ns = self.time_provider.now().timestamp_nanos();
                let ops = vec![DatabaseCatalogOp::CreateDatabase(CreateDatabaseLog {
                    database_id,
                    database_name,
                    retention_period,
                })];
                Ok(DatabaseCatalogTransaction {
                    catalog_sequence: inner.sequence_number(),
                    current_table_count: inner.table_count(),
                    table_limit: self.num_tables_limit(),
                    time_ns,
                    database_schema,
                    tables: Repo::new(),
                    next_table_id: 0.into(),
                    ops,
                    columns_per_table_limit: self.num_columns_per_table_limit(),
                })
            }
        }
    }

    pub async fn commit(
        &self,
        txn: DatabaseCatalogTransaction,
    ) -> Result<Prompt<CatalogSequenceNumber>> {
        if txn.is_empty() {
            return Ok(Prompt::Success(txn.sequence_number()));
        }

        let (batch, seq) = txn.catalog_batch();

        match self.get_permit_and_verify_catalog_batch(batch, seq).await {
            Prompt::Success((ordered_batch, permit)) => {
                match self
                    .persist_ordered_batch_to_object_store(&ordered_batch, &permit)
                    .await?
                {
                    UpdatePrompt::Retry => Ok(Prompt::Retry(())),
                    UpdatePrompt::Applied => {
                        self.apply_ordered_catalog_batch(&ordered_batch, &permit);
                        self.background_checkpoint(&ordered_batch);
                        self.broadcast_update(ordered_batch.into_batch()).await?;
                        Ok(Prompt::Success(self.sequence_number()))
                    }
                }
            }
            Prompt::Retry(_) => Ok(Prompt::Retry(())),
        }
    }

    pub async fn set_gen1_duration(&self, duration: Duration) -> Result<OrderedCatalogBatch> {
        info!(duration_ns = duration.as_nanos(), "set gen1 duration");
        self.catalog_update_with_retry(|| {
            let time_ns = self.time_provider.now().timestamp_nanos();
            if let Some(existing) = self.get_generation_duration(1) {
                if duration != existing {
                    return Err(CatalogError::CannotChangeGenerationDuration {
                        level: 1,
                        existing: existing.into(),
                        attempted: duration.into(),
                    });
                } else {
                    return Err(CatalogError::AlreadyExists);
                }
            }
            Ok(CatalogBatch::generation(
                time_ns,
                vec![GenerationOp::SetGenerationDuration(
                    SetGenerationDurationLog { level: 1, duration },
                )],
            ))
        })
        .await
    }

    pub async fn register_node(
        &self,
        node_id: &str,
        core_count: u64,
        mode: Vec<NodeMode>,
        process_uuid_getter: Arc<dyn ProcessUuidGetter>,
    ) -> Result<OrderedCatalogBatch> {
        info!(node_id, core_count, mode = ?mode, "register node");
        let process_uuid = *process_uuid_getter.get_process_uuid();
        self.catalog_update_with_retry(|| {
            let time_ns = self.time_provider.now().timestamp_nanos();
            let (node_catalog_id, node_id, instance_id) = if let Some(node) = self.node(node_id) {
                if let NodeState::Running { .. } = node.state {
                    // If the node is in the catalog as `Running`, that could mean that a previous
                    // process that started the node did not stop gracefully. We just log this here
                    // and do not fail the operation. It is assumed that this catalog update will be
                    // handled via catalog broadcast to shut any existing processes down that are
                    // operating as this `node_id`.
                    info!(
                        node_id,
                        instance_id = node.instance_id.as_ref(),
                        "registering node to catalog that was not previously de-registered"
                    );
                }
                (
                    node.node_catalog_id,
                    Arc::clone(&node.node_id),
                    Arc::clone(&node.instance_id),
                )
            } else {
                let instance_id = Arc::<str>::from(Uuid::new_v4().to_string().as_str());
                info!(
                    node_id,
                    instance_id = instance_id.as_ref(),
                    "registering new node to the catalog"
                );
                let mut inner = self.inner.write();
                let node_catalog_id = inner.nodes.get_and_increment_next_id();
                (node_catalog_id, node_id.into(), instance_id)
            };
            Ok(CatalogBatch::node(
                time_ns,
                node_catalog_id,
                Arc::clone(&node_id),
                vec![NodeCatalogOp::RegisterNode(RegisterNodeLog {
                    node_id,
                    instance_id,
                    registered_time_ns: time_ns,
                    core_count,
                    mode: mode.clone(),
                    process_uuid,
                })],
            ))
        })
        .await
    }

    pub async fn update_node_state_stopped(
        &self,
        node_id: &str,
        process_uuid_getter: Arc<dyn ProcessUuidGetter>,
    ) -> Result<OrderedCatalogBatch> {
        let process_uuid = *process_uuid_getter.get_process_uuid();
        info!(
            node_id,
            %process_uuid,
            "updating node state to Stopped in catalog"
        );
        self.catalog_update_with_retry(|| {
            let time_ns = self.time_provider.now().timestamp_nanos();
            let Some(node) = self.node(node_id) else {
                return Err(crate::CatalogError::NotFound);
            };
            if !node.is_running() {
                return Err(crate::CatalogError::NodeAlreadyStopped {
                    node_id: Arc::clone(&node.node_id),
                });
            }
            Ok(CatalogBatch::node(
                time_ns,
                node.node_catalog_id,
                Arc::clone(&node.node_id),
                vec![NodeCatalogOp::StopNode(StopNodeLog {
                    node_id: Arc::clone(&node.node_id),
                    stopped_time_ns: time_ns,
                    process_uuid,
                })],
            ))
        })
        .await
    }

    pub async fn create_database(&self, name: &str) -> Result<OrderedCatalogBatch> {
        self.create_database_opts(name, CreateDatabaseOptions::default())
            .await
    }

    pub async fn create_database_opts(
        &self,
        name: &str,
        options: CreateDatabaseOptions,
    ) -> Result<OrderedCatalogBatch> {
        info!(name, "create database");
        self.catalog_update_with_retry(|| {
            let (_, Some(batch)) = self.db_or_create(
                name,
                options.retention_period,
                self.time_provider.now().timestamp_nanos(),
            )?
            else {
                return Err(CatalogError::AlreadyExists);
            };
            Ok(batch)
        })
        .await
    }

    pub async fn soft_delete_database(
        &self,
        name: &str,
        hard_delete_time: HardDeletionTime,
    ) -> Result<OrderedCatalogBatch> {
        self.catalog_update_with_retry(|| {
            if name == INTERNAL_DB_NAME {
                return Err(CatalogError::CannotDeleteInternalDatabase);
            };

            let Some(db) = self.db_schema(name) else {
                return Err(CatalogError::NotFound);
            };

            // If the request specifies the default hard-delete time, and the schema has an existing hard_delete_time,
            // use that for the default, so the DELETE operation is idempotent.
            let resolved_hard_delete_time = if hard_delete_time.is_default() && let Some(existing) = db.hard_delete_time {
                Some(existing)
            } else {
                hard_delete_time.as_time(&self.time_provider, self.default_hard_delete_duration())
            };

            let hard_delete_changed = db.hard_delete_time != resolved_hard_delete_time;
            if db.deleted && !hard_delete_changed {
                return Err(CatalogError::AlreadyDeleted);
            }
            let deletion_time = self.time_provider.now().timestamp_nanos();
            let database_id = db.id;
            Ok(CatalogBatch::database(
                deletion_time,
                database_id,
                db.name(),
                vec![DatabaseCatalogOp::SoftDeleteDatabase(
                    SoftDeleteDatabaseLog {
                        database_id,
                        database_name: db.name(),
                        deletion_time,
                        hard_deletion_time: resolved_hard_delete_time.map(|t|t.timestamp_nanos()),
                    },
                )],
            ))
        })
        .await
        .inspect(|batch| {
            let Some(op) = batch
                .catalog_batch
                .as_database()
                .and_then(|db| db.ops.first())
                .and_then(|op| op.as_soft_delete_database())
            else {
                return;
            };

            info!(db_name = %op.database_name, db_id = %op.database_id, %hard_delete_time, "Delete database.");
        })
    }

    pub async fn create_table(
        &self,
        db_name: &str,
        table_name: &str,
        tags: &[impl AsRef<str> + Send + Sync],
        fields: &[(impl AsRef<str> + Send + Sync, FieldDataType)],
    ) -> Result<OrderedCatalogBatch> {
        self.create_table_opts(
            db_name,
            table_name,
            tags,
            fields,
            CreateTableOptions::default(),
        )
        .await
    }

    pub async fn create_table_opts(
        &self,
        db_name: &str,
        table_name: &str,
        tags: &[impl AsRef<str> + Send + Sync],
        fields: &[(impl AsRef<str> + Send + Sync, FieldDataType)],
        options: CreateTableOptions,
    ) -> Result<OrderedCatalogBatch> {
        info!(db_name, table_name, "create table");
        self.catalog_update_with_retry(|| {
            let mut txn = self.begin(db_name)?;
            txn.create_table(
                table_name,
                Some(CreateTableColumns { tags, fields }),
                options.field_family_mode,
            )?;
            Ok(txn.into())
        })
        .await
    }

    pub async fn soft_delete_table(
        &self,
        db_name: &str,
        table_name: &str,
        hard_delete_time: HardDeletionTime,
    ) -> Result<OrderedCatalogBatch> {
        self.catalog_update_with_retry(|| {
            let Some(db) = self.db_schema(db_name) else {
                return Err(CatalogError::NotFound);
            };
            let Some(tbl_def) = db.table_definition(table_name) else {
                return Err(CatalogError::NotFound);
            };

            // If the request specifies the default hard-delete time, and the schema has an existing hard_delete_time,
            // use that for the default, so the DELETE operation is idempotent.
            let resolved_hard_delete_time = if hard_delete_time.is_default() && let Some(existing) = tbl_def.hard_delete_time {
                Some(existing)
            } else {
                hard_delete_time.as_time(&self.time_provider, self.default_hard_delete_duration())
            };

            let hard_delete_changed = tbl_def.hard_delete_time != resolved_hard_delete_time;
            if tbl_def.deleted && !hard_delete_changed {
                return Err(CatalogError::AlreadyDeleted);
            }
            let deletion_time = self.time_provider.now().timestamp_nanos();
            Ok(CatalogBatch::database(
                deletion_time,
                db.id,
                db.name(),
                vec![DatabaseCatalogOp::SoftDeleteTable(SoftDeleteTableLog {
                    database_id: db.id,
                    database_name: Arc::clone(&db.name),
                    table_id: tbl_def.table_id,
                    table_name: Arc::clone(&tbl_def.table_name),
                    deletion_time,
                    hard_deletion_time: resolved_hard_delete_time.map(|t|t.timestamp_nanos()),
                })],
            ))
        })
        .await
            .inspect(|batch| {
                let Some(op) = batch
                    .catalog_batch
                    .as_database()
                    .and_then(|db| db.ops.first())
                    .and_then(|op| op.as_soft_delete_table())
                else {
                    return;
                };

                info!(db_name = %op.database_name, db_id = %op.database_id, table_name = %op.table_name, table_id = %op.table_id, %hard_delete_time, "Delete table.")
            })
    }

    /// Permanently delete a table from the catalog.
    ///
    /// This function performs a hard deletion of a table, which means the table
    /// will be completely removed from the catalog and marked for cleanup in the
    /// deleted objects tracking system.
    ///
    /// # Errors
    /// * `CatalogError::NotFound` - If the database or table doesn't exist
    pub async fn hard_delete_table(
        &self,
        db_id: &DbId,
        table_id: &TableId,
    ) -> Result<OrderedCatalogBatch> {
        info!(?db_id, ?table_id, "Hard delete table.");
        self.catalog_update_with_retry(|| {
            let Some(db) = self.db_schema_by_id(db_id) else {
                return Err(CatalogError::NotFound);
            };
            let Some(_table_def) = db.table_definition_by_id(table_id) else {
                return Err(CatalogError::NotFound);
            };

            let deletion_time = self.time_provider.now().timestamp_nanos();
            Ok(CatalogBatch::delete(
                deletion_time,
                vec![DeleteOp::DeleteTable(*db_id, *table_id)],
            ))
        })
        .await
    }

    /// Permanently delete a database from the catalog.
    ///
    /// This function performs a hard deletion of a database, which means the database
    /// will be completely removed from the catalog and marked for cleanup in the
    /// deleted objects tracking system. All tables within the database are implicitly
    /// deleted.
    ///
    /// # Errors
    /// * `CatalogError::NotFound` - If the database doesn't exist
    /// * `CatalogError::CannotDeleteInternalDatabase` - If attempting to delete the internal database
    pub async fn hard_delete_database(&self, db_id: &DbId) -> Result<OrderedCatalogBatch> {
        info!(?db_id, "Hard delete database.");
        self.catalog_update_with_retry(|| {
            let Some(db) = self.db_schema_by_id(db_id) else {
                return Err(CatalogError::NotFound);
            };

            // Prevent deletion of internal database
            if db.name.as_ref() == INTERNAL_DB_NAME {
                return Err(CatalogError::CannotDeleteInternalDatabase);
            }

            let deletion_time = self.time_provider.now().timestamp_nanos();
            Ok(CatalogBatch::delete(
                deletion_time,
                vec![DeleteOp::DeleteDatabase(*db_id)],
            ))
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_distinct_cache(
        &self,
        db_name: &str,
        table_name: &str,
        cache_name: Option<&str>,
        columns: &[impl AsRef<str> + Send + Sync],
        max_cardinality: MaxCardinality,
        max_age_seconds: MaxAge,
    ) -> Result<OrderedCatalogBatch> {
        info!(db_name, table_name, cache_name = ?cache_name, "create distinct cache");
        self.catalog_update_with_retry(|| {
            let Some(db) = self.db_schema(db_name) else {
                return Err(CatalogError::NotFound);
            };
            let Some(mut tbl) = db.table_definition(table_name) else {
                return Err(CatalogError::NotFound);
            };
            if columns.is_empty() {
                return Err(CatalogError::invalid_configuration(
                    "no columns provided when creating distinct cache",
                ));
            }

            fn is_valid_distinct_cache_type(def: &ColumnDefinition) -> bool {
                matches!(
                    def.column_type(),
                    InfluxColumnType::Tag | InfluxColumnType::Field(InfluxFieldType::String),
                )
            }

            let (column_ids, col_names) = columns
                .iter()
                .map(|name| {
                    tbl.column_definition(name.as_ref())
                        .ok_or_else(|| {
                            CatalogError::invalid_configuration(
                                format!("invalid column provided: {name}", name = name.as_ref())
                                    .as_str(),
                            )
                        })
                        .and_then(|def| {
                            if is_valid_distinct_cache_type(&def) {
                                Ok((def.id(), name.as_ref().to_string()))
                            } else {
                                Err(CatalogError::InvalidDistinctCacheColumnType)
                            }
                        })
                })
                .collect::<Result<(Vec<ColumnIdentifier>, Vec<String>)>>()?;
            let cache_name = cache_name.map(Arc::from).unwrap_or_else(|| {
                format!(
                    "{table_name}_{cols}_distinct_cache",
                    cols = col_names.join("_")
                )
                .as_str()
                .into()
            });
            if tbl.distinct_caches.contains_name(&cache_name) {
                return Err(CatalogError::AlreadyExists);
            }
            let cache_id = Arc::make_mut(&mut tbl)
                .distinct_caches
                .get_and_increment_next_id();
            Ok(CatalogBatch::database(
                self.time_provider.now().timestamp_nanos(),
                db.id,
                db.name(),
                vec![DatabaseCatalogOp::CreateDistinctCache(
                    DistinctCacheDefinition {
                        table_id: tbl.table_id,
                        table_name: Arc::clone(&tbl.table_name),
                        cache_id,
                        cache_name,
                        column_ids,
                        max_cardinality,
                        max_age_seconds,
                    },
                )],
            ))
        })
        .await
    }

    pub async fn delete_distinct_cache(
        &self,
        db_name: &str,
        table_name: &str,
        cache_name: &str,
    ) -> Result<OrderedCatalogBatch> {
        info!(db_name, table_name, cache_name, "delete distinct cache");
        self.catalog_update_with_retry(|| {
            let Some(db) = self.db_schema(db_name) else {
                return Err(CatalogError::NotFound);
            };
            let Some(tbl) = db.table_definition(table_name) else {
                return Err(CatalogError::NotFound);
            };
            let Some(cache) = tbl.distinct_caches.get_by_name(cache_name) else {
                return Err(CatalogError::NotFound);
            };
            Ok(CatalogBatch::database(
                self.time_provider.now().timestamp_nanos(),
                db.id,
                db.name(),
                vec![DatabaseCatalogOp::DeleteDistinctCache(
                    DeleteDistinctCacheLog {
                        table_id: tbl.table_id,
                        table_name: Arc::clone(&tbl.table_name),
                        cache_id: cache.cache_id,
                        cache_name: Arc::clone(&cache.cache_name),
                    },
                )],
            ))
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_last_cache(
        &self,
        db_name: &str,
        table_name: &str,
        cache_name: Option<&str>,
        key_columns: Option<&[impl AsRef<str> + Send + Sync]>,
        value_columns: Option<&[impl AsRef<str> + Send + Sync]>,
        count: LastCacheSize,
        ttl: LastCacheTtl,
    ) -> Result<OrderedCatalogBatch> {
        info!(db_name, table_name, cache_name = ?cache_name, "create last cache");
        self.catalog_update_with_retry(|| {
            let Some(db) = self.db_schema(db_name) else {
                return Err(CatalogError::NotFound);
            };
            let Some(mut tbl) = db.table_definition(table_name) else {
                return Err(CatalogError::NotFound);
            };

            fn is_valid_last_cache_key_col(def: &ColumnDefinition) -> bool {
                matches!(
                    def.column_type(),
                    InfluxColumnType::Tag
                        | InfluxColumnType::Field(
                            InfluxFieldType::String
                                | InfluxFieldType::Integer
                                | InfluxFieldType::UInteger
                                | InfluxFieldType::Boolean
                        ),
                )
            }

            let (key_ids, key_names) = if let Some(key_columns) = key_columns {
                key_columns
                    .iter()
                    .map(|name| {
                        tbl.column_definition(name.as_ref())
                            .ok_or_else(|| {
                                CatalogError::invalid_configuration(
                                    format!(
                                        "invalid key column provided: {name}",
                                        name = name.as_ref()
                                    )
                                    .as_str(),
                                )
                            })
                            .and_then(|def| {
                                if is_valid_last_cache_key_col(&def) {
                                    Ok((def.id(), name.as_ref().to_string()))
                                } else {
                                    Err(CatalogError::InvalidLastCacheKeyColumnType)
                                }
                            })
                    })
                    .collect::<Result<(Vec<ColumnIdentifier>, Vec<String>)>>()?
            } else {
                tbl.series_key
                    .iter()
                    .map(|id| {
                        tbl.column_definition_by_id(&ColumnIdentifier::tag(*id))
                            .expect("column id in series key should be valid")
                    })
                    .map(|def| Ok((def.id(), def.name().to_string())))
                    .collect::<Result<(Vec<ColumnIdentifier>, Vec<String>)>>()?
            };

            let value_columns = if let Some(value_columns) = value_columns {
                let columns = value_columns
                    .iter()
                    .map(|name| {
                        tbl.column_definition(name.as_ref())
                            .map(|def| def.id())
                            .ok_or_else(|| {
                                CatalogError::invalid_configuration(
                                    format!(
                                        "invalid value column provided: {name}",
                                        name = name.as_ref()
                                    )
                                    .as_str(),
                                )
                            })
                    })
                    .collect::<Result<Vec<ColumnIdentifier>>>()?;
                LastCacheValueColumnsDef::Explicit { columns }
            } else {
                LastCacheValueColumnsDef::AllNonKeyColumns
            };

            let cache_name = cache_name.map(Arc::from).unwrap_or_else(|| {
                format!("{table_name}_{cols}_last_cache", cols = key_names.join("_"))
                    .as_str()
                    .into()
            });
            if tbl.last_caches.contains_name(&cache_name) {
                return Err(CatalogError::AlreadyExists);
            }
            let cache_id = Arc::make_mut(&mut tbl)
                .last_caches
                .get_and_increment_next_id();
            Ok(CatalogBatch::database(
                self.time_provider.now().timestamp_nanos(),
                db.id,
                db.name(),
                vec![DatabaseCatalogOp::CreateLastCache(LastCacheDefinition {
                    table_id: tbl.table_id,
                    table: Arc::clone(&tbl.table_name),
                    id: cache_id,
                    name: cache_name,
                    key_columns: key_ids,
                    value_columns,
                    count,
                    ttl,
                })],
            ))
        })
        .await
    }

    pub async fn delete_last_cache(
        &self,
        db_name: &str,
        table_name: &str,
        cache_name: &str,
    ) -> Result<OrderedCatalogBatch> {
        info!(db_name, table_name, cache_name, "delete last cache");
        self.catalog_update_with_retry(|| {
            let Some(db) = self.db_schema(db_name) else {
                return Err(CatalogError::NotFound);
            };
            let Some(tbl) = db.table_definition(table_name) else {
                return Err(CatalogError::NotFound);
            };
            let Some(cache) = tbl.last_caches.get_by_name(cache_name) else {
                return Err(CatalogError::NotFound);
            };
            Ok(CatalogBatch::database(
                self.time_provider.now().timestamp_nanos(),
                db.id,
                db.name(),
                vec![DatabaseCatalogOp::DeleteLastCache(DeleteLastCacheLog {
                    table_id: tbl.table_id,
                    table_name: Arc::clone(&tbl.table_name),
                    id: cache.id,
                    name: Arc::clone(&cache.name),
                })],
            ))
        })
        .await
    }

    /// Insert a new trigger for the processing engine
    #[allow(clippy::too_many_arguments)]
    pub async fn create_processing_engine_trigger(
        &self,
        db_name: &str,
        trigger_name: &str,
        node_id: Arc<str>,
        plugin_filename: ValidPluginFilename<'_>,
        trigger_specification: &str,
        trigger_settings: TriggerSettings,
        trigger_arguments: &Option<HashMap<String, String>>,
        disabled: bool,
    ) -> Result<OrderedCatalogBatch> {
        info!(db_name, trigger_name, "create processing engine trigger");
        self.catalog_update_with_retry(|| {
            let Some(mut db) = self.db_schema(db_name) else {
                return Err(CatalogError::NotFound);
            };
            let trigger = TriggerSpecificationDefinition::from_string_rep(trigger_specification)?;
            if db.processing_engine_triggers.contains_name(trigger_name) {
                return Err(CatalogError::AlreadyExists);
            }
            let trigger_id = Arc::make_mut(&mut db)
                .processing_engine_triggers
                .get_and_increment_next_id();
            Ok(CatalogBatch::database(
                self.time_provider.now().timestamp_nanos(),
                db.id,
                db.name(),
                vec![DatabaseCatalogOp::CreateTrigger(TriggerDefinition {
                    trigger_id,
                    trigger_name: trigger_name.into(),
                    plugin_filename: plugin_filename.to_string(),
                    database_name: Arc::clone(&db.name),
                    node_id: Arc::clone(&node_id),
                    trigger,
                    trigger_settings,
                    trigger_arguments: trigger_arguments.clone(),
                    disabled,
                })],
            ))
        })
        .await
    }

    pub async fn delete_processing_engine_trigger(
        &self,
        db_name: &str,
        trigger_name: &str,
        force: bool,
    ) -> Result<OrderedCatalogBatch> {
        info!(db_name, trigger_name, "delete processing engine trigger");
        self.catalog_update_with_retry(|| {
            let Some(db) = self.db_schema(db_name) else {
                return Err(CatalogError::NotFound);
            };
            let Some(trigger) = db.processing_engine_triggers.get_by_name(trigger_name) else {
                return Err(CatalogError::NotFound);
            };
            if !trigger.disabled && !force {
                return Err(CatalogError::ProcessingEngineTriggerRunning {
                    trigger_name: trigger.trigger_name.to_string(),
                });
            }
            Ok(CatalogBatch::database(
                self.time_provider.now().timestamp_nanos(),
                db.id,
                db.name(),
                vec![DatabaseCatalogOp::DeleteTrigger(DeleteTriggerLog {
                    trigger_id: trigger.trigger_id,
                    trigger_name: Arc::clone(&trigger.trigger_name),
                    force,
                })],
            ))
        })
        .await
    }

    pub async fn enable_processing_engine_trigger(
        &self,
        db_name: &str,
        trigger_name: &str,
    ) -> Result<OrderedCatalogBatch> {
        info!(db_name, trigger_name, "enable processing engine trigger");
        self.catalog_update_with_retry(|| {
            let Some(db) = self.db_schema(db_name) else {
                return Err(CatalogError::NotFound);
            };
            let Some(trigger) = db.processing_engine_triggers.get_by_name(trigger_name) else {
                return Err(CatalogError::NotFound);
            };
            if !trigger.disabled {
                return Err(CatalogError::TriggerAlreadyEnabled);
            }
            Ok(CatalogBatch::database(
                self.time_provider.now().timestamp_nanos(),
                db.id,
                db.name(),
                vec![DatabaseCatalogOp::EnableTrigger(TriggerIdentifier {
                    db_id: db.id,
                    db_name: Arc::clone(&db.name),
                    trigger_id: trigger.trigger_id,
                    trigger_name: Arc::clone(&trigger.trigger_name),
                })],
            ))
        })
        .await
    }

    pub async fn disable_processing_engine_trigger(
        &self,
        db_name: &str,
        trigger_name: &str,
    ) -> Result<OrderedCatalogBatch> {
        info!(db_name, trigger_name, "disable processing engine trigger");
        self.catalog_update_with_retry(|| {
            let Some(db) = self.db_schema(db_name) else {
                return Err(CatalogError::NotFound);
            };
            let Some(trigger) = db.processing_engine_triggers.get_by_name(trigger_name) else {
                return Err(CatalogError::NotFound);
            };
            if trigger.disabled {
                return Err(CatalogError::TriggerAlreadyDisabled);
            }
            Ok(CatalogBatch::database(
                self.time_provider.now().timestamp_nanos(),
                db.id,
                db.name(),
                vec![DatabaseCatalogOp::DisableTrigger(TriggerIdentifier {
                    db_id: db.id,
                    db_name: Arc::clone(&db.name),
                    trigger_id: trigger.trigger_id,
                    trigger_name: Arc::clone(&trigger.trigger_name),
                })],
            ))
        })
        .await
    }

    pub async fn delete_token(&self, token_name: &str) -> Result<OrderedCatalogBatch> {
        info!(token_name, "delete token");

        if token_name == DEFAULT_OPERATOR_TOKEN_NAME {
            return Err(CatalogError::CannotDeleteOperatorToken);
        }

        self.catalog_update_with_retry(|| {
            if !self.inner.read().tokens.repo().contains_name(token_name) {
                // maybe deleted by another node or genuinely not present
                return Err(CatalogError::NotFound);
            }

            Ok(CatalogBatch::Token(TokenBatch {
                time_ns: self.time_provider.now().timestamp_nanos(),
                ops: vec![TokenCatalogOp::DeleteToken(DeleteTokenDetails {
                    token_name: token_name.to_owned(),
                })],
            }))
        })
        .await
    }

    pub async fn set_retention_period_for_database(
        &self,
        db_name: &str,
        duration: Duration,
    ) -> Result<OrderedCatalogBatch> {
        info!(
            db_name,
            duration_ns = duration.as_nanos(),
            "create new retention policy"
        );
        let Some(db) = self.db_schema(db_name) else {
            return Err(CatalogError::NotFound);
        };
        self.catalog_update_with_retry(|| {
            Ok(CatalogBatch::database(
                self.time_provider.now().timestamp_nanos(),
                db.id,
                db.name(),
                vec![DatabaseCatalogOp::SetRetentionPeriod(
                    SetRetentionPeriodLog {
                        database_name: db.name(),
                        database_id: db.id,
                        retention_period: RetentionPeriod::Duration(duration),
                    },
                )],
            ))
        })
        .await
    }

    pub async fn clear_retention_period_for_database(
        &self,
        db_name: &str,
    ) -> Result<OrderedCatalogBatch> {
        info!(db_name, "delete retention policy");
        let Some(db) = self.db_schema(db_name) else {
            return Err(CatalogError::NotFound);
        };
        self.catalog_update_with_retry(|| {
            Ok(CatalogBatch::database(
                self.time_provider.now().timestamp_nanos(),
                db.id,
                db.name(),
                vec![DatabaseCatalogOp::ClearRetentionPeriod(
                    ClearRetentionPeriodLog {
                        database_name: db.name(),
                        database_id: db.id,
                    },
                )],
            ))
        })
        .await
    }

    /// Perform a catalog update and retry if the catalog has been updated elsewhere until the
    /// operation succeeds or fails
    pub(crate) async fn catalog_update_with_retry<F>(
        &self,
        batch_creator_fn: F,
    ) -> Result<OrderedCatalogBatch>
    where
        F: Fn() -> Result<CatalogBatch>,
    {
        // NOTE(trevor/catalog-refactor): should there be a limit number of retries, or use a
        // timeout somewhere?
        loop {
            let sequence = self.sequence_number();
            let batch = batch_creator_fn()?;
            match self
                .get_permit_and_verify_catalog_batch(batch, sequence)
                .await
            {
                Prompt::Success((ordered_batch, permit)) => {
                    match self
                        .persist_ordered_batch_to_object_store(&ordered_batch, &permit)
                        .await?
                    {
                        UpdatePrompt::Retry => continue,
                        UpdatePrompt::Applied => {
                            self.apply_ordered_catalog_batch(&ordered_batch, &permit);
                            self.background_checkpoint(&ordered_batch);
                            self.broadcast_update(ordered_batch.clone().into_batch())
                                .await?;
                            return Ok(ordered_batch);
                        }
                    }
                }
                Prompt::Retry(_) => continue,
            }
        }
    }

    pub async fn update_to_sequence_number(&self, update_to: CatalogSequenceNumber) -> Result<()> {
        let permit = CATALOG_WRITE_PERMIT.lock().await;
        let start_sequence_number = self.sequence_number().next();
        if start_sequence_number > update_to {
            return Ok(());
        }
        self.load_and_update_from_object_store(start_sequence_number, Some(update_to), &permit)
            .await
    }

    /// Persist an `OrderedCatalogBatch` to the object store catalog. This handles broadcast of
    /// catalog updates for any unseen catalog files that need to be fetched as part of this
    /// operation, as well as the update applied by the ordered batch itself, if successful.
    async fn persist_ordered_batch_to_object_store(
        &self,
        ordered_batch: &OrderedCatalogBatch,
        permit: &CatalogWritePermit,
    ) -> Result<UpdatePrompt> {
        trace!(?ordered_batch, "persisting ordered batch to store");
        // TODO: maybe just an error?
        assert_eq!(
            ordered_batch.sequence_number(),
            **permit,
            "tried to update catalog with invalid sequence"
        );

        match self
            .store
            .persist_catalog_sequenced_log(ordered_batch)
            .await
            .inspect_err(|error| debug!(?error, "failed on persist of next catalog sequence"))?
        {
            PersistCatalogResult::Success => Ok(UpdatePrompt::Applied),
            PersistCatalogResult::AlreadyExists => {
                self.load_and_update_from_object_store(
                    ordered_batch.sequence_number(),
                    None,
                    permit,
                )
                .await?;
                self.metrics.catalog_operation_retries.inc(1);
                Ok(UpdatePrompt::Retry)
            }
        }
    }

    /// Load catalog updates from the object store, starting from `sequence_number`, and going until
    /// `update_until`, or if None is provided, will update until a NOT_FOUND is received from the
    /// object store.
    pub(crate) async fn load_and_update_from_object_store(
        &self,
        mut sequence_number: CatalogSequenceNumber,
        update_until: Option<CatalogSequenceNumber>,
        permit: &CatalogWritePermit,
    ) -> Result<()> {
        while let Some(ordered_catalog_batch) = self
            .store
            .load_catalog_sequenced_log(sequence_number)
            .await
            .inspect_err(|error| debug!(?error, "failed to fetch next catalog sequence"))?
        {
            let batch = self.apply_ordered_catalog_batch(&ordered_catalog_batch, permit);
            self.broadcast_update(batch).await?;
            sequence_number = sequence_number.next();
            if update_until.is_some_and(|max_sequence| sequence_number > max_sequence) {
                break;
            }
            if self.state.lock().is_shutdown() {
                break;
            }
        }
        Ok(())
    }

    /// Broadcast a `CatalogUpdate` to all subscribed components in the system.
    async fn broadcast_update(&self, update: impl Into<CatalogUpdate>) -> Result<()> {
        self.subscriptions
            .write()
            .await
            .send_update(Arc::new(update.into()))
            .await?;
        Ok(())
    }

    /// Persist the catalog as a checkpoint in the background if we are at the _n_th sequence
    /// number.
    fn background_checkpoint(&self, ordered_batch: &OrderedCatalogBatch) {
        if ordered_batch.sequence_number().get() % self.store.checkpoint_interval != 0 {
            return;
        }
        let snapshot = self.snapshot();
        let Err(error) = self.store.background_persist_catalog_checkpoint(&snapshot) else {
            return;
        };
        error!(
            ?error,
            "failed to serialize the catalog to a checkpoint and persist it to \
            object store in the background"
        );
    }
}

impl From<Vec<CatalogBatch>> for CatalogUpdate {
    fn from(batches: Vec<CatalogBatch>) -> Self {
        Self { batches }
    }
}

impl From<CatalogBatch> for CatalogUpdate {
    fn from(batch: CatalogBatch) -> Self {
        Self {
            batches: vec![batch],
        }
    }
}

enum UpdatePrompt {
    Retry,
    Applied,
}

#[derive(Debug)]
pub struct CatalogUpdate {
    batches: Vec<CatalogBatch>,
}

impl CatalogUpdate {
    pub(crate) fn batches(&self) -> impl Iterator<Item = &CatalogBatch> {
        self.batches.iter()
    }
}

/// Contains pending changes for a given table in a [DatabaseCatalogTransaction].
#[derive(Debug, Clone)]
pub struct TableTransaction {
    /// Tracks changes to the table in the current transaction.
    pub(crate) table: TableDefinition,
    /// Reference to the database schema for database name/id
    database_schema: Arc<DatabaseSchema>,
    /// New columns added to the table.
    column_definitions: Vec<ColumnDefinitionLog>,
    /// New field families added to the table.
    field_family_definitions: Vec<FieldFamilyDefinitionLog>,
    /// Maximum number of columns allowed in this table.
    column_limit: usize,
}

impl TableTransaction {
    pub(crate) fn new(
        table: TableDefinition,
        database_schema: Arc<DatabaseSchema>,
        column_limit: usize,
    ) -> Self {
        Self {
            table,
            database_schema,
            column_definitions: vec![],
            field_family_definitions: vec![],
            column_limit,
        }
    }

    /// Returns the number of columns for the table, including an additional ones
    /// added in this transaction.
    pub(crate) fn num_columns(&self) -> usize {
        self.table.num_columns()
    }
}

impl From<TableTransaction> for AddColumnsLog {
    fn from(tx: TableTransaction) -> Self {
        AddColumnsLog {
            database_id: tx.database_schema.id,
            database_name: Arc::clone(&tx.database_schema.name),
            table_id: tx.table.table_id,
            table_name: Arc::clone(&tx.table.table_name),
            column_definitions: tx.column_definitions,
            field_family_definitions: tx.field_family_definitions,
        }
    }
}

impl CatalogResource for TableTransaction {
    type Identifier = TableId;

    fn id(&self) -> Self::Identifier {
        self.table.table_id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.table.table_name)
    }
}

impl TableTransaction {
    pub fn table_id(&self) -> TableId {
        self.table.table_id
    }

    #[inline]
    fn check_columns_limit(&self) -> Result<()> {
        if self.num_columns() >= self.column_limit {
            Err(CatalogError::TooManyColumns(self.column_limit))
        } else {
            Ok(())
        }
    }

    pub fn time_or_create(&mut self) -> Result<Arc<TimestampColumn>> {
        if let Some(def) = self.table.timestamp_column.clone() {
            Ok(def)
        } else {
            self.add_time()
        }
    }

    pub fn tag_or_create(&mut self, name: &str) -> Result<Arc<TagColumn>> {
        if let Some(def) = self.table.tag_columns.get_by_name(name) {
            return Ok(Arc::clone(&def));
        }

        if let Some(def) = self.table.column_definition(name) {
            return Err(CatalogError::InvalidColumnType {
                column_name: Arc::clone(&def.name()),
                expected: def.column_type(),
                got: InfluxColumnType::Tag,
            });
        }

        self.add_tag(name)
    }

    pub fn field_or_create(
        &mut self,
        name: &str,
        r#type: InfluxFieldType,
    ) -> Result<Arc<FieldColumn>> {
        if let Some(def) = self.table.column_definition(name) {
            return match def {
                ColumnDefinition::Field(col) if col.data_type == r#type => Ok(col),
                _ => Err(CatalogError::InvalidColumnType {
                    column_name: Arc::from(name),
                    expected: def.column_type(),
                    got: InfluxColumnType::Field(r#type),
                }),
            };
        }

        self.add_field(name, r#type)
    }

    pub(crate) fn add_tag(&mut self, tag: impl AsRef<str>) -> Result<Arc<TagColumn>> {
        key::is_valid(key::Type::Tag, tag.as_ref())?;

        if self.table.tag_columns.len() >= NUM_TAG_COLUMNS_LIMIT {
            return Err(CatalogError::TooManyTagColumns(NUM_TAG_COLUMNS_LIMIT));
        }

        self.check_columns_limit()?;

        let table_def = &mut self.table;

        table_def.check_name(tag.as_ref())?;

        let id = table_def.tag_columns.next_id();
        // Only increment if id is less than MAX.
        if id < TagId::MAX {
            table_def.tag_columns.set_next_id(id.next())
        }
        let col_id = table_def.columns.get_and_increment_next_id();
        let tag_col = Arc::new(TagColumn::new(id, col_id, tag.as_ref()));

        table_def
            .tag_columns
            .insert(id, Arc::clone(&tag_col))
            .expect("no duplicate tag");

        let col_def = ColumnDefinition::Tag(Arc::clone(&tag_col));
        table_def
            .columns
            .insert(col_def.clone())
            .expect("no duplicate column");

        self.column_definitions.push(col_def.into());

        Ok(tag_col)
    }

    pub(crate) fn add_time(&mut self) -> Result<Arc<TimestampColumn>> {
        if self.table.timestamp_column.is_some() {
            return Err(CatalogError::DuplicateColumn {
                name: TIME_COLUMN_NAME.into(),
                existing: InfluxColumnType::Timestamp,
            });
        }
        let col_id = self.table.columns.get_and_increment_next_id();
        let time_col = Arc::new(TimestampColumn::new(col_id, TIME_COLUMN_NAME));
        self.table.timestamp_column = Some(Arc::clone(&time_col));
        let col_def = ColumnDefinition::Timestamp(Arc::clone(&time_col));
        self.table
            .columns
            .insert(col_def.clone())
            .expect("no duplicate column");

        self.column_definitions.push(col_def.into());

        Ok(time_col)
    }

    pub(crate) fn add_field(
        &mut self,
        name: impl AsRef<str>,
        data_type: InfluxFieldType,
    ) -> Result<Arc<FieldColumn>> {
        key::is_valid(key::Type::Field, name.as_ref())?;
        self.table.check_name(name.as_ref())?;

        self.check_columns_limit()?;

        let parse_field_name = match self.table.field_family_mode {
            FieldFamilyMode::Aware => parse_field_name_aware,
            FieldFamilyMode::Auto => parse_field_name_auto,
        };

        let ff_id = match parse_field_name(name.as_ref()) {
            FieldName::Unqualified(_) => self.next_auto_family_id(),
            FieldName::Qualified(family_name, _) => {
                if let Some(ffd) = self.table.field_families.get_by_name(family_name) {
                    if ffd.fields.len() >= NUM_FIELDS_PER_FAMILY_LIMIT {
                        return Err(CatalogError::TooManyFields {
                            field_family: family_name.to_string(),
                            limit: NUM_FIELDS_PER_FAMILY_LIMIT,
                        });
                    }
                    ffd.id
                } else {
                    // create a new field family
                    let id = self.table.field_families.get_and_increment_next_id();
                    let name = FieldFamilyName::User(family_name.into());
                    let def = FieldFamilyDefinition::new(id, name.clone());

                    self.table
                        .field_families
                        .insert(id, Arc::new(def))
                        .expect("field family ID and name don't exist");

                    self.field_family_definitions
                        .push(FieldFamilyDefinitionLog { id, name });

                    id
                }
            }
        };

        let ffd_arc = self
            .table
            .field_families
            .get_mut_by_id(&ff_id)
            .expect("field family name exists");

        // We know there are no other references, as the table only exists
        // as a field of TableTransaction.
        let ffd = Arc::get_mut(ffd_arc).expect("no other references");

        let id = FieldIdentifier::new(ffd.id, ffd.fields.get_and_increment_next_id());
        let col_id = self.table.columns.get_and_increment_next_id();
        let field_col = Arc::new(FieldColumn::new(id, col_id, name.as_ref(), data_type));
        ffd.fields
            .insert(id.1, Arc::clone(&field_col))
            .expect("field does not exist");
        self.table.field_count += 1;

        let col_def = ColumnDefinition::Field(Arc::clone(&field_col));
        self.table
            .columns
            .insert(col_def.clone())
            .expect("no duplicate column");

        self.column_definitions.push(col_def.into());

        Ok(field_col)
    }

    fn next_auto_family_id(&mut self) -> FieldFamilyId {
        // Check if the current field family still has available slots.
        if let Some(id) = self.table.auto_field_family.as_ref() {
            let ffd = self
                .table
                .field_families
                .get_by_id(id)
                .expect("auto field family exists");

            if ffd.fields.len() < NUM_FIELDS_PER_FAMILY_LIMIT {
                return *id;
            }
        }

        let id = self.table.field_families.get_and_increment_next_id();
        self.table.auto_field_family = Some(id);
        let auto_id = self.table.next_auto_field_family_name;
        self.table.next_auto_field_family_name = auto_id + 1;
        let name = FieldFamilyName::Auto(auto_id);

        let def = FieldFamilyDefinition::new(id, name.clone());

        self.table
            .field_families
            .insert(id, Arc::new(def))
            .expect("field family ID and name don't exist");

        self.field_family_definitions
            .push(FieldFamilyDefinitionLog { id, name });

        id
    }
}

#[derive(Clone, Debug)]
pub struct DatabaseCatalogTransaction {
    catalog_sequence: CatalogSequenceNumber,
    current_table_count: usize,
    table_limit: usize,
    columns_per_table_limit: usize,
    time_ns: i64,
    database_schema: Arc<DatabaseSchema>,
    /// A collection of created or modified tables for the current transaction.
    tables: Repo<TableId, TableTransaction>,
    next_table_id: TableId,
    ops: Vec<DatabaseCatalogOp>,
}

#[derive(Debug)]
pub struct CreateTableColumns<'a, T, F>
where
    T: AsRef<str> + Send + Sync,
    F: AsRef<str> + Send + Sync,
{
    tags: &'a [T],
    fields: &'a [(F, FieldDataType)],
}

impl<'a, T, F> CreateTableColumns<'a, T, F>
where
    T: AsRef<str> + Send + Sync,
    F: AsRef<str> + Send + Sync,
{
    /// Returns the number of columns that will be added to the table, including time
    fn num_columns(&self) -> usize {
        self.tags.len() + self.fields.len() + 1
    }
}

mod create_table_columns {
    use super::CreateTableColumns;
    use std::sync::Arc;

    pub(super) fn none() -> Option<CreateTableColumns<'static, Arc<str>, Arc<str>>> {
        None::<CreateTableColumns<'_, Arc<str>, Arc<str>>>
    }
}

impl DatabaseCatalogTransaction {
    pub fn sequence_number(&self) -> CatalogSequenceNumber {
        self.catalog_sequence
    }

    /// Apply the current transaction to the [super::InnerCatalog] and return the updated [DatabaseSchema].
    ///
    /// # NOTE
    ///
    /// Used for testing purposes.
    pub fn apply_to_inner(&self, inner: &mut super::InnerCatalog) -> Result<Arc<DatabaseSchema>> {
        let (catalog_batch, sequence) = self.clone().catalog_batch();
        inner
            .apply_catalog_batch(&catalog_batch, sequence)
            .map(|_| {
                inner
                    .databases
                    .get_by_id(&self.database_schema.id)
                    .expect("database should exist by id")
            })
    }

    /// Check if the transaction is empty, meaning it has no pending operations.
    fn is_empty(&self) -> bool {
        self.ops.is_empty()
            && self.tables.resource_iter().all(|tx| {
                tx.column_definitions.is_empty() && tx.field_family_definitions.is_empty()
            })
    }

    fn catalog_batch(self) -> (CatalogBatch, CatalogSequenceNumber) {
        let s = self.catalog_sequence;
        (self.into(), s)
    }

    /// Check if a table exists by its name in the current transaction or in the database schema.
    fn table_exists_by_name(&self, name: &str) -> bool {
        self.tables.contains_name(name) || self.database_schema.tables.contains_name(name)
    }

    /// Get a mutable reference to a [TableTransaction] for the given table name.
    ///
    /// If no table transaction exists and the table exists in the current `database_schema`,
    /// a new [TableTransaction] is created and returned.
    pub(crate) fn table_transaction(&mut self, name: &str) -> Option<&mut TableTransaction> {
        if let Some(id) = self.tables.id_for_name(name) {
            self.tables.get_mut_by_id(&id)
        } else if let Some(def) = self.database_schema.table_definition(name) {
            let id = def.table_id;
            // start a transaction for this table
            let tx = TableTransaction::new(
                def.as_ref().clone(),
                Arc::clone(&self.database_schema),
                self.columns_per_table_limit,
            );
            self.tables
                .insert(tx.table.table_id, tx)
                .expect("Existing transaction for table should not exist");
            self.tables.get_mut_by_id(&id)
        } else {
            None
        }
    }

    pub fn table_or_create(&mut self, table_name: &str) -> Result<TableId> {
        match self.table_transaction(table_name) {
            Some(tx) => Ok(tx.table.table_id),
            None => self.create_table(
                table_name,
                create_table_columns::none(),
                FieldFamilyMode::Aware,
            ),
        }
    }

    pub fn table_tx_or_create(&mut self, table_name: &str) -> Result<&mut TableTransaction> {
        let id = self.table_or_create(table_name)?;
        Ok(self
            .tables
            .get_mut_by_id(&id)
            .expect("table should exist by id"))
    }

    pub fn column_or_create(
        &mut self,
        table_name: &str,
        column_name: &str,
        column_type: InfluxColumnType,
    ) -> Result<ColumnDefinition> {
        let Some(table_tx) = self.table_transaction(table_name) else {
            return Err(CatalogError::NotFound);
        };
        match table_tx.table.column_definition(column_name) {
            Some(def) if def.column_type() == column_type => Ok(def),
            Some(def) => Err(CatalogError::InvalidColumnType {
                column_name: Arc::clone(&def.name()),
                expected: def.column_type(),
                got: column_type,
            }),
            None => match column_type {
                InfluxColumnType::Tag => table_tx.add_tag(column_name).map(Into::into),
                InfluxColumnType::Field(ft) => table_tx.add_field(column_name, ft).map(Into::into),
                InfluxColumnType::Timestamp => table_tx.add_time().map(Into::into),
            },
        }
    }

    pub fn create_table<T, F>(
        &mut self,
        table_name: &str,
        columns: Option<CreateTableColumns<'_, T, F>>,
        field_family_mode: FieldFamilyMode,
    ) -> Result<TableId>
    where
        T: AsRef<str> + Send + Sync,
        F: AsRef<str> + Send + Sync,
    {
        debug!(table_name, "create new table");
        if self.table_exists_by_name(table_name) {
            return Err(CatalogError::AlreadyExists);
        }
        if self.current_table_count >= self.table_limit {
            return Err(CatalogError::TooManyTables(self.table_limit));
        }
        if let Some(c) = columns.as_ref() {
            if c.tags.len() > NUM_TAG_COLUMNS_LIMIT {
                return Err(CatalogError::TooManyTagColumns(NUM_TAG_COLUMNS_LIMIT));
            }
            if c.num_columns() > self.columns_per_table_limit {
                return Err(CatalogError::TooManyColumns(self.columns_per_table_limit));
            }
        }

        let table_id = self.next_table_id;
        self.next_table_id = table_id.next();

        debug!("inserting table from transaction");

        self.ops
            .push(DatabaseCatalogOp::CreateTable(CreateTableLog {
                database_id: self.database_schema.id,
                database_name: Arc::clone(&self.database_schema.name),
                table_name: table_name.into(),
                table_id,
                field_family_mode,
            }));

        let mut table_tx = TableTransaction::new(
            TableDefinition::new_empty(table_id, table_name.into(), field_family_mode),
            Arc::clone(&self.database_schema),
            self.columns_per_table_limit,
        );

        if let Some(CreateTableColumns { tags, fields }) = columns {
            for tag in tags {
                table_tx.add_tag(tag)?;
            }

            for (field_name, field_type) in fields {
                table_tx.add_field(field_name, (*field_type).into())?;
            }

            table_tx.add_time()?;
        }

        self.tables
            .insert(table_id, table_tx)
            .expect("no duplicate table by ID or name");

        Ok(table_id)
    }

    pub fn db_schema(&self) -> &Arc<DatabaseSchema> {
        &self.database_schema
    }
}

impl From<DatabaseCatalogTransaction> for CatalogBatch {
    fn from(txn: DatabaseCatalogTransaction) -> Self {
        let mut ops = txn.ops;
        let mut tables = txn.tables;
        ops.extend(tables.repo.drain(..).filter_map(|(_, tx)| {
            if tx.column_definitions.is_empty() && tx.field_family_definitions.is_empty() {
                None
            } else {
                Some(DatabaseCatalogOp::AddColumns(tx.into()))
            }
        }));
        CatalogBatch::database(
            txn.time_ns,
            txn.database_schema.id,
            Arc::clone(&txn.database_schema.name),
            ops,
        )
    }
}

#[derive(Debug)]
pub enum Prompt<Success = (), Retry = ()> {
    Success(Success),
    Retry(Retry),
}

impl<S, R> Prompt<S, R> {
    pub fn unwrap_success(self) -> S {
        let Self::Success(s) = self else {
            panic!("tried to unwrap a retry as success");
        };
        s
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct Repo<K: Hash + Eq + Copy + Ord, V: CatalogResource> {
    /// Store for items in the repository
    pub(crate) repo: IndexMap<K, V>,
    /// Bi-directional map of identifiers to names in the repository
    pub(crate) id_name_map: BiHashMap<K, Arc<str>>,
}

impl<K: Hash + Eq + Copy + Ord, V: CatalogResource> Repo<K, V> {
    pub(crate) fn new() -> Self {
        Self {
            repo: IndexMap::new(),
            id_name_map: BiHashMap::new(),
        }
    }

    pub(crate) fn get_mut_by_id(&mut self, id: &K) -> Option<&mut V> {
        self.repo.get_mut(id)
    }

    pub(crate) fn contains_name(&self, name: &str) -> bool {
        self.id_name_map.contains_right(name)
    }

    pub(crate) fn id_for_name(&self, name: &str) -> Option<K> {
        self.id_name_map.get_by_right(name).cloned()
    }

    /// Check if a resource exists in the repository by `id`
    ///
    /// # Panics
    ///
    /// This panics if the `id` is in the id-to-name map, but not in the actual repository map, as
    /// that would be a bad state for the repository to be in.
    fn id_exists(&self, id: &K) -> bool {
        let id_in_map = self.id_name_map.contains_left(id);
        let id_in_repo = self.repo.contains_key(id);
        assert_eq!(
            id_in_map, id_in_repo,
            "id map and repository are in an inconsistent state, \
            in map: {id_in_map}, in repo: {id_in_repo}"
        );
        id_in_repo
    }

    /// Check if a resource exists in the repository by `id` and `name`
    ///
    /// # Panics
    ///
    /// This panics if the `id` is in the id-to-name map, but not in the actual repository map, as
    /// that would be a bad state for the repository to be in.
    fn id_and_name_exists(&self, id: &K, name: &str) -> bool {
        let name_in_map = self.id_name_map.contains_right(name);
        self.id_exists(id) && name_in_map
    }

    /// Insert a new resource to the repository
    pub(crate) fn insert(&mut self, id: K, resource: impl Into<V>) -> Result<()> {
        let resource = resource.into();
        if self.id_and_name_exists(&id, resource.name().as_ref()) {
            return Err(CatalogError::AlreadyExists);
        }
        self.id_name_map.insert(id, resource.name());
        self.repo.insert(id, resource);

        Ok(())
    }

    pub(crate) fn resource_iter(&self) -> impl Iterator<Item = &V> {
        self.repo.values()
    }
}

impl<K: Default + Hash + Eq + Copy + Ord, V: CatalogResource> Default for Repo<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::CatalogError;
    use crate::catalog::versions::v2::{Catalog, CreateTableOptions};
    use crate::log::versions::v4::FieldDataType;
    use iox_time::{MockProvider, Time};
    use schema::{InfluxColumnType, InfluxFieldType};
    use std::sync::Arc;

    /// Tests which focus on testing the expected behaviour when creating a new table.
    mod create_table {
        use super::*;
        use crate::catalog::versions::v2::{
            CatalogLimits, FieldFamilyMode, NUM_FIELDS_PER_FAMILY_LIMIT,
        };
        use object_store::memory::InMemory;

        #[tokio::test]
        async fn all_auto_defined_field_families() {
            let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
            let store = Arc::new(InMemory::new());

            {
                let catalog = Arc::new(
                    Catalog::new_with_store(
                        "test",
                        Arc::clone(&store) as _,
                        Arc::clone(&time_provider) as _,
                    )
                    .await
                    .unwrap(),
                );

                catalog.create_database("foo").await.unwrap();

                catalog
                    .create_table_opts(
                        "foo",
                        "bar",
                        &["tag"],
                        &[
                            ("str", FieldDataType::String),
                            ("f64", FieldDataType::Float),
                            ("i64", FieldDataType::Integer),
                            ("u64", FieldDataType::UInteger),
                            ("bool", FieldDataType::Boolean),
                        ],
                        CreateTableOptions {
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();

                let foo_db = catalog.db_schema("foo").expect("foo should exist");
                let bar = foo_db.tables.get_by_name("bar").expect("bar should exist");
                assert_eq!(bar.num_columns(), 7);
                assert_eq!(bar.num_tag_columns(), 1);
                assert_eq!(bar.num_field_columns(), 5);
                assert_eq!(bar.num_field_families(), 1);

                insta::assert_json_snapshot!("all_auto_defined_field_families__catalog_snapshot", catalog.snapshot(), {
                    ".catalog_uuid" => "[uuid]",
                    ".nodes.repo[][1].instance_id" => "[uuid]",
                    ".nodes.repo[][1].process_uuids[]" => "[uuid]"
                });
            }

            // Reload the catalog from the object store logs and verify it matches the existing
            // insta snapshot
            {
                let catalog = Arc::new(
                    Catalog::new_with_store(
                        "test",
                        Arc::clone(&store) as _,
                        Arc::clone(&time_provider) as _,
                    )
                    .await
                    .unwrap(),
                );

                let foo_db = catalog.db_schema("foo").expect("foo should exist");
                let bar = foo_db.tables.get_by_name("bar").expect("bar should exist");
                assert_eq!(bar.num_columns(), 7);
                assert_eq!(bar.num_tag_columns(), 1);
                assert_eq!(bar.num_field_columns(), 5);
                assert_eq!(bar.num_field_families(), 1);

                insta::assert_json_snapshot!("all_auto_defined_field_families__catalog_snapshot", catalog.snapshot(), {
                    ".catalog_uuid" => "[uuid]",
                    ".nodes.repo[][1].instance_id" => "[uuid]",
                    ".nodes.repo[][1].process_uuids[]" => "[uuid]"
                });
            }
        }

        /// Ensure that when the current auto field family reaches its field limit, a new auto field family is created,
        /// and any additional fields are added to this new family.
        #[tokio::test]
        async fn auto_family_rolls_to_next() {
            let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
            let store = Arc::new(InMemory::new());

            {
                let catalog = Arc::new(
                    Catalog::new_with_store(
                        "test",
                        Arc::clone(&store) as _,
                        Arc::clone(&time_provider) as _,
                    )
                    .await
                    .unwrap(),
                );

                catalog.create_database("foo").await.unwrap();

                let fields = (0..NUM_FIELDS_PER_FAMILY_LIMIT + 2)
                    .map(|v| (format!("field_{v}"), FieldDataType::Float))
                    .collect::<Vec<_>>();

                catalog
                    .create_table_opts(
                        "foo",
                        "bar",
                        &["tag"],
                        &fields,
                        CreateTableOptions {
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();

                let foo_db = catalog.db_schema("foo").expect("foo should exist");
                let bar = foo_db.tables.get_by_name("bar").expect("bar should exist");
                assert!(bar.field_families.contains_name("__0"));
                assert!(bar.field_families.contains_name("__1"));
                assert_eq!(bar.num_columns(), 2 + NUM_FIELDS_PER_FAMILY_LIMIT + 2);
                assert_eq!(bar.num_tag_columns(), 1);
                assert_eq!(bar.num_field_columns(), NUM_FIELDS_PER_FAMILY_LIMIT + 2);
                assert_eq!(bar.num_field_families(), 2);
            }

            // Reload the catalog from the object store logs and verify it again
            {
                let catalog = Arc::new(
                    Catalog::new_with_store(
                        "test",
                        Arc::clone(&store) as _,
                        Arc::clone(&time_provider) as _,
                    )
                    .await
                    .unwrap(),
                );

                let foo_db = catalog.db_schema("foo").expect("foo should exist");
                let bar = foo_db.tables.get_by_name("bar").expect("bar should exist");
                assert!(bar.field_families.contains_name("__0"));
                assert!(bar.field_families.contains_name("__1"));
                assert_eq!(bar.num_columns(), 2 + NUM_FIELDS_PER_FAMILY_LIMIT + 2);
                assert_eq!(bar.num_tag_columns(), 1);
                assert_eq!(bar.num_field_columns(), NUM_FIELDS_PER_FAMILY_LIMIT + 2);
                assert_eq!(bar.num_field_families(), 2);
            }
        }

        #[tokio::test]
        async fn verify_tags_fields_are_valid() {
            let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
            let store = Arc::new(InMemory::new());
            let catalog = Arc::new(
                Catalog::new_with_store("test", store, time_provider)
                    .await
                    .unwrap(),
            );

            catalog.create_database("foo").await.unwrap();

            let result = catalog
                .create_table_opts(
                    "foo",
                    "bar",
                    &["foo", ""], // Invalid tag name
                    &[("field1", FieldDataType::Float)],
                    CreateTableOptions::default(),
                )
                .await;

            assert!(
                matches!(result, Err(CatalogError::InvalidConfiguration { message }) if message.as_ref() == "tag key cannot be empty")
            );

            let result = catalog
                .create_table_opts(
                    "foo",
                    "bar",
                    &["foo"], // Invalid tag name
                    &[("field1", FieldDataType::Float), ("", FieldDataType::Float)],
                    CreateTableOptions::default(),
                )
                .await;

            assert!(
                matches!(result, Err(CatalogError::InvalidConfiguration { message }) if message.as_ref() == "field key cannot be empty")
            );

            let result = catalog
                .create_table_opts(
                    "foo",
                    "bar",
                    &["foo", "foo\t"], // Invalid tag name
                    &[("field1", FieldDataType::Float)],
                    CreateTableOptions::default(),
                )
                .await;

            assert!(
                matches!(result, Err(CatalogError::InvalidConfiguration { message }) if message.as_ref() == "tag key cannot contain control characters")
            );

            let result = catalog
                .create_table_opts(
                    "foo",
                    "bar",
                    &["foo"], // Invalid tag name
                    &[
                        ("field1", FieldDataType::Float),
                        ("foo\t", FieldDataType::Float),
                    ],
                    CreateTableOptions::default(),
                )
                .await;

            assert!(
                matches!(result, Err(CatalogError::InvalidConfiguration { message }) if message.as_ref() == "field key cannot contain control characters")
            );
        }

        #[tokio::test]
        async fn column_or_create_enforces_column_limit() {
            let catalog = Catalog::new_in_memory_with_limits(
                "test",
                CatalogLimits {
                    num_columns_per_table: 3,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

            let mut txn = catalog.begin("test_db").unwrap();
            txn.table_or_create("test_table").unwrap();

            // Add columns up to the limit (3 total)
            txn.column_or_create("test_table", "tag1", InfluxColumnType::Tag)
                .unwrap();
            txn.column_or_create("test_table", "time", InfluxColumnType::Timestamp)
                .unwrap();
            txn.column_or_create(
                "test_table",
                "field1",
                InfluxColumnType::Field(InfluxFieldType::Float),
            )
            .unwrap();

            // This should fail - we're at the limit now
            let result = txn.column_or_create(
                "test_table",
                "field2",
                InfluxColumnType::Field(InfluxFieldType::Integer),
            );
            assert!(matches!(result, Err(CatalogError::TooManyColumns(3))));
        }

        #[tokio::test]
        async fn verify_tags_fields_cannot_be_named_reserved_columns() {
            let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
            let store = Arc::new(InMemory::new());
            let catalog = Arc::new(
                Catalog::new_with_store("test", store, time_provider)
                    .await
                    .unwrap(),
            );

            catalog.create_database("foo").await.unwrap();

            // Test tag named "time"
            let result = catalog
                .create_table_opts(
                    "foo",
                    "bar",
                    &["time"], // Invalid tag name
                    &[("field1", FieldDataType::Float)],
                    CreateTableOptions::default(),
                )
                .await;

            assert!(
                matches!(result, Err(CatalogError::ReservedColumn(name)) if name.as_ref() == "time")
            );

            // Test field named "time"
            let result = catalog
                .create_table_opts(
                    "foo",
                    "bar",
                    &["tag1"],
                    &[("time", FieldDataType::Float)], // Invalid field name
                    CreateTableOptions::default(),
                )
                .await;

            assert!(
                matches!(result, Err(CatalogError::ReservedColumn(name)) if name.as_ref() == "time")
            );

            // Test tag named "__chunk_order"
            let result = catalog
                .create_table_opts(
                    "foo",
                    "bar2",
                    &["__chunk_order"], // Invalid tag name
                    &[("field1", FieldDataType::Float)],
                    CreateTableOptions::default(),
                )
                .await;

            assert!(
                matches!(result, Err(CatalogError::ReservedColumn(name)) if name.as_ref() == "__chunk_order")
            );

            // Test field named "__chunk_order"
            let result = catalog
                .create_table_opts(
                    "foo",
                    "bar3",
                    &["tag1"],
                    &[("__chunk_order", FieldDataType::Integer)], // Invalid field name
                    CreateTableOptions::default(),
                )
                .await;

            assert!(
                matches!(result, Err(CatalogError::ReservedColumn(name)) if name.as_ref() == "__chunk_order")
            );
        }

        #[tokio::test]
        async fn verify_duplicate_tag_or_field_returns_error() {
            let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
            let store = Arc::new(InMemory::new());
            let catalog = Arc::new(
                Catalog::new_with_store("test", store, time_provider)
                    .await
                    .unwrap(),
            );

            catalog.create_database("foo").await.unwrap();

            // Test duplicate tag names
            let result = catalog
                .create_table_opts(
                    "foo",
                    "bar",
                    &["tag1", "tag2", "tag1"], // Duplicate tag1
                    &[("field1", FieldDataType::Float)],
                    CreateTableOptions::default(),
                )
                .await;

            assert!(matches!(
                result,
                Err(CatalogError::DuplicateColumn { name, existing })
                    if name.as_ref() == "tag1" && existing == InfluxColumnType::Tag
            ));

            // Test duplicate field names
            let result = catalog
                .create_table_opts(
                    "foo",
                    "bar2",
                    &["tag1"],
                    &[
                        ("field1", FieldDataType::Float),
                        ("field2", FieldDataType::Integer),
                        ("field1", FieldDataType::String), // Duplicate field1
                    ],
                    CreateTableOptions::default(),
                )
                .await;

            assert!(matches!(
                result,
                Err(CatalogError::DuplicateColumn { name, existing })
                    if name.as_ref() == "field1" && existing == InfluxColumnType::Field(InfluxFieldType::Float)
            ));

            // Test tag/field name collision
            let result = catalog
                .create_table_opts(
                    "foo",
                    "bar3",
                    &["samename"],
                    &[("samename", FieldDataType::Float)], // Same name as tag
                    CreateTableOptions::default(),
                )
                .await;

            assert!(matches!(
                result,
                Err(CatalogError::DuplicateColumn { name, existing })
                    if name.as_ref() == "samename" && existing == InfluxColumnType::Tag
            ));
        }

        #[tokio::test]
        async fn all_user_defined_field_families() {
            let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
            let store = Arc::new(InMemory::new());

            // Create catalog and schema
            {
                let catalog = Arc::new(
                    Catalog::new_with_store(
                        "test",
                        Arc::clone(&store) as _,
                        Arc::clone(&time_provider) as _,
                    )
                    .await
                    .unwrap(),
                );

                catalog.create_database("foo").await.unwrap();

                catalog
                    .create_table_opts(
                        "foo",
                        "bar",
                        &["tag02", "tag01", "tag03"],
                        &[
                            ("ff1::str", FieldDataType::String),
                            ("ff1::f64", FieldDataType::Float),
                            ("ff2::f64", FieldDataType::Float),
                            ("ff2::i64", FieldDataType::Integer),
                            ("ff3::u64", FieldDataType::UInteger),
                            ("ff3::bool", FieldDataType::Boolean),
                        ],
                        CreateTableOptions {
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();

                let foo_db = catalog.db_schema("foo").expect("foo should exist");
                let bar = foo_db.tables.get_by_name("bar").expect("bar should exist");
                assert_eq!(bar.num_field_families(), 3);

                insta::assert_json_snapshot!("all_user_defined_field_families__catalog_snapshot", catalog.snapshot(), {
                    ".catalog_uuid" => "[uuid]",
                    ".nodes.repo[][1].instance_id" => "[uuid]",
                    ".nodes.repo[][1].process_uuids[]" => "[uuid]"
                });
            }

            // Reload the catalog from the object store logs and verify it matches the existing
            // insta snapshot
            {
                let catalog = Arc::new(
                    Catalog::new_with_store(
                        "test",
                        Arc::clone(&store) as _,
                        Arc::clone(&time_provider) as _,
                    )
                    .await
                    .unwrap(),
                );

                let foo_db = catalog.db_schema("foo").expect("foo should exist");
                let bar = foo_db.tables.get_by_name("bar").expect("bar should exist");
                assert_eq!(bar.num_field_families(), 3);

                insta::assert_json_snapshot!("all_user_defined_field_families__catalog_snapshot", catalog.snapshot(), {
                    ".catalog_uuid" => "[uuid]",
                    ".nodes.repo[][1].instance_id" => "[uuid]",
                    ".nodes.repo[][1].process_uuids[]" => "[uuid]"
                });
            }
        }

        /// This test ensures that when the table is [FieldFamilyMode::Auto], it ignores
        /// the `<field family>::` prefix and all fields are auto-assigned to a field family.
        #[tokio::test]
        async fn table_is_auto_field_family() {
            let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
            let store = Arc::new(InMemory::new());

            // Create catalog and schema
            {
                let catalog = Arc::new(
                    Catalog::new_with_store(
                        "test",
                        Arc::clone(&store) as _,
                        Arc::clone(&time_provider) as _,
                    )
                    .await
                    .unwrap(),
                );

                catalog.create_database("foo").await.unwrap();

                catalog
                    .create_table_opts(
                        "foo",
                        "bar",
                        &["tag02", "tag01", "tag03"],
                        &[
                            ("ff1::str", FieldDataType::String),
                            ("ff1::f64", FieldDataType::Float),
                            ("ff2::f64", FieldDataType::Float),
                            ("ff2::i64", FieldDataType::Integer),
                            ("ff3::u64", FieldDataType::UInteger),
                            ("ff3::bool", FieldDataType::Boolean),
                        ],
                        CreateTableOptions {
                            field_family_mode: FieldFamilyMode::Auto,
                        },
                    )
                    .await
                    .unwrap();

                let foo_db = catalog.db_schema("foo").expect("foo should exist");
                foo_db.tables.get_by_name("bar").expect("bar should exist");

                insta::assert_json_snapshot!("table_is_auto_field_family", catalog.snapshot(), {
                    ".catalog_uuid" => "[uuid]",
                    ".nodes.repo[][1].instance_id" => "[uuid]",
                    ".nodes.repo[][1].process_uuids[]" => "[uuid]"
                });
            }

            // Reload the catalog from the object store logs and verify it matches the existing
            // insta snapshot
            {
                let catalog = Arc::new(
                    Catalog::new_with_store(
                        "test",
                        Arc::clone(&store) as _,
                        Arc::clone(&time_provider) as _,
                    )
                    .await
                    .unwrap(),
                );

                let foo_db = catalog.db_schema("foo").expect("foo should exist");
                foo_db.tables.get_by_name("bar").expect("bar should exist");

                insta::assert_json_snapshot!("table_is_auto_field_family", catalog.snapshot(), {
                    ".catalog_uuid" => "[uuid]",
                    ".nodes.repo[][1].instance_id" => "[uuid]",
                    ".nodes.repo[][1].process_uuids[]" => "[uuid]"
                });
            }
        }
    }

    /// Focus on testing the expected behaviour when updating an existing table.
    mod update_table {
        use super::*;
        use crate::catalog::versions::v2::update::create_table_columns;
        use crate::catalog::versions::v2::{CatalogArgs, CatalogLimits, FieldFamilyMode};
        use object_store::memory::InMemory;

        #[tokio::test]
        async fn test_add_tags_to_existing_table() {
            let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
            let store = Arc::new(InMemory::new());

            {
                let catalog = Arc::new(
                    Catalog::new_with_store(
                        "test",
                        Arc::clone(&store) as _,
                        Arc::clone(&time_provider) as _,
                    )
                    .await
                    .unwrap(),
                );

                // Create database and table with initial tags and fields
                catalog.create_database("foo").await.unwrap();
                catalog
                    .create_table(
                        "foo",
                        "bar",
                        &["tag1", "tag2"],
                        &[("field1", FieldDataType::Float)],
                    )
                    .await
                    .unwrap();

                // Verify initial state
                let foo_db = catalog.db_schema("foo").expect("foo should exist");
                let bar_table = foo_db.tables.get_by_name("bar").expect("bar should exist");
                assert_eq!(bar_table.tag_columns.len(), 2);

                // Add new tags to the existing table
                let mut txn = catalog.begin("foo").unwrap();
                txn.column_or_create("bar", "tag3", InfluxColumnType::Tag)
                    .unwrap();
                txn.column_or_create("bar", "tag4", InfluxColumnType::Tag)
                    .unwrap();
                catalog.commit(txn).await.unwrap();

                // Verify tags were added
                let foo_db = catalog.db_schema("foo").expect("foo should exist");
                let bar_table = foo_db.tables.get_by_name("bar").expect("bar should exist");
                assert_eq!(bar_table.tag_columns.len(), 4);
                assert!(bar_table.column_definition("tag3").is_some());
                assert!(bar_table.column_definition("tag4").is_some());
            }

            // Reload catalog from object store to verify persistence
            {
                let catalog = Arc::new(
                    Catalog::new_with_store(
                        "test",
                        Arc::clone(&store) as _,
                        Arc::clone(&time_provider) as _,
                    )
                    .await
                    .unwrap(),
                );

                let foo_db = catalog.db_schema("foo").expect("foo should exist");
                let bar_table = foo_db.tables.get_by_name("bar").expect("bar should exist");
                assert_eq!(bar_table.tag_columns.len(), 4);
                assert!(bar_table.column_definition("tag3").is_some());
                assert!(bar_table.column_definition("tag4").is_some());
            }
        }

        #[tokio::test]
        async fn test_add_fields_to_existing_table() {
            let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
            let store = Arc::new(InMemory::new());

            {
                let catalog = Arc::new(
                    Catalog::new_with_store(
                        "test",
                        Arc::clone(&store) as _,
                        Arc::clone(&time_provider) as _,
                    )
                    .await
                    .unwrap(),
                );

                // Create database and table with initial fields
                catalog.create_database("foo").await.unwrap();
                catalog
                    .create_table(
                        "foo",
                        "measurements",
                        &["location"],
                        &[("temperature", FieldDataType::Float)],
                    )
                    .await
                    .unwrap();

                // Add various field types to the existing table
                let mut txn = catalog.begin("foo").unwrap();
                txn.column_or_create(
                    "measurements",
                    "humidity",
                    InfluxColumnType::Field(InfluxFieldType::Float),
                )
                .unwrap();
                txn.column_or_create(
                    "measurements",
                    "pressure",
                    InfluxColumnType::Field(InfluxFieldType::Integer),
                )
                .unwrap();
                txn.column_or_create(
                    "measurements",
                    "sensor_id",
                    InfluxColumnType::Field(InfluxFieldType::UInteger),
                )
                .unwrap();
                txn.column_or_create(
                    "measurements",
                    "is_active",
                    InfluxColumnType::Field(InfluxFieldType::Boolean),
                )
                .unwrap();
                txn.column_or_create(
                    "measurements",
                    "status",
                    InfluxColumnType::Field(InfluxFieldType::String),
                )
                .unwrap();
                catalog.commit(txn).await.unwrap();

                // Verify fields were added
                let foo_db = catalog.db_schema("foo").expect("foo should exist");
                let table = foo_db
                    .tables
                    .get_by_name("measurements")
                    .expect("measurements should exist");

                // Check all fields exist with correct types
                let humidity = table
                    .column_definition("humidity")
                    .expect("humidity should exist");
                assert_eq!(
                    humidity.column_type(),
                    InfluxColumnType::Field(InfluxFieldType::Float)
                );

                let pressure = table
                    .column_definition("pressure")
                    .expect("pressure should exist");
                assert_eq!(
                    pressure.column_type(),
                    InfluxColumnType::Field(InfluxFieldType::Integer)
                );

                let sensor_id = table
                    .column_definition("sensor_id")
                    .expect("sensor_id should exist");
                assert_eq!(
                    sensor_id.column_type(),
                    InfluxColumnType::Field(InfluxFieldType::UInteger)
                );

                let is_active = table
                    .column_definition("is_active")
                    .expect("is_active should exist");
                assert_eq!(
                    is_active.column_type(),
                    InfluxColumnType::Field(InfluxFieldType::Boolean)
                );

                let status = table
                    .column_definition("status")
                    .expect("status should exist");
                assert_eq!(
                    status.column_type(),
                    InfluxColumnType::Field(InfluxFieldType::String)
                );

                // Add fields with user-defined field families
                let mut txn = catalog.begin("foo").unwrap();
                txn.column_or_create(
                    "measurements",
                    "metrics::cpu_usage",
                    InfluxColumnType::Field(InfluxFieldType::Float),
                )
                .unwrap();
                txn.column_or_create(
                    "measurements",
                    "metrics::memory_usage",
                    InfluxColumnType::Field(InfluxFieldType::Float),
                )
                .unwrap();
                txn.column_or_create(
                    "measurements",
                    "metadata::version",
                    InfluxColumnType::Field(InfluxFieldType::String),
                )
                .unwrap();
                catalog.commit(txn).await.unwrap();

                // Verify user-defined field families were created
                let foo_db = catalog.db_schema("foo").expect("foo should exist");
                let table = foo_db
                    .tables
                    .get_by_name("measurements")
                    .expect("measurements should exist");
                assert!(table.field_families.contains_name("metrics"));
                assert!(table.field_families.contains_name("metadata"));
            }

            // Reload catalog from object store to verify persistence
            {
                let catalog = Arc::new(
                    Catalog::new_with_store(
                        "test",
                        Arc::clone(&store) as _,
                        Arc::clone(&time_provider) as _,
                    )
                    .await
                    .unwrap(),
                );

                let foo_db = catalog.db_schema("foo").expect("foo should exist");
                let table = foo_db
                    .tables
                    .get_by_name("measurements")
                    .expect("measurements should exist");

                // Verify all fields persist after reload
                assert!(table.column_definition("humidity").is_some());
                assert!(table.column_definition("pressure").is_some());
                assert!(table.column_definition("sensor_id").is_some());
                assert!(table.column_definition("is_active").is_some());
                assert!(table.column_definition("status").is_some());
                assert!(table.column_definition("metrics::cpu_usage").is_some());
                assert!(table.column_definition("metrics::memory_usage").is_some());
                assert!(table.column_definition("metadata::version").is_some());

                // Verify field families persist
                assert!(table.field_families.contains_name("metrics"));
                assert!(table.field_families.contains_name("metadata"));
            }
        }

        #[tokio::test]
        async fn test_reserved_column_time_error() {
            let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
            let store = Arc::new(InMemory::new());
            let catalog = Arc::new(
                Catalog::new_with_store("test", store, time_provider)
                    .await
                    .unwrap(),
            );

            // Create database and table
            catalog.create_database("foo").await.unwrap();
            catalog
                .create_table(
                    "foo",
                    "data",
                    &["tag1"],
                    &[("field1", FieldDataType::Float)],
                )
                .await
                .unwrap();

            // The table already has a "time" column as the timestamp column.
            // Trying to add a tag or field named "time" should result in InvalidColumnType
            // because the column already exists with type Timestamp.

            // Try to add a tag named "time"
            let mut txn = catalog.begin("foo").unwrap();
            let result = txn.column_or_create("data", "time", InfluxColumnType::Tag);
            assert!(matches!(
                result,
                Err(CatalogError::InvalidColumnType { column_name, expected, got })
                    if column_name.as_ref() == "time"
                    && expected == InfluxColumnType::Timestamp
                    && got == InfluxColumnType::Tag
            ));

            // Try to add a field named "time"
            let mut txn = catalog.begin("foo").unwrap();
            let result = txn.column_or_create(
                "data",
                "time",
                InfluxColumnType::Field(InfluxFieldType::Float),
            );
            assert!(matches!(
                result,
                Err(CatalogError::InvalidColumnType { column_name, expected, got })
                    if column_name.as_ref() == "time"
                    && expected == InfluxColumnType::Timestamp
                    && got == InfluxColumnType::Field(InfluxFieldType::Float)
            ));

            // Trying to add "time" as Timestamp should succeed because it already exists
            let mut txn = catalog.begin("foo").unwrap();
            let result = txn.column_or_create("data", "time", InfluxColumnType::Timestamp);
            assert!(result.is_ok());
        }

        #[tokio::test]
        async fn test_duplicate_column_errors() {
            let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
            let store = Arc::new(InMemory::new());
            let catalog = Arc::new(
                Catalog::new_with_store("test", store, time_provider)
                    .await
                    .unwrap(),
            );

            // Create database and table with existing columns
            catalog.create_database("foo").await.unwrap();
            catalog
                .create_table(
                    "foo",
                    "metrics",
                    &["host", "region"],
                    &[
                        ("cpu", FieldDataType::Float),
                        ("memory", FieldDataType::Integer),
                    ],
                )
                .await
                .unwrap();

            // Test 1: Try to add duplicate tag
            let mut txn = catalog.begin("foo").unwrap();
            let result = txn.column_or_create("metrics", "host", InfluxColumnType::Tag);
            // Should succeed because it already exists with same type
            assert!(result.is_ok());

            // Test 2: Try to add tag with same name as existing field
            let mut txn = catalog.begin("foo").unwrap();
            let result = txn.column_or_create("metrics", "cpu", InfluxColumnType::Tag);
            assert!(matches!(
                result,
                Err(CatalogError::InvalidColumnType { column_name, expected, got })
                    if column_name.as_ref() == "cpu"
                    && expected == InfluxColumnType::Field(InfluxFieldType::Float)
                    && got == InfluxColumnType::Tag
            ));

            // Test 3: Try to add field with same name as existing tag
            let mut txn = catalog.begin("foo").unwrap();
            let result = txn.column_or_create(
                "metrics",
                "host",
                InfluxColumnType::Field(InfluxFieldType::String),
            );
            assert!(matches!(
                result,
                Err(CatalogError::InvalidColumnType { column_name, expected, got })
                    if column_name.as_ref() == "host"
                    && expected == InfluxColumnType::Tag
                    && got == InfluxColumnType::Field(InfluxFieldType::String)
            ));

            // Test 4: Try to add field with different type than existing field
            let mut txn = catalog.begin("foo").unwrap();
            let result = txn.column_or_create(
                "metrics",
                "cpu",
                InfluxColumnType::Field(InfluxFieldType::String),
            );
            assert!(matches!(
                result,
                Err(CatalogError::InvalidColumnType { column_name, expected, got })
                    if column_name.as_ref() == "cpu"
                    && expected == InfluxColumnType::Field(InfluxFieldType::Float)
                    && got == InfluxColumnType::Field(InfluxFieldType::String)
            ));

            // Test 5: Adding duplicate within same transaction
            let mut txn = catalog.begin("foo").unwrap();
            txn.column_or_create("metrics", "new_tag", InfluxColumnType::Tag)
                .unwrap();
            let result = txn.column_or_create(
                "metrics",
                "new_tag",
                InfluxColumnType::Field(InfluxFieldType::Float),
            );
            assert!(matches!(
                result,
                Err(CatalogError::InvalidColumnType { column_name, expected, got })
                    if column_name.as_ref() == "new_tag"
                    && expected == InfluxColumnType::Tag
                    && got == InfluxColumnType::Field(InfluxFieldType::Float)
            ));
        }

        #[tokio::test]
        async fn test_update_table_comprehensive_snapshot() {
            let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
            let store = Arc::new(InMemory::new());

            {
                let catalog = Arc::new(
                    Catalog::new_with_store(
                        "test",
                        Arc::clone(&store) as _,
                        Arc::clone(&time_provider) as _,
                    )
                    .await
                    .unwrap(),
                );

                // Create initial database and table
                catalog.create_database("test_db").await.unwrap();
                catalog
                    .create_table_opts(
                        "test_db",
                        "sensors",
                        &["location", "sensor_type"],
                        &[
                            ("temperature", FieldDataType::Float),
                            ("humidity", FieldDataType::Float),
                        ],
                        CreateTableOptions {
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();

                // First update: Add more tags
                let mut txn = catalog.begin("test_db").unwrap();
                txn.column_or_create("sensors", "building", InfluxColumnType::Tag)
                    .unwrap();
                txn.column_or_create("sensors", "floor", InfluxColumnType::Tag)
                    .unwrap();
                catalog.commit(txn).await.unwrap();

                // Second update: Add fields with auto field families
                let mut txn = catalog.begin("test_db").unwrap();
                txn.column_or_create(
                    "sensors",
                    "pressure",
                    InfluxColumnType::Field(InfluxFieldType::Float),
                )
                .unwrap();
                txn.column_or_create(
                    "sensors",
                    "light_level",
                    InfluxColumnType::Field(InfluxFieldType::Integer),
                )
                .unwrap();
                txn.column_or_create(
                    "sensors",
                    "motion_detected",
                    InfluxColumnType::Field(InfluxFieldType::Boolean),
                )
                .unwrap();
                catalog.commit(txn).await.unwrap();

                // Third update: Add fields with user-defined field families
                let mut txn = catalog.begin("test_db").unwrap();
                txn.column_or_create(
                    "sensors",
                    "environmental::co2_level",
                    InfluxColumnType::Field(InfluxFieldType::Float),
                )
                .unwrap();
                txn.column_or_create(
                    "sensors",
                    "environmental::air_quality",
                    InfluxColumnType::Field(InfluxFieldType::UInteger),
                )
                .unwrap();
                txn.column_or_create(
                    "sensors",
                    "metadata::firmware_version",
                    InfluxColumnType::Field(InfluxFieldType::String),
                )
                .unwrap();
                txn.column_or_create(
                    "sensors",
                    "metadata::last_calibration",
                    InfluxColumnType::Field(InfluxFieldType::String),
                )
                .unwrap();
                catalog.commit(txn).await.unwrap();

                // Create a second table to make the snapshot more comprehensive
                catalog
                    .create_table(
                        "test_db",
                        "events",
                        &["event_type"],
                        &[("value", FieldDataType::Float)],
                    )
                    .await
                    .unwrap();

                // Update the second table
                let mut txn = catalog.begin("test_db").unwrap();
                txn.column_or_create("events", "severity", InfluxColumnType::Tag)
                    .unwrap();
                txn.column_or_create(
                    "events",
                    "message",
                    InfluxColumnType::Field(InfluxFieldType::String),
                )
                .unwrap();
                catalog.commit(txn).await.unwrap();

                // Take snapshot
                insta::assert_json_snapshot!("update_table_comprehensive__catalog_snapshot", catalog.snapshot(), {
                    ".catalog_uuid" => "[uuid]",
                    ".nodes.repo[][1].instance_id" => "[uuid]",
                    ".nodes.repo[][1].process_uuids[]" => "[uuid]"
                });
            }

            // Reload catalog from object store and verify snapshot matches
            {
                let catalog = Arc::new(
                    Catalog::new_with_store(
                        "test",
                        Arc::clone(&store) as _,
                        Arc::clone(&time_provider) as _,
                    )
                    .await
                    .unwrap(),
                );

                // Verify all tables and columns exist
                let test_db = catalog.db_schema("test_db").expect("test_db should exist");

                let sensors = test_db
                    .tables
                    .get_by_name("sensors")
                    .expect("sensors should exist");
                assert_eq!(sensors.tag_columns.len(), 4); // location, sensor_type, building, floor
                assert!(sensors.field_families.contains_name("__0")); // auto field family
                assert!(sensors.field_families.contains_name("environmental"));
                assert!(sensors.field_families.contains_name("metadata"));

                let events = test_db
                    .tables
                    .get_by_name("events")
                    .expect("events should exist");
                assert_eq!(events.tag_columns.len(), 2); // event_type, severity

                // Verify snapshot still matches after reload
                insta::assert_json_snapshot!("update_table_comprehensive__catalog_snapshot", catalog.snapshot(), {
                    ".catalog_uuid" => "[uuid]",
                    ".nodes.repo[][1].instance_id" => "[uuid]",
                    ".nodes.repo[][1].process_uuids[]" => "[uuid]"
                });
            }
        }

        #[tokio::test]
        async fn column_or_create_enforces_column_limit() {
            let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
            let limits = CatalogLimits {
                num_columns_per_table: 3,
                ..Default::default()
            };
            let catalog = Arc::new(
                Catalog::new_in_memory_with_args_limits(
                    "test",
                    time_provider,
                    CatalogArgs::default(),
                    limits,
                )
                .await
                .unwrap(),
            );

            // Create the table at the max column count
            {
                let mut txn = catalog.begin("test_db").unwrap();
                txn.create_table(
                    "test_table",
                    create_table_columns::none(),
                    FieldFamilyMode::Auto,
                )
                .unwrap();

                // Add columns up to the limit (3 total)
                txn.column_or_create("test_table", "tag1", InfluxColumnType::Tag)
                    .unwrap();
                txn.column_or_create("test_table", "time", InfluxColumnType::Timestamp)
                    .unwrap();
                txn.column_or_create(
                    "test_table",
                    "field1",
                    InfluxColumnType::Field(InfluxFieldType::Float),
                )
                .unwrap();

                catalog.commit(txn).await.unwrap();
            }

            let mut txn = catalog.begin("test_db").unwrap();
            txn.table_or_create("test_table").unwrap();

            // This should fail - we're at the limit now
            let result = txn.column_or_create(
                "test_table",
                "field2",
                InfluxColumnType::Field(InfluxFieldType::Integer),
            );
            assert!(matches!(result, Err(CatalogError::TooManyColumns(3))));
        }
    }
}
