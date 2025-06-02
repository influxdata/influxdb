use std::sync::Arc;

use hashbrown::HashMap;
use influxdb3_id::ColumnId;
use influxdb3_process::PROCESS_UUID;
use observability_deps::tracing::{debug, error, info, trace};
use schema::{InfluxColumnType, InfluxFieldType};
use std::time::Duration;
use uuid::Uuid;

use super::{
    CATALOG_WRITE_PERMIT, Catalog, CatalogSequenceNumber, CatalogWritePermit, ColumnDefinition,
    DatabaseSchema, NodeState, TIME_COLUMN_NAME, TableDefinition,
};
use crate::{
    CatalogError, Result,
    catalog::{DEFAULT_OPERATOR_TOKEN_NAME, NUM_TAG_COLUMNS_LIMIT, RetentionPeriod},
    log::{
        AddFieldsLog, CatalogBatch, ClearRetentionPeriodLog, CreateDatabaseLog, CreateTableLog,
        DatabaseCatalogOp, DeleteDistinctCacheLog, DeleteLastCacheLog, DeleteTokenDetails,
        DeleteTriggerLog, DistinctCacheDefinition, FieldDataType, FieldDefinition,
        LastCacheDefinition, LastCacheSize, LastCacheTtl, LastCacheValueColumnsDef, MaxAge,
        MaxCardinality, NodeCatalogOp, NodeMode, OrderedCatalogBatch, RegisterNodeLog,
        SetRetentionPeriodLog, SoftDeleteDatabaseLog, SoftDeleteTableLog, StopNodeLog, TokenBatch,
        TokenCatalogOp, TriggerDefinition, TriggerIdentifier, TriggerSettings,
        TriggerSpecificationDefinition, ValidPluginFilename,
    },
    object_store::PersistCatalogResult,
};

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
                let time_ns = self.time_provider.now().timestamp_nanos();
                let ops = vec![DatabaseCatalogOp::CreateDatabase(CreateDatabaseLog {
                    database_id,
                    database_name,
                })];
                Ok(DatabaseCatalogTransaction {
                    catalog_sequence: inner.sequence_number(),
                    current_table_count: inner.table_count(),
                    table_limit: self.num_tables_limit(),
                    time_ns,
                    database_schema,
                    ops,
                    columns_per_table_limit: self.num_columns_per_table_limit(),
                })
            }
        }
    }

    pub async fn commit(
        &self,
        mut txn: DatabaseCatalogTransaction,
    ) -> Result<Prompt<CatalogSequenceNumber>> {
        if txn.ops.is_empty() {
            return Ok(Prompt::Success(txn.sequence_number()));
        }

        match self
            .get_permit_and_verify_catalog_batch(txn.catalog_batch(), txn.sequence_number())
            .await
        {
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

    pub async fn register_node(
        &self,
        node_id: &str,
        core_count: u64,
        mode: Vec<NodeMode>,
    ) -> Result<OrderedCatalogBatch> {
        info!(node_id, core_count, mode = ?mode, "register node");
        let process_uuid = *PROCESS_UUID;
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

    pub async fn update_node_state_stopped(&self, node_id: &str) -> Result<OrderedCatalogBatch> {
        let process_uuid = *PROCESS_UUID;
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
        info!(name, "create database");
        self.catalog_update_with_retry(|| {
            let (_, Some(batch)) =
                self.db_or_create(name, self.time_provider.now().timestamp_nanos())?
            else {
                return Err(CatalogError::AlreadyExists);
            };
            Ok(batch)
        })
        .await
    }

    pub async fn soft_delete_database(&self, name: &str) -> Result<OrderedCatalogBatch> {
        info!(name, "soft delete database");
        self.catalog_update_with_retry(|| {
            let Some(db) = self.db_schema(name) else {
                return Err(CatalogError::NotFound);
            };
            if db.deleted {
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
                    },
                )],
            ))
        })
        .await
    }

    pub async fn create_table(
        &self,
        db_name: &str,
        table_name: &str,
        tags: &[impl AsRef<str> + Send + Sync],
        fields: &[(impl AsRef<str> + Send + Sync, FieldDataType)],
    ) -> Result<OrderedCatalogBatch> {
        info!(db_name, table_name, "create table");
        self.catalog_update_with_retry(|| {
            let mut txn = self.begin(db_name)?;
            txn.create_table(table_name, tags, fields)?;
            Ok(txn.into())
        })
        .await
    }

    pub async fn soft_delete_table(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> Result<OrderedCatalogBatch> {
        info!(db_name, table_name, "soft delete database");
        self.catalog_update_with_retry(|| {
            let Some(db) = self.db_schema(db_name) else {
                return Err(CatalogError::NotFound);
            };
            let Some(tbl_def) = db.table_definition(table_name) else {
                return Err(CatalogError::NotFound);
            };
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
                })],
            ))
        })
        .await
    }

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
                    def.data_type,
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
                                Ok((def.id, name.as_ref().to_string()))
                            } else {
                                Err(CatalogError::InvalidDistinctCacheColumnType)
                            }
                        })
                })
                .collect::<Result<(Vec<ColumnId>, Vec<String>)>>()?;
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
                    def.data_type,
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
                                    Ok((def.id, name.as_ref().to_string()))
                                } else {
                                    Err(CatalogError::InvalidLastCacheKeyColumnType)
                                }
                            })
                    })
                    .collect::<Result<(Vec<ColumnId>, Vec<String>)>>()?
            } else {
                tbl.series_key
                    .iter()
                    .map(|id| {
                        tbl.column_definition_by_id(id)
                            .expect("column id in series key should be valid")
                    })
                    .map(|def| Ok((def.id, def.name.to_string())))
                    .collect::<Result<(Vec<ColumnId>, Vec<String>)>>()?
            };

            let value_columns = if let Some(value_columns) = value_columns {
                let columns = value_columns
                    .iter()
                    .map(|name| {
                        tbl.column_definition(name.as_ref())
                            .map(|def| def.id)
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
                    .collect::<Result<Vec<ColumnId>>>()?;
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
        info!("create new retention policy");
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
        info!("delete retention policy");
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
    async fn load_and_update_from_object_store(
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

#[derive(Debug)]
pub struct DatabaseCatalogTransaction {
    catalog_sequence: CatalogSequenceNumber,
    current_table_count: usize,
    table_limit: usize,
    columns_per_table_limit: usize,
    time_ns: i64,
    database_schema: Arc<DatabaseSchema>,
    ops: Vec<DatabaseCatalogOp>,
}

impl DatabaseCatalogTransaction {
    pub fn sequence_number(&self) -> CatalogSequenceNumber {
        self.catalog_sequence
    }

    pub fn db_schema_cloned(&self) -> Arc<DatabaseSchema> {
        Arc::clone(&self.database_schema)
    }

    pub fn push_op(&mut self, op: DatabaseCatalogOp) {
        self.ops.push(op);
    }

    fn catalog_batch(&mut self) -> CatalogBatch {
        CatalogBatch::database(
            self.time_ns,
            self.database_schema.id,
            Arc::clone(&self.database_schema.name),
            std::mem::take(&mut self.ops),
        )
    }

    pub fn table_or_create(&mut self, table_name: &str) -> Result<Arc<TableDefinition>> {
        match self.database_schema.table_definition(table_name) {
            Some(def) => Ok(def),
            None => {
                if self.current_table_count >= self.table_limit {
                    return Err(CatalogError::TooManyTables(self.table_limit));
                }
                let database_id = self.database_schema.id;
                let database_name = Arc::clone(&self.database_schema.name);
                let db_schema = Arc::make_mut(&mut self.database_schema);
                let table_def = db_schema.create_new_empty_table(table_name)?;
                debug!(
                    table_name,
                    table_id = table_def.table_id.get(),
                    "create new table"
                );
                self.ops
                    .push(DatabaseCatalogOp::CreateTable(CreateTableLog {
                        database_id,
                        database_name: Arc::clone(&database_name),
                        table_name: Arc::clone(&table_def.table_name),
                        table_id: table_def.table_id,
                        field_definitions: vec![],
                        key: vec![],
                    }));
                Ok(table_def)
            }
        }
    }

    pub fn column_or_create(
        &mut self,
        table_name: &str,
        column_name: &str,
        column_type: FieldDataType,
    ) -> Result<ColumnId> {
        let Some(table_def) = self.database_schema.table_definition(table_name) else {
            return Err(CatalogError::NotFound);
        };
        match table_def.column_definition(column_name) {
            Some(def) if def.data_type == column_type.into() => Ok(def.id),
            Some(def) => Err(CatalogError::InvalidColumnType {
                column_name: Arc::clone(&def.name),
                expected: def.data_type,
                got: column_type.into(),
            }),
            None => {
                if table_def.num_columns() >= self.columns_per_table_limit {
                    return Err(CatalogError::TooManyColumns(self.columns_per_table_limit));
                }
                if matches!(column_type, FieldDataType::Tag)
                    && table_def.num_tag_columns() >= NUM_TAG_COLUMNS_LIMIT
                {
                    return Err(CatalogError::TooManyTagColumns);
                }
                let database_id = self.database_schema.id;
                let database_name = Arc::clone(&self.database_schema.name);
                let db_schema = Arc::make_mut(&mut self.database_schema);
                let table_id = table_def.table_id;
                let table_name = Arc::clone(&table_def.table_name);
                let mut table_def = table_def.as_ref().clone();
                let new_col_id = table_def.add_column(column_name.into(), column_type.into())?;
                debug!(
                    table_name = table_name.as_ref(),
                    column_name,
                    column_id = new_col_id.get(),
                    "create new column"
                );
                self.ops.push(DatabaseCatalogOp::AddFields(AddFieldsLog {
                    database_name,
                    database_id,
                    table_name,
                    table_id,
                    field_definitions: vec![FieldDefinition {
                        name: column_name.into(),
                        id: new_col_id,
                        data_type: column_type,
                    }],
                }));
                db_schema.update_table(table_id, Arc::new(table_def))?;
                Ok(new_col_id)
            }
        }
    }

    pub fn create_table(
        &mut self,
        table_name: &str,
        tags: &[impl AsRef<str>],
        fields: &[(impl AsRef<str>, FieldDataType)],
    ) -> Result<()> {
        debug!(table_name, "create table in catalog transaction");
        if self.database_schema.table_definition(table_name).is_some() {
            return Err(CatalogError::AlreadyExists);
        }
        if self.current_table_count >= self.table_limit {
            return Err(CatalogError::TooManyTables(self.table_limit));
        }
        if tags.len() > NUM_TAG_COLUMNS_LIMIT {
            return Err(CatalogError::TooManyTagColumns);
        }
        if tags.len() + fields.len() > self.columns_per_table_limit - 1 {
            return Err(CatalogError::TooManyColumns(self.columns_per_table_limit));
        }
        let db_schema = Arc::make_mut(&mut self.database_schema);
        let mut table_def_arc = db_schema.create_new_empty_table(table_name)?;
        let table_def = Arc::make_mut(&mut table_def_arc);
        let mut key = Vec::new();
        let field_definitions = {
            let mut fd = Vec::new();
            for tag in tags {
                let id = table_def.columns.get_and_increment_next_id();
                key.push(id);
                fd.push(FieldDefinition {
                    name: Arc::from(tag.as_ref()),
                    id,
                    data_type: FieldDataType::Tag,
                });
            }

            for (name, ty) in fields {
                fd.push(FieldDefinition {
                    name: Arc::from(name.as_ref()),
                    id: table_def.columns.get_and_increment_next_id(),
                    data_type: *ty,
                });
            }

            fd.push(FieldDefinition {
                name: TIME_COLUMN_NAME.into(),
                id: table_def.columns.get_and_increment_next_id(),
                data_type: FieldDataType::Timestamp,
            });

            fd
        };

        table_def.add_columns(
            field_definitions
                .iter()
                .map(|fd| (fd.id, Arc::clone(&fd.name), fd.data_type.into()))
                .collect(),
        )?;

        debug!("inserting table from transaction");
        let table_id = table_def.table_id;
        db_schema.update_table(table_id, table_def_arc)?;

        self.ops
            .push(DatabaseCatalogOp::CreateTable(CreateTableLog {
                database_id: self.database_schema.id,
                database_name: Arc::clone(&self.database_schema.name),
                table_name: table_name.into(),
                table_id,
                field_definitions,
                key,
            }));

        Ok(())
    }

    pub fn db_schema(&self) -> &Arc<DatabaseSchema> {
        &self.database_schema
    }

    pub fn db_schema_mut(&mut self) -> &mut Arc<DatabaseSchema> {
        &mut self.database_schema
    }
}

impl From<DatabaseCatalogTransaction> for CatalogBatch {
    fn from(txn: DatabaseCatalogTransaction) -> Self {
        Self::database(
            txn.time_ns,
            txn.database_schema.id,
            Arc::clone(&txn.database_schema.name),
            txn.ops,
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
