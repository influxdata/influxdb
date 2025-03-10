use std::sync::Arc;

use hashbrown::HashMap;
use influxdb3_id::ColumnId;
use observability_deps::tracing::{debug, info, warn};
use schema::{InfluxColumnType, InfluxFieldType};
use uuid::Uuid;

use super::{
    CATALOG_WRITE_PERMIT, Catalog, CatalogSequenceNumber, CatalogWritePermit, ColumnDefinition,
    DatabaseSchema, NodeState, TIME_COLUMN_NAME, TableDefinition,
};
use crate::{
    CatalogError, Result,
    id::IdProvider,
    log::{
        AddFieldsLog, CatalogBatch, CreateDatabaseLog, CreateDistinctCacheLog, CreateLastCacheLog,
        CreateTableLog, CreateTriggerLog, DatabaseCatalogOp, DeleteDistinctCacheLog,
        DeleteLastCacheLog, DeleteTriggerLog, FieldDataType, FieldDefinition, LastCacheSize,
        LastCacheTtl, LastCacheValueColumnsDef, MaxAge, MaxCardinality, NodeCatalogOp, NodeMode,
        OrderedCatalogBatch, RegisterNodeLog, SoftDeleteDatabaseLog, SoftDeleteTableLog,
        TriggerIdentifier, TriggerSettings, TriggerSpecificationDefinition, ValidPluginFilename,
    },
    object_store::PersistCatalogResult,
    serialize::CatalogFile,
};

impl Catalog {
    pub fn begin(&self, db_name: &str) -> Result<DatabaseCatalogTransaction> {
        debug!(db_name, "starting catalog transaction");
        match self.db_schema(db_name) {
            Some(database_schema) => Ok(DatabaseCatalogTransaction {
                catalog_sequence: self.sequence_number(),
                time_ns: self.time_provider.now().timestamp_nanos(),
                database_schema: Arc::clone(&database_schema),
                ops: vec![],
            }),
            None => {
                if self.inner.read().database_count() >= Self::NUM_DBS_LIMIT {
                    return Err(CatalogError::TooManyDbs);
                }
                let mut inner = self.inner.write();
                let database_id = inner.get_and_increment_next_id();
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
                    time_ns,
                    database_schema,
                    ops,
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

        let batch = txn.catalog_batch();
        if let Some((ordered_batch, permit)) =
            self.get_permit_and_verify_catalog_batch(&batch).await?
        {
            match self
                .persist_ordered_batch_to_object_store(ordered_batch.clone(), &permit)
                .await?
            {
                UpdatePrompt::Retry => Ok(Prompt::Retry(())),
                UpdatePrompt::Applied => {
                    self.apply_ordered_catalog_batch(&ordered_batch, &permit);
                    Ok(Prompt::Success(self.sequence_number()))
                }
            }
        } else {
            Ok(Prompt::Success(self.sequence_number()))
        }
    }

    pub async fn register_node(
        &self,
        node_id: &str,
        core_count: u64,
        mode: NodeMode,
    ) -> Result<Option<OrderedCatalogBatch>> {
        self.catalog_update_with_retry(|| {
            let instance_id = if let Some(node) = self.node(node_id) {
                if let NodeState::Running { .. } = node.state {
                    // NOTE(trevor/catalog-refactor): if it is still in running state, but that is
                    // invalid, i.e., if the node stopped unexpectedly without being able to update
                    // the catalog, we would not want to error here. Could probably have a special
                    // error case for that, or do:
                    //
                    // return Err(CatalogError::AlreadyExists);
                    //
                    // For now, just warn, as that is at least more than what the current system
                    // would have done in the event that the same node id was used twice.
                    warn!(
                        node_id,
                        instance_id = node.instance_id.as_ref(),
                        "registering node to catalog that never shutdown properly"
                    );
                }
                Arc::clone(&node.instance_id)
            } else {
                let instance_id = Arc::<str>::from(Uuid::new_v4().to_string().as_str());
                info!(
                    node_id,
                    instance_id = instance_id.as_ref(),
                    "registering new node to the catalog"
                );
                instance_id
            };
            let time_ns = self.time_provider.now().timestamp_nanos();
            Ok(CatalogBatch::node(
                time_ns,
                vec![NodeCatalogOp::RegisterNode(RegisterNodeLog {
                    node_id: node_id.into(),
                    instance_id,
                    registered_time_ns: time_ns,
                    core_count,
                    mode,
                })],
            ))
        })
        .await
    }

    pub async fn create_database(&self, name: &str) -> Result<Option<OrderedCatalogBatch>> {
        debug!(name, "create database");
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

    pub async fn soft_delete_database(&self, name: &str) -> Result<Option<OrderedCatalogBatch>> {
        debug!(name, "soft delete database");
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
    ) -> Result<Option<OrderedCatalogBatch>> {
        debug!(db_name, table_name, "create table");
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
    ) -> Result<Option<OrderedCatalogBatch>> {
        debug!(db_name, table_name, "soft delete database");
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
    ) -> Result<Option<OrderedCatalogBatch>> {
        self.catalog_update_with_retry(|| {
            let Some(db) = self.db_schema(db_name) else {
                return Err(CatalogError::NotFound);
            };
            let Some(tbl) = db.table_definition(table_name) else {
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
                            if is_valid_distinct_cache_type(def) {
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
            if tbl.distinct_caches.contains_key(&cache_name) {
                return Err(CatalogError::AlreadyExists);
            }
            Ok(CatalogBatch::database(
                self.time_provider.now().timestamp_nanos(),
                db.id,
                db.name(),
                vec![DatabaseCatalogOp::CreateDistinctCache(
                    CreateDistinctCacheLog {
                        table_id: tbl.table_id,
                        table_name: Arc::clone(&tbl.table_name),
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
    ) -> Result<Option<OrderedCatalogBatch>> {
        self.catalog_update_with_retry(|| {
            let Some(db) = self.db_schema(db_name) else {
                return Err(CatalogError::NotFound);
            };
            let Some(tbl) = db.table_definition(table_name) else {
                return Err(CatalogError::NotFound);
            };
            if !tbl.distinct_caches.contains_key(cache_name) {
                return Err(CatalogError::NotFound);
            }
            Ok(CatalogBatch::database(
                self.time_provider.now().timestamp_nanos(),
                db.id,
                db.name(),
                vec![DatabaseCatalogOp::DeleteDistinctCache(
                    DeleteDistinctCacheLog {
                        table_name: Arc::clone(&tbl.table_name),
                        table_id: tbl.table_id,
                        cache_name: cache_name.into(),
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
    ) -> Result<Option<OrderedCatalogBatch>> {
        self.catalog_update_with_retry(|| {
            let Some(db) = self.db_schema(db_name) else {
                return Err(CatalogError::NotFound);
            };
            let Some(tbl) = db.table_definition(table_name) else {
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
                                if is_valid_last_cache_key_col(def) {
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
            if tbl.last_caches.contains_key(&cache_name) {
                return Err(CatalogError::AlreadyExists);
            }
            Ok(CatalogBatch::database(
                self.time_provider.now().timestamp_nanos(),
                db.id,
                db.name(),
                vec![DatabaseCatalogOp::CreateLastCache(CreateLastCacheLog {
                    table_id: tbl.table_id,
                    table: Arc::clone(&tbl.table_name),
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
    ) -> Result<Option<OrderedCatalogBatch>> {
        self.catalog_update_with_retry(|| {
            let Some(db) = self.db_schema(db_name) else {
                return Err(CatalogError::NotFound);
            };
            let Some(tbl) = db.table_definition(table_name) else {
                return Err(CatalogError::NotFound);
            };
            if !tbl.last_caches.contains_key(cache_name) {
                return Err(CatalogError::NotFound);
            }
            Ok(CatalogBatch::database(
                self.time_provider.now().timestamp_nanos(),
                db.id,
                db.name(),
                vec![DatabaseCatalogOp::DeleteLastCache(DeleteLastCacheLog {
                    table_name: Arc::clone(&tbl.table_name),
                    table_id: tbl.table_id,
                    name: cache_name.into(),
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
        plugin_filename: ValidPluginFilename<'_>,
        trigger_specification: &str,
        trigger_settings: TriggerSettings,
        trigger_arguments: &Option<HashMap<String, String>>,
        disabled: bool,
    ) -> Result<Option<OrderedCatalogBatch>> {
        self.catalog_update_with_retry(|| {
            let Some(db) = self.db_schema(db_name) else {
                return Err(CatalogError::NotFound);
            };
            let trigger = TriggerSpecificationDefinition::from_string_rep(trigger_specification)?;
            Ok(CatalogBatch::database(
                self.time_provider.now().timestamp_nanos(),
                db.id,
                db.name(),
                vec![DatabaseCatalogOp::CreateTrigger(CreateTriggerLog {
                    trigger_name: trigger_name.to_string(),
                    plugin_filename: plugin_filename.to_string(),
                    database_name: Arc::clone(&db.name),
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
    ) -> Result<Option<OrderedCatalogBatch>> {
        self.catalog_update_with_retry(|| {
            let Some(db) = self.db_schema(db_name) else {
                return Err(CatalogError::NotFound);
            };
            if db.processing_engine_triggers.get(trigger_name).is_none() {
                return Err(CatalogError::NotFound);
            };
            Ok(CatalogBatch::database(
                self.time_provider.now().timestamp_nanos(),
                db.id,
                db.name(),
                vec![DatabaseCatalogOp::DeleteTrigger(DeleteTriggerLog {
                    trigger_name: trigger_name.to_string(),
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
    ) -> Result<Option<OrderedCatalogBatch>> {
        self.catalog_update_with_retry(|| {
            let Some(db) = self.db_schema(db_name) else {
                return Err(CatalogError::NotFound);
            };
            let Some(trigger) = db.processing_engine_triggers.get(trigger_name) else {
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
                    db_name: db_name.to_string(),
                    trigger_name: trigger_name.to_string(),
                })],
            ))
        })
        .await
    }

    pub async fn disable_processing_engine_trigger(
        &self,
        db_name: &str,
        trigger_name: &str,
    ) -> Result<Option<OrderedCatalogBatch>> {
        self.catalog_update_with_retry(|| {
            let Some(db) = self.db_schema(db_name) else {
                return Err(CatalogError::NotFound);
            };
            let Some(trigger) = db.processing_engine_triggers.get(trigger_name) else {
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
                    db_name: db_name.to_string(),
                    trigger_name: trigger_name.to_string(),
                })],
            ))
        })
        .await
    }

    async fn catalog_update_with_retry<F>(
        &self,
        batch_creator_fn: F,
    ) -> Result<Option<OrderedCatalogBatch>>
    where
        F: Fn() -> Result<CatalogBatch>,
    {
        // NOTE(trevor/catalog-refactor): should there be a limit number of retries, or use a
        // timeout somewhere?
        loop {
            let batch = batch_creator_fn()?;
            if let Some((ordered_batch, permit)) =
                self.get_permit_and_verify_catalog_batch(&batch).await?
            {
                debug!(?ordered_batch, "applied batch and got permit");
                match self
                    .persist_ordered_batch_to_object_store(ordered_batch.clone(), &permit)
                    .await?
                {
                    UpdatePrompt::Retry => {
                        debug!("retry after catalog persist attempt");
                        continue;
                    }
                    UpdatePrompt::Applied => {
                        debug!("catalog persist attempt was applied");
                        self.apply_ordered_catalog_batch(&ordered_batch, &permit);
                        return Ok(Some(ordered_batch));
                    }
                }
            } else {
                debug!("applying batch did nothing");
                return Ok(None);
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
        ordered_batch: OrderedCatalogBatch,
        permit: &CatalogWritePermit,
    ) -> Result<UpdatePrompt> {
        debug!(?ordered_batch, "persisting ordered batch to store");
        // TODO: maybe just an error?
        assert_eq!(
            ordered_batch.sequence_number(),
            **permit,
            "tried to update catalog with invalid sequence"
        );
        match self
            .store
            .persist_catalog_sequenced_log(&ordered_batch)
            .await
            .inspect_err(|error| debug!(?error, "failed on persist of next catalog sequence"))?
        {
            PersistCatalogResult::Success => {
                self.broadcast_update(ordered_batch.into_batch());
                Ok(UpdatePrompt::Applied)
            }
            PersistCatalogResult::AlreadyExists => {
                self.load_and_update_from_object_store(
                    ordered_batch.sequence_number(),
                    None,
                    permit,
                )
                .await?;
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
        while let Some(next_file) = self
            .store
            .load_catalog_sequenced_log(sequence_number)
            .await
            .inspect_err(|error| debug!(?error, "failed to fetch next catalog sequence"))?
        {
            match next_file {
                CatalogFile::Log(ordered_catalog_batch) => {
                    let batch = self.apply_ordered_catalog_batch(&ordered_catalog_batch, permit);
                    self.broadcast_update(batch);
                }
                CatalogFile::Snapshot(catalog_snapshot) => {
                    self.update_from_snapshot(catalog_snapshot);
                }
            }
            sequence_number = sequence_number.next();
            if update_until.is_some_and(|max_sequence| sequence_number >= max_sequence) {
                break;
            }
        }
        Ok(())
    }

    fn broadcast_update(&self, update: impl Into<CatalogUpdate>) {
        if let Err(send_error) = self.channel.send(Arc::new(update.into())) {
            warn!(?send_error, "nothing listening for catalog updates");
        }
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
    pub fn batches(&self) -> impl Iterator<Item = &CatalogBatch> {
        self.batches.iter()
    }
}

#[derive(Debug)]
pub struct DatabaseCatalogTransaction {
    catalog_sequence: CatalogSequenceNumber,
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
                debug!(table_name, "create new table");
                let database_id = self.database_schema.id;
                let database_name = Arc::clone(&self.database_schema.name);
                let db_schema = Arc::make_mut(&mut self.database_schema);
                let table_def = db_schema.create_new_empty_table(table_name)?;
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
                let database_id = self.database_schema.id;
                let database_name = Arc::clone(&self.database_schema.name);
                let db_schema = Arc::make_mut(&mut self.database_schema);
                let table_id = table_def.table_id;
                let table_name = Arc::clone(&table_def.table_name);
                let mut table_def = table_def.as_ref().clone();
                let new_col_id = table_def.add_column(column_name.into(), column_type.into())?;
                debug!(next_col_id = table_def.next_column_id.get(), "next col id");
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
        let db_schema = Arc::make_mut(&mut self.database_schema);
        let mut table_def_arc = db_schema.create_new_empty_table(table_name)?;
        let table_def = Arc::make_mut(&mut table_def_arc);
        let mut key = Vec::new();
        let field_definitions = {
            let mut fd = Vec::new();
            for tag in tags {
                let id = table_def.get_and_increment_next_id();
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
                    id: table_def.get_and_increment_next_id(),
                    data_type: *ty,
                });
            }

            fd.push(FieldDefinition {
                name: TIME_COLUMN_NAME.into(),
                id: table_def.get_and_increment_next_id(),
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
